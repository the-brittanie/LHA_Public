"""
Salesforce → QuickBooks Online bill payment sync.

Picks up Payment_Batch__c records where QBO_Status__c = 'Ready',
creates QBO Bill Payments, then marks each record 'Complete' or 'Error'.

Runs as part of the nightly GitHub Actions workflow.
"""

import os
import sys
import requests

sys.path.insert(0, os.path.dirname(__file__))
from qbo_to_sf_sync import refresh_qbo_token, qbo_get, get_sf_connection

BANK_ACCOUNT_NAME = "190 - Truist Checking 3306"


# ── QBO ───────────────────────────────────────────────────────────────────────

def get_bank_account_id(access_token, realm_id):
    """Look up the QBO bank account ID by name."""
    safe = BANK_ACCOUNT_NAME.replace("'", "\\'")
    data = qbo_get(access_token, realm_id, "query",
                   {"query": f"SELECT Id FROM Account WHERE Name = '{safe}' AND AccountType = 'Bank'"})
    accounts = data.get("QueryResponse", {}).get("Account", [])
    if not accounts:
        raise RuntimeError(f"QBO bank account not found: '{BANK_ACCOUNT_NAME}'")
    return accounts[0]["Id"]


def create_qbo_bill_payment(access_token, realm_id, batch, invoices, bank_account_id, vendor_qbo_id):
    """POST a Bill Payment to QBO. Returns the new QBO Bill Payment ID."""

    lines = []
    bills_total = 0
    credits_total = 0

    for inv in invoices:
        amount = inv.get("Amount__c") or 0
        is_credit = amount < 0

        if is_credit:
            qbo_id = inv.get("QBO_Credit_Id__c")
            if not qbo_id:
                raise ValueError(f"Credit {inv.get('Id')} has no QBO_Credit_Id__c")
            lines.append({
                "Amount": abs(amount),
                "LinkedTxn": [{"TxnId": str(qbo_id), "TxnType": "VendorCredit"}],
            })
            credits_total += abs(amount)
        else:
            qbo_id = inv.get("QBO_Bill_Id__c")
            if not qbo_id:
                raise ValueError(f"Invoice {inv.get('Id')} has no QBO_Bill_Id__c")
            lines.append({
                "Amount": amount,
                "LinkedTxn": [{"TxnId": str(qbo_id), "TxnType": "Bill"}],
            })
            bills_total += amount

    total = round(bills_total - credits_total, 2)

    payload = {
        "VendorRef": {"value": str(vendor_qbo_id)},
        "PayType": "Check",
        "CheckPayment": {
            "BankAccountRef": {"value": str(bank_account_id)},
            "PrintStatus": "NotSet",
        },
        "TxnDate": batch.get("Date__c"),
        "DocNumber": batch.get("Name"),
        "TotalAmt": round(total, 2),
        "Line": lines,
    }

    resp = requests.post(
        f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/billpayment",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
    )
    resp.raise_for_status()
    return resp.json().get("BillPayment", {}).get("Id")


# ── Salesforce ────────────────────────────────────────────────────────────────

def get_pending_batches(sf):
    result = sf.query(
        "SELECT Id, Name, Date__c, Supplier__c "
        "FROM Payment_Batch__c "
        "WHERE QBO_Status__c = 'Ready'"
    )
    return result.get("records", [])


def get_vendor_qbo_id(sf, supplier_name):
    """Look up QBO_Id__c on Supplier__c by Name."""
    safe = supplier_name.replace("'", "\\'")
    result = sf.query(
        f"SELECT QBO_Id__c FROM Supplier__c WHERE Name = '{safe}' LIMIT 1"
    )
    records = result.get("records", [])
    if not records:
        raise ValueError(f"No Supplier__c found with Name = '{supplier_name}'")
    qbo_id = records[0].get("QBO_Id__c")
    if not qbo_id:
        raise ValueError(f"Supplier '{supplier_name}' has no QBO_Id__c")
    return qbo_id


def get_batch_invoices(sf, batch_id):
    result = sf.query(
        f"SELECT Id, QBO_Bill_Id__c, QBO_Credit_Id__c, Amount__c "
        f"FROM Supplier_Invoice__c "
        f"WHERE Payment_Batch__c = '{batch_id}'"
    )
    return result.get("records", [])


def update_batch(sf, record_id, status, qbo_id=None):
    fields = {"QBO_Status__c": status}
    if qbo_id:
        fields["QBO_Id__c"] = str(qbo_id)
    sf.Payment_Batch__c.update(record_id, fields)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if os.path.exists("new_refresh_token.txt"):
        with open("new_refresh_token.txt") as f:
            refresh_token = f.read().strip()
    else:
        refresh_token = os.environ["QBO_REFRESH_TOKEN"]

    access_token, new_refresh_token = refresh_qbo_token(
        os.environ["QBO_CLIENT_ID"],
        os.environ["QBO_CLIENT_SECRET"],
        refresh_token,
    )

    with open("new_refresh_token.txt", "w") as f:
        f.write(new_refresh_token)

    realm_id = os.environ["QBO_REALM_ID"]
    sf = get_sf_connection()

    batches = get_pending_batches(sf)
    print(f"Found {len(batches)} payment batch(es) with QBO_Status__c = 'Ready'")

    if not batches:
        return

    bank_account_id = get_bank_account_id(access_token, realm_id)

    sent = errors = 0

    for batch in batches:
        batch_name = batch.get("Name", batch["Id"])
        try:
            invoices = get_batch_invoices(sf, batch["Id"])
            if not invoices:
                raise ValueError("No invoices with QBO_Bill_Id__c found on this batch")

            vendor_qbo_id = get_vendor_qbo_id(sf, batch.get("Supplier__c", ""))
            payment_id = create_qbo_bill_payment(
                access_token, realm_id, batch, invoices, bank_account_id, vendor_qbo_id
            )
            update_batch(sf, batch["Id"], "Complete", qbo_id=payment_id)
            bills = [i for i in invoices if (i.get("Amount__c") or 0) >= 0]
            credits = [i for i in invoices if (i.get("Amount__c") or 0) < 0]
            print(f"  OK    | {batch_name} | {len(bills)} bill(s) / {len(credits)} credit(s) → QBO Bill Payment {payment_id}")
            sent += 1

        except Exception as e:
            update_batch(sf, batch["Id"], "Error")
            print(f"  ERROR | {batch_name} — {e}")
            errors += 1

    print(f"\nDone. Sent: {sent} | Errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
