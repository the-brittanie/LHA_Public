"""
Salesforce → QuickBooks Online bill sync.

Picks up Supplier_Invoice__c records where QBO_Status__c = 'Ready',
creates QBO Bills, then marks each record 'Complete' or 'Error'.

Runs as part of the nightly GitHub Actions workflow.
"""

import os
import sys
import requests

sys.path.insert(0, os.path.dirname(__file__))
from qbo_to_sf_sync import refresh_qbo_token, qbo_get, get_sf_connection

EXPENSE_ACCOUNT_NAME = "Cost of Goods Sold (Direct Job Costs Only):COG - Materials:Parts & Consumables"


# ── QBO ───────────────────────────────────────────────────────────────────────

def get_expense_account_id(access_token, realm_id):
    """Look up the QBO account ID by FullyQualifiedName."""
    safe = EXPENSE_ACCOUNT_NAME.replace("'", "\\'")
    data = qbo_get(access_token, realm_id, "query",
                   {"query": f"SELECT Id FROM Account WHERE FullyQualifiedName = '{safe}'"})
    accounts = data.get("QueryResponse", {}).get("Account", [])
    if not accounts:
        raise RuntimeError(f"QBO Account not found: '{EXPENSE_ACCOUNT_NAME}'")
    return accounts[0]["Id"]


def create_qbo_bill(access_token, realm_id, invoice, expense_account_id):
    """POST a new Bill to QBO. Returns the new QBO Bill ID."""
    vendor_qbo_id = (invoice.get("Vendor__r") or {}).get("QBO_Id__c")
    if not vendor_qbo_id:
        raise ValueError("Vendor has no QBO_Id__c — cannot create bill")

    classification = invoice.get("Classification__c") or ""
    description = f"Parts & Materials - {classification}".strip(" -")

    payload = {
        "VendorRef": {"value": vendor_qbo_id},
        "TxnDate": invoice.get("Date__c"),
        "Line": [
            {
                "Amount": invoice.get("Amount__c"),
                "DetailType": "AccountBasedExpenseLineDetail",
                "Description": description,
                "AccountBasedExpenseLineDetail": {
                    "AccountRef": {"value": expense_account_id}
                },
            }
        ],
    }

    if invoice.get("Due_Date__c"):
        payload["DueDate"] = invoice["Due_Date__c"]
    if invoice.get("Invoice_Number__c"):
        payload["DocNumber"] = invoice["Invoice_Number__c"]

    resp = requests.post(
        f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/bill",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
    )
    resp.raise_for_status()
    return resp.json().get("Bill", {}).get("Id")


# ── Salesforce ────────────────────────────────────────────────────────────────

def get_pending_invoices(sf):
    result = sf.query(
        "SELECT Id, Amount__c, Date__c, Due_Date__c, Invoice_Number__c, "
        "Classification__c, Vendor__r.QBO_Id__c "
        "FROM Supplier_Invoice__c "
        "WHERE QBO_Status__c = 'Ready'"
    )
    return result.get("records", [])


def update_status(sf, record_id, status):
    sf.Supplier_Invoice__c.update(record_id, {"QBO_Status__c": status})


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    access_token, new_refresh_token = refresh_qbo_token(
        os.environ["QBO_CLIENT_ID"],
        os.environ["QBO_CLIENT_SECRET"],
        os.environ["QBO_REFRESH_TOKEN"],
    )

    with open("new_refresh_token.txt", "w") as f:
        f.write(new_refresh_token)

    realm_id = os.environ["QBO_REALM_ID"]
    sf = get_sf_connection()

    invoices = get_pending_invoices(sf)
    print(f"Found {len(invoices)} invoice(s) with QBO_Status__c = 'Ready'")

    if not invoices:
        return

    expense_account_id = get_expense_account_id(access_token, realm_id)

    sent = errors = 0

    for inv in invoices:
        inv_number = inv.get("Invoice_Number__c") or inv["Id"]
        try:
            bill_id = create_qbo_bill(access_token, realm_id, inv, expense_account_id)
            update_status(sf, inv["Id"], "Complete")
            print(f"  OK    | {inv_number} | ${inv.get('Amount__c')} → QBO Bill {bill_id}")
            sent += 1
        except Exception as e:
            update_status(sf, inv["Id"], "Error")
            print(f"  ERROR | {inv_number} — {e}")
            errors += 1

    print(f"\nDone. Sent: {sent} | Errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
