"""
Salesforce → QuickBooks Online bill sync.

Picks up Supplier_Invoice__c records where QBO_Status__c = 'Ready',
creates QBO Bills with the attached PDF, then marks each record 'Complete' or 'Error'.

Runs as part of the nightly GitHub Actions workflow.
"""

import json
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


def create_qbo_vendor_credit(access_token, realm_id, invoice, expense_account_id):
    """POST a new Vendor Credit to QBO. Returns the new QBO Vendor Credit ID."""
    vendor_qbo_id = (invoice.get("Vendor__r") or {}).get("QBO_Id__c")
    if not vendor_qbo_id:
        raise ValueError("Vendor has no QBO_Id__c — cannot create vendor credit")

    classification = invoice.get("Classification__c") or ""
    description = f"Parts & Materials - {classification}".strip(" -")

    payload = {
        "VendorRef": {"value": vendor_qbo_id},
        "TxnDate": invoice.get("Date__c"),
        "Line": [
            {
                "Amount": abs(invoice.get("Amount__c") or 0),
                "DetailType": "AccountBasedExpenseLineDetail",
                "Description": description,
                "AccountBasedExpenseLineDetail": {
                    "AccountRef": {"value": expense_account_id}
                },
            }
        ],
    }

    if invoice.get("Invoice_Number__c"):
        payload["DocNumber"] = invoice["Invoice_Number__c"]

    resp = requests.post(
        f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/vendorcredit",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
    )
    resp.raise_for_status()
    return resp.json().get("VendorCredit", {}).get("Id")


def attach_pdf_to_qbo_bill(access_token, realm_id, qbo_id, file_content, filename, entity_type="Bill"):
    """Upload a PDF and attach it to an existing QBO Bill or VendorCredit."""
    metadata = json.dumps({
        "AttachableRef": [{"EntityRef": {"type": entity_type, "value": str(qbo_id)}}],
        "ContentType": "application/pdf",
        "FileName": filename,
    })

    resp = requests.post(
        f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/upload",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        },
        files={
            "file_metadata_0": (None, metadata, "application/json"),
            "file_content_0": (filename, file_content, "application/pdf"),
        },
    )
    resp.raise_for_status()


# ── Salesforce ────────────────────────────────────────────────────────────────

def get_pending_invoices(sf):
    result = sf.query(
        "SELECT Id, Amount__c, Date__c, Due_Date__c, Invoice_Number__c, "
        "Classification__c, Vendor__r.QBO_Id__c, QBO_Bill_Id__c, QBO_Credit_Id__c "
        "FROM Supplier_Invoice__c "
        "WHERE QBO_Status__c = 'Ready'"
    )
    return result.get("records", [])


def get_pdf_attachment(sf, invoice_id):
    """Download the latest PDF attached to a Supplier_Invoice__c record."""
    links = sf.query(
        f"SELECT ContentDocumentId FROM ContentDocumentLink "
        f"WHERE LinkedEntityId = '{invoice_id}' LIMIT 1"
    )
    records = links.get("records", [])
    if not records:
        raise ValueError("No file attachment found on this invoice")

    doc_id = records[0]["ContentDocumentId"]

    versions = sf.query(
        f"SELECT Id, Title, FileExtension FROM ContentVersion "
        f"WHERE ContentDocumentId = '{doc_id}' AND IsLatest = true LIMIT 1"
    )
    version_records = versions.get("records", [])
    if not version_records:
        raise ValueError("No file version found for attachment")

    version = version_records[0]
    filename = f"{version['Title']}.{version.get('FileExtension', 'pdf')}"

    url = f"{sf.base_url}sobjects/ContentVersion/{version['Id']}/VersionData"
    resp = requests.get(url, headers={"Authorization": f"Bearer {sf.session_id}"})
    resp.raise_for_status()

    return resp.content, filename


def update_status(sf, record_id, status):
    sf.Supplier_Invoice__c.update(record_id, {"QBO_Status__c": status})


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Use the rotated refresh token from the payment sync if available,
    # otherwise fall back to the environment variable
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

    invoices = get_pending_invoices(sf)
    print(f"Found {len(invoices)} invoice(s) with QBO_Status__c = 'Ready'")

    if not invoices:
        return

    expense_account_id = get_expense_account_id(access_token, realm_id)

    sent = errors = 0

    for inv in invoices:
        inv_number = inv.get("Invoice_Number__c") or inv["Id"]
        amount = inv.get("Amount__c") or 0
        is_credit = amount < 0

        # Skip if already synced
        if is_credit and inv.get("QBO_Credit_Id__c"):
            print(f"  SKIP  | {inv_number} | already has QBO_Credit_Id__c {inv['QBO_Credit_Id__c']}")
            continue
        if not is_credit and inv.get("QBO_Bill_Id__c"):
            print(f"  SKIP  | {inv_number} | already has QBO_Bill_Id__c {inv['QBO_Bill_Id__c']}")
            continue

        try:
            if is_credit:
                qbo_id = create_qbo_vendor_credit(access_token, realm_id, inv, expense_account_id)
                file_content, filename = get_pdf_attachment(sf, inv["Id"])
                attach_pdf_to_qbo_bill(access_token, realm_id, qbo_id, file_content, filename, entity_type="VendorCredit")
                sf.Supplier_Invoice__c.update(inv["Id"], {
                    "QBO_Status__c": "Complete",
                    "QBO_Credit_Id__c": str(qbo_id),
                })
                print(f"  OK    | {inv_number} | ${amount} → QBO Vendor Credit {qbo_id} + PDF attached")
            else:
                qbo_id = create_qbo_bill(access_token, realm_id, inv, expense_account_id)
                file_content, filename = get_pdf_attachment(sf, inv["Id"])
                attach_pdf_to_qbo_bill(access_token, realm_id, qbo_id, file_content, filename, entity_type="Bill")
                sf.Supplier_Invoice__c.update(inv["Id"], {
                    "QBO_Status__c": "Complete",
                    "QBO_Bill_Id__c": str(qbo_id),
                })
                print(f"  OK    | {inv_number} | ${amount} → QBO Bill {qbo_id} + PDF attached")
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
