"""
QuickBooks Online → Salesforce daily payment sync.
Creates/updates Customer_Payment__c records from yesterday's QBO payments.

- Matches existing Housecall Pro payments by Account + Amount + Date
  and stamps the QBO Payment ID rather than creating a duplicate.
- Extracts job number from invoice description to link Job__c.
- Detects 'RSA' in description to classify Membership payments.

Required environment variables (set as GitHub Actions secrets):
    QBO_CLIENT_ID, QBO_CLIENT_SECRET, QBO_REFRESH_TOKEN, QBO_REALM_ID
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN
    SF_DOMAIN (optional, default 'login' — use 'test' for sandbox)
"""

import os
import re
import sys
import base64
import requests
from datetime import date, timedelta
from simple_salesforce import Salesforce


# ── QBO Auth ──────────────────────────────────────────────────────────────────

def refresh_qbo_token(client_id, client_secret, refresh_token):
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = requests.post(
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
    )
    resp.raise_for_status()
    data = resp.json()
    return data["access_token"], data["refresh_token"]


# ── QBO API ───────────────────────────────────────────────────────────────────

def qbo_get(access_token, realm_id, path, params=None):
    resp = requests.get(
        f"https://quickbooks.api.intuit.com/v3/company/{realm_id}/{path}",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        params=params,
    )
    resp.raise_for_status()
    return resp.json()


def get_payments_since(access_token, realm_id, since_date):
    data = qbo_get(
        access_token, realm_id, "query",
        {"query": f"SELECT * FROM Payment WHERE TxnDate >= '{since_date}' MAXRESULTS 1000"},
    )
    return data.get("QueryResponse", {}).get("Payment", [])


def get_invoice(access_token, realm_id, invoice_id):
    try:
        data = qbo_get(access_token, realm_id, f"invoice/{invoice_id}")
        return data.get("Invoice")
    except Exception:
        return None


def extract_job_number(text):
    """Extract job number — looks for #NUMBER pattern first, then first standalone number."""
    match = re.search(r'#(\w+)', text)
    if match:
        return match.group(1)
    match = re.search(r'\b(\d+)\b', text)
    return match.group(1) if match else None


def get_invoice_info(payment, access_token, realm_id):
    """
    Returns (type, job_number) by inspecting linked invoice text.
    - 'RSA' anywhere in invoice text → ('Membership', None)
    - Job number found → ('Job', job_number)
    - Otherwise → ('Other', None)
    """
    for line in payment.get("Line", []):
        for linked in line.get("LinkedTxn", []):
            if linked.get("TxnType") != "Invoice":
                continue
            invoice = get_invoice(access_token, realm_id, linked["TxnId"])
            if not invoice:
                continue

            # Collect all invoice text
            all_text = (
                invoice.get("CustomerMemo", {}).get("value", "") + " " +
                invoice.get("PrivateNote", "")
            )
            for inv_line in invoice.get("Line", []):
                all_text += " " + inv_line.get("SalesItemLineDetail", {}).get("ItemRef", {}).get("name", "")
                all_text += " " + inv_line.get("Description", "")

            if "rsa" in all_text.lower() or "membership" in all_text.lower():
                return "Membership", None

            job_number = extract_job_number(all_text)
            if job_number:
                return "Job", job_number

    return "Other", None


def determine_method(payment):
    raw = payment.get("PaymentMethodRef", {}).get("name", "").lower()
    if "cash" in raw:
        return "Cash"
    if "check" in raw or "cheque" in raw:
        return "Check"
    if "ach" in raw or "bank transfer" in raw or "e-check" in raw or "echeck" in raw:
        return "ACH"
    if "finance" in raw or "financing" in raw:
        return "Finance"
    if "credit" in raw or "visa" in raw or "mastercard" in raw or "amex" in raw or "discover" in raw:
        return "Credit Card"
    return None


# ── Salesforce ────────────────────────────────────────────────────────────────

def get_sf_connection():
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
        domain=os.environ.get("SF_DOMAIN", "login"),
    )


def find_account_id(sf, customer_name):
    safe_name = customer_name.replace("'", "\\'")
    result = sf.query(f"SELECT Id FROM Account WHERE Name = '{safe_name}' LIMIT 1")
    records = result.get("records", [])
    return records[0]["Id"] if records else None


def find_job_id(sf, job_number):
    safe_number = job_number.replace("'", "\\'")
    result = sf.query(f"SELECT Id FROM Job__c WHERE Name = '{safe_number}' LIMIT 1")
    records = result.get("records", [])
    return records[0]["Id"] if records else None


def find_existing_payment(sf, account_id, amount, payment_date):
    """Find a Housecall Pro payment with no QBO ID that matches on Account + Amount + Date."""
    result = sf.query(
        f"SELECT Id FROM Customer_Payment__c "
        f"WHERE Account__c = '{account_id}' "
        f"AND Amount__c = {float(amount)} "
        f"AND Payment_Date__c = {payment_date} "
        f"AND QBO_Payment_Id__c = null "
        f"LIMIT 1"
    )
    records = result.get("records", [])
    return records[0]["Id"] if records else None


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
    since_date = (date.today() - timedelta(days=1)).isoformat()

    payments = get_payments_since(access_token, realm_id, since_date)
    print(f"Found {len(payments)} payment(s) since {since_date}")

    sf = get_sf_connection()
    upserted = linked = skipped = errors = 0

    for pmt in payments:
        try:
            customer_name = pmt.get("CustomerRef", {}).get("name", "")
            account_id = find_account_id(sf, customer_name)

            if not account_id:
                print(f"  SKIP   | No Account found for '{customer_name}'")
                skipped += 1
                continue

            payment_type, job_number = get_invoice_info(pmt, access_token, realm_id)
            method = determine_method(pmt)
            notes = pmt.get("PrivateNote") or pmt.get("CustomerMemo", {}).get("value")
            qbo_id = pmt["Id"]
            amount = pmt.get("TotalAmt")
            txn_date = pmt.get("TxnDate")

            # Look up Job__c for Job type payments
            job_id = None
            if payment_type == "Job" and job_number:
                job_id = find_job_id(sf, job_number)
                if not job_id:
                    print(f"  WARN   | No Job__c found for job number '{job_number}' ({customer_name})")

            record = {
                "Account__c": account_id,
                "Amount__c": amount,
                "Payment_Date__c": txn_date,
                "Type__c": payment_type,
                "Method__c": method,
                "Notes__c": notes,
                "Job__c": job_id,
                "QBO_Payment_Id__c": qbo_id,
            }
            record = {k: v for k, v in record.items() if v is not None}

            # Match existing Housecall Pro record by Account + Amount + Date
            existing_id = find_existing_payment(sf, account_id, amount, txn_date)

            if existing_id:
                sf.Customer_Payment__c.update(existing_id, record)
                print(f"  LINKED | {customer_name} | ${amount} | {payment_type} | matched existing record")
                linked += 1
            else:
                sf.Customer_Payment__c.upsert(f"QBO_Payment_Id__c/{qbo_id}", record)
                print(f"  OK     | {customer_name} | ${amount} | {payment_type} | {method or '—'}")
                upserted += 1

        except Exception as e:
            print(f"  ERROR  | QBO Payment {pmt.get('Id')} — {e}")
            errors += 1

    print(f"\nDone. Created: {upserted} | Linked to existing: {linked} | Skipped: {skipped} | Errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
