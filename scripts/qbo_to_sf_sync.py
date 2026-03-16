"""
QuickBooks Online → Salesforce daily payment sync.
Creates/updates Customer_Payment__c records from yesterday's QBO payments.

Required environment variables (set as GitHub Actions secrets):
    QBO_CLIENT_ID, QBO_CLIENT_SECRET, QBO_REFRESH_TOKEN, QBO_REALM_ID
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN
    SF_DOMAIN (optional, default 'login' — use 'test' for sandbox)
"""

import os
import sys
import base64
import requests
from datetime import date, timedelta
from simple_salesforce import Salesforce


# ── QBO Auth ──────────────────────────────────────────────────────────────────

def refresh_qbo_token(client_id, client_secret, refresh_token):
    """Exchange refresh token for a new access token + refresh token."""
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
    """Return all QBO payments with TxnDate >= since_date."""
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


def determine_method(payment):
    """
    Map QBO PaymentMethodRef.name to the Method__c picklist.
    Matches keywords (case-insensitive); defaults to None (left blank).
    """
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


def determine_type(payment, access_token, realm_id):
    """
    Infer Type__c by inspecting invoice line item names and descriptions.
    Looks for 'membership' or 'job' keywords (case-insensitive); defaults to 'Other'.
    """
    for line in payment.get("Line", []):
        for linked in line.get("LinkedTxn", []):
            if linked.get("TxnType") != "Invoice":
                continue
            invoice = get_invoice(access_token, realm_id, linked["TxnId"])
            if not invoice:
                continue
            for inv_line in invoice.get("Line", []):
                item_name = (
                    inv_line.get("SalesItemLineDetail", {})
                    .get("ItemRef", {})
                    .get("name", "")
                    .lower()
                )
                desc = inv_line.get("Description", "").lower()
                text = item_name + " " + desc
                if "membership" in text:
                    return "Membership"
                if "job" in text:
                    return "Job"
    return "Other"


# ── Salesforce ────────────────────────────────────────────────────────────────

def get_sf_connection():
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
        domain=os.environ.get("SF_DOMAIN", "login"),
    )


def find_account_id(sf, customer_name):
    """Find Salesforce Account by exact name match."""
    safe_name = customer_name.replace("'", "\\'")
    result = sf.query(f"SELECT Id FROM Account WHERE Name = '{safe_name}' LIMIT 1")
    records = result.get("records", [])
    return records[0]["Id"] if records else None


def upsert_payment(sf, record):
    """Upsert Customer_Payment__c on the QBO_Payment_Id__c external ID."""
    external_id = record.pop("QBO_Payment_Id__c")
    sf.Customer_Payment__c.upsert(f"QBO_Payment_Id__c/{external_id}", record)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Refresh QBO tokens
    access_token, new_refresh_token = refresh_qbo_token(
        os.environ["QBO_CLIENT_ID"],
        os.environ["QBO_CLIENT_SECRET"],
        os.environ["QBO_REFRESH_TOKEN"],
    )

    # Write new refresh token to file so the workflow can rotate the secret
    with open("new_refresh_token.txt", "w") as f:
        f.write(new_refresh_token)

    realm_id = os.environ["QBO_REALM_ID"]
    since_date = (date.today() - timedelta(days=1)).isoformat()

    payments = get_payments_since(access_token, realm_id, since_date)
    print(f"Found {len(payments)} payment(s) since {since_date}")

    sf = get_sf_connection()
    created = skipped = errors = 0

    for pmt in payments:
        try:
            customer_name = pmt.get("CustomerRef", {}).get("name", "")
            account_id = find_account_id(sf, customer_name)

            if not account_id:
                print(f"  SKIP  | No Account found for '{customer_name}'")
                skipped += 1
                continue

            payment_type = determine_type(pmt, access_token, realm_id)
            method = determine_method(pmt)
            notes = pmt.get("PrivateNote") or pmt.get("CustomerMemo", {}).get("value")

            record = {
                "QBO_Payment_Id__c": pmt["Id"],
                "Account__c": account_id,
                "Amount__c": pmt.get("TotalAmt"),
                "Payment_Date__c": pmt.get("TxnDate"),
                "Type__c": payment_type,
                "Method__c": method,
                "Notes__c": notes,
            }

            # Remove None values so we don't overwrite existing data with blanks
            record = {k: v for k, v in record.items() if v is not None}

            upsert_payment(sf, record)
            print(f"  OK    | {customer_name} | ${pmt.get('TotalAmt')} | {payment_type} | {method or '—'}")
            created += 1

        except Exception as e:
            print(f"  ERROR | QBO Payment {pmt.get('Id')} — {e}")
            errors += 1

    print(f"\nDone. Upserted: {created} | Skipped: {skipped} | Errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
