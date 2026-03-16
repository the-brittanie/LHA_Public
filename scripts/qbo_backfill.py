"""
One-time backfill: syncs all QBO payments from a start date to yesterday.
Uses the same logic as the daily sync.

Usage:
    python scripts/qbo_backfill.py 2026-01-01

Requires the same environment variables as qbo_to_sf_sync.py.
"""

import os
import re
import sys
import base64
import requests
from datetime import date, timedelta
from simple_salesforce import Salesforce

from qbo_to_sf_sync import (
    refresh_qbo_token,
    get_payment_methods,
    get_invoice_info,
    determine_method,
    find_account_id,
    find_job_id,
    find_existing_payment,
    qbo_get,
)


def get_payments_between(access_token, realm_id, start_date, end_date):
    data = qbo_get(
        access_token, realm_id, "query",
        {"query": f"SELECT * FROM Payment WHERE TxnDate >= '{start_date}' AND TxnDate <= '{end_date}' MAXRESULTS 1000"},
    )
    return data.get("QueryResponse", {}).get("Payment", [])


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/qbo_backfill.py <start_date>  e.g. 2026-01-01")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = (date.today() - timedelta(days=1)).isoformat()

    print(f"Backfilling payments from {start_date} to {end_date}...")

    access_token, new_refresh_token = refresh_qbo_token(
        os.environ["QBO_CLIENT_ID"],
        os.environ["QBO_CLIENT_SECRET"],
        os.environ["QBO_REFRESH_TOKEN"],
    )

    with open("new_refresh_token.txt", "w") as f:
        f.write(new_refresh_token)

    realm_id = os.environ["QBO_REALM_ID"]
    payments = get_payments_between(access_token, realm_id, start_date, end_date)
    print(f"Found {len(payments)} payment(s)")

    payment_methods = get_payment_methods(access_token, realm_id)
    sf = Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
        domain=os.environ.get("SF_DOMAIN", "login"),
    )

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
            method = determine_method(pmt, payment_methods)
            ref_num = pmt.get("PaymentRefNum")
            memo = pmt.get("PrivateNote") or pmt.get("CustomerMemo", {}).get("value")
            notes = " | ".join(filter(None, [f"No. {ref_num}" if ref_num else None, memo]))
            qbo_id = pmt["Id"]
            amount = pmt.get("TotalAmt")
            txn_date = pmt.get("TxnDate")

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
                "Notes__c": notes,
                "Job__c": job_id,
                "QBO_Payment_Id__c": qbo_id,
            }
            if method:
                record["Method__c"] = method
            record = {k: v for k, v in record.items() if v is not None}

            existing_id = find_existing_payment(sf, account_id, amount, txn_date)

            if existing_id:
                sf.Customer_Payment__c.update(existing_id, record)
                print(f"  LINKED | {customer_name} | ${amount} | {txn_date} | {payment_type}")
                linked += 1
            else:
                upsert_record = {k: v for k, v in record.items() if k != "QBO_Payment_Id__c"}
                sf.Customer_Payment__c.upsert(f"QBO_Payment_Id__c/{qbo_id}", upsert_record)
                print(f"  OK     | {customer_name} | ${amount} | {txn_date} | {payment_type} | {method or '—'}")
                upserted += 1

        except Exception as e:
            print(f"  ERROR  | QBO Payment {pmt.get('Id')} — {e}")
            errors += 1

    print(f"\nDone. Created: {upserted} | Linked to existing: {linked} | Skipped: {skipped} | Errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
