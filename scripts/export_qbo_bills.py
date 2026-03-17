"""
One-time utility: exports all open QBO Bills to a CSV.
Use this to populate QBO_Bill_Id__c on existing Salesforce Supplier_Invoice__c records.

Usage: triggered via GitHub Actions — set export_bills input to "true"

Outputs: qbo_open_bills.csv
"""

import os
import sys
import csv

sys.path.insert(0, os.path.dirname(__file__))
from qbo_to_sf_sync import refresh_qbo_token, qbo_get


def main():
    access_token, new_refresh_token = refresh_qbo_token(
        os.environ["QBO_CLIENT_ID"],
        os.environ["QBO_CLIENT_SECRET"],
        os.environ["QBO_REFRESH_TOKEN"],
    )

    with open("new_refresh_token.txt", "w") as f:
        f.write(new_refresh_token)

    realm_id = os.environ["QBO_REALM_ID"]

    data = qbo_get(access_token, realm_id, "query",
                   {"query": "SELECT * FROM Bill WHERE Balance > '0' MAXRESULTS 1000"})
    bills = data.get("QueryResponse", {}).get("Bill", [])

    print(f"Found {len(bills)} open bill(s)")

    with open("qbo_open_bills.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["QBO_Bill_Id", "Vendor_Name", "Invoice_Number", "Date", "Due_Date", "Amount", "Balance"])
        for b in bills:
            writer.writerow([
                b.get("Id"),
                b.get("VendorRef", {}).get("name", ""),
                b.get("DocNumber", ""),
                b.get("TxnDate", ""),
                b.get("DueDate", ""),
                b.get("TotalAmt", ""),
                b.get("Balance", ""),
            ])

    print("Exported to qbo_open_bills.csv")


if __name__ == "__main__":
    main()
