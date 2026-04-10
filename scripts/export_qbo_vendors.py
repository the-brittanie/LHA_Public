"""
One-time utility: exports all QBO vendors to a CSV with Id and Name.
Use this to populate QBO_Vendor_Id__c on your Salesforce vendor/supplier records.

Usage:
    python scripts/export_qbo_vendors.py

Outputs: qbo_vendors.csv
"""

import os
import csv
import sys

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
                   {"query": "SELECT * FROM Vendor MAXRESULTS 1000"})
    vendors = data.get("QueryResponse", {}).get("Vendor", [])

    print(f"Found {len(vendors)} vendor(s)")

    with open("qbo_vendors.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["QBO_Vendor_Id", "Vendor_Name", "Active"])
        for v in vendors:
            writer.writerow([
                v.get("Id"),
                v.get("DisplayName", ""),
                v.get("Active", True),
            ])

    print("Exported to qbo_vendors.csv")


if __name__ == "__main__":
    main()
