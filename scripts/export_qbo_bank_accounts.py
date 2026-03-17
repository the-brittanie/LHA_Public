"""
One-time utility: exports all QBO bank accounts to a CSV.
Use this to identify the correct bank account name for the bill payment sync.

Usage: triggered via GitHub Actions — set export_bank_accounts input to "true"

Outputs: qbo_bank_accounts.csv
"""

import os
import sys
import csv

sys.path.insert(0, os.path.dirname(__file__))
from qbo_to_sf_sync import refresh_qbo_token, qbo_get


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

    data = qbo_get(access_token, realm_id, "query",
                   {"query": "SELECT Id, Name, AccountType, AccountSubType FROM Account WHERE AccountType = 'Bank'"})
    accounts = data.get("QueryResponse", {}).get("Account", [])

    print(f"Found {len(accounts)} bank account(s)")

    with open("qbo_bank_accounts.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["QBO_Account_Id", "Name", "AccountType", "AccountSubType"])
        for a in accounts:
            writer.writerow([
                a.get("Id"),
                a.get("Name", ""),
                a.get("AccountType", ""),
                a.get("AccountSubType", ""),
            ])

    print("Exported to qbo_bank_accounts.csv")


if __name__ == "__main__":
    main()
