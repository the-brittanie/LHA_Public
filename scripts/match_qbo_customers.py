"""
One-time utility: matches QBO customers to Salesforce Accounts and
writes back QBO_Id__c so future payment syncs can look up by ID instead
of name.

Usage: triggered via GitHub Actions — set match_customers input to "true"
       or run locally: python scripts/match_qbo_customers.py

Outputs a summary of matched and unmatched customers.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from qbo_to_sf_sync import (
    refresh_qbo_token, qbo_get, get_sf_connection,
    load_name_map, _name_candidates,
)


def get_all_customers(access_token, realm_id):
    """Fetch all active QBO customers."""
    all_customers = []
    start = 1
    page = 100
    while True:
        data = qbo_get(
            access_token, realm_id, "query",
            {"query": f"SELECT Id, DisplayName, CompanyName, GivenName, FamilyName "
                      f"FROM Customer WHERE Active = true "
                      f"STARTPOSITION {start} MAXRESULTS {page}"},
        )
        batch = data.get("QueryResponse", {}).get("Customer", [])
        all_customers.extend(batch)
        if len(batch) < page:
            break
        start += page
    return all_customers


def find_sf_account(sf, customer, name_map):
    """Try to find a Salesforce Account for this QBO customer."""
    # Build candidate names from QBO customer fields
    display = customer.get("DisplayName", "")
    company = customer.get("CompanyName", "")
    given = customer.get("GivenName", "")
    family = customer.get("FamilyName", "")

    # Build full name variants to try
    names_to_try = []
    if display:
        names_to_try.extend(list(_name_candidates(display, name_map)))
    if company and company != display:
        names_to_try.extend(list(_name_candidates(company, name_map)))
    if given and family:
        full = f"{given} {family}"
        if full != display:
            names_to_try.extend(list(_name_candidates(full, name_map)))

    seen = set()
    for name in names_to_try:
        if name in seen:
            continue
        seen.add(name)
        safe = name.replace("'", "\\'")
        result = sf.query(f"SELECT Id, Name FROM Account WHERE Name = '{safe}' LIMIT 1")
        records = result.get("records", [])
        if records:
            return records[0]["Id"], records[0]["Name"]
    return None, None


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
    name_map = load_name_map()

    customers = get_all_customers(access_token, realm_id)
    print(f"Found {len(customers)} active QBO customer(s)")

    matched = already_set = skipped = 0
    unmatched = []

    for customer in customers:
        qbo_id = customer["Id"]
        display = customer.get("DisplayName", "")

        # Check if SF Account already has this QBO ID
        existing = sf.query(
            f"SELECT Id FROM Account WHERE QBO_Id__c = '{qbo_id}' LIMIT 1"
        )
        if existing.get("records"):
            already_set += 1
            continue

        sf_id, sf_name = find_sf_account(sf, customer, name_map)
        if sf_id:
            sf.Account.update(sf_id, {"QBO_Id__c": qbo_id})
            print(f"  MATCHED | {display} → {sf_name}")
            matched += 1
        else:
            unmatched.append(display)

    print(f"\nDone. Matched: {matched} | Already set: {already_set} | Unmatched: {len(unmatched)}")

    if unmatched:
        print("\nUnmatched QBO customers (set QBO_Id__c manually or add to account_name_map.json):")
        for name in unmatched:
            print(f"  - {name}")


if __name__ == "__main__":
    main()
