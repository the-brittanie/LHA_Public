"""
Housecall Pro → Salesforce daily time entry sync.
Pulls yesterday's technician clock-in/clock-out entries from HCP and
upserts them into Time_Entry__c for daily labor expense tracking.

Required environment variables:
    HCP_API_KEY
    SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN
    SF_DOMAIN (optional, default 'login' — use 'test' for sandbox)
"""

import os
import sys
import requests
from datetime import date, timedelta, datetime
from simple_salesforce import Salesforce

HCP_BASE_URL = "https://api.housecallpro.com"


# ── HCP API ───────────────────────────────────────────────────────────────────

def hcp_get(api_key, path, params=None):
    resp = requests.get(
        f"{HCP_BASE_URL}{path}",
        headers={
            "Authorization": f"Token {api_key}",
            "Accept": "application/json",
        },
        params=params,
    )
    resp.raise_for_status()
    return resp.json()


def get_time_entries(api_key, target_date):
    """Fetch all time sheet entries for a given date, handling pagination."""
    entries = []
    page = 1
    date_str = target_date.isoformat()

    while True:
        data = hcp_get(api_key, "/v1/time_sheets", {
            "start_date": date_str,
            "end_date": date_str,
            "page": page,
            "page_size": 100,
        })
        batch = data.get("time_sheets", [])
        entries.extend(batch)

        total = data.get("total_count", len(entries))
        if len(entries) >= total or not batch:
            break
        page += 1

    return entries


def parse_hours(entry):
    """Calculate hours from started_at/ended_at, or fall back to duration_seconds."""
    duration_seconds = entry.get("duration_seconds")
    if duration_seconds is not None:
        return round(duration_seconds / 3600, 2)

    started = entry.get("started_at")
    ended = entry.get("ended_at")
    if started and ended:
        start_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(ended.replace("Z", "+00:00"))
        return round((end_dt - start_dt).total_seconds() / 3600, 2)

    return None


# ── Salesforce ────────────────────────────────────────────────────────────────

def get_sf_connection():
    return Salesforce(
        username=os.environ["SF_USERNAME"],
        password=os.environ["SF_PASSWORD"],
        security_token=os.environ["SF_SECURITY_TOKEN"],
        domain=os.environ.get("SF_DOMAIN", "login"),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    api_key = os.environ["HCP_API_KEY"]
    sf = get_sf_connection()
    target_date = date.today() - timedelta(days=1)

    print(f"Fetching HCP time entries for {target_date}...")
    entries = get_time_entries(api_key, target_date)
    print(f"Found {len(entries)} time entry/entries")

    upserted = errors = 0

    for entry in entries:
        try:
            hcp_id = entry["id"]
            employee = entry.get("employee", {})
            tech_name = f"{employee.get('first_name', '')} {employee.get('last_name', '')}".strip()

            started_at = entry.get("started_at")
            ended_at = entry.get("ended_at")
            entry_date = started_at[:10] if started_at else target_date.isoformat()
            hours = parse_hours(entry)

            record = {
                "HCP_Time_Entry_Id__c": hcp_id,
                "Technician_Name__c": tech_name or None,
                "Date__c": entry_date,
                "Clock_In__c": started_at,
                "Clock_Out__c": ended_at,
                "Hours__c": hours,
            }
            record = {k: v for k, v in record.items() if v is not None}

            sf.Time_Entry__c.upsert(f"HCP_Time_Entry_Id__c/{hcp_id}", record)
            print(f"  OK    | {tech_name} | {entry_date} | {hours}h | {started_at} → {ended_at}")
            upserted += 1

        except Exception as e:
            print(f"  ERROR | HCP entry {entry.get('id')} — {e}")
            errors += 1

    print(f"\nDone. Upserted: {upserted} | Errors: {errors}")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
