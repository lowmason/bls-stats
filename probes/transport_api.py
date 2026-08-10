# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§7.1 api profile: structural success checking on api.bls.gov, demonstrated live.

Uses the v1 single-series GET (no key; unregistered quota is 25 queries/day — this
probe spends exactly one). If BLS_API_KEY is set, spends one v2 query too. The key is
read from the environment and never written to results.
"""
import json
import os

from _lib import make_client, probe, write_results

V1_URL = "https://api.bls.gov/publicAPI/v1/timeseries/data/CES0000000001"
V2_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"


def payload_verdict(body_text: str) -> dict:
    """§7.1: never trust HTTP 200 or REQUEST_SUCCEEDED; inspect message[] and data."""
    d = json.loads(body_text)
    series = d.get("Results", {}).get("series", []) if isinstance(d.get("Results"), dict) else []
    n_points = sum(len(s.get("data", [])) for s in series)
    return {
        "api_status": d.get("status"),
        "messages": d.get("message", []),
        "n_series": len(series),
        "n_datapoints": n_points,
        "success_by_payload": bool(series) and n_points > 0,
    }


def main() -> None:
    records = []
    with make_client() as client:
        rec = probe(client, "GET", V1_URL, keep_body=True)
        body = rec.pop("body_text", "")  # keep results lean; verdict captures the substance
        rec |= {"why": "v1 unregistered single-series GET"} | payload_verdict(body)
        print(f"v1: http={rec.get('status')} api_status={rec.get('api_status')} "
              f"datapoints={rec.get('n_datapoints')} messages={rec.get('messages')}")
        records.append(rec)

        key = os.environ.get("BLS_API_KEY")
        if key:
            payload = {"seriesid": ["CES0000000001"], "startyear": "2024",
                       "endyear": "2025", "registrationkey": key}
            rec = probe(client, "POST", V2_URL, keep_body=True, json=payload)
            body = rec.pop("body_text", "")
            rec |= {"why": "v2 registered POST (key redacted)"} | payload_verdict(body)
            print(f"v2: http={rec.get('status')} api_status={rec.get('api_status')} "
                  f"datapoints={rec.get('n_datapoints')}")
            records.append(rec)
        else:
            records.append({"why": "v2 registered POST", "skipped": "BLS_API_KEY not set"})
            print("v2: skipped (BLS_API_KEY not set) — registered path untested")

    out = write_results("transport_api", records)
    print(f"\nresults: {out}")
    v1_ok = records[0].get("success_by_payload")
    print(f"API SURFACE {'CONFIRMED' if v1_ok else 'NOT CONFIRMED'} by payload inspection")


if __name__ == "__main__":
    main()
