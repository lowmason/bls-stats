# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§20 issue 1: is browser-shaped httpx sufficient for www.bls.gov, or is the
headless HtmlFetcher backend (§7.2 mitigation 3) mandatory?

One contact-UA canary first (compliant UA, so R12-safe; dates the known-403
baseline from review §0.3), then a browser-shaped pass over every surface class
the design needs. The verdict is per-surface: §7.2's ladder degrades per surface,
not all-or-nothing.
"""
from _lib import make_client, probe, write_results

# (surface, url, content marker expected in the first 500 bytes)
SURFACES = [
    ("robots_txt", "https://www.bls.gov/robots.txt", "User-agent"),
    ("ics_schedule", "https://www.bls.gov/schedule/news_release/bls.ics", "BEGIN:VCALENDAR"),
    ("atom_feed", "https://www.bls.gov/feed/empsit.rss", "<feed"),
    ("errata_table", "https://www.bls.gov/errata/", "<html"),
    ("newsrels_index", "https://www.bls.gov/bls/newsrels.htm", "<html"),
    ("schedule_index", "https://www.bls.gov/schedule/", "<html"),
]


def main() -> None:
    records = []
    with make_client() as client:  # contact UA — compliant, single request
        rec = probe(client, "GET", "https://www.bls.gov/errata/")
        rec["surface"] = "canary_contact_ua"
        print(f"canary (contact UA): {rec.get('status')} (403 expected per review §0.3)")
        records.append(rec)

    with make_client(browser_shaped=True) as client:
        for name, url, marker in SURFACES:
            rec = probe(client, "GET", url)
            head = (rec.get("body_head") or "").lower()
            rec |= {"surface": name, "marker_found": marker.lower() in head}
            print(f"{rec.get('status')}  marker={str(rec['marker_found']):5}  {name}")
            records.append(rec)

    out = write_results("transport_html", records)
    browser = [r for r in records if r["surface"] != "canary_contact_ua"]
    blocked = [r["surface"] for r in browser
               if r.get("status") != 200 or not r.get("marker_found")]
    print(f"\nresults: {out}")
    if blocked:
        print(f"HEADLESS BACKEND MANDATORY for: {', '.join(blocked)} (§7.2 mitigation 3)")
    else:
        print("BROWSER-SHAPED HTTPX SUFFICIENT on all probed surfaces (§7.2 mitigation 2)")


if __name__ == "__main__":
    main()
