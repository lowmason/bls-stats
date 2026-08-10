# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx[http2]>=0.27"]
# ///
"""§20 issue 1 addendum: does the compliant *contact* profile pass on the five
`www.bls.gov` HTML surfaces where the *browser-shaped* profile
(`probes/transport_html.py`, run 2026-08-10) was blocked on all six?

Why this script exists (see specs/bls-stats-stage1-findings.md §3): that run's canary
(contact profile, one request, `/errata/`) got 200; the browser-shaped pass over six
surfaces — including that same `/errata/` URL ~15 s later — got 403 on every one, an
identical 1,323-byte Akamai block page. That is a hole in the exit criterion ("posture
stated per surface"), not a posture: 1 of 6 surfaces has a contact-profile data point,
0 of 6 has a passing browser-shaped one. This script fills in the other five surfaces
under the contact profile. `/errata/` is deliberately NOT re-probed — that data point
already exists.

Two confounds this design cannot and does not resolve — see
specs/bls-stats-stage1-findings.md §3 for the full discussion:

  1. Header confound: `_lib.CONTACT_HEADERS` and `_lib.BROWSER_HEADERS` differ in five
     headers at once (User-Agent, Accept, Accept-Language, Accept-Encoding,
     Upgrade-Insecure-Requests), and the browser-shaped profile claims Chrome 126 while
     sending none of sec-ch-ua/sec-ch-ua-platform/Sec-Fetch-*. No result from this
     script supports a claim that the User-Agent specifically caused any block —
     mechanism is untested. Only "profile as configured" claims are in scope.

  2. Sequence-position confound: in the prior run the one pass was request #1 and every
     block was #2-#7 — a per-connection velocity/session rule explains that pattern
     exactly as well as a profile difference does. This script probes robots.txt first
     (both the most compliance-load-bearing surface and the velocity control — the same
     role position 1 played in the prior run) and records `position` and `probed_at` on
     every record so the pass/fail pattern can be read against sequence position, not
     assumed to be about profile.

Every request in this script goes through `_lib.make_client()` — the contact profile,
never `browser_shaped=True`. Five sequential requests, `_lib.POLITE_DELAY_S` apart, run
exactly once.
"""
from _lib import make_client, probe, write_results

# (surface, url, any-of markers). Markers are chosen to fail on the known Akamai block
# page (body_bytes == 1323, "Access Denied" title, no site content) — see
# _looks_like_block_page below for the belt-and-suspenders check on top of the marker.
SURFACES = [
    # Most compliance-load-bearing surface, probed first; its position also serves as
    # the velocity control (position 1 was the only pass in the prior run).
    ("robots_txt", "https://www.bls.gov/robots.txt", ["User-agent"]),
    ("ics_schedule", "https://www.bls.gov/schedule/news_release/bls.ics", ["BEGIN:VCALENDAR"]),
    # URL says .rss; do not assume Atom's "<feed" (the pinned transport_html.py's
    # marker) is right for this path — accept a real RSS 2.0, RSS 1.0/RDF, or Atom
    # root element and record which one matched.
    ("release_feed", "https://www.bls.gov/feed/empsit.rss", ["<rss", "<feed", "<rdf"]),
    # "<html" (the pinned script's marker) is satisfied by the Akamai block page too —
    # that was the false-positive mechanism in the prior run. Use the site's own
    # title-suffix convention (confirmed present in the 2026-08-10 errata canary body)
    # instead: the block page's title is "Access Denied", never this string.
    ("newsrels_index", "https://www.bls.gov/bls/newsrels.htm", ["U.S. Bureau of Labor Statistics"]),
    ("schedule_index", "https://www.bls.gov/schedule/", ["U.S. Bureau of Labor Statistics"]),
]


def _looks_like_block_page(rec: dict, head_lower: str) -> bool:
    """The known Akamai block signature from transport_html-2026-08-10.jsonl: title
    'Access Denied' and a `server: AkamaiGHost` response header. A marker match alone
    isn't enough to call a surface passing if this signature is also present."""
    server = (rec.get("headers") or {}).get("server", "")
    return "access denied" in head_lower or server.lower() == "akamaighost"


def _classify(rec: dict, matched: list[str], block_page: bool) -> str:
    """Three outcomes, not two. "block" is the Akamai-signature outcome confound 2 is
    actually about (status 403 or the block-page signature); "other" catches anything
    that fails for a different reason (e.g. a 404, a timeout, or a 200 whose marker
    didn't match) so it isn't silently folded into "block" and misread as WAF
    behavior."""
    if rec.get("status") == 200 and matched and not block_page:
        return "pass"
    if rec.get("status") == 403 or block_page:
        return "block"
    return "other"


def main() -> None:
    records = []
    with make_client() as client:  # contact profile — every request in this script
        for i, (name, url, markers) in enumerate(SURFACES, start=1):
            # Only robots.txt needs the full body: judging whether a Disallow rule
            # bears on this project's §7.1 paths requires the whole document, not just
            # the first 500 bytes body_head captures. The other four surfaces keep the
            # default (head-only) to avoid bloating the committed JSONL.
            rec = probe(client, "GET", url, keep_body=(name == "robots_txt"))
            head = (rec.get("body_head") or "").lower()
            matched = [m for m in markers if m.lower() in head]
            block_page = _looks_like_block_page(rec, head)
            outcome = _classify(rec, matched, block_page)
            rec |= {
                "surface": name,
                "position": i,
                "marker_found": bool(matched),
                "matched_markers": matched,
                "looks_like_block_page": block_page,
                "outcome": outcome,
                "passed": outcome == "pass",
            }
            print(
                f"#{i}  {rec.get('status')}  outcome={outcome:5}  "
                f"bytes={rec.get('body_bytes')}  markers={matched}  {name}"
            )
            records.append(rec)

    out = write_results("transport_html_contact", records)
    print(f"\nresults: {out}")

    print("\nposture per surface (contact profile):")
    for rec in records:
        print(f"  #{rec['position']} {rec['surface']}: {rec['outcome'].upper()} (status={rec.get('status')})")

    # Confound 2: the prior run's pattern was position 1 pass, positions 2-7 Akamai
    # block. Key this off the block-page signature specifically (not the composite
    # pass/fail), so a 404 or marker miss doesn't get misread as a WAF block.
    block_positions = sorted(r["position"] for r in records if r["outcome"] == "block")
    other_positions = sorted(r["position"] for r in records if r["outcome"] == "other")
    nonblock_positions = sorted(r["position"] for r in records if r["outcome"] != "block")

    if not block_positions:
        seq_note = (
            "no Akamai-style block occurred in this run — there is nothing to key "
            "confound 2 off; the sequence-position hypothesis is neither supported "
            "nor contradicted by an absence of blocks."
        )
    elif not nonblock_positions:
        seq_note = (
            "all five requests were blocked, same as the browser-shaped arm — there "
            "is no non-blocked position in this run to contrast against, so sequence "
            "position cannot be read off this result alone."
        )
    elif min(block_positions) > max(nonblock_positions):
        seq_note = (
            f"block positions {block_positions} are strictly later than every "
            f"non-block position {nonblock_positions} — CONSISTENT with a "
            "velocity/sequence-position explanation (mirrors the prior run: position "
            "1 passed, 2-7 blocked), not only a profile explanation; this run cannot "
            "tell the two apart."
        )
    else:
        seq_note = (
            f"block positions {block_positions} are interleaved with non-block "
            f"positions {nonblock_positions}, not confined to the tail — this does "
            "NOT reproduce the prior run's pass-then-block-tail pattern, which "
            "weakens (without eliminating) a pure sequence-position explanation for "
            "these blocks."
        )
    if other_positions:
        seq_note += (
            f" Positions {other_positions} were neither a pass nor an Akamai block "
            "(e.g. a non-403 non-200, or a 200 with no marker match) — inspect "
            "body_head for those before drawing any conclusion from them."
        )
    print(f"\nconfound-2 (sequence position) read: {seq_note}")


if __name__ == "__main__":
    main()
