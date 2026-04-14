# Spec: Release-Dates + Download Pipeline (V2 API, +BED, +JOLTS)

## Context

The pipeline currently downloads QCEW via a CSV open-data API (industry slices only) and CES/SAE via the BLS V2 JSON API. This spec extends QCEW to all three CSV slice types (industry, area, size), adds two new V2 API programs — BED (Business Employment Dynamics) and JOLTS (Job Openings and Labor Turnover Survey) — and covers the first two pipeline steps: **release-dates** and **download**.

---

## 1. Programs Registry — `src/bls_stats/bls/programs.py`

Add two new program definitions using the existing `_register(BLSProgram(...))` pattern:

**BD (Business Employment Dynamics):**
- Verify exact field positions from BLS format help (`https://www.bls.gov/help/hlpforma.htm#BD`)
- Fields: prefix, seasonal, state_code, county, industry_code, data_element, size_class, data_class, time_period, ownership

**JT (Job Openings and Labor Turnover Survey):**
- Fields: prefix(2), seasonal(1), industry_or_area(14), data_element(3)
- Length: 20 characters (e.g. `JTS00000000000000JOR`)

---

## 2. Industry Map — `src/bls_stats/series/industries.py`

Add optional fields to `IndustryEntry`:
```
bed_industry: str = ''    # 6-digit BD industry code
jolts_industry: str = ''  # 14-char JT industry_or_area code
```

Populate mappings for each entry where the program publishes data. Industries without a mapping get `''` and are skipped during series ID generation.

---

## 3. Config — `src/bls_stats/config.py`

Add:
- `BED_DIR = DATA_DIR / 'bed'`
- `JOLTS_DIR = DATA_DIR / 'jolts'`
- `BED_ESTIMATES_FILE = 'bed_estimates.parquet'`
- `JOLTS_ESTIMATES_FILE = 'jolts_estimates.parquet'`

BED and JOLTS output schemas include `measure` and `value` columns (instead of `employment`). Existing downloaders keep `employment` unchanged. The `combine` step (out of scope) will reconcile later.

---

## 4. Series Builder — `src/bls_stats/series/builder.py`

Add helpers parallel to `en_series_id()`:

- `bd_series_id(industry_entry, data_element, state_code='00', seasonal='U', ...)` — builds BD series ID
- `jt_series_id(industry_or_area, data_element, seasonal='S')` — builds JT series ID

Export from `series/__init__.py`.

---

## 5. New Downloader: BED — `src/bls_stats/download/bed.py`

**Measures (all available):**
- Gross job gains (total, expanding establishments, openings)
- Gross job losses (total, contracting establishments, closings)
- Rates for each of the above
- Verify full data_element list from BLS BD tables

**Geographic scope (all available):**
- National (state_code='00', county='000')
- State (52 FIPS codes, county='000')
- County (state+county FIPS — enumerate from BLS area file)
- MSA (if BED publishes MSA-level via the API)

**Function:** `download_bed(ref_date: date, api_key, out_dir) -> pl.DataFrame`

**Flow:** Build BD series IDs (geo x industry x data_element x SA/NSA) → BLSClient.get_series() → join metadata → write parquet

**Output schema:** source='bed', series_id, seasonally_adjusted, geographic_type, geographic_code, industry_type, industry_code, measure, ref_date, value

**Frequency:** Quarterly (Q01-Q04 period codes, handled by existing client)

---

## 6. New Downloader: JOLTS — `src/bls_stats/download/jolts.py`

**Measures (all available):**
- Job openings (level + rate): JOL, JOR
- Hires (level + rate): HIL, HIR
- Total separations (level + rate): TSL, TSR
- Quits (level + rate): QUL, QUR
- Layoffs/discharges (level + rate): LDL, LDR
- Other separations (level + rate): OSL, OSR

**Geographic scope (all available):**
- National by industry: industry_or_area = JOLTS industry code (14 chars)
- State total nonfarm: industry_or_area = state area code (14 chars)
- Region total nonfarm: industry_or_area = region area code (14 chars)
- Verify area code format from BLS JT area mapping

**Function:** `download_jolts(ref_date: date, api_key, out_dir) -> pl.DataFrame`

**Output schema:** Same as BED (source='jolts')

**Frequency:** Monthly (M01-M12)

---

## 7. QCEW CSV Download (All Slices) — `src/bls_stats/download/qcew.py`

**Modify in place.** The existing `download/qcew.py` already uses `QCEWClient` (CSV open-data API) with industry slices. Extend it to download all three slice types.

The `QCEWClient` supports three slice types: **industry**, **area**, and **size**. Currently only industry is used.

**Industry slices** (existing — keep as-is):
- Loop over `INDUSTRY_MAP`, call `QCEWClient.get_industry()` for each
- Filter to national + state rows

**Area slices** (new):
- Loop over area FIPS codes (national `US000`, state codes, county codes, MSAs)
- Call `QCEWClient.get_area(area_code, start_year, end_year, quarters)` for each
- Returns all industries for a given area — useful for complete county/MSA coverage

**Size slices** (new):
- Loop over size class codes (0-9)
- Call `QCEWClient.get_size(size_code, start_year, end_year)` for each
- Note: size class data is Q1-only (annual averages)
- Returns employment by establishment size class

**Function:** Update existing `download_qcew()` signature:
`download_qcew(ref_date: date, slices=['industry','area','size'], cache_dir, out_dir) -> pl.DataFrame`

Translates `ref_date` into the appropriate `year` and `qtr` for the CSV API.

**Post-download:** Concat all three slice DataFrames and deduplicate (rows may overlap between industry and area slices for the same area/industry/quarter). Write single output to `data/qcew/qcew_estimates.parquet`.

**Output schema:** Same as current QCEW output (source='qcew', series_id, seasonally_adjusted=False, geographic_type, geographic_code, industry_type, industry_code, ref_date, employment).

---

## 8. Release-Dates: Publication Config — `src/bls_stats/release_dates/config.py`

Add publications:
- BED: `name='bed'`, `series='bdm'`, frequency='quarterly', verify archive URL pattern
- JOLTS: `name='jolts'`, `series='jolts'`, frequency='monthly', verify archive URL pattern

The existing scraper and parser are generic and work for any publication following the standard BLS archive format.

---

## 9. Release-Dates: Vintage Logic — `src/bls_stats/release_dates/vintage_dates.py`

Add revision functions:

**`_add_bed_revisions(df)`:** BED has initial (revision=0) and one revision (revision=1). Quarterly offset pattern similar to QCEW but simpler.

**`_add_jolts_revisions(df)`:** JOLTS has initial (revision=0) and one revision (revision=1). Monthly, similar to SAE.

No benchmark revision logic for BED or JOLTS (they don't have the annual benchmark cycle).

Update `build_vintage_dates()` to include BED and JOLTS in the concat.

---

## 10. CLI — `src/bls_stats/__main__.py`

Update the `download` subcommand to accept:
- `--program` (required): one of `qcew`, `ces`, `sae`, `bed`, `jolts`
- `--ref-date` (required): a single reference date (e.g. `2025-01`)

Only the selected program is downloaded, and only for the given reference date. Each downloader accepts a single ref_date instead of a start_year/end_year range.

For V2 API programs (CES, SAE, BED, JOLTS): translate ref_date into the appropriate `start_year=end_year=ref_date.year` and filter the API response to the matching period.

For QCEW CSV: translate ref_date into the appropriate `year` and `qtr` parameters for the CSV API call.

Update `download/__init__.py` to export the new functions.

---

## 11. Implementation Order

| Step | File(s) | Risk |
|------|---------|------|
| 1 | `bls/programs.py` — add BD, JT definitions | Low (additive) |
| 2 | `series/industries.py` — add bed_industry, jolts_industry fields | Low (defaults) |
| 3 | `config.py` — add BED/JOLTS dirs and files | Low (additive) |
| 4 | `series/builder.py` + `series/__init__.py` — add bd/jt helpers | Low (additive) |
| 5 | `download/bed.py` — new file | Low (new code) |
| 6 | `download/jolts.py` — new file | Low (new code) |
| 7 | `download/qcew.py` — add area + size slices | Low (extends existing code) |
| 8 | `release_dates/config.py` — add BED/JOLTS pubs | Low (additive) |
| 9 | `release_dates/vintage_dates.py` — add revision functions | Low (additive) |
| 10 | `download/__init__.py` + `__main__.py` — wire CLI | Low (wiring) |

---

## 12. Open Questions to Verify During Implementation

- BD series ID exact field positions and lengths (from BLS format help page)
- JOLTS state/region area codes (from BLS JT area mapping files)
- BED state/county/MSA coverage per industry level
- BED and JOLTS archive URL patterns for release-date scraping
- Whether BED publishes seasonally adjusted data (or NSA only)

---

## 13. Verification

- `ruff check src/` passes
- `pytest tests/` passes
- Manual test: `bls-stats download --program ces --ref-date 2025-01` downloads a single program for a single ref date
- Manual test: `bls-stats release-dates` scrapes and parses BED/JOLTS archives
- Inspect output parquets for expected schema and row counts
