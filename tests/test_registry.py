from bls_stats.registry import REGISTRY, SERIES_LAYOUTS, Frequency

EXPECTED_LAYOUT_LEN = {"CE": 13, "SM": 20, "BD": 28, "JT": 21, "LN": 11, "OE": 26, "EP": 15}


def test_eight_programs() -> None:
    assert set(REGISTRY) == {"ces", "sae", "jolts", "cps", "bed", "qcew", "oews", "ep"}


def test_layout_widths_sum_to_documented_totals() -> None:
    for prefix, total in EXPECTED_LAYOUT_LEN.items():
        widths = sum(w for _, w in SERIES_LAYOUTS[prefix])
        assert widths == total, prefix


def test_periodic_programs_have_feeds_and_archives() -> None:
    for name, spec in REGISTRY.items():
        if name == "ep":
            assert spec.feed_url is None
            continue
        assert spec.feed_url and spec.feed_url.startswith("https://www.bls.gov/feed/")
        assert spec.archive_url


def test_ces_cps_share_empsit() -> None:
    assert REGISTRY["ces"].feed_url == REGISTRY["cps"].feed_url


def test_profiles_match_spec_table() -> None:  # ARCH §2.1/§2.2
    assert REGISTRY["ces"].profile.routine_slots == 3
    assert REGISTRY["ces"].profile.benchmark_window_years == 5
    assert REGISTRY["sae"].profile.routine_slots == 2
    assert REGISTRY["cps"].profile.routine_slots == 1
    assert REGISTRY["qcew"].profile.routine_rule == "year_to_date"
    assert REGISTRY["bed"].profile.benchmark_window_years == 2
    assert REGISTRY["oews"].profile.benchmark_rule is None


def test_unit_columns() -> None:  # ARCH §4.3
    assert REGISTRY["ces"].unit_columns == ("series_id",)
    assert REGISTRY["qcew"].unit_columns == (
        "area_fips", "own_code", "industry_code", "agglvl_code", "size_code",
    )
    assert REGISTRY["ep"].unit_columns == ("occupation_code", "industry_code")


def test_frequencies() -> None:
    assert REGISTRY["qcew"].frequency == Frequency.QUARTERLY
    assert REGISTRY["oews"].frequency == Frequency.ANNUAL
    assert REGISTRY["ep"].frequency == Frequency.NONE
