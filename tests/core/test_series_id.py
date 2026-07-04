import pytest

from bls_stats.core.series_id import SeriesIdError, decode


def test_decode_ces() -> None:
    parts = decode("CES0500000003")
    assert parts == {
        "prefix": "CE",
        "seasonal": "S",
        "supersector": "05",
        "industry": "000000",
        "data_type": "03",
    }


def test_decode_cps() -> None:
    assert decode("LNS14000000") == {"prefix": "LN", "seasonal": "S", "series_code": "14000000"}


def test_decode_jolts_length_21() -> None:
    parts = decode("JTS000000000000000HIL")
    assert parts["data_element"] == "HI"
    assert parts["rate_level"] == "L"


def test_whitespace_tolerated() -> None:
    assert decode(" LNS14000000 ")["series_code"] == "14000000"


@pytest.mark.parametrize("bad", ["XX123", "CES05000000", ""])  # unknown prefix / wrong length
def test_errors(bad: str) -> None:
    with pytest.raises(SeriesIdError):
        decode(bad)
