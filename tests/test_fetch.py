"""Tests for HTTP download utilities."""

import io
import zipfile
from unittest.mock import patch, MagicMock

import httpx
import polars as pl
import pytest

from bls_stats.download.fetch import (
    download_text,
    download_bytes,
    read_tsv,
    read_zip_csvs,
    USER_AGENT,
)


class TestDownloadText:
    @patch("bls_stats.download.fetch._make_client")
    def test_success(self, mock_make_client):
        mock_resp = MagicMock()
        mock_resp.text = "hello world"
        mock_client = MagicMock()
        mock_client.get.return_value = mock_resp
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_make_client.return_value = mock_client

        result = download_text("https://example.com/data.txt")
        assert result == "hello world"

    @patch("bls_stats.download.fetch._make_client")
    @patch("bls_stats.download.fetch.time.sleep")
    def test_retries_on_failure(self, mock_sleep, mock_make_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        mock_resp_fail = MagicMock()
        mock_resp_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
            "503", request=MagicMock(), response=MagicMock()
        )
        mock_resp_ok = MagicMock()
        mock_resp_ok.text = "recovered"

        mock_client.get.side_effect = [mock_resp_fail, mock_resp_ok]
        mock_make_client.return_value = mock_client

        result = download_text("https://example.com/data.txt")
        assert result == "recovered"
        assert mock_sleep.call_count == 1

    @patch("bls_stats.download.fetch._make_client")
    @patch("bls_stats.download.fetch.time.sleep")
    def test_raises_after_max_retries(self, mock_sleep, mock_make_client):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "503", request=MagicMock(), response=MagicMock()
        )
        mock_client.get.return_value = mock_resp
        mock_make_client.return_value = mock_client

        with pytest.raises(httpx.HTTPStatusError):
            download_text("https://example.com/data.txt")


class TestDownloadBytes:
    @patch("bls_stats.download.fetch._make_client")
    def test_success(self, mock_make_client):
        mock_resp = MagicMock()
        mock_resp.content = b"binary data"
        mock_client = MagicMock()
        mock_client.get.return_value = mock_resp
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_make_client.return_value = mock_client

        result = download_bytes("https://example.com/data.bin")
        assert result == b"binary data"


class TestReadTsv:
    @patch("bls_stats.download.fetch.download_text")
    def test_parses_tsv(self, mock_download):
        mock_download.return_value = "col_a\tcol_b\n1\thello\n2\tworld\n"
        df = read_tsv("https://example.com/data.tsv")
        assert df.shape == (2, 2)
        assert df.columns == ["col_a", "col_b"]

    @patch("bls_stats.download.fetch.download_text")
    def test_strips_column_names(self, mock_download):
        mock_download.return_value = " col_a \t col_b \n1\thello\n"
        df = read_tsv("https://example.com/data.tsv")
        assert df.columns == ["col_a", "col_b"]


class TestReadZipCsvs:
    @patch("bls_stats.download.fetch.download_bytes")
    def test_reads_csvs_from_zip(self, mock_download):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("data1.csv", "a,b\n1,2\n")
            zf.writestr("data2.csv", "a,b\n3,4\n")
        mock_download.return_value = buf.getvalue()

        df = read_zip_csvs("https://example.com/data.zip")
        assert len(df) == 2
        assert df.columns == ["a", "b"]

    @patch("bls_stats.download.fetch.download_bytes")
    def test_empty_zip(self, mock_download):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no csvs here")
        mock_download.return_value = buf.getvalue()

        df = read_zip_csvs("https://example.com/data.zip")
        assert df.is_empty()

    @patch("bls_stats.download.fetch.download_bytes")
    def test_schema_overrides(self, mock_download):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("data.csv", "code,val\n001,10\n")
        mock_download.return_value = buf.getvalue()

        df = read_zip_csvs(
            "https://example.com/data.zip",
            schema_overrides={"code": pl.Utf8},
        )
        assert df["code"].dtype == pl.Utf8


class TestUserAgent:
    def test_contains_project_name(self):
        assert "bls-stats" in USER_AGENT
