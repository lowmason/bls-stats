import os

import pytest

from bls_stats.core.config import Settings
from bls_stats.storage.doctor import check_conditional_put, check_deltalake, check_env


def test_check_env_flags_default_email_and_local_store() -> None:
    results = {r.name: r for r in check_env(Settings())}
    assert results["contact_email"].ok is False  # default email → warn
    assert results["store_uri"].ok is False  # local path → warn (ARCH §10)
    assert results["api_key"].ok is False


def test_check_env_passes_with_real_config() -> None:
    s = Settings(
        store_uri="s3://bls-stats/store",
        contact_email="me@example.org",
        contact_email_is_default=False,
        api_key="k",
    )
    assert all(r.ok for r in check_env(s))


def test_check_deltalake_importable() -> None:
    assert check_deltalake().ok is True


def test_conditional_put_skips_on_local_store() -> None:
    r = check_conditional_put(Settings(store_uri="./data/store"))
    assert r.ok is True and "skipped" in r.detail


@pytest.mark.real_store
def test_conditional_put_against_minio() -> None:
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("no AWS_ENDPOINT_URL configured")
    r = check_conditional_put(
        Settings(store_uri="s3://bls-stats/test-store", aws_endpoint_url=endpoint)
    )
    assert r.ok is True and "supported" in r.detail
