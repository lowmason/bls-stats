"""Pre-flight probes (ARCH §8 doctor): env, delta-rs, store, conditional PUT, BLS reachability."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from bls_stats.core.config import Settings, storage_options
from bls_stats.core.http import build_client, get


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


def check_env(settings: Settings) -> list[CheckResult]:
    return [
        CheckResult(
            "contact_email",
            not settings.contact_email_is_default,
            settings.contact_email
            if not settings.contact_email_is_default
            else "BLS_CONTACT_EMAIL unset — using placeholder; BLS expects a real contact",
        ),
        CheckResult(
            "store_uri",
            settings.store_uri.startswith("s3://"),
            settings.store_uri
            if settings.store_uri.startswith("s3://")
            else f"{settings.store_uri} is a local path — laptop-only convenience (ARCH §10)",
        ),
        CheckResult(
            "api_key",
            settings.api_key is not None,
            "set" if settings.api_key else "BLS_API_KEY unset — api_v2 engine unavailable",
        ),
    ]


def check_deltalake() -> CheckResult:
    try:
        import deltalake

        return CheckResult("deltalake", True, f"deltalake {deltalake.__version__}")
    except Exception as exc:  # pragma: no cover - import failure environment-specific
        return CheckResult("deltalake", False, str(exc))


def check_store(settings: Settings) -> CheckResult:
    from bls_stats.storage.delta import VintageStore

    try:
        VintageStore(settings.store_uri, storage_options(settings)).read_state("ledger")
        return CheckResult("store", True, f"reachable: {settings.store_uri}")
    except Exception as exc:
        return CheckResult("store", False, f"{settings.store_uri}: {exc}")


def check_conditional_put(settings: Settings) -> CheckResult:
    """If-None-Match probe (ARCH §4.1) — decides delta commit-safety mode."""
    if not settings.store_uri.startswith("s3://"):
        return CheckResult("conditional_put", True, "skipped: local store")
    import boto3
    from botocore.exceptions import ClientError

    bucket = settings.store_uri.removeprefix("s3://").split("/", 1)[0]
    key = f"_doctor/probe-{uuid.uuid4().hex}"
    s3 = boto3.client("s3", endpoint_url=settings.aws_endpoint_url)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=b"a", IfNoneMatch="*")
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=b"b", IfNoneMatch="*")
            return CheckResult(
                "conditional_put",
                False,
                "NOT honored — set BLS_S3_UNSAFE_RENAME=true (single-writer mode)",
            )
        except ClientError as exc:
            code = exc.response["ResponseMetadata"]["HTTPStatusCode"]
            ok = code == 412
            return CheckResult(
                "conditional_put",
                ok,
                "supported (412 on overwrite)" if ok else f"odd status {code}",
            )
        finally:
            s3.delete_object(Bucket=bucket, Key=key)
    except Exception as exc:
        return CheckResult("conditional_put", False, f"probe failed: {exc}")


def check_bls(settings: Settings) -> CheckResult:
    try:
        client = build_client(settings, timeout=30.0)
        resp = get(client, "https://download.bls.gov/pub/time.series/jt/", method="HEAD")
        return CheckResult("bls", resp.status_code == 200, f"HTTP {resp.status_code}")
    except Exception as exc:
        return CheckResult("bls", False, str(exc))


def run_all(settings: Settings) -> list[CheckResult]:
    return [
        *check_env(settings),
        check_deltalake(),
        check_store(settings),
        check_conditional_put(settings),
        check_bls(settings),
    ]
