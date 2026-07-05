"""Pre-flight probes (ARCH §8 doctor): env, delta-rs, store, conditional PUT, BLS reachability."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from bls_stats.core.config import Settings, storage_options
from bls_stats.core.http import build_client, get


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one `doctor` pre-flight probe.

    Attributes:
        name: Stable machine-readable check identifier (e.g. `"conditional_put"`), used as a
            dict key by callers that index results by name.
        ok: Whether the check passed. A probe that is skipped as not applicable (e.g.
            conditional-PUT on a local store) reports `ok=True` with a `"skipped: ..."` detail.
        detail: Human-readable explanation — the value observed, or the reason for failure.
    """

    name: str
    ok: bool
    detail: str


def check_env(settings: Settings) -> list[CheckResult]:
    """Check contact email, store URI, and API key configuration for production-readiness.

    Each sub-check warns (does not hard-fail) on a placeholder or laptop-only setting: a
    default contact email is a BLS-etiquette problem, not a crash; a local `store_uri` is
    explicitly supported for laptop use (ARCH §10) but flagged since it's rarely the deployment
    intent; a missing API key merely disables the `api_v2` fetch engine.

    Args:
        settings: Loaded application settings.

    Returns:
        Three `CheckResult`s named `"contact_email"`, `"store_uri"`, `"api_key"`.
    """
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
    """Verify the `deltalake` package imports in this environment.

    Returns:
        `CheckResult` named `"deltalake"`; `detail` carries the installed version on success.
    """
    try:
        import deltalake

        return CheckResult("deltalake", True, f"deltalake {deltalake.__version__}")
    except Exception as exc:  # pragma: no cover - import failure environment-specific
        return CheckResult("deltalake", False, str(exc))


def check_store(settings: Settings) -> CheckResult:
    """Verify the configured store is reachable by reading the ledger state table.

    Uses `VintageStore.read_state`, which tolerates an absent ledger table (returns `None`
    rather than raising) — so this only fails on genuine connectivity/permission problems, not
    on a fresh store that hasn't ingested anything yet.

    Args:
        settings: Loaded application settings.

    Returns:
        `CheckResult` named `"store"`.
    """
    from bls_stats.storage.delta import VintageStore

    try:
        VintageStore(settings.store_uri, storage_options(settings)).read_state("ledger")
        return CheckResult("store", True, f"reachable: {settings.store_uri}")
    except Exception as exc:
        return CheckResult("store", False, f"{settings.store_uri}: {exc}")


def check_conditional_put(settings: Settings) -> CheckResult:
    """If-None-Match probe (ARCH §4.1) — decides delta commit-safety mode.

    Writes the same object key twice with `IfNoneMatch="*"`, which asks S3 to accept the PUT
    only if the key does not already exist. A store that honors conditional PUT lets `delta-rs`
    make commits that are safe under concurrent writers (each commit file is written exactly
    once, contention rejected rather than silently overwritten). Proof is empirical, not
    documentation-based, because conditional-PUT support varies across S3-compatible object
    stores.

    The probe: PUT once (expected to succeed — the key doesn't exist yet), then PUT the same key
    again with the same conditional header. A second success would mean the precondition was
    not enforced (not honored); a `412 Precondition Failed` on the second PUT proves the store
    rejected the write because the key already existed, i.e. conditional PUT — and therefore
    concurrent-writer-safe Delta commits — is supported. Any other status is treated as failure
    (unexpected behavior, don't assume safety). Skipped entirely for non-S3 (local) stores,
    where this mode question doesn't apply.

    Args:
        settings: Loaded application settings; `store_uri` selects S3 vs. local, and
            `aws_endpoint_url` targets the S3-compatible endpoint (e.g. for MinIO in dev).

    Returns:
        `CheckResult` named `"conditional_put"`. `ok=True` with `"skipped: local store"` for
        non-S3 stores; otherwise `ok` reflects whether the second PUT was rejected with 412.
    """
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
    """Verify `download.bls.gov` is reachable with a lightweight `HEAD` request.

    Args:
        settings: Loaded application settings (HTTP client config: contact email, timeouts).

    Returns:
        `CheckResult` named `"bls"`; `ok` iff the response status is 200.
    """
    try:
        client = build_client(settings, timeout=30.0)
        resp = get(client, "https://download.bls.gov/pub/time.series/jt/", method="HEAD")
        return CheckResult("bls", resp.status_code == 200, f"HTTP {resp.status_code}")
    except Exception as exc:
        return CheckResult("bls", False, str(exc))


def run_all(settings: Settings) -> list[CheckResult]:
    """Run every pre-flight probe in order: env, deltalake, store, conditional PUT, BLS.

    The full `bls-stats doctor` battery (ARCH §8) — run before deployment or when diagnosing a
    misconfigured environment.

    Args:
        settings: Loaded application settings.

    Returns:
        All `CheckResult`s concatenated, in probe order.
    """
    return [
        *check_env(settings),
        check_deltalake(),
        check_store(settings),
        check_conditional_put(settings),
        check_bls(settings),
    ]
