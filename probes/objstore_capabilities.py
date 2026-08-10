# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3>=1.35.35"]
# ///
"""Endpoint capability matrix (§1.4, §17.4; §20 issue 14).

Same checks against whatever AWS_ENDPOINT_URL names, so matrices are comparable
across endpoints; run from a deployment container it settles container→endpoint
reachability. Creates ONLY throwaway bls-stats-probe-* buckets and deletes them.
NEVER creates the real buckets: §7.3 — those are created in Stage 2 from the
findings' parameter sheet.
"""
import argparse
import datetime as dt
import json
import os
import statistics
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

RESULTS_DIR = Path(__file__).parent / "results"
LOCK_HOLD_S = 90


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise SystemExit(f"error: {name} required (§1.4: the endpoint is configuration)")
    return v


def error_code(e: ClientError) -> str:
    return e.response.get("Error", {}).get("Code", str(e))


def finish(context: str, endpoint: str, region: str, checks: list[dict]) -> None:
    host = urlparse(endpoint).hostname or "unknown"
    matrix = {
        "probed_at": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "context": context,
        "endpoint_host": host,  # hostname only — never credentials
        "region": region,
        "checks": checks,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"objstore-{context}-{host}-{dt.date.today().isoformat()}.json"
    out.write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")
    print(f"\nresults: {out}\n")
    print(json.dumps(matrix, indent=2))  # copy-paste path for container runs


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--context", required=True,
                    help="where this runs: workstation | container | <label>")
    args = ap.parse_args()

    endpoint = require_env("AWS_ENDPOINT_URL")
    require_env("AWS_ACCESS_KEY_ID")
    require_env("AWS_SECRET_ACCESS_KEY")
    region = os.environ.get("AWS_REGION", "us-east-1")
    c = boto3.client(
        "s3", endpoint_url=endpoint, region_name=region,
        config=Config(retries={"max_attempts": 2}, connect_timeout=10,
                      read_timeout=60, s3={"addressing_style": "path"}),
    )

    today = dt.date.today().isoformat()
    plain = f"bls-stats-probe-plain-{today}"
    lock = f"bls-stats-probe-lock-{today}"
    checks: list[dict] = []
    state: dict = {}

    def record(name: str, fn) -> None:
        try:
            checks.append({"check": name, "outcome": "ok", "detail": fn()})
        except ClientError as e:
            checks.append({"check": name, "outcome": "error", "detail": error_code(e)})
        except Exception as e:
            checks.append({"check": name, "outcome": "unreachable",
                           "detail": f"{type(e).__name__}: {e}"})
        print(f"{checks[-1]['outcome']:12} {name}: {checks[-1]['detail']}")

    def create_bucket(name: str, *, object_lock: bool = False) -> str:
        kw: dict = {"Bucket": name}
        if region != "us-east-1":
            kw["CreateBucketConfiguration"] = {"LocationConstraint": region}
        if object_lock:
            kw["ObjectLockEnabledForBucket"] = True
        c.create_bucket(**kw)
        return "created"

    record("reachability.list_buckets",
           lambda: f"{len(c.list_buckets().get('Buckets', []))} buckets visible")
    if checks[-1]["outcome"] != "ok":
        finish(args.context, endpoint, region, checks)  # unreachable IS the issue-14 answer
        return

    record("create_bucket.plain", lambda: create_bucket(plain))

    def conditional_put() -> str:
        c.put_object(Bucket=plain, Key="cond", Body=b"1", IfNoneMatch="*")
        try:
            c.put_object(Bucket=plain, Key="cond", Body=b"2", IfNoneMatch="*")
            return "NOT ENFORCED: second If-None-Match=* PUT succeeded (header ignored)"
        except ClientError as e:
            code = error_code(e)
            ok = code in ("PreconditionFailed", "412")
            return f"enforced ({code})" if ok else f"rejected ({code})"
    record("conditional_put.if_none_match", conditional_put)

    def versioning() -> str:
        c.put_bucket_versioning(Bucket=plain,
                                VersioningConfiguration={"Status": "Enabled"})
        status = c.get_bucket_versioning(Bucket=plain).get("Status")
        c.put_object(Bucket=plain, Key="v", Body=b"1")
        c.put_object(Bucket=plain, Key="v", Body=b"2")
        n = len(c.list_object_versions(Bucket=plain, Prefix="v").get("Versions", []))
        return f"status={status}, versions_after_two_puts={n}"
    record("versioning", versioning)

    def lock_create() -> str:
        create_bucket(lock, object_lock=True)
        conf = c.get_object_lock_configuration(Bucket=lock)
        return str(conf.get("ObjectLockConfiguration", {}).get("ObjectLockEnabled", "absent"))
    record("object_lock.create_enabled", lock_create)

    if checks[-1]["outcome"] == "ok":
        def lock_retention() -> str:
            r = c.put_object(Bucket=lock, Key="locked", Body=b"x")
            state["lock_version"] = r.get("VersionId")
            state["lock_until"] = time.monotonic() + LOCK_HOLD_S
            until = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=LOCK_HOLD_S)
            c.put_object_retention(
                Bucket=lock, Key="locked", VersionId=state["lock_version"],
                Retention={"Mode": "GOVERNANCE", "RetainUntilDate": until})
            return f"GOVERNANCE +{LOCK_HOLD_S}s on version {state['lock_version']}"
        record("object_lock.put_retention", lock_retention)

        def lock_delete_denied() -> str:
            try:
                c.delete_object(Bucket=lock, Key="locked",
                                VersionId=state["lock_version"])
                return "NOT ENFORCED: versioned delete of a locked object succeeded"
            except ClientError as e:
                return f"enforced ({error_code(e)})"
        record("object_lock.delete_denied", lock_delete_denied)

        def lock_default_retention() -> str:
            c.put_object_lock_configuration(
                Bucket=lock,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 3650}}})
            return "default retention GOVERNANCE/3650d accepted (a *duration* — §17.4)"
        record("object_lock.default_retention_with_duration", lock_default_retention)

    def delete_deny() -> str:
        policy = {"Version": "2012-10-17", "Statement": [{
            "Sid": "DenyDelete", "Effect": "Deny", "Principal": {"AWS": ["*"]},
            "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
            "Resource": [f"arn:aws:s3:::{plain}/*"]}]}
        c.put_bucket_policy(Bucket=plain, Policy=json.dumps(policy))
        try:
            c.delete_object(Bucket=plain, Key="cond")
            detail = ("NOT ENFORCED for this credential (admin/root bypasses bucket "
                      "policy?) — Stage 2 needs a distinct runtime credential")
        except ClientError as e:
            detail = f"enforced ({error_code(e)})"
        c.delete_bucket_policy(Bucket=plain)
        return detail
    record("delete_deny_policy", delete_deny)

    def lifecycle() -> str:
        c.put_bucket_lifecycle_configuration(
            Bucket=plain,
            LifecycleConfiguration={"Rules": [{
                "ID": "probe", "Status": "Enabled",
                "Filter": {"Prefix": "lifecycle-test/"},
                "Expiration": {"Days": 3650}}]})
        n = len(c.get_bucket_lifecycle_configuration(Bucket=plain).get("Rules", []))
        c.delete_bucket_lifecycle(Bucket=plain)
        return f"accepted, {n} rule(s) readable"
    record("lifecycle_api", lifecycle)

    def replication() -> str:
        try:
            c.get_bucket_replication(Bucket=plain)
            return "replication configured (unexpected on a fresh bucket)"
        except ClientError as e:
            code = error_code(e)
            if code in ("ReplicationConfigurationNotFoundError",
                        "NoSuchReplicationConfiguration"):
                return f"API present, none configured ({code})"
            return f"API answered {code} — likely unsupported"
    record("replication_api", replication)

    def timings() -> dict:
        def timed(fn) -> float:
            s = time.monotonic()
            fn()
            return (time.monotonic() - s) * 1000
        out = {}
        ops = [
            ("put", lambda: c.put_object(Bucket=plain, Key="timing", Body=b"x" * 1024)),
            ("head", lambda: c.head_object(Bucket=plain, Key="timing")),
            ("get", lambda: c.get_object(Bucket=plain, Key="timing")["Body"].read()),
            ("list", lambda: c.list_objects_v2(Bucket=plain, Prefix="timing")),
        ]
        for op, fn in ops:
            out[f"{op}_ms_median"] = round(statistics.median(timed(fn) for _ in range(5)), 1)
        return out
    record("timings_1KiB", timings)

    # Cleanup — best effort; a leftover bucket is named in the output for manual removal.
    if state.get("lock_until"):
        wait = state["lock_until"] - time.monotonic() + 5
        if wait > 0:
            print(f"waiting {wait:.0f}s for the probe object's retention to lapse...")
            time.sleep(wait)
    for bucket in (plain, lock):
        try:
            versions = c.list_object_versions(Bucket=bucket)
            for v in versions.get("Versions", []) + versions.get("DeleteMarkers", []):
                c.delete_object(Bucket=bucket, Key=v["Key"], VersionId=v["VersionId"])
            c.delete_bucket(Bucket=bucket)
            print(f"cleaned up {bucket}")
        except ClientError as e:
            print(f"cleanup {bucket}: {error_code(e)} "
                  f"(fine if never created; otherwise remove manually)")

    finish(args.context, endpoint, region, checks)


if __name__ == "__main__":
    main()
