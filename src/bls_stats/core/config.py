"""Environment-driven settings (ARCH §10). Loaded from .project.env via python-dotenv."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ENV_FILE = ".project.env"


@dataclass(frozen=True)
class Settings:
    """Resolved runtime configuration (ARCH §10). Immutable; construct via `load_settings`.

    Attributes:
        store_uri: Vintage store root. A local path is a laptop-only convenience (ARCH §1);
            deployment must use an `s3://` URI — `doctor` warns otherwise.
        contact_email: Contact address embedded in the HTTP `User-Agent` (ARCH §10).
        contact_email_is_default: `True` when `contact_email` fell back to the placeholder
            because `BLS_CONTACT_EMAIL` was unset — `doctor` surfaces this as a warning.
        api_key: BLS API v2 key for the utility engine (ARCH §6.1), or `None` if unset.
        log_level: stderr log verbosity, e.g. `"INFO"`.
        aws_endpoint_url: S3-compatible endpoint override (MinIO vs. a corporate endpoint
            differ by this one variable), or `None` to use the AWS default resolution.
        metadata_cache_dir: Local cache dir for CPS dimension tables (series catalog +
            `ln.*` mappings), configurable so it doesn't silently depend on cwd.
    """

    store_uri: str = "./data/store"
    contact_email: str = "research@example.com"
    contact_email_is_default: bool = True
    api_key: str | None = None
    log_level: str = "INFO"
    aws_endpoint_url: str | None = None
    metadata_cache_dir: str = "data/cps_metadata"


def load_settings(env_file: str | Path = ENV_FILE) -> Settings:
    """Build `Settings` from the environment, loading `env_file` first via python-dotenv.

    Existing environment variables take precedence over the file's values (python-dotenv
    default). Missing `env_file` is silently a no-op — not an error — so tests and containers
    without a `.project.env` still resolve to defaults.

    Args:
        env_file: Path to the dotenv file to load before reading variables. Defaults to
            `.project.env` (ARCH §1), the project's gitignored secrets file.

    Returns:
        A `Settings` populated from `BLS_STORE_URI`, `BLS_CONTACT_EMAIL`, `BLS_API_KEY`,
        `BLS_LOG_LEVEL`, and `AWS_ENDPOINT_URL`, falling back to documented defaults.
    """
    load_dotenv(env_file)  # silently a no-op when the file is absent
    email = os.getenv("BLS_CONTACT_EMAIL")
    return Settings(
        store_uri=os.getenv("BLS_STORE_URI", "./data/store"),
        contact_email=email or "research@example.com",
        contact_email_is_default=email is None,
        api_key=os.getenv("BLS_API_KEY"),
        log_level=os.getenv("BLS_LOG_LEVEL", "INFO"),
        aws_endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        metadata_cache_dir=os.getenv("BLS_METADATA_CACHE", "data/cps_metadata"),
    )


def storage_options(s: Settings) -> dict[str, str]:
    """Build delta-rs storage options, selecting commit-safety mode per ARCH §4.1.

    Local paths need no S3 options at all. For `s3://` stores, conditional PUT
    (`aws_conditional_put=etag`) is the default — fully safe atomic commits — unless
    `BLS_S3_UNSAFE_RENAME=true` is set, which switches to single-writer mode for endpoints
    that don't support conditional PUT (sound only because the design has exactly one writer,
    the daily cron; `doctor` advises which mode an endpoint needs).

    Args:
        s: Resolved settings; `store_uri` and `aws_endpoint_url` drive the decision.

    Returns:
        A dict of delta-rs storage option keys/values, empty for a local (non-`s3://`) store.
    """
    opts: dict[str, str] = {}
    if not s.store_uri.startswith("s3://"):
        return opts  # local store: no S3 options (laptop-only convenience, ARCH §10)
    if s.aws_endpoint_url:
        opts["AWS_ENDPOINT_URL"] = s.aws_endpoint_url
        if s.aws_endpoint_url.startswith("http://"):
            opts["AWS_ALLOW_HTTP"] = "true"
    if os.getenv("BLS_S3_UNSAFE_RENAME", "").lower() == "true":
        opts["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    else:
        opts["aws_conditional_put"] = "etag"
    return opts
