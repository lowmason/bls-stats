"""Environment-driven settings (ARCH §10). Loaded from .project.env via python-dotenv."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ENV_FILE = ".project.env"


@dataclass(frozen=True)
class Settings:
    store_uri: str = "./data/store"
    contact_email: str = "research@example.com"
    contact_email_is_default: bool = True
    api_key: str | None = None
    log_level: str = "INFO"
    aws_endpoint_url: str | None = None


def load_settings(env_file: str | Path = ENV_FILE) -> Settings:
    load_dotenv(env_file)  # silently a no-op when the file is absent
    email = os.getenv("BLS_CONTACT_EMAIL")
    return Settings(
        store_uri=os.getenv("BLS_STORE_URI", "./data/store"),
        contact_email=email or "research@example.com",
        contact_email_is_default=email is None,
        api_key=os.getenv("BLS_API_KEY"),
        log_level=os.getenv("BLS_LOG_LEVEL", "INFO"),
        aws_endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
    )


def storage_options(s: Settings) -> dict[str, str]:
    """delta-rs storage options. Commit-safety mode per ARCH §4.1: conditional PUT
    by default; BLS_S3_UNSAFE_RENAME=true switches to single-writer mode (doctor advises)."""
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
