from pathlib import Path

from bls_stats.core.config import Settings, load_settings, storage_options


def test_defaults_when_env_absent(monkeypatch, tmp_path: Path) -> None:
    for var in ("BLS_STORE_URI", "BLS_CONTACT_EMAIL", "BLS_API_KEY", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(var, raising=False)
    s = load_settings(env_file=tmp_path / "missing.env")
    assert s.store_uri == "./data/store"
    assert s.contact_email == "research@example.com"
    assert s.contact_email_is_default is True
    assert s.api_key is None


def test_reads_dotenv_file(monkeypatch, tmp_path: Path) -> None:
    for var in ("BLS_STORE_URI", "BLS_CONTACT_EMAIL"):
        monkeypatch.delenv(var, raising=False)
    env = tmp_path / ".project.env"
    env.write_text("BLS_STORE_URI=s3://bls-stats/store\nBLS_CONTACT_EMAIL=me@example.org\n")
    s = load_settings(env_file=env)
    assert s.store_uri == "s3://bls-stats/store"
    assert s.contact_email == "me@example.org"
    assert s.contact_email_is_default is False


def test_storage_options_http_endpoint() -> None:
    s = Settings(store_uri="s3://bls-stats/store", aws_endpoint_url="http://127.0.0.1:9000")
    opts = storage_options(s)
    assert opts["AWS_ENDPOINT_URL"] == "http://127.0.0.1:9000"
    assert opts["AWS_ALLOW_HTTP"] == "true"
    assert opts["aws_conditional_put"] == "etag"


def test_storage_options_unsafe_rename(monkeypatch) -> None:
    monkeypatch.setenv("BLS_S3_UNSAFE_RENAME", "true")
    opts = storage_options(
        Settings(store_uri="s3://bls-stats/store", aws_endpoint_url="https://s3.example.com")
    )
    assert opts["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"
    assert "aws_conditional_put" not in opts


def test_storage_options_empty_for_local_store() -> None:
    assert storage_options(Settings()) == {}  # local path: no S3 commit options
