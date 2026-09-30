"""
tests/test_fast_download.py
---------------------------
Unit tests for the accelerated multi-threaded dataset downloader.
"""

from click.testing import CliRunner
from scripts.fast_download import (
    ALL_DATASET_URLS,
    REMAINDER_URLS,
    format_bytes,
    main,
)


def test_format_bytes():
    assert format_bytes(500) == "500.00 B"
    assert format_bytes(1024) == "1.00 KB"
    assert format_bytes(1024 * 1024) == "1.00 MB"
    assert format_bytes(1024 * 1024 * 1024 * 2.75) == "2.75 GB"


def test_dataset_url_lists():
    assert len(ALL_DATASET_URLS) == 7
    assert len(REMAINDER_URLS) == 5
    for url in ALL_DATASET_URLS:
        assert url.startswith("https://data.rees46.com/datasets/marketplace/")
        assert url.endswith(".csv.gz")


def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Multi-threaded range downloader" in result.output
    assert "--all" in result.output
    assert "--all-remainders" in result.output
    assert "--threads" in result.output


def test_cli_no_args_shows_error():
    runner = CliRunner()
    result = runner.invoke(main, [])
    assert result.exit_code == 1
    assert "Error: No URLs specified" in result.output
