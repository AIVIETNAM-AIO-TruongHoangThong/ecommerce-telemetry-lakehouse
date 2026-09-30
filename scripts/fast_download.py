"""
scripts/fast_download.py
------------------------
Multi-threaded HTTP Range dataset downloader with resume capability
and gzip integrity verification.
"""

import concurrent.futures
import gzip
import os
import shutil
import sys
import time
import urllib.error
import urllib.request
from typing import List, Optional, Tuple

import click

ALL_DATASET_URLS: List[str] = [
    "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2019-Nov.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2019-Dec.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2020-Jan.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2020-Feb.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2020-Mar.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2020-Apr.csv.gz",
]

REMAINDER_URLS: List[str] = [
    "https://data.rees46.com/datasets/marketplace/2019-Dec.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2020-Jan.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2020-Feb.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2020-Mar.csv.gz",
    "https://data.rees46.com/datasets/marketplace/2020-Apr.csv.gz",
]

USER_AGENT = "Mozilla/5.0 (compatible; FastRangeDownloader/1.0)"


def format_bytes(bytes_count: float) -> str:
    """Format byte counts into human-readable strings."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(bytes_count) < 1024.0 or unit == "TB":
            return f"{bytes_count:.2f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.2f} PB"


def verify_gzip_integrity(file_path: str) -> bool:
    """
    Test gzip stream integrity by reading through the compressed file.
    Returns True if valid, False if corrupted or truncated.
    """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return False
    try:
        with gzip.open(file_path, "rb") as gz:
            # Read first block and iterate to end of file to verify all blocks and CRC
            buf_size = 16 * 1024 * 1024
            while True:
                chunk = gz.read(buf_size)
                if not chunk:
                    break
        return True
    except (OSError, EOFError, gzip.BadGzipFile, Exception):
        return False


def get_remote_file_size(url: str) -> Tuple[int, bool]:
    """
    Inspect remote file to determine total size and HTTP Range support.
    Returns (content_length, supports_range).
    """
    headers = {"User-Agent": USER_AGENT}
    try:
        req = urllib.request.Request(url, headers=headers, method="HEAD")
        with urllib.request.urlopen(req, timeout=15) as resp:
            content_length = int(resp.headers.get("Content-Length", 0))
            accept_ranges = resp.headers.get("Accept-Ranges", "").lower()
            supports_range = "bytes" in accept_ranges
            if content_length > 0:
                return content_length, supports_range
    except Exception:
        pass

    # Fallback to 1-byte GET probe to test Range support
    headers["Range"] = "bytes=0-0"
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            content_range = resp.headers.get("Content-Range", "")
            if "/" in content_range:
                total_size = int(content_range.split("/")[-1])
                return total_size, True
            content_length = int(resp.headers.get("Content-Length", 0))
            return content_length, False
    except Exception as e:
        raise RuntimeError(f"Failed to inspect remote resource {url}: {e}")


def download_chunk(
    url: str,
    start_byte: int,
    end_byte: int,
    part_path: str,
    max_retries: int = 5,
) -> None:
    """
    Download a byte range into a part file. Resumes existing partial chunk if present.
    """
    expected_size = end_byte - start_byte + 1

    # Check if part file already fully downloaded
    if os.path.exists(part_path) and os.path.getsize(part_path) == expected_size:
        return

    # Check if partial chunk exists to resume
    current_size = os.path.getsize(part_path) if os.path.exists(part_path) else 0
    if current_size > expected_size:
        # Corrupted partial chunk; reset
        os.remove(part_path)
        current_size = 0

    chunk_start = start_byte + current_size
    if chunk_start > end_byte:
        return

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Range": f"bytes={chunk_start}-{end_byte}",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                mode = "ab" if current_size > 0 else "wb"
                with open(part_path, mode) as out_f:
                    shutil.copyfileobj(resp, out_f, length=1024 * 1024)

            # Validate chunk size
            if os.path.getsize(part_path) == expected_size:
                return
            else:
                current_size = os.path.getsize(part_path)
                chunk_start = start_byte + current_size
        except Exception as e:
            if attempt == max_retries - 1:
                raise RuntimeError(
                    f"Failed to download chunk [{start_byte}-{end_byte}] after {max_retries} attempts: {e}"
                )
            time.sleep(1.5 * (attempt + 1))


def download_dataset(
    url: str,
    output_dir: str,
    threads: int = 16,
) -> str:
    """
    Download a single dataset file using multi-threaded HTTP Range requests with resume support.
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.basename(url)
    target_path = os.path.join(output_dir, filename)

    # 1. Quick check: Is file already downloaded and valid?
    if os.path.exists(target_path):
        click.echo(f"Checking existing file integrity: {target_path} ...")
        if verify_gzip_integrity(target_path):
            click.echo(
                f"✅ File {filename} already complete and verified. Skipping download."
            )
            return target_path
        else:
            click.echo(
                f"⚠️ Existing {filename} is corrupt or truncated. Re-downloading from scratch..."
            )
            os.remove(target_path)

    # 2. Query remote file metadata
    total_size, supports_range = get_remote_file_size(url)
    click.echo(
        f"Downloading {filename} ({format_bytes(total_size)}) using {threads} threads "
        f"[Range Support: {'Yes' if supports_range else 'No'}]"
    )

    if not supports_range or total_size == 0 or threads <= 1:
        # Fallback to single stream
        temp_target = target_path + ".tmp"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req) as resp, open(temp_target, "wb") as f:
            shutil.copyfileobj(resp, f, length=2 * 1024 * 1024)
        os.replace(temp_target, target_path)
        return target_path

    # 3. Partition file into chunks
    parts_dir = os.path.join(output_dir, f".{filename}.parts")
    os.makedirs(parts_dir, exist_ok=True)

    chunk_size = total_size // threads
    chunks = []
    for i in range(threads):
        start = i * chunk_size
        end = total_size - 1 if i == threads - 1 else (i + 1) * chunk_size - 1
        part_file = os.path.join(parts_dir, f"chunk_{i:03d}.part")
        chunks.append((i, start, end, part_file))

    start_time = time.time()

    # 4. Multi-threaded download
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(download_chunk, url, start, end, part_file): i
            for i, start, end, part_file in chunks
        }
        for future in concurrent.futures.as_completed(futures):
            future.result()

    # 5. Concatenate chunks into final file
    temp_target = target_path + ".assembling"
    click.echo(f"Assembling {threads} chunks into {filename} ...")
    with open(temp_target, "wb") as dest_f:
        for _, _, _, part_file in chunks:
            with open(part_file, "rb") as src_f:
                shutil.copyfileobj(src_f, dest_f, length=8 * 1024 * 1024)

    # 6. Verify GZIP integrity
    click.echo(f"Verifying gzip integrity for {filename} ...")
    if not verify_gzip_integrity(temp_target):
        if os.path.exists(temp_target):
            os.remove(temp_target)
        raise RuntimeError(
            f"Gzip integrity verification failed for {filename}. Data is corrupted."
        )

    # 7. Atomic finalize
    os.replace(temp_target, target_path)
    shutil.rmtree(parts_dir, ignore_errors=True)

    elapsed = time.time() - start_time
    rate = total_size / elapsed if elapsed > 0 else 0
    click.echo(
        f"✅ Successfully downloaded {filename} ({format_bytes(total_size)}) "
        f"in {elapsed:.1f}s ({format_bytes(rate)}/s)"
    )
    return target_path


@click.command(help="Multi-threaded range downloader for marketplace datasets.")
@click.option(
    "--all",
    "download_all",
    is_flag=True,
    help="Download all 7 monthly datasets (Oct 2019 - Apr 2020).",
)
@click.option(
    "--all-remainders",
    is_flag=True,
    help="Download remaining 5 datasets (Dec 2019 - Apr 2020).",
)
@click.option(
    "--url",
    "urls",
    multiple=True,
    help="Specific dataset URL(s) to download.",
)
@click.option(
    "--threads",
    default=16,
    type=int,
    show_default=True,
    help="Number of concurrent download worker threads.",
)
@click.option(
    "--output-dir",
    default="./data/scratch",
    show_default=True,
    help="Target scratch directory for downloaded datasets.",
)
def main(
    download_all: bool,
    all_remainders: bool,
    urls: tuple,
    threads: int,
    output_dir: str,
) -> None:
    """
    CLI Entrypoint for fast dataset acquisition.
    """
    target_urls: List[str] = []

    if download_all:
        target_urls = ALL_DATASET_URLS
    elif all_remainders:
        target_urls = REMAINDER_URLS
    elif urls:
        target_urls = list(urls)
    else:
        click.echo("Error: No URLs specified")
        sys.exit(1)

    click.echo(
        f"Starting accelerated download for {len(target_urls)} dataset(s) into '{output_dir}'"
    )
    for url in target_urls:
        download_dataset(url=url, output_dir=output_dir, threads=threads)


if __name__ == "__main__":
    main()
