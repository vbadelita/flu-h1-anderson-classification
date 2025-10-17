import asyncio
import json
from pathlib import Path
from typing import Set, List

import aiofiles
import aiohttp
from aiohttp import ClientTimeout
from tqdm import tqdm


import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Download from BV-BRC async for provided accession list.")
    parser.add_argument("--bv_brc_url", type=str,
                        default="https://www.bv-brc.org/api/genome_sequence/?accession={accession}",
                        help="BV-BRC endpoint URL (use {accession} as placeholder)")
    parser.add_argument("--accs", "-i", type=str, required=True,
                        help="Path to accession list")
    parser.add_argument("--output_dir", "-o", type=str, required=True,
                        help="Output directory")
    parser.add_argument("--max_concurrency", type=int, default=10,
                        help="Maximum concurrency")
    parser.add_argument("--request_timeout_seconds", type=int, default=30,
                        help="HTTP request timeout")
    args = parser.parse_args()

    # Set defaults for file paths, relative to output_dir, if not provided
    output_dir = Path(args.output_dir)
    args.raw_jsonl_file = output_dir / "raw_data.jsonl"
    args.downloaded_list = output_dir / "downloaded.txt"
    args.failed_list = output_dir / "failed_accessions.txt"
    return args

args = parse_args()

BV_BRC_URL = args.bv_brc_url
INPUT_ACC_FILE = args.accs
OUTPUT_DIR = Path(args.output_dir)
RAW_JSONL_FILE = Path(args.raw_jsonl_file)
DOWNLOADED_LIST = Path(args.downloaded_list)
FAILED_LIST = Path(args.failed_list)

# Concurrency controls
MAX_CONCURRENCY = args.max_concurrency
REQUEST_TIMEOUT_SECONDS = args.request_timeout_seconds


def load_accessions(path: str) -> List[str]:
    accessions: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            acc = line.strip()
            if acc:
                accessions.append(acc)
    return accessions


def load_downloaded(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    completed: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            acc = line.strip()
            if acc:
                completed.add(acc)
    return completed


async def append_line(path: Path, line: str) -> None:
    async with aiofiles.open(path, "a", encoding="utf-8") as f:
        await f.write(line + "\n")


async def fetch_once(session: aiohttp.ClientSession, accession: str) -> dict:
    url = BV_BRC_URL.format(accession=accession)
    async with session.get(url) as resp:
        resp.raise_for_status()
        text = await resp.text()
        # BV-BRC returns CSV by default for this endpoint; request JSON explicitly if supported
        # Many BV-BRC endpoints honor "Accept: application/json"; we pass that in session headers
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Fallback: wrap raw text as a field for later inspection
            return {"accession": accession, "raw": text}


async def download_accession(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    accession: str,
    pbar: tqdm,
) -> bool:
    async with semaphore:
        # Try once, retry once on network/HTTP errors
        for attempt in range(2):
            try:
                payload = await fetch_once(session, accession)
                # Persist as JSONL (one object per line)
                record = {"accession": accession, "data": payload}
                await append_line(RAW_JSONL_FILE, json.dumps(record))
                await append_line(DOWNLOADED_LIST, accession)
                pbar.update(1)
                return True
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == 0:
                    await asyncio.sleep(2)
                    continue
                await append_line(FAILED_LIST, f"{accession}\t{type(e).__name__}: {str(e)}")
                pbar.update(1)  # Update progress even for failures
                return False
            except Exception as e:  # Non-network errors
                await append_line(FAILED_LIST, f"{accession}\t{type(e).__name__}: {str(e)}")
                pbar.update(1)  # Update progress even for failures
                return False


async def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # Ensure tracking files exist
    for p in [RAW_JSONL_FILE, DOWNLOADED_LIST, FAILED_LIST]:
        if not p.exists():
            p.touch()

    all_accessions = load_accessions(INPUT_ACC_FILE)
    completed = load_downloaded(DOWNLOADED_LIST)
    todo = [acc for acc in all_accessions if acc not in completed]

    if not todo:
        print("All accessions already downloaded!")
        return

    print(f"Downloading {len(todo)} accessions...")

    timeout = ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    headers = {"Accept": "application/json"}

    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, ssl=False)
    
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as session:
        # Process in batches to avoid creating too many tasks at once
        batch_size = 100
        with tqdm(total=len(todo), desc="Downloading BV-BRC", unit="acc") as pbar:
            for i in range(0, len(todo), batch_size):
                batch = todo[i:i + batch_size]
                tasks = []
                
                for acc in batch:
                    task = asyncio.create_task(download_accession(session, semaphore, acc, pbar))
                    tasks.append(task)
                
                # Wait for this batch to complete
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                # Small pause between batches
                await asyncio.sleep(0.1)
    
    # Show summary
    downloaded_count = len(load_downloaded(DOWNLOADED_LIST))
    failed_count = 0
    if FAILED_LIST.exists():
        with open(FAILED_LIST, "r") as f:
            failed_count = len([line for line in f if line.strip()])
    
    print("Download completed!")
    print(f"Successfully downloaded: {downloaded_count}")
    print(f"Failed: {failed_count}")
    print(f"Total processed: {downloaded_count + failed_count}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


