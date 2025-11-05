#!/usr/bin/env python3

"""
Scrape YC company pages into a CSV.

The script loads URLs from an input CSV with a `YC Link` column, fetches each page,
parses the embedded `__NEXT_DATA__` JSON (falling back to HTML heuristics), and then
writes the requested data columns to the output CSV. It supports async concurrency,
polite rate limiting, retries with backoff, and resumable checkpoints.
"""

import asyncio
import csv
import json
import random
import re
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from bs4 import BeautifulSoup

# ------------------------------
# Helpers
# ------------------------------


def deep_find_all_keys(obj: Any, key: str) -> List[Any]:
    """Recursively find all values for a given key in a nested dict/list."""
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                found.append(v)
            found.extend(deep_find_all_keys(v, key))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(deep_find_all_keys(item, key))
    return found


def deep_find_first(obj: Any, keys: List[str]) -> Optional[Any]:
    """Return the first value found for any of the keys."""
    for key in keys:
        vals = deep_find_all_keys(obj, key)
        if vals:
            return vals[0]
    return None


def norm_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(str(x).strip())
    except Exception:
        # Try to extract digits
        m = re.search(r"\d+", str(x))
        return int(m.group()) if m else None


def as_semicolon(values: List[str]) -> str:
    cleaned = [v.strip() for v in values if v and str(v).strip()]
    return "; ".join(dict.fromkeys(cleaned))  # preserve order, dedupe


def parse_html_fallback(html: str) -> Dict[str, Any]:
    """Loose HTML parser when __NEXT_DATA__ is not usable."""
    soup = BeautifulSoup(html, "html.parser")
    out: Dict[str, Any] = {}

    # Try to parse right-rail label-value pairs (dt/dd or text-based)
    def get_value_by_label(label_texts: List[str]) -> Optional[str]:
        for label in label_texts:
            # Match visible text like "Primary Partner:"
            el = soup.find(string=re.compile(rf"^\s*{re.escape(label)}\s*:?$", re.I))
            if el and el.parent:
                # sibling value
                sib = el.find_next()
                if sib:
                    return sib.get_text(strip=True)
        return None

    out["primary_partner"] = get_value_by_label(["Primary Partner"])
    out["status"] = get_value_by_label(["Status"])
    out["location"] = get_value_by_label(["Location"])
    founded = get_value_by_label(["Founded"])
    out["founded_year"] = norm_int(founded)
    team_size = get_value_by_label(["Team Size", "Team size"])
    out["team_size"] = norm_int(team_size)
    out["batch"] = get_value_by_label(["Batch"])

    # Website link (look for external link icon or direct anchor with the domain)
    # Prefer obvious homepage-looking hrefs that aren't YC-internal.
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "ycombinator.com" not in href:
            out["website"] = href
            break

    # Founders: sometimes listed on the page; heuristics
    founders: List[str] = []
    linkedin_urls: List[str] = []
    # Look for LinkedIn icons/links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "linkedin.com" in href:
            linkedin_urls.append(href)
    # Names often appear near "Founder" strings; grab nearby strong/em tags
    for tag in soup.find_all(string=re.compile(r"Founder", re.I)):
        # grab previous/next siblings for names
        for sib in [tag.parent.previous_sibling, tag.parent.next_sibling]:
            if hasattr(sib, "get_text"):
                txt = sib.get_text(" ", strip=True)
                if (
                    txt
                    and len(txt.split()) <= 5
                    and not re.search(r"Founder", txt, re.I)
                ):
                    founders.append(txt)
    out["founders"] = founders or None
    out["founders_linkedin"] = linkedin_urls or None
    return out


def extract_from_next_data(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    out: Dict[str, Any] = {}

    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return out

    try:
        data = json.loads(script.string)
    except Exception:
        return out

    # Heuristic keys (YC has changed shapes across time; we try several spellings)
    company_obj = deep_find_first(
        data, ["company", "startup", "startupData", "pageData"]
    )
    root = company_obj if isinstance(company_obj, (dict, list)) else data

    # Website
    website = deep_find_first(root, ["website", "websiteUrl", "url"])
    if isinstance(website, list):
        website = website[0]
    if isinstance(website, dict) and "url" in website:
        website = website["url"]
    if website and isinstance(website, str) and "ycombinator.com" not in website:
        out["website"] = website

    # Status
    status = deep_find_first(root, ["status", "companyStatus"])
    if isinstance(status, dict):
        status = status.get("label") or status.get("text")
    out["status"] = status

    # Primary Partner
    pp = deep_find_first(
        root, ["primaryPartner", "primary_partner", "primary_partner_name"]
    )
    if isinstance(pp, dict):
        pp = pp.get("name") or pp.get("full_name")
    out["primary_partner"] = pp

    # Founded Year
    founded = deep_find_first(root, ["founded", "foundedYear", "founded_year"])
    out["founded_year"] = norm_int(founded)

    # Team Size
    ts = deep_find_first(root, ["teamSize", "team_size", "teamsize"])
    out["team_size"] = norm_int(ts)

    # Batch
    batch = deep_find_first(root, ["batch", "ycBatch", "yc_batch"])
    if isinstance(batch, dict):
        batch = batch.get("name") or batch.get("label")
    out["batch"] = batch

    # Location
    loc = deep_find_first(root, ["location", "hqLocation", "city"])
    if isinstance(loc, dict):
        loc = loc.get("name") or loc.get("displayName")
    out["location"] = loc

    # Founders
    founders_list = deep_find_first(root, ["founders", "team", "founderData"])
    active_names: List[str] = []
    linkedin_urls: List[str] = []
    try:
        if isinstance(founders_list, list):
            for f in founders_list:
                if not isinstance(f, dict):
                    continue
                name = f.get("name") or f.get("full_name") or f.get("display_name")
                is_active = f.get("is_active")
                if is_active is None:
                    # if not provided, include all
                    is_active = True
                if name and is_active:
                    active_names.append(name)
                # LinkedIn URL(s)
                li = (
                    f.get("linkedin_url")
                    or f.get("linkedin")
                    or f.get("linkedinUrl")
                    or f.get("social", {}).get("linkedin")
                )
                if li:
                    linkedin_urls.append(li)
    except Exception:
        pass

    if active_names:
        out["active_founders"] = active_names
    if linkedin_urls:
        out["founders_linkedin"] = linkedin_urls

    return out


def merge_preferring_left(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if k not in out or out[k] in (None, "", [], {}):
            out[k] = v
    return out


# ------------------------------
# Scraper
# ------------------------------


async def fetch(client: httpx.AsyncClient, url: str) -> Optional[str]:
    try:
        r = await client.get(url, follow_redirects=True, timeout=30.0)
        if r.status_code == 200:
            return r.text
        return None
    except Exception:
        return None


async def scrape_one(
    url: str, client: httpx.AsyncClient, rpm: int, retries: int = 4
) -> Dict[str, Any]:
    # rate limit: rpm requests per minute -> delay between requests ~ 60/rpm
    delay = 60.0 / max(1, rpm)
    attempt = 0
    while True:
        attempt += 1
        html = await fetch(client, url)
        if html:
            # Parse: first try Next.js data, then HTML fallback
            out = extract_from_next_data(html)
            if not out:
                out = {}
            fb = parse_html_fallback(html)
            out = merge_preferring_left(out, fb)

            # Normalize lists to strings
            if "active_founders" in out and isinstance(out["active_founders"], list):
                out["Active Founders"] = as_semicolon(out["active_founders"])
            if "founders_linkedin" in out and isinstance(
                out["founders_linkedin"], list
            ):
                out["Founders LinkedIn Link"] = as_semicolon(out["founders_linkedin"])

            # Map to output keys
            mapped = {
                "YC Link": url,
                "Website": out.get("website"),
                "Status": out.get("status"),
                "Primary Partner": out.get("primary_partner"),
                "Founded Year": out.get("founded_year"),
                "Team Size": out.get("team_size"),
                "Batch": out.get("batch"),
                "Location": out.get("location"),
                "Active Founders": out.get("Active Founders"),
                "Founders LinkedIn Link": out.get("Founders LinkedIn Link"),
            }
            return mapped

        if attempt > retries:
            return {"YC Link": url}

        # backoff with jitter
        sleep_s = delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
        await asyncio.sleep(sleep_s)


async def worker(
    name: int,
    queue: asyncio.Queue,
    client: httpx.AsyncClient,
    rpm: int,
    results: Dict[str, Dict[str, Any]],
    total: int,
):
    delay = 60.0 / max(1, rpm)
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        idx, url = item
        try:
            ordinal = idx + 1
            print(
                f"[Worker {name}] Starting {ordinal}/{total}: {url}",
                flush=True,
            )
            data = await scrape_one(url, client, rpm)
            results[url] = data
            if any(k != "YC Link" and data.get(k) for k in data):
                print(
                    f"[Worker {name}] Success {ordinal}/{total}: {url}",
                    flush=True,
                )
            else:
                print(
                    f"[Worker {name}] No data after retries {ordinal}/{total}: {url}",
                    flush=True,
                )
        except Exception as exc:
            print(
                f"[Worker {name}] Error on {idx + 1}/{total}: {url} -> {exc}",
                flush=True,
            )
        finally:
            # polite pacing between individual requests by this worker
            await asyncio.sleep(delay + random.uniform(0, 0.25))
            queue.task_done()


def load_links(path: Path) -> List[str]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "YC Link" not in reader.fieldnames:
            raise ValueError('Input CSV must have a "YC Link" column')
        return [row["YC Link"] for row in reader if row.get("YC Link")]


def write_csv(path: Path, rows: List[Dict[str, Any]]):
    # Fixed column order to match the user's sheet
    fieldnames = [
        "YC Link",
        "Active Founders",
        "Founders LinkedIn Link",
        "Status",
        "Website",
        "Primary Partner",
        "Founded Year",
        "Team Size",
        "Batch",
        "Location",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in fieldnames})


def load_checkpoint(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_checkpoint(path: Path, data: Dict[str, Dict[str, Any]]):
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


async def main():
    parser = argparse.ArgumentParser(description="Scrape YC company pages into CSV.")
    parser.add_argument(
        "--input", required=True, help="CSV file containing 'YC Link' column"
    )
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="Number of concurrent workers (default 8)",
    )
    parser.add_argument(
        "--rpm",
        type=int,
        default=120,
        help="Max requests per minute per process (default 120)",
    )
    parser.add_argument(
        "--resume", action="store_true", help="Resume from checkpoint if present"
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    ckpt_path = output_path.with_suffix(output_path.suffix + ".ckpt.json")

    links = load_links(input_path)
    print(f"Loaded {len(links)} links.")

    results: Dict[str, Dict[str, Any]] = {}
    if args.resume:
        results = load_checkpoint(ckpt_path)
        if results:
            print(f"Resuming: {len(results)} rows already scraped.")

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Cache-Control": "no-cache",
    }

    timeout = httpx.Timeout(30.0, connect=30.0, read=30.0)
    limits = httpx.Limits(
        max_connections=args.concurrency * 2,
        max_keepalive_connections=args.concurrency * 2,
    )

    queue: asyncio.Queue = asyncio.Queue()
    total = len(links)
    for idx, url in enumerate(links):
        if url in results:
            continue
        queue.put_nowait((idx, url))

    async with httpx.AsyncClient(
        headers=headers, timeout=timeout, limits=limits
    ) as client:
        workers = [
            asyncio.create_task(worker(i, queue, client, args.rpm, results, total))
            for i in range(args.concurrency)
        ]

        # Periodic checkpointing
        async def checkpoint_loop():
            while any(not w.done() for w in workers):
                await asyncio.sleep(10)
                if results:
                    save_checkpoint(ckpt_path, results)

        cp_task = asyncio.create_task(checkpoint_loop())

        await queue.join()
        for _ in workers:
            queue.put_nowait(None)
        await asyncio.gather(*workers)
        cp_task.cancel()
        try:
            await cp_task
        except asyncio.CancelledError:
            pass

    # Merge order according to input
    rows = [results.get(url, {"YC Link": url}) for url in links]
    write_csv(output_path, rows)
    save_checkpoint(ckpt_path, results)  # final

    print(f"Done. Wrote {len(rows)} rows to {output_path}")
    print(f"Checkpoint at {ckpt_path} (you can delete it if not needed).")


if __name__ == "__main__":
    asyncio.run(main())
