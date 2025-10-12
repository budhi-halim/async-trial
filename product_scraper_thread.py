#!/usr/bin/env python3
"""
Product scraper (ThreadPoolExecutor + requests): collects catalog, fetches marketing prices,
writes products.json and last_updated.txt
"""

import os
import sys
import json
import requests
import itertools
import string
from datetime import datetime
from typing import List

from concurrent.futures import ThreadPoolExecutor, as_completed

# Endpoints
LOGIN_URL = "http://apps.islandsunindonesia.com:81/islandsun/index.php/login"
PRODUCT_URL = "http://apps.islandsunindonesia.com:81/islandsun/samplerequest/getAjaxproduct/null"
PRICE_URL = "http://apps.islandsunindonesia.com:81/islandsun/samplerequest/getMarketingPrice"

# Maximum worker threads for HTTP requests
MAX_WORKERS: int = 10  # tweak as needed


def _parse_code_and_name(text: str) -> tuple[str, str]:
    """Parse product text into (code, name)."""
    parts = [p.strip() for p in text.split("/") if p.strip()]
    if not parts:
        return "", text.strip()
    code = parts[0]
    name = " / ".join(parts[1:]) if len(parts) > 1 else text.strip()
    return code, name


def login() -> requests.Session:
    """
    Login using requests.Session and return an authenticated session.
    Exits with same codes/messages as original script on failure.
    """
    username = os.getenv("ISLANDSUN_USERNAME")
    password = os.getenv("ISLANDSUN_PASSWORD")
    if not username or not password:
        print("ERROR: Environment variables ISLANDSUN_USERNAME and ISLANDSUN_PASSWORD must be provided.", file=sys.stderr)
        sys.exit(2)

    s = requests.Session()
    try:
        s.post(LOGIN_URL, data={"user": username}, timeout=15)
        resp = s.post(LOGIN_URL, data={"user": username, "password": password}, timeout=15)
        resp.raise_for_status()
        print("[LOGIN] Logged in successfully.")
        return s
    except Exception as e:
        print(f"[LOGIN] Failed: {e}", file=sys.stderr)
        sys.exit(3)


def _fetch_products_for_term_blocking(session: requests.Session, term: str) -> List[dict]:
    """Blocking fetch wrapper used by the thread pool."""
    try:
        resp = session.post(PRODUCT_URL, data={"param": term}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def collect_full_catalog(session: requests.Session) -> List[dict]:
    """
    Collect full catalog using a ThreadPoolExecutor to parallelize term lookups.
    Preserves original deduplication logic and status messages.
    """
    alphabet = "0123456789" + string.ascii_lowercase
    terms = [''.join(p) for p in itertools.product(alphabet, repeat=2)]
    catalog: dict[tuple[str, str], dict] = {}
    total_terms = len(terms)

    print("[CATALOG] Collecting products...")
    # Use thread pool to parallelize blocking HTTP calls
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_term = {executor.submit(_fetch_products_for_term_blocking, session, term): term for term in terms}
        completed = 0
        for future in as_completed(future_to_term):
            completed += 1
            items = future.result()
            for item in items:
                key = (str(item.get("id", "")).strip(), str(item.get("text", "")).strip())
                if key not in catalog and key[0] and key[1]:
                    catalog[key] = {"id": key[0], "text": key[1]}
            print(f"\r[CATALOG] Term {completed}/{total_terms} | Unique={len(catalog)}", end="")
    print()
    print(f"[CATALOG] Finished. Terms={total_terms}, Unique entries={len(catalog)}")
    return list(catalog.values())


def _fetch_price_blocking(session: requests.Session, pid: str) -> str:
    """Blocking price fetch wrapper used by the thread pool."""
    try:
        resp = session.post(PRICE_URL, data={"id": str(pid)}, timeout=15)
        if resp.status_code == 200:
            price = resp.text.strip()
            return price if price else ""
    except Exception:
        return ""
    return ""


def enrich_with_prices(session: requests.Session, catalog: List[dict]) -> List[dict]:
    """
    Enrich catalog entries with prices. Uses in-memory cache id_to_price to avoid duplicate requests.
    Uses ThreadPoolExecutor to parallelize price queries when beneficial.
    """
    id_to_price: dict[str, str] = {}
    enriched: List[dict] = []
    total = len(catalog)
    print("[PRICES] Fetching marketing prices...")

    # Build unique list of ids to fetch
    unique_ids = []
    for product in catalog:
        pid = product["id"]
        if pid not in id_to_price:
            id_to_price[pid] = ""  # placeholder
            unique_ids.append(pid)

    # Parallel fetch prices for unique ids
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_pid = {executor.submit(_fetch_price_blocking, session, pid): pid for pid in unique_ids}
        for future in as_completed(future_to_pid):
            pid = future_to_pid[future]
            id_to_price[pid] = future.result() or ""

    # Build enriched list in original order
    for idx, product in enumerate(catalog, start=1):
        pid = product["id"]
        text = product["text"]
        price = id_to_price.get(pid, "")

        code, name = _parse_code_and_name(text)

        enriched.append({
            "product_name": name,
            "product_code": code,
            "marketing_price": price,
        })

        print(f"\r[PRICES] {idx}/{total} products processed", end="")
    print()
    return enriched


def save_json(products: List[dict]) -> List[dict]:
    """Save products.json with same formatting and return sorted products."""
    products_sorted = sorted(products, key=lambda x: (x["product_name"], x["product_code"]))
    os.makedirs("data", exist_ok=True)
    with open("data/products.json", "w", encoding="utf-8") as f:
        json.dump(products_sorted, f, ensure_ascii=False, indent=2)
    print("[DONE] Saved products.json")
    return products_sorted


def write_last_updated_file(date_obj: datetime.date) -> None:
    """Write last_updated.txt with ISO date."""
    iso_date = date_obj.strftime("%Y-%m-%d")
    os.makedirs("data", exist_ok=True)
    with open("data/last_updated.txt", "w", encoding="utf-8") as f:
        f.write(iso_date)
    print(f"[DONE] Saved last_updated.txt ({iso_date})")


def main() -> None:
    """Main procedural flow identical to original script."""
    session = login()
    catalog = collect_full_catalog(session)
    products = enrich_with_prices(session, catalog)
    save_json(products)
    write_last_updated_file(datetime.now().date())
    print("[ALL DONE]")


if __name__ == "__main__":
    main()