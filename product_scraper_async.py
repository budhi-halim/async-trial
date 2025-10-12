#!/usr/bin/env python3
"""
Product scraper (asyncio + httpx): collects catalog, fetches marketing prices,
writes products.json and last_updated.txt
"""

import os
import sys
import json
import itertools
import string
from datetime import datetime
from typing import List

import asyncio
import httpx

# Endpoints
LOGIN_URL = "http://apps.islandsunindonesia.com:81/islandsun/index.php/login"
PRODUCT_URL = "http://apps.islandsunindonesia.com:81/islandsun/samplerequest/getAjaxproduct/null"
PRICE_URL = "http://apps.islandsunindonesia.com:81/islandsun/samplerequest/getMarketingPrice"

# Concurrency limit for outgoing HTTP requests to avoid overloading the server
MAX_CONCURRENCY: int = 10  # tweak as needed


def _parse_code_and_name(text: str) -> tuple[str, str]:
    """Parse product text into (code, name)."""
    parts = [p.strip() for p in text.split("/") if p.strip()]
    if not parts:
        return "", text.strip()
    code = parts[0]
    name = " / ".join(parts[1:]) if len(parts) > 1 else text.strip()
    return code, name


async def login() -> httpx.AsyncClient:
    """
    Login using httpx AsyncClient and return an authenticated client.
    Exits with same codes/messages as original script on failure.
    """
    username = os.getenv("ISLANDSUN_USERNAME")
    password = os.getenv("ISLANDSUN_PASSWORD")
    if not username or not password:
        print("ERROR: Environment variables ISLANDSUN_USERNAME and ISLANDSUN_PASSWORD must be provided.", file=sys.stderr)
        sys.exit(2)

    client = httpx.AsyncClient(timeout=15.0)
    try:
        # First post (mirrors original two-step post)
        await client.post(LOGIN_URL, data={"user": username})
        resp = await client.post(LOGIN_URL, data={"user": username, "password": password})
        resp.raise_for_status()
        print("[LOGIN] Logged in successfully.")
        return client
    except Exception as e:
        await client.aclose()
        print(f"[LOGIN] Failed: {e}", file=sys.stderr)
        sys.exit(3)


async def fetch_products_for_term(client: httpx.AsyncClient, term: str, semaphore: asyncio.Semaphore) -> List[dict]:
    """Fetch products for a search term (async)."""
    async with semaphore:
        try:
            resp = await client.post(PRODUCT_URL, data={"param": term})
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception:
            return []


async def collect_full_catalog(client: httpx.AsyncClient) -> List[dict]:
    """
    Collect full catalog by iterating search terms.
    Preserves original deduplication logic and status messages.
    """
    alphabet = "0123456789" + string.ascii_lowercase
    terms = [''.join(p) for p in itertools.product(alphabet, repeat=2)]
    catalog: dict[tuple[str, str], dict] = {}
    total_terms = len(terms)

    print("[CATALOG] Collecting products...")
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    # Launch tasks in batches to avoid creating 1,296 tasks simultaneously
    # but still allow concurrency controlled by semaphore.
    tasks = [fetch_products_for_term(client, term, semaphore) for term in terms]
    for idx, coro in enumerate(asyncio.as_completed(tasks), start=1):
        items = await coro
        for item in items:
            key = (str(item.get("id", "")).strip(), str(item.get("text", "")).strip())
            if key not in catalog and key[0] and key[1]:
                catalog[key] = {"id": key[0], "text": key[1]}
        # Compute completed count based on idx; show progress close to original style.
        print(f"\r[CATALOG] Term {idx}/{total_terms} | Unique={len(catalog)}", end="")
    print()
    print(f"[CATALOG] Finished. Terms={total_terms}, Unique entries={len(catalog)}")
    return list(catalog.values())


async def fetch_price(client: httpx.AsyncClient, pid: str, semaphore: asyncio.Semaphore) -> str:
    """Fetch marketing price for a product id (async)."""
    async with semaphore:
        try:
            resp = await client.post(PRICE_URL, data={"id": str(pid)})
            if resp.status_code == 200:
                price = resp.text.strip()
                return price if price else ""
        except Exception:
            return ""
    return ""


async def enrich_with_prices(client: httpx.AsyncClient, catalog: List[dict]) -> List[dict]:
    """
    Enrich catalog entries with prices. Uses in-memory cache id_to_price to avoid duplicate requests.
    Maintains output format and print messages identical to original.
    """
    id_to_price: dict[str, str] = {}
    enriched: List[dict] = []
    total = len(catalog)
    print("[PRICES] Fetching marketing prices...")
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    # Create tasks for price fetching, but map back to product order to keep logs similar
    fetch_tasks = []
    for product in catalog:
        pid = product["id"]
        if pid not in id_to_price:
            # schedule a fetch task
            fetch_tasks.append((pid, asyncio.create_task(fetch_price(client, pid, semaphore))))

    # Await all fetch tasks and populate cache
    for i, (pid, task) in enumerate(fetch_tasks, start=1):
        id_to_price[pid] = await task

    # Now build enriched list in the original single-pass order
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


async def async_main() -> None:
    """Top-level async main that mirrors the original procedural flow."""
    client = await login()
    try:
        catalog = await collect_full_catalog(client)
        products = await enrich_with_prices(client, catalog)
        save_json(products)
        write_last_updated_file(datetime.now().date())
        print("[ALL DONE]")
    finally:
        await client.aclose()


def main() -> None:
    """Synchronous entrypoint that runs the async main."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()