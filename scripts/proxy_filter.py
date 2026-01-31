#!/usr/bin/env python3
import argparse
import sys
import time
import urllib.request
from typing import List


def fetch_proxy_list(url: str) -> List[str]:
    with urllib.request.urlopen(url, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    return lines


def test_proxy(proxy: str, test_url: str, timeout: int) -> bool:
    proxy_handler = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
    opener = urllib.request.build_opener(proxy_handler)
    try:
        with opener.open(test_url, timeout=timeout) as resp:
            body = resp.read(20000).decode("utf-8", errors="ignore")
            if resp.status != 200:
                return False
            lowered = body.lower()
            if "imperva" in lowered or "incapsula" in lowered or "<iframe" in lowered:
                return False
            return len(body) > 500
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and filter HTTPS proxies for BDO scraping.")
    parser.add_argument(
        "--source-url",
        default="https://raw.githubusercontent.com/fyvri/fresh-proxy-list/archive/storage/classic/https.txt",
        help="Proxy list URL",
    )
    parser.add_argument(
        "--test-url",
        default="https://www.naeu.playblackdesert.com/en-US/Adventure/Guild",
        help="URL to test proxies against",
    )
    parser.add_argument("--limit", type=int, default=50, help="Max proxies to test")
    parser.add_argument("--timeout", type=int, default=8, help="Per-proxy timeout in seconds")
    parser.add_argument("--sleep", type=float, default=0.2, help="Delay between tests in seconds")
    parser.add_argument("--out", default="working_proxies.txt", help="Output file")
    args = parser.parse_args()

    proxies = fetch_proxy_list(args.source_url)
    if not proxies:
        print("No proxies found.")
        sys.exit(1)

    tested = 0
    working = []
    for proxy in proxies:
        if tested >= args.limit:
            break
        tested += 1
        ok = test_proxy(proxy, args.test_url, args.timeout)
        print(f"{proxy} -> {'OK' if ok else 'FAIL'}")
        if ok:
            working.append(proxy)
        time.sleep(args.sleep)

    if working:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write("\n".join(working) + "\n")
        print(f"Saved {len(working)} working proxies to {args.out}")
        print("Use them like: -proxy \"" + " ".join(working) + "\"")
    else:
        print("No working proxies found.")
        sys.exit(2)


if __name__ == "__main__":
    main()
