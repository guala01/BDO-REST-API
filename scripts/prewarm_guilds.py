#!/usr/bin/env python3
import argparse
import json
import re
import time
import urllib.parse
import urllib.request
from typing import List, Optional

GUILDS = [
    "Mythic",
    "RAW",
    "Control",
    "Insecure",
    "Ace",
    "Demise",
    "Fall",
    "Yeah",
    "Orca",
    "Armbrows",
    "Combat",
    "Cold",
    "Ruin",
    "Act",
    "Dominate",
    "BLNK",
    "RAT",
    "Deported",
    "Necrotic",
    "Bliss",
    "Ecchi_Squad",
    "Carrots",
    "BugCat",
    "Skill_Issue",
    "ManshaSlayers",
]


def http_get_json(url: str, timeout: int = 30) -> (int, Optional[dict], str):
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            if not body:
                return resp.status, None, ""
            return resp.status, json.loads(body), ""
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8")
            return e.code, json.loads(body) if body else None, body
        except Exception:
            return e.code, None, ""
    except Exception:
        return 0, None, ""


def wait_for_200(url: str, label: str, max_attempts: int, delay: float) -> Optional[dict]:
    last_status = 0
    for attempt in range(1, max_attempts + 1):
        status, data, raw = http_get_json(url)
        last_status = status
        if status == 200:
            return data
        if status == 202:
            print(f"[{label}] 202 (in progress) attempt {attempt}/{max_attempts}")
            time.sleep(delay)
            continue
        if status == 429:
            print(f"[{label}] 429 (rate limited) attempt {attempt}/{max_attempts}")
            time.sleep(max(5.0, delay * 2))
            continue
        if status in (500, 503) or status == 0:
            code = status if status != 0 else "no connection"
            print(f"[{label}] {code} (server/backoff) attempt {attempt}/{max_attempts}")
            time.sleep(max(10.0, delay * 5))
            continue
        msg = raw.strip()
        extra = f" body={msg}" if msg else ""
        print(f"[{label}] unexpected status {status}{extra}; retrying ({attempt}/{max_attempts})")
        time.sleep(delay)
    hint = " (connection failed?)" if last_status == 0 else ""
    print(f"[{label}] failed after {max_attempts} attempts{hint}")
    return None


def build_url(base_url: str, path: str, params: dict) -> str:
    return f"{base_url.rstrip('/')}{path}?{urllib.parse.urlencode(params)}"


def collect_profile_targets(guild_json: dict) -> List[str]:
    members = guild_json.get("members") or []
    profile_targets = []
    for member in members:
        pt = member.get("profileTarget")
        if pt:
            profile_targets.append(pt)
    return profile_targets


def collect_family_names(guild_json: dict) -> List[str]:
    members = guild_json.get("members") or []
    family_names = []
    for member in members:
        fn = member.get("familyName")
        if fn:
            family_names.append(fn)
    return family_names


def is_valid_search_query(query: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_]{3,16}", query))


def main() -> None:
    parser = argparse.ArgumentParser(description="Prewarm BDO REST API cache for guild members.")
    parser.add_argument("--base-url", default="http://localhost:8001", help="API base URL")
    parser.add_argument("--region", default="EU", help="Region (default: EU)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between retries in seconds")
    parser.add_argument("--max-attempts", type=int, default=20, help="Max retry attempts for 202 responses")
    parser.add_argument("--throttle", type=float, default=0.2, help="Delay between requests in seconds")
    parser.add_argument("--warm-search", action="store_true", help="Warm /v1/adventurer/search cache for family names")
    args = parser.parse_args()

    all_profile_targets = []
    all_family_names = []

    for guild in GUILDS:
        guild_url = build_url(
            args.base_url,
            "/v1/guild",
            {"guildName": guild, "region": args.region},
        )
        print(f"Fetching guild: {guild}")
        guild_json = wait_for_200(
            guild_url,
            f"guild:{guild}",
            args.max_attempts,
            args.delay,
        )
        if not guild_json:
            continue
        profile_targets = collect_profile_targets(guild_json)
        family_names = collect_family_names(guild_json)
        print(f"  members: {len(profile_targets)}")
        all_profile_targets.extend(profile_targets)
        all_family_names.extend(family_names)
        time.sleep(args.throttle)

    unique_profile_targets = list(dict.fromkeys(all_profile_targets))
    print(f"Total unique profiles: {len(unique_profile_targets)}")

    failed_profile_targets = []

    for idx, profile_target in enumerate(unique_profile_targets, 1):
        adv_url = build_url(
            args.base_url,
            "/v1/adventurer",
            {"profileTarget": profile_target, "region": args.region},
        )
        label = f"adventurer:{idx}/{len(unique_profile_targets)}"
        print(f"Fetching {label}")
        result = wait_for_200(adv_url, label, args.max_attempts, args.delay)
        if result is None:
            failed_profile_targets.append(profile_target)
        time.sleep(args.throttle)

    if failed_profile_targets:
        with open("failed_profiles.txt", "w", encoding="utf-8") as fh:
            for pt in failed_profile_targets:
                fh.write(pt + "\n")
        print(f"Failed profiles: {len(failed_profile_targets)} (saved to failed_profiles.txt)")

    if args.warm_search:
        unique_family_names = list(dict.fromkeys(all_family_names))
        print(f"Total unique family names: {len(unique_family_names)}")

        failed_searches = []
        for idx, family_name in enumerate(unique_family_names, 1):
            if not is_valid_search_query(family_name):
                continue
            search_url = build_url(
                args.base_url,
                "/v1/adventurer/search",
                {"query": family_name, "searchType": "familyName", "region": args.region},
            )
            label = f"search:{idx}/{len(unique_family_names)}"
            print(f"Fetching {label}")
            result = wait_for_200(search_url, label, args.max_attempts, args.delay)
            if result is None:
                failed_searches.append(family_name)
            time.sleep(args.throttle)

        if failed_searches:
            with open("failed_searches.txt", "w", encoding="utf-8") as fh:
                for fn in failed_searches:
                    fh.write(fn + "\n")
            print(f"Failed searches: {len(failed_searches)} (saved to failed_searches.txt)")


if __name__ == "__main__":
    main()
