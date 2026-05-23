#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import threading
import time
from urllib.parse import urlencode
from urllib.request import urlopen

def _get_json(url: str):
    with urlopen(url) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)

def _write_csv(path: str, data: dict):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["key", "value"])
        for k, v in data.items():
            writer.writerow([k, v])

def view_all(base_url: str) -> str:
    ts = int(time.time())
    out_file = f"{ts}.csv"
    data = _get_json(f"{base_url}/all")
    if not isinstance(data, dict):
        raise ValueError("Expected JSON object from /all endpoint")
    _write_csv(out_file, data)
    return out_file

def view_key(base_url: str, key: str) -> None:
    query = urlencode({"key": key})
    data = _get_json(f"{base_url}/get?{query}")
    if not isinstance(data, dict):
        raise ValueError("Expected JSON object from /get endpoint")
    print(f"Key:   {data.get('key', '')}")
    print(f"Value: {data.get('value', '')}")

def _perf_worker(base_url: str, ts: int, thread_num: int):
    out_file = f"{ts}_thread_{thread_num}.csv"
    data = _get_json(f"{base_url}/all")
    if not isinstance(data, dict):
        raise ValueError("Expected JSON object from /all endpoint")
    _write_csv(out_file, data)

def perf(base_url: str, clients: int):
    ts = int(time.time())
    threads = []
    for i in range(1, clients + 1):
        t = threading.Thread(target=_perf_worker, args=(base_url, ts, i), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print(f"All {clients} threads completed.")


def main():
    parser = argparse.ArgumentParser(description="BitCask client")
    parser.add_argument("--view-all", action="store_true", help="Dump all keys to CSV")
    parser.add_argument("--view", action="store_true", help="View single key")
    parser.add_argument("--key", help="Key to fetch")
    parser.add_argument("--perf", action="store_true", help="Run concurrent clients")
    parser.add_argument("--clients", type=int, default=10, help="Number of concurrent clients")

    args = parser.parse_args()
    base_url = os.environ.get("BASE_URL", "http://localhost:8080/bitcask")

    if args.view_all:
        out_file = view_all(base_url)
        print(f"Saved to {out_file}")
        return

    if args.view:
        if not args.key:
            print("Error: --key is required with --view", file=sys.stderr)
            sys.exit(1)
        view_key(base_url, args.key)
        return

    if args.perf:
        perf(base_url, args.clients)
        return

    parser.print_help()


if __name__ == "__main__":
    main()

