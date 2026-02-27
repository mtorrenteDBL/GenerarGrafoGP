#!/usr/bin/env python3
"""
fetch_nifi_flows.py

Connects via SSH to each NiFi cluster defined in clusters.csv,
retrieves flow.json (preferred) or flow.xml from the configured path,
and saves it flat under flows/. Compressed files are decompressed automatically.

SSH credentials are loaded from a .env file with the following keys:
    SSH_USER=your_username
    SSH_PASSWORD=your_password          # optional if using key auth
    SSH_KEY_PATH=/path/to/private_key   # optional if using password auth
    SSH_PORT=22                         # optional, defaults to 22
"""

import csv
import gzip
import os
import shutil
import stat
import logging
from pathlib import Path
from dotenv import load_dotenv
import paramiko
from log_setup import setup_logging

_SCRIPT_DIR = Path(__file__).resolve().parent
CSV_PATH = _SCRIPT_DIR / "info_clusters.csv"
OUTPUT_DIR = _SCRIPT_DIR / "flows"

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_env() -> dict:
    load_dotenv()
    cfg = {
        "username": os.getenv("SSH_USER"),
        "port": int(os.getenv("SSH_PORT", "22")),
        "password": os.getenv("SSH_PASSWORD"),
        "key_path": os.getenv("SSH_KEY_PATH"),
    }
    if not cfg["username"]:
        raise ValueError("SSH_USER is not set in the .env file.")
    if not cfg["password"] and not cfg["key_path"]:
        raise ValueError("Set SSH_PASSWORD or SSH_KEY_PATH in the .env file.")
    return cfg


def parse_clusters() -> list[dict]:
    """Parse clusters CSV file and return a list of cluster dicts."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Clusters CSV not found: {CSV_PATH.resolve()}")
    clusters = []
    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ips = [ip.strip() for ip in row["IP"].splitlines() if ip.strip()]
            supports_json = "JSON" in row["tipo de flow"].upper()
            clusters.append({
                "name": row["Nombre"].strip(),
                "ips": ips,
                "path": row["path al flow"].strip().rstrip("/"),
                "supports_json": supports_json,
            })
    return clusters


def build_ssh_client(host: str, cfg: dict) -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_kwargs = dict(
        hostname=host,
        port=cfg["port"],
        username=cfg["username"],
        timeout=15,
    )
    if cfg["key_path"]:
        connect_kwargs["key_filename"] = cfg["key_path"]
    if cfg["password"]:
        connect_kwargs["password"] = cfg["password"]

    client.connect(**connect_kwargs)
    return client


def remote_file_exists(sftp: paramiko.SFTPClient, path: str) -> bool:
    try:
        mode = sftp.stat(path).st_mode
        return stat.S_ISREG(mode)
    except FileNotFoundError:
        return False


def decompress_if_needed(path: Path) -> Path:
    """If path is a .gz file, decompress it in place and return the new path."""
    if path.suffix != ".gz":
        return path
    decompressed = path.with_suffix("")  # e.g. flow.xml.gz -> flow.xml
    log.info("  ↳ Decompressing %s …", path.name)
    with gzip.open(path, "rb") as src, decompressed.open("wb") as dst:
        shutil.copyfileobj(src, dst)          # stream in chunks – no 200 MB malloc
    size = decompressed.stat().st_size
    if size == 0:
        decompressed.unlink(missing_ok=True)
        raise RuntimeError(f"Decompressed file {decompressed.name} is empty (0 bytes)")
    path.unlink()
    log.info("  ✔ Decompressed to %s (%d bytes)", decompressed.name, size)
    return decompressed


def fetch_flow(cluster: dict, cfg: dict) -> bool:
    """
    Try each IP in turn until one connects successfully.
    On a successful connection, look for flow files in strict priority order:
      - JSON-capable clusters: flow.json > flow.json.gz > flow.xml > flow.xml.gz
      - XML-only clusters:     flow.xml > flow.xml.gz
    Files saved flat into OUTPUT_DIR/<cluster_name>.<ext>.
    Returns True on success.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = cluster["name"].replace(" ", "_")

    if cluster["supports_json"]:
        candidates = [
            f"{cluster['path']}/flow.json",
            f"{cluster['path']}/flow.json.gz",
            f"{cluster['path']}/flow.xml",
            f"{cluster['path']}/flow.xml.gz",
        ]
    else:
        candidates = [
            f"{cluster['path']}/flow.xml",
            f"{cluster['path']}/flow.xml.gz",
        ]

    for ip in cluster["ips"]:
        log.info("[%s] Trying %s …", cluster["name"], ip)
        client = None
        try:
            client = build_ssh_client(ip, cfg)
            sftp = client.open_sftp()

            for remote_path in candidates:
                if not remote_file_exists(sftp, remote_path):
                    log.debug("  – %s not found", remote_path)
                    continue

                remote_filename = remote_path.split("/")[-1]
                # Derive the local stem: flow.json.gz -> safe_name.json.gz, etc.
                suffix = remote_filename[len("flow"):]  # e.g. ".json" or ".xml.gz"
                local_path = OUTPUT_DIR / f"{safe_name}{suffix}"

                log.info("  ✔ Found %s — downloading …", remote_path)
                sftp.get(remote_path, str(local_path))

                dl_size = local_path.stat().st_size
                if dl_size == 0:
                    log.warning("  ✘ Downloaded file %s is empty (0 bytes) — skipping", local_path.name)
                    local_path.unlink(missing_ok=True)
                    continue

                local_path = decompress_if_needed(local_path)
                final_size = local_path.stat().st_size
                if final_size == 0:
                    log.warning("  ✘ Final file %s is empty (0 bytes) — skipping", local_path.name)
                    local_path.unlink(missing_ok=True)
                    continue

                log.info("  ✔ Saved %s (%d bytes)", local_path, final_size)
                return True

            log.warning("  ✘ No flow file found at %s", cluster["path"])
            return False

        except Exception as exc:
            log.warning("  ✘ Could not connect/fetch from %s: %s", ip, exc)
        finally:
            if client:
                client.close()

    log.error("[%s] All IPs exhausted — could not retrieve flow.", cluster["name"])
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    setup_logging(name="fetch_flows")
    cfg = load_env()
    clusters = parse_clusters()

    log.info("Found %d clusters. Output directory: %s", len(clusters), OUTPUT_DIR.resolve())

    results = {"ok": [], "fail": []}
    for cluster in clusters:
        success = fetch_flow(cluster, cfg)
        key = "ok" if success else "fail"
        results[key].append(cluster["name"])

    print("\n" + "=" * 60)
    print(f"  ✔ Success ({len(results['ok'])}): {', '.join(results['ok']) or '—'}")
    print(f"  ✘ Failed  ({len(results['fail'])}): {', '.join(results['fail']) or '—'}")
    print("=" * 60)


if __name__ == "__main__":
    main()