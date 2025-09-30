import json
import os
import csv
from utils.enums import Info


def banner() -> None:
    """
    Prints a banner with the name of the tool and its version number.
    """
    print(Info.BANNER)


def read_cookies():
    """
    Loads the config file and returns it.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "cookies.json")
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def read_telegram_config():
    """
    Loads the telegram config file and returns it.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "telegram.json")
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def read_proxy_config():
    """
    Loads the proxy config from a CSV file and returns it as a dict.
    Each row should be in the format: proxy:port:username:password
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "proxies.csv")
    proxies = []

    try:
        with open(config_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if not row or not row[0]:
                    continue
                parts = row[0].strip().split(":")
                if len(parts) < 2:
                    continue  # skip invalid rows
                proxy = {
                    "host": parts[0],
                    "port": parts[1],
                    "username": parts[2] if len(parts) > 2 else None,
                    "password": parts[3] if len(parts) > 3 else None
                }
                proxies.append(proxy)
    except FileNotFoundError:
        pass

    return {"proxies": proxies}


def read_tiktok_live_config():
    """
    Loads the TikTokLive config file and returns it.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "tiktok_live_config.json")
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def read_proxy_optimization_config():
    """
    Loads the proxy optimization config file and returns it.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "config", "proxy_config.json")
    try:
        with open(config_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        # Default configuration if file doesn't exist
        return {
            "proxy_enabled": True,
            "proxy_optimization": {
                "max_retries": 3,
                "reduce_retries_for_protocol_errors": True,
                "protocol_error_max_retries": 2,
                "user_cache_enabled": True,
                "user_cache_duration_minutes": 60,
                "fail_fast_errors": [
                    "HTTP/2 stream 0 was not closed cleanly: PROTOCOL_ERROR",
                    "Connection reset by peer",
                    "Remote end closed connection without response"
                ]
            }
        }