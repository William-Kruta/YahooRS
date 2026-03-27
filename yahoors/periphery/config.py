import os
import json
import platform
from pathlib import Path

ENV_VAR = "YAHOO_FINANCE_DB"
CONFIG_DIR = "yahoors"
CONFIG_FILE = "config.json"
DEFAULT_DB = "yahoo_rs.db"


def _get_config_dir() -> Path:
    system = platform.system()
    if system == "Windows":
        # %APPDATA%/yahoo-finance
        base = os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming")
    elif system == "Darwin":
        # ~/Library/Application Support/yahoo-finance
        base = Path.home() / "Library" / "Application Support"
    else:
        # ~/.config/yahoo-finance (Linux/BSD)
        base = os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")

    return Path(base) / CONFIG_DIR


def get_db_path() -> Path:
    # 1. Environment variable
    env_path = os.environ.get(ENV_VAR, "")
    if env_path:
        return Path(env_path)

    # 2. Config file
    config_dir = _get_config_dir()
    config_path = config_dir / CONFIG_FILE

    if config_path.exists():
        try:
            with open(config_path) as f:
                config = json.load(f)
            db_path = config.get("database", "")
            if db_path:
                return Path(db_path)
        except (json.JSONDecodeError, OSError):
            pass

    # 3. Default: inside the config directory
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / DEFAULT_DB
