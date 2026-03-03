import json
import logging
import os
import sys
from enum import Enum
from pathlib import Path

from utils.logger import setup_logger


logger = setup_logger(level=logging.DEBUG)

DEBUG = False
CONFIGFILE = "config.json"
USERDATAFILE = "usersData.json"
BASE_DIR = Path(__file__).resolve().parent.parent
config = None
userData = None


class Environment(Enum):
    GITHUBACTION = "GITHUB_ACTION"
    LOCAL = "LOCAL"
    PACKED = "PACKED"
    HUGGINGFACE = "HUGGINGFACE"

    def __str__(self):
        return self.value


def get_environment():
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Environment.PACKED
    if os.getenv("GITHUB_ACTIONS") == "true":
        return Environment.GITHUBACTION
    if os.getenv("SPACE_ID") or os.getenv("HF_SPACE_ID"):
        return Environment.HUGGINGFACE
    return Environment.LOCAL


def _resolve_runtime_file(file_name):
    env = get_environment()
    if env == Environment.PACKED:
        return os.path.join(os.path.dirname(sys.executable), file_name)
    return str((BASE_DIR / file_name).resolve())


def get_config():
    global config

    if config is not None:
        return config

    config_path = _resolve_runtime_file(CONFIGFILE)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.loads(f.read())
    return config


def reload_config():
    global config
    config = None
    return get_config()


def get_userData():
    global userData

    if userData is not None:
        return userData

    env = get_environment()
    if env == Environment.GITHUBACTION:
        user_data_json = os.getenv("USER_DATA", None)
        if not user_data_json:
            logger.error("Environment variable USER_DATA is not set.")
            raise RuntimeError("USER_DATA is required in GitHub Actions.")
    else:
        user_data_path = _resolve_runtime_file(USERDATAFILE)
        if not os.path.exists(user_data_path):
            raise FileNotFoundError(f"Missing required file: {user_data_path}")
        with open(user_data_path, "r", encoding="utf-8") as f:
            user_data_json = f.read()

    userData = json.loads(user_data_json)
    return userData


def reload_userData():
    global userData
    userData = None
    return get_userData()
