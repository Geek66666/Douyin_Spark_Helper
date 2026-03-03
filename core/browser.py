import os
import sys
import traceback
from pathlib import Path

from playwright.async_api import async_playwright
from rich.console import Console

from utils.config import DEBUG, Environment, get_environment


PLAYWRIGHT_BROWSERS_PATH = "../chrome"
console = Console()


def _looks_like_bundled_windows_playwright(path: Path):
    if not path.exists():
        return False
    return any(p.name.startswith("chromium-") for p in path.iterdir())


def _configure_playwright_browser_path(env: Environment):
    # Respect explicit runtime override first.
    if os.getenv("PLAYWRIGHT_BROWSERS_PATH"):
        return

    # Hugging Face Docker should use browsers installed in the image.
    if env == Environment.HUGGINGFACE:
        return

    # Local mode may have a bundled playwright browser directory.
    if env == Environment.LOCAL:
        bundled = Path(__file__).resolve().parent / PLAYWRIGHT_BROWSERS_PATH
        # The repo bundles Windows binaries only. Avoid forcing this path on Linux/macOS.
        if os.name == "nt" and _looks_like_bundled_windows_playwright(bundled):
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(bundled.resolve())
        return

    # Packed mode keeps runtime assets near executable.
    if env == Environment.PACKED:
        packed = Path(sys.executable).resolve().parent / PLAYWRIGHT_BROWSERS_PATH
        if packed.exists():
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(packed.resolve())


async def get_browser(GUI=False):
    headless = True
    env = get_environment()
    _configure_playwright_browser_path(env)

    if env == Environment.LOCAL and DEBUG:
        headless = False
    if GUI:
        headless = False

    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        return playwright, browser
    except Exception:
        traceback.print_exc()
        raise

