"""Interactive and headless Teams Web browser authentication and session capture."""

import os
import sys
import json
import time
import base64
import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright
import yaml

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("teams_auth")

CONFIG_FILE = Path("/workspace/beeper-discord-bridge/config.yaml")
SESSION_FILE = Path("/workspace/beeper-discord-bridge/teams_session.json")
PROFILE_DIR = "/workspace/beeper-discord-bridge/.teams_browser_profile"


def extract_jwt_payload(token: str) -> Optional[Dict[str, Any]]:
    """Decode JWT payload without verifying signature."""
    if not token or "." not in token:
        return None
    try:
        parts = token.split(".")
        if len(parts) >= 2:
            raw = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
            return json.loads(base64.urlsafe_b64decode(raw.encode()))
    except Exception:
        pass
    return None


def update_config_token(token: str) -> bool:
    """Save the fresh token to config.yaml."""
    try:
        if not CONFIG_FILE.exists():
            return False
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        if "teams" not in data:
            data["teams"] = {}
        data["teams"]["auth_token"] = token

        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.dump(data, f, sort_keys=False, allow_unicode=True)

        logger.info("✅ Saved fresh Teams auth_token to %s", CONFIG_FILE)
        return True
    except Exception as e:
        logger.error("Error updating %s: %s", CONFIG_FILE, e)
        return False


async def refresh_teams_token_headless(
    user_data_dir: str = PROFILE_DIR, timeout_seconds: int = 40
) -> Optional[str]:
    """Launch headless Chromium to silently refresh Teams IC3 token using saved cookies/session."""
    logger.info("Attempting silent headless Teams token refresh...")
    if not os.path.exists(user_data_dir):
        logger.warning("No browser profile found at %s", user_data_dir)
        return None

    captured_token = None

    try:
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=True,
                viewport={"width": 1280, "height": 800},
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

            page = context.pages[0] if context.pages else await context.new_page()

            async def handle_req(req):
                nonlocal captured_token
                auth = req.headers.get("authorization", "")
                if "Bearer " in auth:
                    raw_tok = auth.split("Bearer ", 1)[1].strip()
                    payload = extract_jwt_payload(raw_tok)
                    if payload:
                        aud = str(payload.get("aud", ""))
                        if "ic3" in aud or "teams.office.com" in aud:
                            exp = payload.get("exp", 0)
                            if exp > time.time():
                                captured_token = raw_tok
                                logger.info(
                                    "🎉 Headless session captured fresh IC3 token! (Expires: %s)",
                                    time.ctime(exp),
                                )

            page.on("request", handle_req)

            try:
                await page.goto(
                    "https://teams.cloud.microsoft",
                    wait_until="domcontentloaded",
                    timeout=timeout_seconds * 1000,
                )
            except Exception as e:
                logger.debug("Page goto note: %s", e)

            # Wait a few seconds for background network calls to fire
            start_wait = time.time()
            while time.time() - start_wait < 15:
                if captured_token:
                    break
                await asyncio.sleep(1)

            await context.close()

    except Exception as exc:
        logger.error("Error during headless Teams token refresh: %s", exc)

    if captured_token:
        update_config_token(captured_token)
        return captured_token
    return None


async def capture_teams_session():
    """Launch Chromium on DISPLAY=:99 and monitor for interactive authentication."""
    os.environ["DISPLAY"] = ":99"
    os.makedirs(PROFILE_DIR, exist_ok=True)

    logger.info("=================================================================")
    logger.info("🚀 Launching interactive Teams Web login browser on DISPLAY=:99")
    logger.info("👉 Open: http://10.126.200.53:6080/vnc.html to log in")
    logger.info("=================================================================")

    auth_data = {
        "tokens": {},
        "cookies": [],
        "authenticated": False,
        "user": {},
    }

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=False,
            viewport={"width": 1280, "height": 800},
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        page = context.pages[0] if context.pages else await context.new_page()

        async def handle_request(request):
            headers = request.headers
            auth_header = headers.get("authorization")
            skype_token = headers.get("x-skypetoken") or headers.get("skypetoken")

            if auth_header and "Bearer" in auth_header:
                tok = auth_header.split("Bearer ", 1)[1].strip()
                payload = extract_jwt_payload(tok)
                if payload:
                    aud = str(payload.get("aud", ""))
                    if "ic3" in aud or "teams.office.com" in aud:
                        auth_data["tokens"]["ic3"] = tok
                        auth_data["authenticated"] = True
                        name = payload.get("name") or payload.get("unique_name", "")
                        auth_data["user"]["name"] = name
                        auth_data["user"]["upn"] = payload.get("upn", "")
                        logger.info(
                            "🎉 Captured valid Teams IC3 Bearer Token for %s! Expires: %s",
                            name,
                            time.ctime(payload.get("exp", 0)),
                        )
                        update_config_token(tok)

            if skype_token:
                auth_data["tokens"]["skypetoken"] = skype_token

        page.on("request", handle_request)

        logger.info("Navigating to https://teams.cloud.microsoft...")
        try:
            await page.goto("https://teams.cloud.microsoft", wait_until="commit")
        except Exception:
            pass

        logger.info("Waiting for you to complete login in the noVNC browser window...")

        while True:
            await asyncio.sleep(2)
            cookies = await context.cookies()
            auth_data["cookies"] = [
                {
                    "name": c["name"],
                    "value": c["value"],
                    "domain": c["domain"],
                    "path": c["path"],
                }
                for c in cookies
            ]

            if auth_data.get("authenticated") and auth_data["tokens"].get("ic3"):
                logger.info("Authentication complete! Saving session...")
                break

        # Save session file
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(auth_data, f, indent=2)

        logger.info("✅ Session successfully captured and saved!")
        logger.info(
            "Browser profile preserved in %s for headless auto-renewal.", PROFILE_DIR
        )
        await context.close()


if __name__ == "__main__":
    asyncio.run(capture_teams_session())
