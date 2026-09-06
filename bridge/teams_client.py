"""Microsoft Teams client integration supporting both IC3 Messaging API and Microsoft Graph REST API."""

import asyncio
import base64
import html
import json
import logging
import os
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional, Callable, Dict, Any, List
import aiohttp
import discord

from .config import TeamsConfig
from .database import Database

logger = logging.getLogger("beeper_bridge.teams")

# Directory the bridge is actually installed in (not the dev-container path
# this used to be hardcoded to), so a saved Teams browser profile is found
# wherever this checkout lives.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Standard Microsoft Multi-Tenant Public Client ID (Developer / VS Code Multi-Tenant)
DEFAULT_CLIENT_ID = "aebc6443-996d-45c2-90f0-388ff96faa56"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
IC3_BASE = "https://amer.ng.msg.teams.microsoft.com/v1/users/ME"
OAUTH_BASE = "https://login.microsoftonline.com/common/oauth2/v2.0"
SCOPES = "https://graph.microsoft.com/Chat.ReadWrite https://graph.microsoft.com/User.Read offline_access"


from bs4 import BeautifulSoup


def clean_teams_html(raw_html: str) -> str:
    """Convert Teams HTML to readable Discord-safe text."""
    if not raw_html:
        return ""
    try:
        # Decode HTML entities first so double-encoded tags (e.g. &lt;p&gt;)
        # become real tags that BeautifulSoup can strip.
        decoded = html.unescape(raw_html)

        # Pre-process emojis and mentions with BeautifulSoup
        soup = BeautifulSoup(decoded, "html.parser")

        # Replace emoji tags with their alt text or emoji char
        for emoji in soup.find_all("emoji"):
            alt = emoji.get("alt") or emoji.get("title") or ""
            emoji.replace_with(alt)

        # Replace at mentions
        for at in soup.find_all("at"):
            mention_text = at.get_text()
            if mention_text and not mention_text.startswith("@"):
                mention_text = f"@{mention_text}"
            at.replace_with(mention_text)

        # Keep link destinations without feeding Teams formatting into Discord Markdown.
        for anchor in soup.find_all("a"):
            label = anchor.get_text(" ", strip=True)
            href = anchor.get("href", "")
            anchor.replace_with(
                f"{label} ({href})" if href and href != label else label or href
            )
        for br in soup.find_all("br"):
            br.replace_with("\n")
        for block in soup.find_all(["p", "div", "li"]):
            if block.name == "li":
                block.insert_before("• ")
            block.append("\n")

        cleaned = soup.get_text("", strip=False).strip()
        # Replace non-breaking spaces
        cleaned = cleaned.replace("\xa0", " ").replace("&nbsp;", " ")
        # Clean redundant empty lines
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return discord.utils.escape_markdown(
            cleaned.strip(), as_needed=False, ignore_links=True
        )
    except Exception as e:
        logger.debug("Error in clean_teams_html: %s", e)
        # Fallback basic cleaner
        decoded = html.unescape(raw_html)
        clean = re.sub(r"<br\s*/?>", "\n", decoded, flags=re.IGNORECASE)
        clean = re.sub(r"</p>", "\n", clean, flags=re.IGNORECASE)
        clean = re.sub(r"</div>", "\n", clean, flags=re.IGNORECASE)
        clean = re.sub(r"<[^>]+>", "", clean)
        return discord.utils.escape_markdown(
            clean.strip(), as_needed=False, ignore_links=True
        )


def _valid_display_name(value: Any) -> str:
    """Return a human-readable Teams display name, rejecting IC3 identity blobs."""
    if not isinstance(value, str):
        return ""
    name = re.sub(r"\s+", " ", value).strip()
    lowered = name.lower()
    if (
        not name
        or len(name) > 100
        or "orgid:" in lowered
        or lowered.startswith(("8:", "29:"))
        or re.fullmatch(r"[\d:_\-]+", name)
    ):
        return ""
    return name


def _ic3_sender(msg: Dict[str, Any]) -> tuple[str, str]:
    """Extract IC3 sender identity without treating wire IDs as display names."""
    sender_id = str(msg.get("from") or "")
    sender_name = (
        _valid_display_name(msg.get("fromDisplayNameInToken"))
        or _valid_display_name(msg.get("imdisplayname"))
        or "Teams User"
    )
    return sender_id, sender_name


class TeamsBridgeClient:
    """Async Microsoft Teams bridge client for personal chats and group DMs."""

    def __init__(
        self,
        config: TeamsConfig,
        db: Database,
        on_message_callback: Optional[Callable] = None,
        on_auth_prompt: Optional[Callable] = None,
    ):
        self.config = config
        self.db = db
        self.on_message_callback = on_message_callback
        self.on_auth_prompt = on_auth_prompt
        self.client_id = config.client_id or DEFAULT_CLIENT_ID
        self.tenant_id = config.tenant_id or "common"

        self.access_token: Optional[str] = config.auth_token or None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: float = 0
        self.my_user_id: Optional[str] = None
        self.my_display_name: Optional[str] = None
        self._is_ic3: bool = False
        self._known_chat_titles: Dict[str, str] = {}
        self._known_chat_ids: set = set()

        self.is_running = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._sync_task: Optional[asyncio.Task] = None

        self._load_tokens_from_db()
        self._inspect_token(self.access_token)

    def _inspect_token(self, token: Optional[str]):
        """Inspect JWT token claims to determine API mode (IC3 vs Graph)."""
        if not token or "." not in token:
            return
        try:
            parts = token.split(".")
            if len(parts) >= 2:
                payload_raw = parts[1]
                payload_raw += "=" * ((4 - len(payload_raw) % 4) % 4)
                payload = json.loads(base64.urlsafe_b64decode(payload_raw.encode()))
                aud = str(payload.get("aud", ""))
                self._is_ic3 = "ic3" in aud or "office.com" in aud or "teams" in aud
                self.token_expires_at = float(payload.get("exp", time.time() + 86400))
                self.my_user_id = (
                    payload.get("oid")
                    or payload.get("sub")
                    or payload.get("puid")
                    or self.my_user_id
                )
                self.my_display_name = (
                    payload.get("name")
                    or payload.get("preferred_username")
                    or payload.get("upn")
                    or self.my_display_name
                    or "Teams User"
                )
                logger.info(
                    "Initialized Teams authentication for %s (Tenant: %s, Mode: %s)",
                    self.my_display_name,
                    payload.get("tid", self.tenant_id),
                    "IC3 Messaging" if self._is_ic3 else "Graph API",
                )
        except Exception as e:
            logger.debug("Could not parse JWT claims: %s", e)

    def _load_tokens_from_db(self):
        """Load stored OAuth tokens from database if present."""
        if not self.access_token:
            self.access_token = self.db.get_value("teams_access_token")
        self.refresh_token = self.db.get_value("teams_refresh_token")
        expires_str = self.db.get_value("teams_token_expires_at")
        if expires_str and not self.token_expires_at:
            self.token_expires_at = float(expires_str)
        self.my_user_id = self.db.get_value("teams_user_id") or self.my_user_id

    def _save_tokens_to_db(self):
        """Persist tokens to database."""
        if self.access_token:
            self.db.set_value("teams_access_token", self.access_token)
        if self.refresh_token:
            self.db.set_value("teams_refresh_token", self.refresh_token)
        if self.token_expires_at:
            self.db.set_value("teams_token_expires_at", str(self.token_expires_at))
        if self.my_user_id:
            self.db.set_value("teams_user_id", self.my_user_id)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    # ---------------- Authentication & Device Code Flow ---------------- #

    async def initiate_device_code_login(self) -> Dict[str, Any]:
        """Request a device login code from Microsoft."""
        session = await self._ensure_session()
        url = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/devicecode"
        )
        data = {
            "client_id": self.client_id,
            "scope": SCOPES,
        }
        async with session.post(url, data=data) as resp:
            res = await resp.json()
            if "user_code" in res:
                logger.info(
                    "👉 [MS Teams Authentication]: Go to %s and enter code: %s",
                    res.get("verification_uri", "https://microsoft.com/devicelogin"),
                    res["user_code"],
                )
                if self.on_auth_prompt:
                    try:
                        await self.on_auth_prompt(
                            uri=res.get(
                                "verification_uri",
                                "https://microsoft.com/devicelogin",
                            ),
                            code=res["user_code"],
                            message=res.get("message", ""),
                        )
                    except Exception as e:
                        logger.warning("Error in on_auth_prompt: %s", e)
                return res
            else:
                logger.error("Failed to initiate Microsoft device code flow: %s", res)
                raise RuntimeError(f"Device code request failed: {res}")

    async def poll_device_code_token(
        self, device_code: str, interval: int = 5, expires_in: int = 900
    ) -> bool:
        """Poll Microsoft OAuth endpoint until user completes login in browser."""
        session = await self._ensure_session()
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "client_id": self.client_id,
            "device_code": device_code,
        }

        deadline = time.time() + expires_in
        while time.time() < deadline:
            await asyncio.sleep(interval)
            async with session.post(url, data=data) as resp:
                res = await resp.json()
                if "access_token" in res:
                    self.access_token = res["access_token"]
                    self.refresh_token = res.get("refresh_token")
                    self.token_expires_at = time.time() + res.get("expires_in", 3600)
                    self._inspect_token(self.access_token)
                    self._save_tokens_to_db()
                    logger.info(
                        "Microsoft Teams authentication successful for %s (%s)",
                        self.my_display_name,
                        self.my_user_id,
                    )
                    return True
                elif res.get("error") == "authorization_pending":
                    continue
                elif res.get("error") == "slow_down":
                    await asyncio.sleep(interval)
                    continue
                else:
                    logger.error("Device code authorization failed: %s", res)
                    return False
        return False

    async def refresh_access_token(self) -> bool:
        """Refresh expired access token using refresh_token."""
        if not self.refresh_token:
            return False

        session = await self._ensure_session()
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "refresh_token": self.refresh_token,
            "scope": SCOPES,
        }
        try:
            async with session.post(url, data=data) as resp:
                res = await resp.json()
                if "access_token" in res:
                    self.access_token = res["access_token"]
                    self.refresh_token = res.get("refresh_token", self.refresh_token)
                    self.token_expires_at = time.time() + res.get("expires_in", 3600)
                    self._inspect_token(self.access_token)
                    self._save_tokens_to_db()
                    return True
                else:
                    logger.warning("Teams token refresh failed: %s", res)
                    return False
        except Exception as e:
            logger.error("Error during Teams token refresh: %s", e)
            return False

    async def get_valid_token(self) -> Optional[str]:
        """Ensure we have a valid access token."""
        if self.access_token:
            # If token is valid and has at least 5 minutes before expiring
            if self.token_expires_at and time.time() < (self.token_expires_at - 300):
                return self.access_token
            elif not self.token_expires_at:
                return self.access_token

        # 1. Try standard OAuth refresh token if configured
        if self.refresh_token:
            success = await self.refresh_access_token()
            if success:
                return self.access_token

        # 2. Try headless browser session renewal if browser profile exists
        browser_profile = str(PROJECT_ROOT / ".teams_browser_profile")
        if os.path.exists(browser_profile):
            try:
                from .teams_browser_auth import refresh_teams_token_headless

                new_token = await refresh_teams_token_headless(
                    user_data_dir=browser_profile
                )
                if new_token:
                    self.access_token = new_token
                    self._inspect_token(new_token)
                    self._save_tokens_to_db()
                    logger.info(
                        "Successfully renewed Teams access token via headless browser session!"
                    )
                    return self.access_token
            except Exception as e:
                logger.error("Headless browser token renewal failed: %s", e)

        return self.access_token

    # ---------------- Teams API Operations ---------------- #

    async def list_chats(self) -> List[Dict[str, Any]]:
        """List user's conversations and active chats."""
        token = await self.get_valid_token()
        if not token:
            return []

        session = await self._ensure_session()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        if self._is_ic3:
            url = f"{IC3_BASE}/conversations?pageSize=50&view=msnp24Equivalent"
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        convs = data.get("conversations", [])
                        return [
                            c
                            for c in convs
                            if c.get("id") and not str(c.get("id")).startswith("48:")
                        ]
                    else:
                        logger.warning(
                            "Failed to list IC3 Teams chats: HTTP %d", resp.status
                        )
                        return []
            except Exception as e:
                logger.error("Error listing IC3 Teams chats: %s", e)
                return []
        else:
            url = f"{GRAPH_BASE}/me/chats?$expand=members&$top=50"
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("value", [])
                    else:
                        logger.warning(
                            "Failed to list Graph Teams chats: HTTP %d", resp.status
                        )
                        return []
            except Exception as e:
                logger.error("Error listing Graph Teams chats: %s", e)
                return []

    async def list_chat_messages(
        self, chat_id: str, top: int = 10
    ) -> List[Dict[str, Any]]:
        """List latest messages in a chat."""
        token = await self.get_valid_token()
        if not token:
            return []

        session = await self._ensure_session()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        }

        if self._is_ic3:
            url = f"{IC3_BASE}/conversations/{chat_id}/messages?pageSize={top}"
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("messages", [])
                    else:
                        logger.warning(
                            "Failed to fetch messages for IC3 Teams chat %s: HTTP %d",
                            chat_id,
                            resp.status,
                        )
                        return []
            except Exception as e:
                logger.error("Error fetching IC3 Teams messages for %s: %s", chat_id, e)
                return []
        else:
            url = f"{GRAPH_BASE}/me/chats/{chat_id}/messages?$top={top}"
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("value", [])
                    else:
                        logger.warning(
                            "Failed to fetch messages for Graph Teams chat %s: HTTP %d",
                            chat_id,
                            resp.status,
                        )
                        return []
            except Exception as e:
                logger.error(
                    "Error fetching Graph Teams messages for %s: %s", chat_id, e
                )
                return []

    async def send_chat_message(self, chat_id: str, content: str) -> Optional[str]:
        """Send a message to a Teams chat/DM."""
        token = await self.get_valid_token()
        if not token:
            logger.error("Cannot send Teams message: No valid token")
            return None

        session = await self._ensure_session()

        if self._is_ic3:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            }
            url = f"{IC3_BASE}/conversations/{chat_id}/messages"
            payload = {
                "content": f"<p>{html.escape(content)}</p>",
                "messagetype": "RichText/Html",
                "amsreferences": [],
                "clientmessageid": str(int(time.time() * 1000)),
                "imdisplayname": self.my_display_name or "",
            }
            self.db.record_outgoing_tx(payload["clientmessageid"])
            try:
                async with session.post(url, headers=headers, json=payload) as resp:
                    if resp.status in (200, 201):
                        data = await resp.json()
                        msg_id = data.get("OriginalArrivalTime") or str(
                            int(time.time() * 1000)
                        )
                        logger.info(
                            "Sent message to IC3 Teams chat %s: %s",
                            chat_id,
                            content[:50],
                        )
                        return msg_id
                    else:
                        err_text = await resp.text()
                        logger.error(
                            "Failed sending IC3 Teams message to %s (HTTP %d): %s",
                            chat_id,
                            resp.status,
                            err_text,
                        )
                        return None
            except Exception as e:
                logger.error("Error sending IC3 Teams message to %s: %s", chat_id, e)
                return None
        else:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            url = f"{GRAPH_BASE}/me/chats/{chat_id}/messages"
            payload = {
                "body": {
                    "contentType": "text",
                    "content": content,
                }
            }
            try:
                async with session.post(url, headers=headers, json=payload) as resp:
                    if resp.status in (200, 201):
                        data = await resp.json()
                        msg_id = data.get("id")
                        logger.info(
                            "Sent message to Graph Teams chat %s: %s",
                            chat_id,
                            content[:50],
                        )
                        return msg_id
                    else:
                        err_text = await resp.text()
                        logger.error(
                            "Failed sending Graph Teams message to %s (HTTP %d): %s",
                            chat_id,
                            resp.status,
                            err_text,
                        )
                        return None
            except Exception as e:
                logger.error("Error sending Graph Teams message to %s: %s", chat_id, e)
                return None

    # ---------------- Sync Loop & Lifecycle ---------------- #

    async def start(self):
        """Start Teams chat sync loop."""
        token = await self.get_valid_token()
        if not token:
            logger.info("No active Microsoft Teams session. Requesting device code...")
            try:
                code_data = await self.initiate_device_code_login()
                asyncio.create_task(
                    self.poll_device_code_token(
                        device_code=code_data["device_code"],
                        interval=code_data.get("interval", 5),
                        expires_in=code_data.get("expires_in", 900),
                    )
                )
            except Exception as e:
                logger.error("Could not start Teams device code flow: %s", e)
                return

        self.is_running = True
        self._sync_task = asyncio.create_task(self._sync_forever())
        logger.info(
            "Microsoft Teams chat sync loop started (User: %s)", self.my_display_name
        )

    async def stop(self):
        """Stop Teams bridge client."""
        self.is_running = False
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("Microsoft Teams client stopped")

    async def _sync_forever(self):
        """Poll Teams chats for new messages."""
        poll_interval = max(5, self.config.poll_interval_seconds)
        is_initial_poll = True
        try:
            saved_known = self.db.get_value("teams_known_chat_ids")
            if saved_known:
                self._known_chat_ids = set(json.loads(saved_known))
        except Exception:
            pass

        while self.is_running:
            try:
                token = await self.get_valid_token()
                if token:
                    chats = await self.list_chats()
                    for chat in chats:
                        chat_id = chat.get("id")
                        if not chat_id:
                            continue
                        chat_is_new = chat_id not in self._known_chat_ids

                        # Resolve chat name and type
                        is_group = (
                            "@thread.v2" in chat_id
                            or chat.get("chatType") == "group"
                            or chat.get("properties", {}).get("threadType") == "topic"
                        )
                        props = chat.get("properties", {})
                        thread_props = chat.get("threadProperties", {})
                        topic = (
                            chat.get("topic")
                            or props.get("topic")
                            or props.get("friendlyName")
                            or thread_props.get("topic")
                            or thread_props.get("friendlyName")
                        )

                        # Cache or resolve chat title
                        if topic:
                            chat_title = topic
                        elif chat_id in self._known_chat_titles:
                            chat_title = self._known_chat_titles[chat_id]
                        else:
                            stable_id = re.sub(r"[^A-Za-z0-9]", "", chat_id)[-8:]
                            chat_title = (
                                f"Teams Chat {stable_id}" if stable_id else "Teams Chat"
                            )

                        # Fetch recent messages
                        messages = await self.list_chat_messages(chat_id, top=10)

                        # In 1-on-1 chats, determine other participant's name from message history
                        if not topic and chat_id not in self._known_chat_titles:
                            for m in messages:
                                sender_id, name = (
                                    _ic3_sender(m) if self._is_ic3 else ("", "")
                                )
                                from_val = m.get("from")
                                if not name and isinstance(from_val, dict):
                                    name = _valid_display_name(
                                        from_val.get("user", {}).get("displayName")
                                    )
                                if (
                                    name
                                    and name != "Teams User"
                                    and name.lower()
                                    != (self.my_display_name or "").lower()
                                    and not (
                                        self.my_user_id
                                        and self.my_user_id.lower() in sender_id.lower()
                                    )
                                ):
                                    chat_title = name
                                    self._known_chat_titles[chat_id] = name
                                    break

                        # Sort messages chronologically
                        if self._is_ic3:
                            # IC3 messages contain sequenceId or composetime
                            messages = sorted(
                                messages,
                                key=lambda m: m.get("sequenceId")
                                or str(m.get("composetime", "")),
                            )
                        else:
                            messages = sorted(
                                messages,
                                key=lambda m: str(m.get("createdDateTime", "")),
                            )

                        for msg in messages:
                            msg_id = str(msg.get("id") or msg.get("sequenceId") or "")
                            if not msg_id:
                                continue

                            # Skip already processed messages
                            if self.db.is_message_recorded(msg_id):
                                continue

                            client_message_id = str(msg.get("clientmessageid") or "")
                            if client_message_id and self.db.is_outgoing_tx(
                                client_message_id
                            ):
                                self.db.record_message(
                                    msg_id, 0, 0, self.my_user_id or ""
                                )
                                continue

                            # Extract sender, text, and metadata
                            if self._is_ic3:
                                msg_type = msg.get("messagetype", "")
                                if msg_type in (
                                    "Control/Typing",
                                    "Control/ClearTyping",
                                ):
                                    continue
                                sender_id, sender_name = _ic3_sender(msg)
                                raw_content = msg.get("content", "")
                                body_text = clean_teams_html(raw_content)
                                is_self = (
                                    sender_name.lower()
                                    == (self.my_display_name or "").lower()
                                    or (
                                        self.my_user_id and self.my_user_id in sender_id
                                    )
                                    or bool(msg.get("isfromme") or msg.get("isFromMe"))
                                )
                            else:
                                if msg.get("messageType") != "message":
                                    continue
                                sender = msg.get("from", {}).get("user", {})
                                sender_id = sender.get("id", "")
                                sender_name = sender.get("displayName", "Teams User")
                                raw_content = msg.get("body", {}).get("content", "")
                                body_text = clean_teams_html(raw_content)
                                is_self = (
                                    sender_id == self.my_user_id
                                    or sender_name.lower()
                                    == (self.my_display_name or "").lower()
                                )

                            if not body_text:
                                self.db.record_message(
                                    matrix_event_id=msg_id,
                                    discord_message_id=0,
                                    channel_id=0,
                                    sender_id=sender_id,
                                )
                                continue

                            # On cold initial startup (or the first time we ever see this
                            # chat), record existing history so we don't spam Discord with it.
                            # Otherwise relay normally -- including messages we typed directly
                            # in Teams, so Discord sees the full conversation.
                            if is_initial_poll or chat_is_new:
                                self.db.record_message(
                                    matrix_event_id=msg_id,
                                    discord_message_id=0,
                                    channel_id=0,
                                    sender_id=sender_id,
                                )
                                continue

                            # Relay incoming message via callback
                            if self.on_message_callback:
                                encoded_name = urllib.parse.quote(sender_name)
                                avatar_url = f"https://ui-avatars.com/api/{encoded_name}/256/505ac9/ffffff"
                                try:
                                    await self.on_message_callback(
                                        chat_id=chat_id,
                                        chat_name=chat_title,
                                        sender_name=sender_name,
                                        sender_id=sender_id,
                                        body=body_text,
                                        avatar_url=avatar_url,
                                        is_group=is_group,
                                    )
                                    self.db.record_message(
                                        matrix_event_id=msg_id,
                                        discord_message_id=0,
                                        channel_id=0,
                                        sender_id=sender_id,
                                    )
                                except Exception as e:
                                    logger.error(
                                        "Error dispatching Teams message to Discord: %s",
                                        e,
                                    )

                        if chat_is_new:
                            self._known_chat_ids.add(chat_id)
                            self.db.set_value(
                                "teams_known_chat_ids",
                                json.dumps(list(self._known_chat_ids)),
                            )

                    is_initial_poll = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in Teams sync loop: %s", e, exc_info=True)

            await asyncio.sleep(poll_interval)
