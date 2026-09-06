"""BlueBubbles iMessage integration using BlueBubbles REST API."""

import asyncio
import logging
import re
import time
import uuid
from typing import Optional, Callable, Dict, Any, List, Tuple
import aiohttp

from .config import BlueBubblesConfig
from .database import Database

logger = logging.getLogger("beeper_bridge.bluebubbles")


class BlueBubblesBridgeClient:
    """Async client for BlueBubbles iMessage server."""

    def __init__(
        self,
        config: BlueBubblesConfig,
        db: Database,
        on_message_callback: Optional[Callable] = None,
    ):
        self.config = config
        self.db = db
        self.on_message_callback = on_message_callback
        self.server_url = config.server_url.rstrip("/")
        self.password = config.password

        self.is_running = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._sync_task: Optional[asyncio.Task] = None
        self._known_chat_names: Dict[str, str] = {}
        self._contacts_lookup: Dict[str, str] = {}
        self._last_contact_sync: float = 0

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session and self._session.closed:
            self._session = None
        if not self._session:
            connector = aiohttp.TCPConnector(
                force_close=True, enable_cleanup_closed=True
            )
            timeout = aiohttp.ClientTimeout(total=20, connect=5)
            self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self._session

    def _api_url(self, path: str) -> str:
        clean_path = path.lstrip("/")
        return f"{self.server_url}/api/v1/{clean_path}?password={self.password}"

    @staticmethod
    def _normalize_phone(phone: str) -> str:
        digits = re.sub(r"\D", "", phone)
        if digits.startswith("1") and len(digits) == 11:
            return digits[1:]
        return digits

    def resolve_address(self, address: Optional[str]) -> str:
        """Resolve phone number or email address to a contact display name."""
        if not address:
            return "Unknown"
        addr_str = str(address).strip()
        if not addr_str:
            return "Unknown"

        if addr_str in self._contacts_lookup:
            return self._contacts_lookup[addr_str]

        if "@" in addr_str:
            return self._contacts_lookup.get(addr_str.lower(), addr_str)

        norm = self._normalize_phone(addr_str)
        if norm and norm in self._contacts_lookup:
            return self._contacts_lookup[norm]

        if len(norm) == 10:
            if f"+1{norm}" in self._contacts_lookup:
                return self._contacts_lookup[f"+1{norm}"]
            if f"1{norm}" in self._contacts_lookup:
                return self._contacts_lookup[f"1{norm}"]

        return addr_str

    async def refresh_contacts(self) -> int:
        """Fetch all contacts from BlueBubbles server and build lookup map."""
        try:
            session = await self._ensure_session()
            url = self._api_url("contact")
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    contacts = data.get("data", [])
                    new_lookup: Dict[str, str] = {}

                    for c in contacts:
                        display_name = (
                            c.get("displayName")
                            or f"{c.get('firstName', '')} {c.get('lastName', '')}".strip()
                            or c.get("nickname")
                        )
                        if not display_name:
                            continue

                        for p in c.get("phoneNumbers", []):
                            raw_p = p.get("address", "").strip()
                            if raw_p:
                                new_lookup[raw_p] = display_name
                                norm = self._normalize_phone(raw_p)
                                if norm:
                                    new_lookup[norm] = display_name
                                    if len(norm) == 10:
                                        new_lookup[f"1{norm}"] = display_name
                                        new_lookup[f"+1{norm}"] = display_name

                        for e in c.get("emails", []):
                            raw_e = e.get("address", "").strip()
                            if raw_e:
                                new_lookup[raw_e.lower()] = display_name

                    self._contacts_lookup = new_lookup
                    self._last_contact_sync = time.time()
                    logger.info(
                        "Synced %d BlueBubbles contacts (%d address mappings)",
                        len(contacts),
                        len(new_lookup),
                    )
                    return len(contacts)
                else:
                    logger.warning(
                        "Failed fetching BlueBubbles contacts: HTTP %d", resp.status
                    )
                    return 0
        except Exception as e:
            logger.error("Error refreshing BlueBubbles contacts: %s", e)
            return 0

    async def ping(self) -> bool:
        """Check if BlueBubbles server is reachable and password is valid."""
        if not self.server_url or not self.password:
            return False
        try:
            session = await self._ensure_session()
            url = self._api_url("ping")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("status") == 200 or data.get("message") == "pong"
                else:
                    logger.warning("BlueBubbles ping failed (HTTP %d)", resp.status)
                    return False
        except Exception as e:
            logger.debug("BlueBubbles ping error: %s", e)
            return False

    async def list_recent_messages(self, limit: int = 25) -> List[Dict[str, Any]]:
        """Fetch latest messages globally across all chats in a single request."""
        session = await self._ensure_session()
        url = self._api_url("message/query")
        payload = {
            "limit": limit,
            "sort": "DESC",
            "with": ["chats", "handle", "attachments"],
        }
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", [])
                else:
                    logger.warning(
                        "Failed fetching BlueBubbles messages: HTTP %d", resp.status
                    )
                    return []
        except Exception as e:
            logger.error("Error querying BlueBubbles messages: %s", e)
            return []

    def _determine_chat_title(self, chat: Dict[str, Any], chat_guid: str) -> str:
        display_name = chat.get("displayName")
        is_group = bool(chat.get("isGroup") or ";+;" in chat_guid)
        participants = chat.get("participants", [])

        if (
            display_name
            and not is_group
            and (
                display_name.startswith("+") or display_name.replace("-", "").isdigit()
            )
        ):
            resolved = self.resolve_address(display_name)
            return resolved if resolved != display_name else display_name
        elif display_name:
            return display_name
        elif participants:
            names = [
                p.get("displayName")
                or self.resolve_address(p.get("address"))
                or "Unknown"
                for p in participants
            ]
            return ", ".join(names)
        else:
            parts = chat_guid.split(";")
            raw_addr = parts[-1] if len(parts) >= 3 else chat_guid
            return self.resolve_address(raw_addr)

    async def download_attachment(
        self, attachment_guid: str
    ) -> Optional[Tuple[bytes, Optional[str]]]:
        """Download an attachment's raw bytes from the BlueBubbles server."""
        session = await self._ensure_session()
        url = self._api_url(f"attachment/{attachment_guid}/download")
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return data, resp.headers.get("Content-Type")
                else:
                    logger.warning(
                        "Failed downloading BlueBubbles attachment %s: HTTP %d",
                        attachment_guid,
                        resp.status,
                    )
                    return None
        except Exception as e:
            logger.error(
                "Error downloading BlueBubbles attachment %s: %s", attachment_guid, e
            )
            return None

    async def send_message(self, chat_guid: str, text: str) -> Optional[str]:
        """Send an iMessage / SMS text message to a chat."""
        session = await self._ensure_session()
        url = self._api_url("message/text")
        temp_guid = f"temp-{uuid.uuid4().hex}"

        target_guid = chat_guid
        if target_guid.startswith("any;"):
            target_guid = "iMessage;" + target_guid[4:]

        payload = {
            "chatGuid": target_guid,
            "message": text,
            "tempGuid": temp_guid,
            "method": "apple-script",
        }
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status in (200, 201):
                    data = await resp.json()
                    msg_data = data.get("data", {}) or {}
                    msg_id = msg_data.get("guid") or temp_guid
                    logger.info(
                        "Sent BlueBubbles message to %s: %s", target_guid, text[:50]
                    )
                    return msg_id
                else:
                    err_text = await resp.text()
                    logger.warning(
                        "Failed sending BlueBubbles iMessage to %s (HTTP %d): %s. Trying SMS...",
                        target_guid,
                        resp.status,
                        err_text,
                    )
                    if chat_guid.startswith("any;"):
                        sms_guid = "SMS;" + chat_guid[4:]
                        payload["chatGuid"] = sms_guid
                        async with session.post(
                            url, json=payload, timeout=aiohttp.ClientTimeout(total=15)
                        ) as resp2:
                            if resp2.status in (200, 201):
                                data = await resp2.json()
                                msg_data = data.get("data", {}) or {}
                                return msg_data.get("guid") or temp_guid
                    return None
        except Exception as e:
            logger.error("Error sending BlueBubbles message to %s: %s", target_guid, e)
            return None

    async def start(self):
        """Start BlueBubbles sync loop."""
        if not self.config.enabled:
            return

        is_connected = await self.ping()
        if not is_connected:
            logger.warning(
                "BlueBubbles server is not reachable at %s. Will retry in background...",
                self.server_url,
            )

        # Initial contact sync
        await self.refresh_contacts()

        self.is_running = True
        self._sync_task = asyncio.create_task(self._sync_forever())
        logger.info("BlueBubbles iMessage sync loop started (%s)", self.server_url)

    async def stop(self):
        """Stop BlueBubbles client."""
        self.is_running = False
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("BlueBubbles client stopped")

    async def _sync_forever(self):
        """Poll BlueBubbles server for new messages using single global query."""
        poll_interval = max(2, self.config.poll_interval_seconds)
        is_initial_poll = True

        while self.is_running:
            try:
                # Periodically refresh contacts (every 5 mins)
                if time.time() - self._last_contact_sync > 300:
                    await self.refresh_contacts()

                messages = await self.list_recent_messages(limit=25)
                # Sort ascending by dateCreated so messages are processed chronologically
                messages = sorted(messages, key=lambda m: m.get("dateCreated") or 0)

                for msg in messages:
                    msg_guid = msg.get("guid")
                    if not msg_guid:
                        continue

                    if self.db.is_message_recorded(msg_guid):
                        continue

                    chats = msg.get("chats", [])
                    chat = chats[0] if chats else {}
                    chat_guid = chat.get("guid")
                    if not chat_guid:
                        continue

                    is_group = bool(chat.get("isGroup") or ";+;" in chat_guid)
                    chat_title = self._determine_chat_title(chat, chat_guid)
                    self._known_chat_names[chat_guid] = chat_title

                    text = msg.get("text") or ""
                    is_from_me = bool(msg.get("isFromMe"))
                    handle = msg.get("handle") or {}
                    sender_addr = handle.get("address") or ""
                    raw_sender_name = handle.get("displayName")

                    if is_from_me:
                        sender_name = "You"
                        sender_id = "me"
                    else:
                        if raw_sender_name and not (
                            raw_sender_name.startswith("+")
                            or raw_sender_name.replace("-", "").isdigit()
                        ):
                            sender_name = raw_sender_name
                        else:
                            sender_name = (
                                self.resolve_address(sender_addr) or chat_title
                            )
                        sender_id = sender_addr or ""

                    if is_from_me and not self.config.sync_self_messages:
                        self.db.record_message(
                            matrix_event_id=msg_guid,
                            discord_message_id=0,
                            channel_id=0,
                            sender_id=sender_id,
                        )
                        continue

                    if is_initial_poll:
                        self.db.record_message(
                            matrix_event_id=msg_guid,
                            discord_message_id=0,
                            channel_id=0,
                            sender_id=sender_id,
                        )
                        continue

                    files_to_send: List[Tuple[str, bytes]] = []
                    attachments = msg.get("attachments", []) or []
                    max_bytes = self.config.max_attachment_size_mb * 1024 * 1024
                    for att in attachments:
                        att_guid = att.get("guid")
                        if not att_guid:
                            continue
                        downloaded = await self.download_attachment(att_guid)
                        if not downloaded:
                            continue
                        data, content_type = downloaded
                        if len(data) > max_bytes:
                            logger.info(
                                "Skipping oversized BlueBubbles attachment %s (%d bytes)",
                                att_guid,
                                len(data),
                            )
                            continue
                        filename = att.get("transferName") or f"{att_guid}.bin"
                        files_to_send.append((filename, data))

                    if not text:
                        if attachments:
                            text = ""
                        else:
                            self.db.record_message(
                                matrix_event_id=msg_guid,
                                discord_message_id=0,
                                channel_id=0,
                                sender_id=sender_id,
                            )
                            continue

                    if not text and not files_to_send:
                        self.db.record_message(
                            matrix_event_id=msg_guid,
                            discord_message_id=0,
                            channel_id=0,
                            sender_id=sender_id,
                        )
                        continue

                    if self.on_message_callback:
                        try:
                            await self.on_message_callback(
                                chat_id=chat_guid,
                                chat_title=chat_title,
                                sender_name=sender_name,
                                sender_id=sender_id,
                                text=text,
                                files=files_to_send,
                                is_group=is_group,
                                msg_id=msg_guid,
                                is_self=is_from_me,
                            )
                            self.db.record_message(
                                matrix_event_id=msg_guid,
                                discord_message_id=0,
                                channel_id=0,
                                sender_id=sender_id,
                            )
                        except Exception as e:
                            logger.error(
                                "Error dispatching BlueBubbles message to Discord: %s",
                                e,
                            )

                is_initial_poll = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in BlueBubbles sync loop: %s", e, exc_info=True)

            await asyncio.sleep(poll_interval)
