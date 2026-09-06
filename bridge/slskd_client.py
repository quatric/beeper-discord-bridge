"""SLSKD Soulseek client integration using SLSKD REST API."""

import asyncio
import json
import logging
import time
import urllib.parse
import uuid
from typing import Optional, Callable, Dict, Any, List, Set
import aiohttp

from .config import SLSKDConfig
from .database import Database

logger = logging.getLogger("beeper_bridge.slskd")


class SLSKDBridgeClient:
    """Async client connecting to SLSKD Soulseek REST API."""

    def __init__(
        self,
        config: SLSKDConfig,
        db: Database,
        on_message_callback: Optional[Callable] = None,
    ):
        self.config = config
        self.db = db
        self.on_message_callback = on_message_callback
        self.base_url = config.url.rstrip("/")
        self.api_key = config.api_key

        self.is_running = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._sync_task: Optional[asyncio.Task] = None
        self._processed_msg_ids: Set[str] = set()

    def _headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session and self._session.closed:
            self._session = None
        if not self._session:
            connector = aiohttp.TCPConnector(
                force_close=True, enable_cleanup_closed=True
            )
            timeout = aiohttp.ClientTimeout(total=20, connect=5)
            self._session = aiohttp.ClientSession(
                connector=connector, timeout=timeout, headers=self._headers()
            )
        return self._session

    async def start(self):
        """Start SLSKD message polling loop."""
        self.is_running = True
        logger.info("Connecting to SLSKD API at %s...", self.base_url)

        try:
            session = await self._ensure_session()
            async with session.get(f"{self.base_url}/api/v0/application") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    version_info = data.get("version", {})
                    version = (
                        version_info.get("current", "unknown")
                        if isinstance(version_info, dict)
                        else str(version_info)
                    )
                    user_info = data.get("user", {})
                    username = (
                        user_info.get("username", "unknown")
                        if isinstance(user_info, dict)
                        else ""
                    )
                    logger.info(
                        "✅ Connected to SLSKD API (Version: %s, User: %s)",
                        version,
                        username,
                    )
                elif resp.status == 401:
                    logger.error(
                        "SLSKD authentication failed (HTTP 401). Please check api_key in config.yaml"
                    )
                else:
                    logger.warning("SLSKD API check returned HTTP %d", resp.status)
        except Exception as e:
            logger.error("Failed connecting to SLSKD API: %s", e)
            logger.info("Will continue retrying in background loop...")

        self._sync_task = asyncio.create_task(self._sync_loop())

    async def stop(self):
        """Stop SLSKD message polling loop."""
        self.is_running = False
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        logger.info("SLSKD client stopped.")

    async def _sync_loop(self):
        """Main polling loop fetching new private messages and room chats."""
        interval = max(2, self.config.poll_interval_seconds)
        conn_error_logged = False
        is_initial_poll = True

        while self.is_running:
            try:
                if self.config.sync_private:
                    await self._sync_private_messages(is_initial_poll=is_initial_poll)
                if self.config.sync_rooms:
                    await self._sync_room_messages(is_initial_poll=is_initial_poll)
                if is_initial_poll:
                    logger.info("✅ Initial SLSKD history scan complete. Live sync active.")
                    is_initial_poll = False
                conn_error_logged = False
            except asyncio.CancelledError:
                break
            except (
                aiohttp.ClientConnectorError,
                aiohttp.ClientConnectionError,
                ConnectionError,
            ) as e:
                if not conn_error_logged:
                    logger.warning(
                        "Unable to reach SLSKD API at %s: %s (will retry in background)",
                        self.base_url,
                        e,
                    )
                    conn_error_logged = True
            except Exception as e:
                logger.error("Error in SLSKD sync loop: %s", e)

            await asyncio.sleep(interval)

    async def _sync_private_messages(self, is_initial_poll: bool = False):
        """Fetch private conversations and relay new messages."""
        session = await self._ensure_session()
        url = f"{self.base_url}/api/v0/conversations"

        async with session.get(url) as resp:
            if resp.status != 200:
                return
            conversations = await resp.json()

        if not isinstance(conversations, list):
            return

        for conv in conversations:
            if not isinstance(conv, dict):
                continue
            username = conv.get("username")
            if not username or username == "..":
                continue

            user_url = f"{self.base_url}/api/v0/conversations/{urllib.parse.quote(username)}/messages"
            try:
                async with session.get(user_url) as user_resp:
                    if user_resp.status == 200:
                        messages = await user_resp.json()
                    else:
                        messages = []
            except Exception:
                messages = []

            if not isinstance(messages, list):
                continue

            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                msg_id = str(msg.get("id") or "")
                raw_text = msg.get("message") or msg.get("text") or ""
                if not raw_text:
                    continue

                direction = msg.get("direction", "In")
                is_self = direction in (
                    1,
                    "Out",
                    "Outgoing",
                    "out",
                    "outgoing",
                ) or msg.get("isOutgoing", False)

                if is_self and not self.config.sync_self_messages:
                    continue

                timestamp = msg.get("timestamp") or time.time()
                unique_key = f"slsk_pm_{username}_{msg_id}_{timestamp}_{raw_text[:20]}"

                if unique_key in self._processed_msg_ids:
                    continue
                if msg_id and msg_id != "0" and self.db.is_message_recorded(msg_id):
                    continue

                self._processed_msg_ids.add(unique_key)
                if len(self._processed_msg_ids) > 10000:
                    self._processed_msg_ids.clear()

                record_id = msg_id if (msg_id and msg_id != "0") else unique_key

                # Suppress re-sending history on initial startup
                if is_initial_poll:
                    self.db.record_message(
                        matrix_event_id=record_id,
                        discord_message_id=0,
                        channel_id=0,
                        sender_id="me" if is_self else username,
                    )
                    continue

                logger.info(
                    "Received SLSKD private message %s %s: %s",
                    "to" if is_self else "from",
                    username,
                    raw_text[:50],
                )

                if self.on_message_callback:
                    try:
                        await self.on_message_callback(
                            chat_id=username,
                            chat_title=username,
                            sender_name=username,
                            text=raw_text,
                            is_room=False,
                            msg_id=record_id,
                            is_self=is_self,
                        )
                    except Exception as e:
                        logger.error("Error in SLSKD private message callback: %s", e)

    async def _sync_room_messages(self, is_initial_poll: bool = False):
        """Fetch joined room chats and relay new messages."""
        session = await self._ensure_session()
        rooms_url = f"{self.base_url}/api/v0/rooms/joined"

        async with session.get(rooms_url) as resp:
            if resp.status != 200:
                return
            rooms = await resp.json()

        if not isinstance(rooms, list):
            return

        for room_info in rooms:
            room_name = (
                room_info.get("name")
                or room_info.get("roomName")
                or (room_info if isinstance(room_info, str) else "")
            )
            if not room_name:
                continue

            msg_url = f"{self.base_url}/api/v0/rooms/joined/{urllib.parse.quote(room_name)}/messages"
            try:
                async with session.get(msg_url) as msg_resp:
                    if msg_resp.status != 200:
                        continue
                    room_messages = await msg_resp.json()
            except Exception:
                continue

            if not isinstance(room_messages, list):
                continue

            for msg in room_messages:
                if not isinstance(msg, dict):
                    continue
                msg_id = str(msg.get("id") or "")
                sender = msg.get("username") or "Anonymous"
                raw_text = msg.get("message") or msg.get("text") or ""
                if not raw_text:
                    continue

                timestamp = msg.get("timestamp") or time.time()
                unique_key = f"slsk_rm_{room_name}_{msg_id}_{timestamp}_{sender}_{raw_text[:20]}"

                if unique_key in self._processed_msg_ids:
                    continue
                if msg_id and msg_id != "0" and self.db.is_message_recorded(msg_id):
                    continue

                self._processed_msg_ids.add(unique_key)
                if len(self._processed_msg_ids) > 10000:
                    self._processed_msg_ids.clear()

                record_id = msg_id if (msg_id and msg_id != "0") else unique_key

                # Suppress re-sending history on initial startup
                if is_initial_poll:
                    self.db.record_message(
                        matrix_event_id=record_id,
                        discord_message_id=0,
                        channel_id=0,
                        sender_id=sender,
                    )
                    continue

                logger.info(
                    "Received SLSKD room message in #%s from %s: %s",
                    room_name,
                    sender,
                    raw_text[:50],
                )

                if self.on_message_callback:
                    try:
                        await self.on_message_callback(
                            chat_id=f"room:{room_name}",
                            chat_title=f"#{room_name}",
                            sender_name=sender,
                            text=raw_text,
                            is_room=True,
                            msg_id=record_id,
                            is_self=False,
                        )
                    except Exception as e:
                        logger.error("Error in SLSKD room message callback: %s", e)

    async def send_private_message(self, username: str, text: str) -> Optional[str]:
        """Send a private message to a Soulseek user."""
        try:
            session = await self._ensure_session()
            url = f"{self.base_url}/api/v0/conversations/{urllib.parse.quote(username)}"
            headers = self._headers()
            headers["Content-Type"] = "application/json"

            async with session.post(url, data=json.dumps(text), headers=headers) as resp:
                if resp.status in (200, 201, 204):
                    logger.info(
                        "Sent SLSKD private message to %s: %s", username, text[:50]
                    )
                    msg_id = f"slsk_pm_sent_{uuid.uuid4().hex[:12]}"
                    self._processed_msg_ids.add(
                        f"slsk_pm_{username}_{msg_id}_{time.time()}_{text[:20]}"
                    )
                    return msg_id
                else:
                    logger.error(
                        "Failed sending SLSKD PM to %s: HTTP %d %s",
                        username,
                        resp.status,
                        await resp.text(),
                    )
                    return None
        except Exception as e:
            logger.error("Error sending SLSKD PM to %s: %s", username, e)
            return None

    async def send_room_message(self, room_name: str, text: str) -> Optional[str]:
        """Send a message to a Soulseek room."""
        try:
            session = await self._ensure_session()
            url = f"{self.base_url}/api/v0/rooms/joined/{urllib.parse.quote(room_name)}/messages"
            headers = self._headers()
            headers["Content-Type"] = "application/json"

            async with session.post(url, data=json.dumps(text), headers=headers) as resp:
                if resp.status in (200, 201, 204):
                    logger.info(
                        "Sent SLSKD room message in #%s: %s", room_name, text[:50]
                    )
                    msg_id = f"slsk_rm_sent_{uuid.uuid4().hex[:12]}"
                    self._processed_msg_ids.add(
                        f"slsk_rm_{room_name}_{msg_id}_{time.time()}_{text[:20]}"
                    )
                    return msg_id
                else:
                    logger.error(
                        "Failed sending SLSKD room message in #%s: HTTP %d %s",
                        room_name,
                        resp.status,
                        await resp.text(),
                    )
                    return None
        except Exception as e:
            logger.error("Error sending SLSKD room message in #%s: %s", room_name, e)
            return None
