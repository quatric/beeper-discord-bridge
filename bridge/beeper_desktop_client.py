"""Beeper Desktop API client integration for official local Beeper server/desktop."""

import asyncio
import logging
import os
import urllib.parse
from typing import Optional, Callable, Dict, Any, List, Tuple
from datetime import datetime, timezone
import aiohttp

from beeper_desktop_api import AsyncBeeperDesktop
from beeper_desktop_api.types import Chat, Message, Attachment

from .config import BeeperDesktopConfig
from .database import Database

logger = logging.getLogger("beeper_bridge.beeper_desktop")

NETWORK_COLORS: Dict[str, str] = {
    "whatsapp": "25D366",
    "telegram": "229ED9",
    "instagram": "E1306C",
    "signal": "3A76F0",
    "googlemessages": "1A73E8",
    "sms": "1A73E8",
    "rcs": "1A73E8",
    "discord": "5865F2",
    "slack": "4A154B",
    "linkedin": "0A66C2",
    "twitter": "000000",
    "matrix": "00B159",
}


class BeeperDesktopBridgeClient:
    """Async client connecting to Beeper Desktop / Headless Server API."""

    def __init__(
        self,
        config: BeeperDesktopConfig,
        db: Database,
        on_message_callback: Optional[Callable] = None,
    ):
        self.config = config
        self.db = db
        self.on_message_callback = on_message_callback
        self.client: Optional[AsyncBeeperDesktop] = None
        self._sync_task: Optional[asyncio.Task] = None
        self._running = False
        self._last_message_sort_keys: Dict[str, str] = {}
        self._known_chat_names: Dict[str, str] = {}

    def _init_client(self):
        """Initialize AsyncBeeperDesktop client instance."""
        base_url = self.config.api_url.rstrip("/")
        self.client = AsyncBeeperDesktop(
            access_token=self.config.access_token,
            base_url=base_url,
        )

    async def start(self):
        """Start polling loop for Beeper Desktop API."""
        if not self.config.access_token:
            logger.warning(
                "Beeper Desktop API enabled, but no access_token provided. Please configure access_token in config.yaml."
            )
            return

        self._init_client()
        self._running = True
        logger.info("Connecting to Beeper Desktop API at %s...", self.config.api_url)

        try:
            # Test connection
            accounts = await self.client.accounts.list()
            account_names = [
                f"{getattr(a, 'network', '')} ({getattr(a, 'account_id', '')})"
                for a in (
                    accounts
                    if isinstance(accounts, list)
                    else getattr(accounts, "items", [])
                )
            ]
            logger.info(
                "✅ Connected to Beeper Desktop API! Connected accounts: %s",
                ", ".join(account_names) if account_names else "None",
            )
        except Exception as e:
            logger.error("Failed connecting to Beeper Desktop API: %s", e)
            logger.info("Will continue retrying in background loop...")

        self._sync_task = asyncio.create_task(self._sync_loop())

    async def stop(self):
        """Stop Beeper Desktop sync loop."""
        self._running = False
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        if self.client:
            await self.client.close()
        logger.info("Beeper Desktop client stopped.")

    async def download_attachment(
        self, attachment: Attachment
    ) -> Optional[Tuple[str, bytes]]:
        """Download attachment bytes from Beeper Desktop."""
        try:
            # 1. If src_url points to a local file or HTTP URL
            if attachment.src_url:
                src = str(attachment.src_url)
                if src.startswith("file://"):
                    file_path = src[7:]
                    if os.path.exists(file_path):
                        with open(file_path, "rb") as f:
                            data = f.read()
                        fname = (
                            attachment.file_name
                            or os.path.basename(file_path)
                            or "image.png"
                        )
                        return fname, data
                elif src.startswith("http://") or src.startswith("https://"):
                    async with aiohttp.ClientSession() as sess:
                        async with sess.get(src) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                fname = attachment.file_name or "image.png"
                                return fname, data

            # 2. Try client.assets.download
            att_url = attachment.id or attachment.src_url
            if self.client and att_url:
                resp = await self.client.assets.download(url=att_url)
                if resp and resp.src_url:
                    local_src = resp.src_url
                    if local_src.startswith("file://"):
                        local_src = local_src[7:]
                    if os.path.exists(local_src):
                        with open(local_src, "rb") as f:
                            data = f.read()
                        fname = (
                            attachment.file_name
                            or os.path.basename(local_src)
                            or "image.png"
                        )
                        return fname, data
        except Exception as e:
            logger.debug("Error downloading Beeper Desktop attachment: %s", e)
        return None

    async def _sync_loop(self):
        """Periodic sync loop polling for active chats and new messages."""
        logger.info("Beeper Desktop message watcher started.")
        is_initial_poll = True

        while self._running:
            try:
                await self._poll_chats_and_messages(is_initial_poll=is_initial_poll)
                is_initial_poll = False
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in Beeper Desktop poll cycle: %s", e, exc_info=True)

            await asyncio.sleep(self.config.poll_interval_seconds)

    async def _poll_chats_and_messages(self, is_initial_poll: bool = False):
        """Poll recent chats and fetch new messages."""
        if not self.client:
            return

        try:
            chats_page = await self.client.chats.list()
            chats: List[Chat] = (
                chats_page.items if hasattr(chats_page, "items") else list(chats_page)
            )
        except Exception as e:
            logger.warning("Could not list Beeper chats: %s", e)
            return

        for chat in chats:
            chat_id = chat.id
            network = (chat.network or "matrix").lower()
            title = chat.title or f"{network.capitalize()} Chat"
            avatar_url = chat.img_url or ""
            is_group = chat.type == "group"

            self._known_chat_names[chat_id] = title

            # Fetch recent messages for this chat
            try:
                msgs_page = await self.client.messages.list(chat_id=chat_id)
                messages: List[Message] = (
                    msgs_page.items if hasattr(msgs_page, "items") else list(msgs_page)
                )
            except Exception as e:
                logger.debug("Could not fetch messages for chat %s: %s", chat_id, e)
                continue

            # Sort ascending by sort_key or timestamp
            messages = sorted(
                messages, key=lambda m: m.sort_key or str(m.timestamp or "")
            )

            for msg in messages:
                msg_id = msg.id
                sort_key = msg.sort_key or str(msg.timestamp or "")

                # Record seen message
                if self.db.is_message_recorded(msg_id):
                    continue

                # Skip self messages if configured
                if msg.is_sender and not self.config.sync_self_messages:
                    self.db.record_message(
                        matrix_event_id=msg_id,
                        discord_message_id=0,
                        channel_id=0,
                        sender_id=msg.sender_id or "",
                    )
                    continue

                # Relay to Discord callback
                sender_name = msg.sender_name or (
                    "You" if msg.is_sender else (chat.title or "Unknown")
                )
                text = msg.text or ""

                resolved_avatar = avatar_url
                if not (resolved_avatar and str(resolved_avatar).startswith("http")):
                    color = NETWORK_COLORS.get(network.lower(), "39D9B7")
                    encoded_name = urllib.parse.quote(sender_name.replace(" (You)", ""))
                    resolved_avatar = (
                        f"https://ui-avatars.com/api/{encoded_name}/256/{color}/ffffff"
                    )

                files_to_send: List[Tuple[str, bytes]] = []
                if msg.attachments:
                    for att in msg.attachments:
                        dl = await self.download_attachment(att)
                        if dl:
                            files_to_send.append(dl)

                if self.on_message_callback:
                    try:
                        await self.on_message_callback(
                            chat_id=chat_id,
                            chat_title=title,
                            network=network,
                            sender_name=sender_name,
                            sender_id=msg.sender_id or "",
                            text=text,
                            avatar_url=resolved_avatar,
                            files=files_to_send,
                            is_group=is_group,
                            msg_id=msg_id,
                            is_self=bool(msg.is_sender),
                        )
                    except Exception as e:
                        logger.error(
                            "Error dispatching Beeper message to Discord: %s", e
                        )

                self.db.record_message(
                    matrix_event_id=msg_id,
                    discord_message_id=0,
                    channel_id=0,
                    sender_id=msg.sender_id or "",
                )

    async def send_chat_message(self, chat_id: str, text: str) -> Optional[str]:
        """Send message from Discord to Beeper Desktop API."""
        if not self.client:
            logger.error("Beeper Desktop client not initialized")
            return None

        try:
            res = await self.client.messages.send(chat_id=chat_id, text=text)
            msg_id = (
                getattr(res, "id", None)
                or f"sent_{datetime.now(timezone.utc).timestamp()}"
            )
            logger.info("Sent message to Beeper chat %s: %s", chat_id, text[:50])
            return msg_id
        except Exception as e:
            logger.error("Failed sending message to Beeper chat %s: %s", chat_id, e)
            return None
