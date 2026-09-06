"""XMPP / Jabber client integration using slixmpp."""

import asyncio
import hashlib
import logging
import urllib.parse
from typing import Optional, Callable, Dict, Any, List, Tuple
import aiohttp
import slixmpp

from .config import XMPPConfig

logger = logging.getLogger("beeper_bridge.xmpp")


class XMPPBridgeClient(slixmpp.ClientXMPP):
    """Async XMPP client for bridging Jabber conversations with Discord."""

    def __init__(
        self,
        config: XMPPConfig,
        on_message_callback: Optional[Callable] = None,
        on_room_discovered: Optional[Callable] = None,
    ):
        jid = config.jid
        password = config.password
        super().__init__(jid, password)

        self.config = config
        self.use_tls = config.use_tls
        if not self.use_tls:
            self.enable_direct_tls = False
            self.enable_starttls = False
            self.enable_plaintext = True
        self.on_message_callback = on_message_callback
        self.on_room_discovered = on_room_discovered
        self.is_connected_event = asyncio.Event()

        # Register standard XMPP extensions (XEPs)
        self.register_plugin("xep_0030")  # Service Discovery
        self.register_plugin("xep_0045")  # Multi-User Chat (MUC)
        self.register_plugin("xep_0085")  # Chat State Notifications
        self.register_plugin("xep_0199")  # XMPP Ping
        self.register_plugin("xep_0078")  # Non-SASL Authentication
        self.register_plugin("xep_0280")  # Message Carbons

        # Register event handlers
        self.add_event_handler("session_start", self._on_session_start)
        self.add_event_handler("message", self._on_message)
        self.add_event_handler("groupchat_message", self._on_groupchat_message)
        self.add_event_handler("carbon_received", self._on_carbon_received)
        self.add_event_handler("carbon_sent", self._on_carbon_sent)
        self.add_event_handler("disconnected", self._on_disconnected)

    async def _on_session_start(self, event):
        """Handle session establishment and presence announcement."""
        self.send_presence(pstatus=self.config.status_message)
        try:
            await self.get_roster()
        except Exception as e:
            logger.warning("Could not fetch XMPP roster: %s", e)
        try:
            await self.plugin["xep_0280"].enable()
            logger.info("Enabled XMPP Message Carbons (XEP-0280)")
        except Exception as e:
            logger.debug("Could not enable XMPP Message Carbons: %s", e)
        self.is_connected_event.set()
        logger.info("XMPP connected successfully for JID %s", self.boundjid.bare)

    async def _on_disconnected(self, event):
        """Handle disconnection."""
        self.is_connected_event.clear()
        logger.warning("XMPP client disconnected.")

    def get_contact_display_name(self, jid_str: str) -> str:
        """Get the clean display name for a contact from roster or JID."""
        bare_jid = jid_str.split("/")[0].lower()
        if hasattr(self, "client_roster") and self.client_roster:
            roster_entry = self.client_roster.get(bare_jid)
            if roster_entry and roster_entry.get("name"):
                return roster_entry["name"]
        user = bare_jid.split("@")[0]
        return user.capitalize() if user else jid_str

    def get_contact_avatar_url(self, jid_str: str, display_name: str) -> str:
        """Get the Libravatar / Gravatar / initial avatar URL for an XMPP JID."""
        bare_jid = jid_str.split("/")[0].lower()
        jid_hash = hashlib.md5(bare_jid.encode("utf-8")).hexdigest()
        encoded_name = urllib.parse.quote(display_name)
        fallback_url = urllib.parse.quote(
            f"https://ui-avatars.com/api/{encoded_name}/256/39D9B7/ffffff"
        )
        return f"https://seccdn.libravatar.org/avatar/{jid_hash}?s=256&d={fallback_url}"

    async def _try_download_image(self, url: str) -> Optional[Tuple[str, bytes]]:
        """Download image if URL points to an image/media file."""
        if not (url.startswith("http://") or url.startswith("https://")):
            return None
        lower = url.lower().split("?")[0]
        if any(
            lower.endswith(ext)
            for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4"]
        ):
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(
                        url, timeout=aiohttp.ClientTimeout(total=15)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if len(data) <= 15 * 1024 * 1024:
                                fname = lower.split("/")[-1] or "image.png"
                                return fname, data
            except Exception as e:
                logger.debug("Could not download XMPP image URL %s: %s", url, e)
        return None

    async def _on_message(self, msg):
        """Handle incoming direct 1-on-1 chat messages."""
        if msg["type"] in ("chat", "normal") and msg["body"]:
            from_jid = str(msg["from"].bare)
            sender_nick = self.get_contact_display_name(from_jid)
            avatar_url = self.get_contact_avatar_url(from_jid, sender_nick)
            body = msg["body"]

            files_to_send: List[Tuple[str, bytes]] = []
            img_dl = await self._try_download_image(body.strip())
            if img_dl:
                files_to_send.append(img_dl)

            logger.info("Received XMPP message from %s: %s", from_jid, body[:50])
            if self.on_message_callback:
                try:
                    await self.on_message_callback(
                        sender_jid=from_jid,
                        sender_name=sender_nick,
                        body=body,
                        is_groupchat=False,
                        room_jid=from_jid,
                        avatar_url=avatar_url,
                        files=files_to_send,
                        is_self=False,
                    )
                except Exception as e:
                    logger.error(
                        "Error processing XMPP message callback: %s", e, exc_info=True
                    )

    async def _on_groupchat_message(self, msg):
        """Handle incoming Multi-User Chat (MUC) messages."""
        if msg["body"]:
            room_jid = str(msg["from"].bare)
            sender_nick = msg["from"].resource or "Anonymous"

            # Skip self-messages in group chats
            if sender_nick == self.boundjid.user:
                return

            avatar_url = self.get_contact_avatar_url(str(msg["from"]), sender_nick)
            body = msg["body"]

            files_to_send: List[Tuple[str, bytes]] = []
            img_dl = await self._try_download_image(body.strip())
            if img_dl:
                files_to_send.append(img_dl)

            logger.info(
                "Received XMPP MUC message from %s in %s: %s",
                sender_nick,
                room_jid,
                body[:50],
            )
            if self.on_message_callback:
                try:
                    await self.on_message_callback(
                        sender_jid=str(msg["from"]),
                        sender_name=sender_nick,
                        body=body,
                        is_groupchat=True,
                        room_jid=room_jid,
                        avatar_url=avatar_url,
                        files=files_to_send,
                        is_self=False,
                    )
                except Exception as e:
                    logger.error(
                        "Error processing XMPP MUC callback: %s", e, exc_info=True
                    )

    async def _on_carbon_received(self, msg):
        """Handle carbon copy of incoming message delivered to another client."""
        try:
            forwarded = msg["carbon_received"]["forwarded"]["message"]
            if forwarded["body"]:
                from_jid = str(forwarded["from"].bare)
                sender_nick = self.get_contact_display_name(from_jid)
                avatar_url = self.get_contact_avatar_url(from_jid, sender_nick)
                body = forwarded["body"]

                files_to_send: List[Tuple[str, bytes]] = []
                img_dl = await self._try_download_image(body.strip())
                if img_dl:
                    files_to_send.append(img_dl)

                logger.info(
                    "Received XMPP carbon message from %s: %s", from_jid, body[:50]
                )
                if self.on_message_callback:
                    await self.on_message_callback(
                        sender_jid=from_jid,
                        sender_name=sender_nick,
                        body=body,
                        is_groupchat=False,
                        room_jid=from_jid,
                        avatar_url=avatar_url,
                        files=files_to_send,
                        is_self=False,
                    )
        except Exception as e:
            logger.debug("Error handling carbon_received: %s", e)

    async def _on_carbon_sent(self, msg):
        """Handle carbon copy of outgoing message sent from another client."""
        try:
            forwarded = msg["carbon_sent"]["forwarded"]["message"]
            if forwarded["body"]:
                to_jid = str(forwarded["to"].bare)
                sender_nick = f"{self.boundjid.user} (You)"
                body = forwarded["body"]

                files_to_send: List[Tuple[str, bytes]] = []
                img_dl = await self._try_download_image(body.strip())
                if img_dl:
                    files_to_send.append(img_dl)

                logger.info("Received XMPP carbon sent to %s: %s", to_jid, body[:50])
                if self.on_message_callback:
                    await self.on_message_callback(
                        sender_jid=to_jid,
                        sender_name=sender_nick,
                        body=body,
                        is_groupchat=False,
                        room_jid=to_jid,
                        avatar_url="",
                        files=files_to_send,
                        is_self=True,
                    )
        except Exception as e:
            logger.debug("Error handling carbon_sent: %s", e)

    async def send_chat_message(
        self, recipient_jid: str, body: str, is_groupchat: bool = False
    ) -> bool:
        """Send a message to a direct contact JID or MUC room.

        Returns False (and logs) instead of silently swallowing a send made
        while the stream is down, so Discord doesn't add a success checkmark
        for a message that never actually reached the XMPP server.
        """
        if not self.is_connected_event.is_set():
            logger.warning(
                "Dropped outgoing XMPP message to %s: client is not connected",
                recipient_jid,
            )
            return False
        mtype = "groupchat" if is_groupchat else "chat"
        try:
            self.send_message(mto=recipient_jid, mbody=body, mtype=mtype)
        except Exception as e:
            logger.error(
                "Error sending XMPP message to %s: %s", recipient_jid, e, exc_info=True
            )
            return False
        logger.info("Sent XMPP message to %s (%s): %s", recipient_jid, mtype, body[:50])
        return True

    async def start_client(self):
        """Initiate connection to the XMPP server."""
        server = self.config.server
        port = self.config.port

        logger.info(
            "Connecting XMPP client (JID: %s, host: %s, port: %s)...",
            self.config.jid,
            server or "DNS-SRV",
            port if server else "auto",
        )
        if server:
            self.connect(host=server, port=port)
        else:
            self.connect()

    async def stop_client(self):
        """Gracefully disconnect from XMPP."""
        self.disconnect()
        logger.info("XMPP client stopped.")
