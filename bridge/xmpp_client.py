"""XMPP / Jabber client integration using slixmpp."""

import asyncio
import hashlib
import json
import logging
import os
import sys
import urllib.parse
from pathlib import Path
from typing import Optional, Callable, Dict, Any, List, Tuple, FrozenSet, Set
import aiohttp
import slixmpp
from slixmpp.jid import JID

from .config import XMPPConfig

logger = logging.getLogger("beeper_bridge.xmpp")

try:
    import oldmemo  # noqa: F401  (registers the OMEMO:1 backend on import)
    import twomemo  # noqa: F401  (registers the OMEMO:2 backend on import)
    from omemo.storage import Just, Maybe, Nothing, Storage
    from omemo.types import DeviceInformation, JSONType
    from slixmpp.plugins import register_plugin
    from slixmpp_omemo import TrustLevel, XEP_0384

    OMEMO_LIBS_AVAILABLE = True
except ImportError as e:
    OMEMO_LIBS_AVAILABLE = False
    logger.warning(
        "OMEMO libraries not installed (%s); XMPP messages will be sent/received "
        "in plaintext only. Install slixmpp-omemo, oldmemo and twomemo to enable "
        "OMEMO support.",
        e,
    )

if OMEMO_LIBS_AVAILABLE:

    class _OmemoJSONStorage(Storage):
        """Persists OMEMO device/session/trust state to a single JSON file."""

        def __init__(self, json_file_path: Path) -> None:
            super().__init__()
            self.__json_file_path = json_file_path
            self.__data: Dict[str, JSONType] = {}
            try:
                with open(self.__json_file_path, encoding="utf8") as f:
                    self.__data = json.load(f)
            except Exception:
                pass

        async def _load(self, key: str) -> "Maybe[JSONType]":
            if key in self.__data:
                return Just(self.__data[key])
            return Nothing()

        async def _store(self, key: str, value: "JSONType") -> None:
            self.__data[key] = value
            os.makedirs(os.path.dirname(self.__json_file_path) or ".", exist_ok=True)
            with open(self.__json_file_path, "w", encoding="utf8") as f:
                json.dump(self.__data, f)

        async def _delete(self, key: str) -> None:
            self.__data.pop(key, None)
            os.makedirs(os.path.dirname(self.__json_file_path) or ".", exist_ok=True)
            with open(self.__json_file_path, "w", encoding="utf8") as f:
                json.dump(self.__data, f)

    class _XEP_0384Impl(XEP_0384):
        """OMEMO plugin wiring: JSON-backed storage, blind trust-before-verify."""

        default_config = {
            "fallback_message": "This message is OMEMO encrypted.",
            "json_file_path": None,
        }

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.__storage: Storage

        def plugin_init(self) -> None:
            if not self.json_file_path:
                raise RuntimeError("OMEMO json_file_path not specified.")
            self.__storage = _OmemoJSONStorage(Path(self.json_file_path))
            super().plugin_init()

        @property
        def storage(self) -> Storage:
            return self.__storage

        @property
        def _btbv_enabled(self) -> bool:
            # Blind trust-before-verify: trust new devices on first contact,
            # like every mainstream OMEMO client does by default. There is no
            # interactive UI on a bridge bot to do manual fingerprint checks.
            return True

        async def _devices_blindly_trusted(
            self,
            blindly_trusted: "FrozenSet[DeviceInformation]",
            identifier: Optional[str],
        ) -> None:
            logger.info("OMEMO devices blindly trusted for %s: %s", identifier, blindly_trusted)

        async def _prompt_manual_trust(
            self,
            manually_trusted: "FrozenSet[DeviceInformation]",
            identifier: Optional[str],
        ) -> None:
            # BTBV is enabled above, so this should never actually be reached.
            logger.warning(
                "OMEMO manual trust prompt requested for %s (no interactive UI available): %s",
                identifier,
                manually_trusted,
            )

    register_plugin(_XEP_0384Impl)


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

        self.omemo_available = OMEMO_LIBS_AVAILABLE and config.omemo_enabled
        if self.omemo_available:
            self.register_plugin("xep_0060")  # PubSub (OMEMO device list storage)
            self.register_plugin("xep_0163")  # Personal Eventing Protocol (PEP)
            self.register_plugin("xep_0380")  # Explicit Message Encryption
            self.register_plugin(
                "xep_0384",
                {"json_file_path": config.omemo_data_path},
                module=sys.modules[__name__],
            )  # OMEMO
            logger.info(
                "OMEMO support enabled for XMPP (data file: %s)",
                config.omemo_data_path,
            )
        elif config.omemo_enabled and not OMEMO_LIBS_AVAILABLE:
            logger.warning(
                "OMEMO was requested in config but the required libraries are not "
                "installed; falling back to plaintext XMPP."
            )

        # Register event handlers
        self.add_event_handler("session_start", self._on_session_start)
        self.add_event_handler("message", self._on_message)
        self.add_event_handler("groupchat_message", self._on_groupchat_message)
        self.add_event_handler("carbon_received", self._on_carbon_received)
        self.add_event_handler("carbon_sent", self._on_carbon_sent)
        self.add_event_handler("disconnected", self._on_disconnected)
        self.add_event_handler("presence_subscribe", self._on_presence_subscribe)

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

    async def _on_presence_subscribe(self, presence):
        """Auto-accept subscription requests so contacts can always reach us.

        OMEMO device-bundle lookups go through PEP/PubSub, which requires a
        mutual roster subscription; refusing/ignoring requests here silently
        blocks OMEMO from working for that contact.
        """
        from_jid = presence["from"]
        try:
            logger.info("Received XMPP subscription request from %s, auto-approving", from_jid)
            self.send_presence_subscription(pto=from_jid, ptype="subscribed")
            self.send_presence_subscription(pto=from_jid, ptype="subscribe")
        except Exception as e:
            logger.error("Error auto-approving subscription from %s: %s", from_jid, e)

    def get_contact_display_name(self, jid_str: str) -> str:
        """Get the clean display name for a contact from roster or JID."""
        bare_jid = jid_str.split("/")[0].lower()
        try:
            if bare_jid in self.client_roster:
                name = self.client_roster[bare_jid]["name"]
                if name:
                    return name
        except Exception as e:
            logger.debug("Could not look up roster name for %s: %s", bare_jid, e)
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

    async def _extract_body(self, msg) -> Optional[str]:
        """Return the plaintext body of a message stanza, decrypting it first
        if it carries OMEMO-encrypted content."""
        if self.omemo_available:
            xep_0384 = self["xep_0384"]
            if xep_0384:
                try:
                    namespaces = xep_0384.is_encrypted(msg)
                except Exception as e:
                    logger.error("is_encrypted() raised for %s: %s", msg["from"], e, exc_info=True)
                    namespaces = set()
                logger.debug(
                    "_extract_body: namespaces=%s body=%r for msg from %s",
                    namespaces,
                    msg["body"],
                    msg["from"],
                )
                if namespaces:
                    logger.debug(
                        "Decrypting OMEMO message (namespaces=%s) from %s",
                        namespaces,
                        msg["from"],
                    )
                    try:
                        decrypted, device_info = await asyncio.wait_for(
                            xep_0384.decrypt_message(msg), timeout=20
                        )
                        logger.debug(
                            "Decrypted OMEMO message (namespaces=%s) from device: %s",
                            namespaces,
                            device_info,
                        )
                        return decrypted["body"] or None
                    except asyncio.TimeoutError:
                        logger.error(
                            "Timed out decrypting OMEMO message from %s after 20s",
                            msg["from"],
                        )
                        return None
                    except Exception as e:
                        logger.error(
                            "Failed to decrypt OMEMO message from %s: %s",
                            msg["from"],
                            e,
                            exc_info=True,
                        )
                        return None
        return msg["body"] or None

    async def _on_message(self, msg):
        """Handle incoming direct 1-on-1 chat messages."""
        if msg["type"] in ("chat", "normal"):
            body = await self._extract_body(msg)
            if not body:
                return
            from_jid = str(msg["from"].bare)
            sender_nick = self.get_contact_display_name(from_jid)
            avatar_url = self.get_contact_avatar_url(from_jid, sender_nick)

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
        room_jid = str(msg["from"].bare)
        sender_nick = msg["from"].resource or "Anonymous"

        # Skip self-messages in group chats
        if sender_nick == self.boundjid.user:
            return

        body = await self._extract_body(msg)
        if body:
            avatar_url = self.get_contact_avatar_url(str(msg["from"]), sender_nick)

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
        logger.debug("carbon_received event fired")
        try:
            forwarded = msg["carbon_received"]["forwarded"]["message"]
            body = await self._extract_body(forwarded)
            if body:
                from_jid = str(forwarded["from"].bare)
                sender_nick = self.get_contact_display_name(from_jid)
                avatar_url = self.get_contact_avatar_url(from_jid, sender_nick)

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
            logger.error("Error handling carbon_received: %s", e, exc_info=True)

    async def _on_carbon_sent(self, msg):
        """Handle carbon copy of outgoing message sent from another client."""
        logger.debug("carbon_sent event fired")
        try:
            forwarded = msg["carbon_sent"]["forwarded"]["message"]
            body = await self._extract_body(forwarded)
            if body:
                to_jid = str(forwarded["to"].bare)
                sender_nick = f"{self.boundjid.user} (You)"

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
            logger.error("Error handling carbon_sent: %s", e, exc_info=True)

    async def _send_encrypted(self, recipient_jid: str, body: str, mtype: str) -> bool:
        """Try to send body as an OMEMO-encrypted message. Returns False (and
        logs) if encryption isn't possible, so the caller can fall back to
        plaintext rather than silently dropping the message."""
        xep_0384 = self["xep_0384"]
        if not xep_0384:
            return False
        try:
            stanza = self.make_message(mto=recipient_jid, mtype=mtype)
            stanza["body"] = body
            message, encryption_errors = await xep_0384.encrypt_message(
                stanza, {JID(recipient_jid)}
            )
            if encryption_errors:
                logger.info(
                    "Non-critical OMEMO encryption errors for %s: %s",
                    recipient_jid,
                    encryption_errors,
                )
            if message is None:
                logger.warning(
                    "OMEMO encryption produced no message for %s, falling back to plaintext",
                    recipient_jid,
                )
                return False
            message.send()
            return True
        except Exception as e:
            logger.warning(
                "OMEMO encryption failed for %s, falling back to plaintext: %s",
                recipient_jid,
                e,
            )
            return False

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
            sent = False
            if self.omemo_available and not is_groupchat:
                sent = await self._send_encrypted(recipient_jid, body, mtype)
            if not sent:
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
