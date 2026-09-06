"""Matrix / Beeper client integration using matrix-nio."""

import asyncio
import io
import logging
import mimetypes
import os
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional, Callable, Dict, Any, Tuple, List

import aiohttp
from nio import (
    AsyncClient,
    AsyncClientConfig,
    MatrixRoom,
    RoomMessageText,
    RoomMessageMedia,
    RoomMessageImage,
    RoomMessageFile,
    RoomMessageAudio,
    RoomMessageVideo,
    RoomMessageNotice,
    RoomMessageEmote,
    RoomMemberEvent,
    InviteMemberEvent,
    MegolmEvent,
    SyncResponse,
    LoginResponse,
    UploadResponse,
    RoomSendResponse,
    RoomKeyEvent,
    RoomKeyRequestError,
)
from nio.exceptions import EncryptionError, LocalProtocolError

from .config import MatrixConfig

logger = logging.getLogger("beeper_bridge.matrix")


class MatrixBridgeClient:
    def __init__(
        self,
        config: MatrixConfig,
        on_message_callback: Optional[Callable] = None,
        on_room_discovered: Optional[Callable] = None,
    ):
        self.config = config
        self.on_message_callback = on_message_callback
        self.on_room_discovered = on_room_discovered
        self.client: Optional[AsyncClient] = None
        self.is_running = False
        self._sync_task: Optional[asyncio.Task] = None
        self._media_session: Optional[aiohttp.ClientSession] = None
        self.initial_sync_event = asyncio.Event()
        self._initial_sync_complete = False
        self._pending_megolm_events: Dict[str, List[Tuple[MatrixRoom, MegolmEvent]]] = (
            {}
        )

    def _ensure_store_path(self):
        Path(self.config.store_path).mkdir(parents=True, exist_ok=True)

    async def initialize(self):
        """Initialize the matrix-nio AsyncClient."""
        self._ensure_store_path()
        self._media_session = aiohttp.ClientSession()

        client_config = AsyncClientConfig(
            max_limit_exceeded=0,
            max_timeouts=0,
            store_sync_tokens=False,
            encryption_enabled=self.config.encryption_enabled,
        )

        self.client = AsyncClient(
            homeserver=self.config.homeserver,
            user=self.config.user_id,
            device_id=self.config.device_id,
            store_path=self.config.store_path,
            config=client_config,
        )

        # Restore login session and load crypto store
        self.client.restore_login(
            user_id=self.config.user_id,
            device_id=self.config.device_id,
            access_token=self.config.access_token,
        )
        if self.config.encryption_enabled:
            self.client.load_store()

        # Register event callbacks
        self.client.add_event_callback(self._on_message_event, RoomMessageText)
        self.client.add_event_callback(self._on_message_event, RoomMessageMedia)
        self.client.add_event_callback(self._on_message_event, RoomMessageImage)
        self.client.add_event_callback(self._on_message_event, RoomMessageFile)
        self.client.add_event_callback(self._on_message_event, RoomMessageAudio)
        self.client.add_event_callback(self._on_message_event, RoomMessageVideo)
        self.client.add_event_callback(self._on_message_event, RoomMessageNotice)
        self.client.add_event_callback(self._on_message_event, RoomMessageEmote)
        self.client.add_event_callback(self._on_megolm_event, MegolmEvent)
        self.client.add_to_device_callback(self._on_room_key_event, RoomKeyEvent)
        self.client.add_event_callback(self._on_invite, InviteMemberEvent)

        logger.info(
            "Matrix client initialized for user %s on %s",
            self.config.user_id,
            self.config.homeserver,
        )

    async def start(self):
        """Start the Matrix sync loop in the background."""
        if not self.client:
            await self.initialize()

        self.is_running = True
        self._sync_task = asyncio.create_task(self._sync_forever())
        logger.info("Matrix sync loop started")

    async def wait_for_initial_sync(self, timeout: float = 30.0) -> bool:
        """Wait until initial full_state sync completes."""
        try:
            await asyncio.wait_for(self.initial_sync_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            logger.warning(
                "Timed out waiting for initial Matrix sync after %ss", timeout
            )
            return False

    async def stop(self):
        """Stop client and close connections."""
        self.is_running = False
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

        if self.client:
            await self.client.close()

        if self._media_session and not self._media_session.closed:
            await self._media_session.close()

        logger.info("Matrix client stopped")

    async def _sync_forever(self):
        """Continuously sync with the Matrix homeserver."""
        try:
            logger.info("Performing initial Matrix sync (full_state)...")
            response = await self.client.sync(
                timeout=30000,
                full_state=True,
                set_presence="online",
            )
            if isinstance(response, SyncResponse):
                logger.info(
                    "Initial sync completed. Discovered %d joined rooms.",
                    len(self.client.rooms),
                )
            else:
                logger.warning("Initial sync response: %s", response)
        except Exception as e:
            logger.error("Error during initial sync: %s", e, exc_info=True)
        finally:
            self._initial_sync_complete = True
            self.initial_sync_event.set()

        while self.is_running:
            try:
                response = await self.client.sync(
                    timeout=30000,
                    set_presence="online",
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Matrix sync error: %s. Retrying in 5s...", e)
                await asyncio.sleep(5)

    async def _on_invite(self, room: MatrixRoom, event: InviteMemberEvent):
        """Auto-accept room invites if invited."""
        if event.state_key == self.config.user_id:
            logger.info("Received invite to room %s. Auto-joining...", room.room_id)
            try:
                await self.client.join(room.room_id)
            except Exception as e:
                logger.error("Failed to auto-join room %s: %s", room.room_id, e)

    async def _on_message_event(self, room: MatrixRoom, event: Any):
        """Handle incoming Matrix message events."""
        is_self = event.sender == self.config.user_id
        if is_self and not self.config.sync_self_messages:
            logger.debug(
                "Skipping self-message %s in room %s", event.event_id, room.room_id
            )
            return

        if self.on_message_callback:
            try:
                await self.on_message_callback(room, event, is_self=is_self)
            except Exception as e:
                logger.error(
                    "Error handling matrix message callback for %s: %s",
                    event.event_id,
                    e,
                    exc_info=True,
                )

    async def _on_megolm_event(self, room: MatrixRoom, event: MegolmEvent):
        """Provision active encrypted rooms and request missing decryption keys."""
        # A full-state startup sync may contain old timeline events for hundreds of
        # rooms. Only live events should trigger on-demand channel provisioning.
        if not self._initial_sync_complete:
            return

        if self.on_room_discovered:
            try:
                await self.on_room_discovered(room.room_id, room, only_if_named=False)
            except Exception as e:
                logger.error(
                    "Error provisioning room for encrypted event %s: %s",
                    event.event_id,
                    e,
                    exc_info=True,
                )

        pending = self._pending_megolm_events.setdefault(event.session_id, [])
        if not any(item.event_id == event.event_id for _, item in pending):
            pending.append((room, event))

        if not self.client:
            return

        try:
            response = await self.client.request_room_key(event)
            if isinstance(response, RoomKeyRequestError):
                logger.warning(
                    "Room key request failed for encrypted event %s: %s",
                    event.event_id,
                    response,
                )
            else:
                logger.info(
                    "Requested missing room key for encrypted event %s in %s",
                    event.event_id,
                    room.room_id,
                )
        except LocalProtocolError:
            logger.debug(
                "Room key request already active for session %s", event.session_id
            )
        except Exception as e:
            logger.warning(
                "Could not request room key for encrypted event %s: %s",
                event.event_id,
                e,
            )

    async def _on_room_key_event(self, event: RoomKeyEvent):
        """Retry queued encrypted messages as soon as a requested key arrives."""
        pending = self._pending_megolm_events.pop(event.session_id, [])
        if not pending or not self.client:
            return

        for room, encrypted_event in pending:
            try:
                decrypted_event = self.client.decrypt_event(encrypted_event)
            except EncryptionError as e:
                logger.warning(
                    "Received a room key but still could not decrypt event %s: %s",
                    encrypted_event.event_id,
                    e,
                )
                self._pending_megolm_events.setdefault(event.session_id, []).append(
                    (room, encrypted_event)
                )
                continue

            logger.info(
                "Decrypted queued event %s after receiving its room key",
                encrypted_event.event_id,
            )
            await self._on_message_event(room, decrypted_event)

    # ---------------- Media & URL Helpers ---------------- #

    def mxc_to_http_url(self, mxc_url: Any) -> Optional[str]:
        """Convert a Matrix MXC URI (mxc://server/media_id) to an HTTP download URL."""
        if callable(mxc_url):
            try:
                mxc_url = mxc_url()
            except Exception:
                return None
        if (
            not mxc_url
            or not isinstance(mxc_url, str)
            or not mxc_url.startswith("mxc://")
        ):
            return None
        parts = mxc_url[6:].split("/", 1)
        if len(parts) != 2:
            return None
        server_name, media_id = parts
        homeserver = self.config.homeserver.rstrip("/")
        return f"{homeserver}/_matrix/media/r0/download/{server_name}/{media_id}"

    async def download_media(
        self, mxc_url: str, encryption_info: Optional[Dict[str, Any]] = None
    ) -> Optional[Tuple[bytes, str]]:
        """Download media from MXC URI (decrypting if encrypted) and return bytes and content type."""
        http_url = self.mxc_to_http_url(mxc_url)
        if not http_url:
            return None

        if not self._media_session or self._media_session.closed:
            self._media_session = aiohttp.ClientSession()

        headers = {}
        if self.config.access_token:
            headers["Authorization"] = f"Bearer {self.config.access_token}"

        try:
            async with self._media_session.get(http_url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    content_type = resp.headers.get(
                        "Content-Type", "application/octet-stream"
                    )

                    # Decrypt if encryption info is provided
                    if encryption_info:
                        try:
                            from nio.crypto.attachments import decrypt_attachment

                            key = encryption_info.get("key", {}).get("k")
                            hashes = encryption_info.get("hashes", {})
                            sha256 = hashes.get("sha256")
                            iv = encryption_info.get("iv")
                            if key and sha256 and iv:
                                decrypted = decrypt_attachment(data, key, sha256, iv)
                                return decrypted, content_type
                        except Exception as e:
                            logger.error("Error decrypting Matrix media: %s", e)

                    return data, content_type
                else:
                    logger.warning(
                        "Failed downloading media %s: HTTP %d", http_url, resp.status
                    )
                    return None
        except Exception as e:
            logger.error("Error downloading media %s: %s", http_url, e)
            return None

    # ---------------- Room Metadata & Platform Detection ---------------- #

    def detect_bridge_network(self, room: MatrixRoom) -> str:
        """Detect underlying chat service/network from Beeper Matrix room info."""
        room_id = room.room_id.lower()
        canonical_alias = (room.canonical_alias or "").lower()

        checks = [
            ("whatsapp", ["whatsapp", "wa_", "local-whatsapp"]),
            ("telegram", ["telegram", "tg_", "local-telegram"]),
            ("gvoice", ["gvoice", "voice", "local-gvoice"]),
            ("googlechat", ["googlechat", "hangouts", "local-googlechat"]),
            ("linkedin", ["linkedin", "local-linkedin"]),
            ("imessage", ["imessage", "imsg_", "local-imessage"]),
            ("signal", ["signal", "sig_", "local-signal"]),
            (
                "googlemessages",
                ["googlemessages", "rcs", "sms", "gm_", "local-googlemessages"],
            ),
            ("instagram", ["instagram", "ig_", "local-instagram"]),
            ("twitter", ["twitter", "x_", "local-twitter"]),
            ("facebook", ["facebook", "messenger", "fb_", "local-facebook"]),
            ("slack", ["slack", "local-slack"]),
            ("msteams", ["msteams", "teams", "local-msteams", "local-teams"]),
            ("discord", ["discord", "local-discord"]),
        ]

        target_str = f"{room_id} {canonical_alias}"
        for member_id in room.users:
            target_str += f" {member_id.lower()}"

        for network, keywords in checks:
            for kw in keywords:
                if kw in target_str:
                    return network

        if len(room.users) <= 2:
            return "direct_messages"
        return "matrix"

    def get_room_display_name(self, room: MatrixRoom) -> str:
        """Get a clean display name for a Matrix room."""
        named = (
            room.named_room_name()
            if callable(getattr(room, "named_room_name", None))
            else None
        )
        if named:
            return named.strip()
        if room.name and room.name.lower() not in ("empty room", "empty_room"):
            return room.name.strip()
        if room.display_name and room.display_name.lower() not in (
            "empty room",
            "empty_room",
        ):
            return room.display_name.strip()

        # Generate name from other participants
        other_users = [
            room.user_name(u) or u
            for u in room.users
            if u != self.config.user_id and not u.startswith("@_")
        ]
        if other_users:
            return ", ".join(other_users[:3])

        # If still empty, use bridge network + short ID
        network = self.detect_bridge_network(room)
        short_id = room.room_id.split(":")[0].replace("!", "")[:8]
        return f"{network.capitalize()} ({short_id})"

    def get_sender_display_name(self, room: MatrixRoom, sender_id: str) -> str:
        """Get the sender's display name in the room."""
        if room and sender_id in room.users:
            name = room.user_name(sender_id)
            if name:
                return name
        if sender_id.startswith("@") and ":" in sender_id:
            return sender_id[1:].split(":", 1)[0]
        return sender_id

    def get_sender_avatar_url(self, room: MatrixRoom, sender_id: str) -> Optional[str]:
        """Get the HTTP avatar URL of the sender if available."""
        if room and sender_id in room.users:
            avatar_mxc = room.avatar_url(sender_id)
            if avatar_mxc:
                http_url = self.mxc_to_http_url(avatar_mxc)
                if http_url:
                    return http_url
        name = self.get_sender_display_name(room, sender_id)
        network = self.detect_bridge_network(room)
        color = "00b159" if network == "matrix" else "39d9b7"
        encoded_name = urllib.parse.quote(name.replace(" (You)", ""))
        return f"https://ui-avatars.com/api/{encoded_name}/256/{color}/ffffff"

    # ---------------- Outgoing Message Methods ---------------- #

    async def send_text_message(
        self,
        room_id: str,
        text: str,
        reply_to_event_id: Optional[str] = None,
    ) -> Optional[str]:
        """Send a text message to a Matrix room, optionally replying to an event."""
        if not self.client:
            logger.error("Cannot send text message: Matrix client not initialized")
            return None

        content: Dict[str, Any] = {
            "body": text,
            "msgtype": "m.text",
        }

        if reply_to_event_id:
            content["m.relates_to"] = {"m.in_reply_to": {"event_id": reply_to_event_id}}

        try:
            resp = await self.client.room_send(
                room_id=room_id,
                message_type="m.room.message",
                content=content,
            )
            if isinstance(resp, RoomSendResponse):
                logger.debug(
                    "Sent message to %s (event_id: %s)", room_id, resp.event_id
                )
                return resp.event_id
            else:
                logger.error("Failed sending message to %s: %s", room_id, resp)
                return None
        except Exception as e:
            logger.error("Exception sending message to %s: %s", room_id, e)
            return None

    async def upload_and_send_media(
        self,
        room_id: str,
        file_bytes: bytes,
        filename: str,
        mime_type: Optional[str] = None,
        caption: str = "",
    ) -> Optional[str]:
        """Upload a file to Matrix media repository and send as a media message."""
        if not self.client:
            logger.error("Cannot send media: Matrix client not initialized")
            return None

        if not mime_type:
            mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = "application/octet-stream"

        try:
            bio = io.BytesIO(file_bytes)
            upload_resp, _ = await self.client.upload(
                bio,
                content_type=mime_type,
                filename=filename,
                filesize=len(file_bytes),
            )

            if not isinstance(upload_resp, UploadResponse):
                logger.error("Media upload failed for %s: %s", filename, upload_resp)
                return None

            content_uri = upload_resp.content_uri

            if mime_type.startswith("image/"):
                msgtype = "m.image"
            elif mime_type.startswith("video/"):
                msgtype = "m.video"
            elif mime_type.startswith("audio/"):
                msgtype = "m.audio"
            else:
                msgtype = "m.file"

            content: Dict[str, Any] = {
                "body": caption if caption else filename,
                "filename": filename,
                "msgtype": msgtype,
                "url": content_uri,
                "info": {
                    "size": len(file_bytes),
                    "mimetype": mime_type,
                },
            }

            resp = await self.client.room_send(
                room_id=room_id,
                message_type="m.room.message",
                content=content,
            )

            if isinstance(resp, RoomSendResponse):
                logger.debug(
                    "Sent media %s to %s (event_id: %s)",
                    filename,
                    room_id,
                    resp.event_id,
                )
                return resp.event_id
            else:
                logger.error("Failed sending media message to %s: %s", room_id, resp)
                return None
        except Exception as e:
            logger.error("Exception uploading and sending media to %s: %s", room_id, e)
            return None

    async def send_typing(self, room_id: str, typing: bool = True, timeout: int = 4000):
        """Send typing notification to Matrix room."""
        if not self.client:
            return
        try:
            await self.client.room_typing(room_id, typing=typing, timeout=timeout)
        except Exception as e:
            logger.debug("Failed sending typing to %s: %s", room_id, e)

    async def send_read_receipt(self, room_id: str, event_id: str):
        """Send read receipt to Matrix room."""
        if not self.client:
            return
        try:
            await self.client.room_read_markers(
                room_id, fully_read_event=event_id, read_event=event_id
            )
        except Exception as e:
            logger.debug("Failed sending read receipt to %s: %s", room_id, e)
