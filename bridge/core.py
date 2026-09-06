"""Central coordinator bridging Matrix / Beeper and Discord."""

import asyncio
import io
import logging
import uuid
import time
from typing import Optional, Any, List, Tuple

import aiohttp
import discord
from nio import (
    MatrixRoom,
    RoomMessageText,
    RoomMessageMedia,
    RoomMessageImage,
    RoomMessageFile,
    RoomMessageAudio,
    RoomMessageVideo,
    RoomMessageNotice,
    RoomMessageEmote,
)

from .config import Config
from .database import Database
from .matrix_client import MatrixBridgeClient
from .discord_client import DiscordBridgeClient
from .xmpp_client import XMPPBridgeClient
from .teams_client import TeamsBridgeClient
from .beeper_desktop_client import BeeperDesktopBridgeClient
from .bluebubbles_client import (
    BlueBubblesBridgeClient,
    TAPBACK_TO_EMOJI,
    EMOJI_TO_TAPBACK,
)
from .aim_client import AIMBridgeClient
from .slskd_client import SLSKDBridgeClient
from .email_client import EmailBridgeClient

logger = logging.getLogger("beeper_bridge.core")


class BeeperDiscordBridge:
    def __init__(self, config: Config):
        self.config = config
        self.db = Database(config.bridge.db_path)

        self.matrix_client = MatrixBridgeClient(
            config=config.matrix,
            on_message_callback=self.handle_matrix_message,
            on_room_discovered=self.handle_matrix_room_discovered,
        )

        self.xmpp_client = (
            XMPPBridgeClient(
                config=config.xmpp,
                on_message_callback=self.handle_xmpp_message,
            )
            if config.xmpp.enabled and config.xmpp.jid
            else None
        )

        self.teams_client = (
            TeamsBridgeClient(
                config=config.teams,
                db=self.db,
                on_message_callback=self.handle_teams_message,
                on_auth_prompt=self.handle_teams_auth_prompt,
            )
            if config.teams.enabled
            else None
        )

        self.beeper_desktop_client = (
            BeeperDesktopBridgeClient(
                config=config.beeper_desktop,
                db=self.db,
                on_message_callback=self.handle_beeper_desktop_message,
            )
            if config.beeper_desktop.enabled
            else None
        )

        self.bluebubbles_client = (
            BlueBubblesBridgeClient(
                config=config.bluebubbles,
                db=self.db,
                on_message_callback=self.handle_bluebubbles_message,
                on_reaction_callback=self.handle_bluebubbles_reaction,
            )
            if config.bluebubbles.enabled
            else None
        )

        self.aim_client = (
            AIMBridgeClient(
                config=config.aim,
                on_message_callback=self.handle_aim_message,
                on_buddy_update=self.handle_aim_buddy_update,
            )
            if config.aim.enabled and config.aim.screen_name
            else None
        )

        self.slskd_client = (
            SLSKDBridgeClient(
                config=config.slskd,
                db=self.db,
                on_message_callback=self.handle_slskd_message,
            )
            if config.slskd.enabled
            else None
        )

        self.email_client = (
            EmailBridgeClient(
                config=config.email,
                db=self.db,
                on_message_callback=self.handle_email_message,
            )
            if config.email.enabled
            else None
        )

        self.discord_client = DiscordBridgeClient(
            config=config.discord,
            bridge_config=config.bridge,
            db=self.db,
            on_discord_message_callback=self.handle_discord_message,
            on_manual_sync_callback=self.sync_all_rooms,
            on_teams_token_callback=self.update_teams_token,
            on_discord_reaction_callback=self.handle_discord_reaction,
        )

        self._http_session: Optional[aiohttp.ClientSession] = None
        self._discord_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._recent_discord_sends: Dict[Tuple[str, str], float] = {}
        self._recent_bb_reaction_echoes: Dict[Tuple[str, str, str], float] = {}

    def record_discord_send(self, target_id: str, text: str):
        """Record an outgoing message sent from Discord to prevent echoing back."""
        norm_text = text.strip()
        if not norm_text:
            return
        now = time.time()
        self._recent_discord_sends[(target_id, norm_text)] = now
        clean_target = target_id.split(":", 1)[-1] if ":" in target_id else target_id
        self._recent_discord_sends[(clean_target, norm_text)] = now

        # Prune entries older than 90 seconds
        cutoff = now - 90
        to_delete = [k for k, ts in self._recent_discord_sends.items() if ts < cutoff]
        for k in to_delete:
            del self._recent_discord_sends[k]

    def is_recent_discord_send(self, target_id: str, text: str) -> bool:
        """Check if message matches a recently sent Discord message to prevent echo."""
        norm_text = text.strip()
        if not norm_text:
            return False
        now = time.time()
        clean_target = target_id.split(":", 1)[-1] if ":" in target_id else target_id
        for key in [(target_id, norm_text), (clean_target, norm_text)]:
            if key in self._recent_discord_sends:
                if now - self._recent_discord_sends[key] < 60:
                    return True
        return False

    def record_recent_bb_reaction_echo(self, key: Tuple[str, str, str]):
        """Record a tapback we just sent to BlueBubbles from Discord, so the
        BlueBubbles poll picking it back up (as an is_from_me tapback) doesn't
        get relayed back to Discord as a duplicate reaction."""
        now = time.time()
        self._recent_bb_reaction_echoes[key] = now
        cutoff = now - 30
        to_delete = [
            k for k, ts in self._recent_bb_reaction_echoes.items() if ts < cutoff
        ]
        for k in to_delete:
            del self._recent_bb_reaction_echoes[k]

    def is_recent_bb_reaction_echo(self, key: Tuple[str, str, str]) -> bool:
        ts = self._recent_bb_reaction_echoes.get(key)
        return ts is not None and (time.time() - ts) < 30

    async def _apply_chat_title(self, channel: Any, title: str):
        """Keep an existing channel aligned with its chat title, without transport prefixes."""
        desired_name = self.discord_client.clean_channel_name(title)
        if channel and channel.name != desired_name:
            try:
                await channel.edit(
                    name=desired_name, reason="Use chat title without bridge prefix"
                )
            except Exception as exc:
                logger.warning("Could not rename channel %s: %s", channel.id, exc)

    async def start(self):
        """Start both Matrix and Discord clients."""
        logger.info("Starting Beeper <-> Discord Bridge...")
        self._http_session = aiohttp.ClientSession()

        # Login to Discord
        await self.discord_client.login(self.config.discord.bot_token)
        self._discord_task = asyncio.create_task(self.discord_client.connect())

        # Wait until Discord gateway connection and cache is ready
        logger.info("Waiting for Discord client to log in and sync...")
        await self.discord_client.wait_until_ready()
        if self.config.bridge.channel_inactivity_ttl_hours > 0:
            if not self.db.get_value("inactivity_tracking_initialized"):
                self.db.touch_all_room_mappings()
                self.db.set_value("inactivity_tracking_initialized", "1")
            self._cleanup_task = asyncio.create_task(self._cleanup_inactive_channels())
        logger.info("Discord client ready. Initializing Matrix client...")

        # Start Matrix client
        await self.matrix_client.start()
        await self.matrix_client.wait_for_initial_sync(timeout=45.0)

        # Start XMPP client if configured
        if self.xmpp_client:
            logger.info("Starting XMPP client...")
            await self.xmpp_client.start_client()

        # Start Teams client if configured
        if self.teams_client:
            logger.info("Starting Microsoft Teams client...")
            await self.teams_client.start()

        # Start Beeper Desktop client if configured
        if self.beeper_desktop_client:
            logger.info("Starting Beeper Desktop API client...")
            await self.beeper_desktop_client.start()

        # Start BlueBubbles client if configured
        if self.bluebubbles_client:
            logger.info("Starting BlueBubbles iMessage client...")
            await self.discord_client.get_or_create_category(
                self.config.bluebubbles.category_name
            )
            await self.bluebubbles_client.start()

        # Migrate any existing XMPP channel names to remove xmpp- prefix
        await self._migrate_xmpp_channel_names()

        # Start AIM Phoenix client if configured
        if self.aim_client:
            logger.info("Starting AIM Phoenix client...")
            await self.discord_client.get_or_create_category(
                self.config.aim.category_name
            )
            await self.aim_client.start()

        # Start SLSKD Soulseek client if configured
        if self.slskd_client:
            logger.info("Starting SLSKD Soulseek client...")
            await self.discord_client.get_or_create_category(
                self.config.slskd.category_name
            )
            await self.slskd_client.start()

        # Start Email client if configured
        if self.email_client:
            logger.info("Starting Email bridge client...")
            await self.discord_client.get_or_create_category(
                self.config.email.category_name
            )
            await self.email_client.start()

        # Channels are provisioned dynamically on real message activity (on-demand mode)
        logger.info(
            "Dynamic on-demand message sync active (channels created as messages arrive)."
        )

        # Send startup notification to Discord status channel
        xmpp_status = (
            f"**XMPP:** `Enabled` (`{self.config.xmpp.jid}`)\n"
            if self.xmpp_client
            else ""
        )
        teams_status = f"**MS Teams:** `Enabled`\n" if self.teams_client else ""
        desktop_status = (
            f"**Beeper Desktop:** `Enabled`\n" if self.beeper_desktop_client else ""
        )
        bluebubbles_status = (
            f"**BlueBubbles:** `Enabled`\n" if self.bluebubbles_client else ""
        )
        aim_status = (
            f"**AIM Phoenix:** `Enabled` (`{self.config.aim.screen_name}`)\n"
            if self.aim_client
            else ""
        )
        slskd_status = (
            f"**Soulseek (SLSKD):** `Enabled` (`{self.config.slskd.url}`)\n"
            if self.slskd_client
            else ""
        )
        email_status = (
            f"**Email:** `Enabled` (`{len(self.config.email.accounts)} account(s)`)\n"
            if self.email_client
            else ""
        )
        await self.discord_client.send_status_message(
            embed=discord.Embed(
                title="🚀 Beeper & Multi-Chat Bridge Connected",
                description=(
                    f"**Homeserver:** `{self.config.matrix.homeserver}`\n"
                    f"**User ID:** `{self.config.matrix.user_id}`\n"
                    f"{xmpp_status}"
                    f"{teams_status}"
                    f"{desktop_status}"
                    f"{bluebubbles_status}"
                    f"{aim_status}"
                    f"{slskd_status}"
                    f"{email_status}"
                    f"**Category Mode:** `{self.config.discord.category_mode}`\n"
                    f"**Auto Provisioning:** `{'Enabled' if self.config.bridge.auto_create_channels else 'Disabled'}`"
                ),
                color=discord.Color.green(),
            )
        )

        try:
            await self._discord_task
        except asyncio.CancelledError:
            pass

    async def stop(self):
        """Gracefully stop bridge services."""
        logger.info("Stopping Beeper <-> Discord Bridge...")
        await self.matrix_client.stop()
        if self.xmpp_client:
            await self.xmpp_client.stop_client()
        if self.teams_client:
            await self.teams_client.stop()
        if self.beeper_desktop_client:
            await self.beeper_desktop_client.stop()
        if self.bluebubbles_client:
            await self.bluebubbles_client.stop()
        if self.aim_client:
            await self.aim_client.stop()
        if self.slskd_client:
            await self.slskd_client.stop()
        if self.email_client:
            await self.email_client.stop()
        if not self.discord_client.is_closed():
            await self.discord_client.close()
        if self._discord_task and not self._discord_task.done():
            self._discord_task.cancel()
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
        logger.info("Bridge stopped cleanly.")

    # ---------------- Matrix -> Discord Relay ---------------- #

    async def handle_matrix_room_discovered(
        self, room_id: str, room: MatrixRoom, only_if_named: bool = True
    ) -> bool:
        """Called when a room is discovered or joined in Matrix."""
        if not self.config.bridge.auto_create_channels:
            return False

        room_name = self.matrix_client.get_room_display_name(room)

        # Skip empty/unnamed rooms during discovery unless there are actual participants
        if only_if_named:
            is_generic = (
                room_name.lower().startswith("empty")
                or "(" in room_name
                and ")" in room_name
            )
            other_users = [
                u
                for u in room.users
                if u != self.config.matrix.user_id and not u.startswith("@_")
            ]
            if is_generic and not other_users:
                return False

        network = self.matrix_client.detect_bridge_network(room)
        topic = room.topic or ""
        raw_avatar = getattr(room, "room_avatar_url", None)
        if not raw_avatar and callable(getattr(room, "avatar_url", None)):
            try:
                raw_avatar = room.avatar_url()
            except Exception:
                raw_avatar = None
        avatar_url = (
            self.matrix_client.mxc_to_http_url(raw_avatar) if raw_avatar else ""
        )

        channel = await self.discord_client.get_or_create_channel_for_room(
            matrix_room_id=room_id,
            room_name=room_name,
            network=network,
            topic=topic,
            avatar_url=avatar_url,
        )
        return channel is not None

    async def handle_matrix_message(
        self, room: MatrixRoom, event: Any, is_self: bool = False
    ):
        """Handle incoming Matrix message event and forward to Discord."""
        # Deduplication check
        if self.db.is_message_recorded(event.event_id):
            return

        # Ensure channel exists (force create on real message activity)
        room_name = self.matrix_client.get_room_display_name(room)
        network = self.matrix_client.detect_bridge_network(room)
        channel = await self.discord_client.get_or_create_channel_for_room(
            matrix_room_id=room.room_id,
            room_name=room_name,
            network=network,
            topic=room.topic or "",
        )

        if not channel:
            return

        # Update last activity in DB
        self.db.update_room_activity(room.room_id, room_name)

        # Get sender details
        sender_id = event.sender
        sender_name = self.matrix_client.get_sender_display_name(room, sender_id)
        if is_self:
            if self.is_recent_discord_send(room.room_id, body_text):
                logger.debug(
                    "Suppressing Matrix self-echo for Discord message in %s",
                    room.room_id,
                )
                return
            sender_name = (
                self.discord_client.user_display_name or f"{sender_name} (You)"
            )
            avatar_url = (
                self.discord_client.user_avatar_url
                or self.matrix_client.get_sender_avatar_url(room, sender_id)
            )
        else:
            avatar_url = self.matrix_client.get_sender_avatar_url(room, sender_id)

        # Parse message content & files
        body_text = ""
        files_to_send: List[Tuple[str, bytes]] = []

        if isinstance(event, (RoomMessageText, RoomMessageNotice, RoomMessageEmote)):
            body_text = event.body or ""
            if isinstance(event, RoomMessageEmote):
                body_text = f"*{body_text}*"

        elif isinstance(
            event,
            (
                RoomMessageMedia,
                RoomMessageImage,
                RoomMessageFile,
                RoomMessageAudio,
                RoomMessageVideo,
            ),
        ):
            body_text = getattr(event, "body", "")
            media_url = getattr(event, "url", None)
            enc_file = None

            # Extract encrypted media metadata if standard url is None
            if not media_url:
                enc_file = getattr(event, "file", None)
                if (
                    not enc_file
                    and hasattr(event, "source")
                    and isinstance(event.source, dict)
                ):
                    enc_file = event.source.get("content", {}).get("file")
                if enc_file and isinstance(enc_file, dict):
                    media_url = enc_file.get("url")

            # Download media if configured
            if media_url and self.config.bridge.download_media:
                media_info = await self.matrix_client.download_media(
                    media_url,
                    encryption_info=enc_file if isinstance(enc_file, dict) else None,
                )
                if media_info:
                    data, content_type = media_info
                    max_bytes = self.config.bridge.max_attachment_size_mb * 1024 * 1024
                    if len(data) <= max_bytes:
                        filename = (
                            getattr(event, "filename", None) or body_text or "image.png"
                        )
                        if "." not in filename:
                            if "image" in content_type or isinstance(
                                event, RoomMessageImage
                            ):
                                filename = f"{filename}.png"
                            elif "video" in content_type or isinstance(
                                event, RoomMessageVideo
                            ):
                                filename = f"{filename}.mp4"
                            elif "audio" in content_type or isinstance(
                                event, RoomMessageAudio
                            ):
                                filename = f"{filename}.ogg"
                        files_to_send.append((filename, data))
                    else:
                        http_link = self.matrix_client.mxc_to_http_url(media_url)
                        body_text = f"📎 [{body_text or 'Attachment'}]({http_link}) *(File too large for Discord)*"

        # Relay to Discord channel
        await self.discord_client.relay_matrix_message_to_discord(
            channel=channel,
            sender_name=sender_name,
            text=body_text,
            avatar_url=avatar_url,
            files=files_to_send,
            matrix_event_id=event.event_id,
        )

    # ---------------- XMPP -> Discord Relay ---------------- #

    async def handle_xmpp_message(
        self,
        sender_jid: str,
        sender_name: str,
        body: str,
        is_groupchat: bool = False,
        room_jid: str = "",
        avatar_url: str = "",
        files: Optional[List[Tuple[str, bytes]]] = None,
        is_self: bool = False,
    ):
        """Handle an incoming message from XMPP and relay to Discord."""
        if is_self:
            if self.is_recent_discord_send(
                room_jid, body
            ) or self.is_recent_discord_send(f"xmpp:{room_jid}", body):
                logger.debug(
                    "Suppressing XMPP self-echo for Discord message to %s",
                    room_jid,
                )
                return

        target_id = f"xmpp:{room_jid}"
        channel_id = self.db.get_channel_by_matrix_room(target_id)
        channel = None

        if channel_id:
            channel = self.discord_client.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.discord_client.fetch_channel(channel_id)
                except Exception:
                    channel = None

        if not channel:
            clean_nick = sender_name.split("@")[0].replace("/", "-")
            channel_name = clean_nick.lower()
            clean_name = self.discord_client.clean_channel_name(channel_name)
            category = self.config.xmpp.category_name
            topic = f"XMPP {'MUC room' if is_groupchat else 'chat'} with {room_jid}"

            channel = await self.discord_client.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                channel_id = channel.id
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel_id,
                    room_name=clean_nick,
                    bridge_network="xmpp",
                    topic=topic,
                    avatar_url=avatar_url,
                )

        if channel:
            await self._apply_chat_title(channel, sender_name.split("@")[0])
            self.db.update_room_activity(target_id, sender_name)
            if is_self:
                sender_display = (
                    self.discord_client.user_display_name or f"{sender_name} (You)"
                )
                clean_avatar = self.discord_client.user_avatar_url or avatar_url
            else:
                sender_display = sender_name
                clean_avatar = avatar_url

            await self.discord_client.relay_matrix_message_to_discord(
                channel=channel,
                sender_name=sender_display,
                text=body,
                avatar_url=clean_avatar,
                files=files or [],
                matrix_event_id=f"xmpp_{uuid.uuid4().hex[:12]}",
            )

    # ---------------- MS Teams -> Discord Relay ---------------- #

    async def handle_teams_message(
        self,
        chat_id: str,
        chat_name: str,
        sender_name: str,
        sender_id: str,
        body: str,
        avatar_url: str = "",
        is_group: bool = False,
    ):
        """Handle incoming MS Teams chat message and relay to Discord."""
        is_self = bool(
            sender_id and sender_id == getattr(self.teams_client, "user_id", "")
        )
        if is_self:
            if self.is_recent_discord_send(
                chat_id, body
            ) or self.is_recent_discord_send(f"teams:{chat_id}", body):
                logger.debug(
                    "Suppressing Teams self-echo for Discord message in %s",
                    chat_id,
                )
                return

        target_id = f"teams:{chat_id}"
        channel_id = self.db.get_channel_by_matrix_room(target_id)
        channel = None

        if channel_id:
            channel = self.discord_client.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.discord_client.fetch_channel(channel_id)
                except Exception:
                    channel = None

        if not channel:
            clean_name = self.discord_client.clean_channel_name(chat_name)
            category = self.config.teams.category_name
            topic = f"MS Teams {'group chat' if is_group else 'DM'} with {chat_name} (Chat ID: {chat_id})"

            channel = await self.discord_client.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                channel_id = channel.id
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel_id,
                    room_name=chat_name,
                    bridge_network="msteams",
                    topic=topic,
                    avatar_url=avatar_url,
                )

        if channel:
            await self._apply_chat_title(channel, chat_name)
            self.db.update_room_activity(target_id, chat_name)
            if is_self:
                sender_display = (
                    self.discord_client.user_display_name or f"{sender_name} (You)"
                )
                clean_avatar = self.discord_client.user_avatar_url or avatar_url
            else:
                sender_display = sender_name
                clean_avatar = avatar_url

            await self.discord_client.relay_matrix_message_to_discord(
                channel=channel,
                sender_name=sender_display,
                text=body,
                avatar_url=clean_avatar,
                files=[],
                matrix_event_id=f"teams_{uuid.uuid4().hex[:12]}",
            )

    async def update_teams_token(self, new_token: str) -> tuple[bool, str]:
        """Update Microsoft Teams authentication token dynamically."""
        from .teams_browser_auth import extract_jwt_payload, update_config_token

        payload = extract_jwt_payload(new_token)
        if not payload:
            return False, "❌ Invalid JWT token format."
        aud = str(payload.get("aud", ""))
        if not ("ic3" in aud or "teams" in aud or "office" in aud):
            return (
                False,
                f"⚠️ Token audience `{aud}` does not appear to be an IC3 / Teams token.",
            )

        update_config_token(new_token)
        if self.teams_client:
            self.teams_client.access_token = new_token
            self.teams_client.token_expires_at = float(payload.get("exp", 0))
            self.teams_client._inspect_token(new_token)
            self.teams_client._save_tokens_to_db()
        name = payload.get("name") or payload.get("unique_name", "Teams User")
        exp_str = time.ctime(payload.get("exp", 0))
        return (
            True,
            f"✅ Successfully updated Microsoft Teams token for **{name}**!\nExpires: `{exp_str}`",
        )

    # ---------------- Beeper Desktop API -> Discord Relay ---------------- #

    async def handle_beeper_desktop_message(
        self,
        chat_id: str,
        chat_title: str,
        network: str,
        sender_name: str,
        sender_id: str,
        text: str,
        avatar_url: str = "",
        files: Optional[List[Tuple[str, bytes]]] = None,
        is_group: bool = False,
        msg_id: str = "",
        is_self: bool = False,
    ):
        """Handle incoming message from Beeper Desktop API and relay to Discord."""
        if is_self:
            if (
                self.is_recent_discord_send(chat_id, text)
                or self.is_recent_discord_send(f"beeper:{chat_id}", text)
                or (msg_id and self.db.is_message_recorded(msg_id))
            ):
                logger.debug(
                    "Suppressing Beeper Desktop self-echo for Discord message in %s",
                    chat_id,
                )
                return

        target_id = chat_id if chat_id.startswith("!") else f"beeper:{chat_id}"
        channel_id = self.db.get_channel_by_matrix_room(target_id)
        channel = None

        if channel_id:
            channel = self.discord_client.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.discord_client.fetch_channel(channel_id)
                except Exception:
                    channel = None

        if not channel:
            clean_name = self.discord_client.clean_channel_name(chat_title)
            category = self.discord_client.get_category_for_network(network)
            topic = f"Beeper ({network.capitalize()}) {'group' if is_group else 'chat'} with {chat_title}"

            channel = await self.discord_client.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                channel_id = channel.id
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel_id,
                    room_name=chat_title,
                    bridge_network=network,
                    topic=topic,
                    avatar_url=avatar_url,
                )

        if channel:
            await self._apply_chat_title(channel, chat_title)
            self.db.update_room_activity(target_id, chat_title)
            if is_self:
                sender_display = (
                    self.discord_client.user_display_name or f"{sender_name} (You)"
                )
                clean_avatar = self.discord_client.user_avatar_url or avatar_url
            else:
                sender_display = sender_name
                clean_avatar = avatar_url

            await self.discord_client.relay_matrix_message_to_discord(
                channel=channel,
                sender_name=sender_display,
                text=text,
                avatar_url=clean_avatar,
                files=files or [],
                matrix_event_id=msg_id or f"beep_{uuid.uuid4().hex[:12]}",
            )

    # ---------------- BlueBubbles -> Discord Relay ---------------- #

    async def handle_bluebubbles_message(
        self,
        chat_id: str,
        chat_title: str,
        sender_name: str,
        sender_id: str,
        text: str,
        avatar_url: str = "",
        files: Optional[List[Tuple[str, bytes]]] = None,
        is_group: bool = False,
        msg_id: str = "",
        is_self: bool = False,
    ):
        """Handle incoming message from BlueBubbles iMessage and relay to Discord."""
        if is_self:
            if (
                self.is_recent_discord_send(chat_id, text)
                or self.is_recent_discord_send(f"bb:{chat_id}", text)
                or (msg_id and self.db.is_message_recorded(msg_id))
            ):
                logger.debug(
                    "Suppressing BlueBubbles self-echo for Discord message in %s",
                    chat_id,
                )
                return

        target_id = f"bb:{chat_id}"
        channel_id = self.db.get_channel_by_matrix_room(target_id)
        channel = None

        if channel_id:
            channel = self.discord_client.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.discord_client.fetch_channel(channel_id)
                except Exception:
                    channel = None

        if not channel:
            clean_name = self.discord_client.clean_channel_name(chat_title)
            category = self.discord_client.get_category_for_network("imessage")
            topic = f"iMessage (BlueBubbles) {'group' if is_group else 'chat'} with {chat_title}"

            channel = await self.discord_client.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                channel_id = channel.id
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel_id,
                    room_name=chat_title,
                    bridge_network="imessage",
                    topic=topic,
                    avatar_url=avatar_url,
                )

        if channel:
            await self._apply_chat_title(channel, chat_title)
            self.db.update_room_activity(target_id, chat_title)
            if is_self:
                sender_display = (
                    self.discord_client.user_display_name or f"{sender_name} (You)"
                )
                clean_avatar = self.discord_client.user_avatar_url or avatar_url
            else:
                sender_display = sender_name
                clean_avatar = avatar_url

            await self.discord_client.relay_matrix_message_to_discord(
                channel=channel,
                sender_name=sender_display,
                text=text,
                avatar_url=clean_avatar,
                files=files or [],
                matrix_event_id=msg_id or f"bb_{uuid.uuid4().hex[:12]}",
            )

            if not is_self:
                await self.bluebubbles_client.mark_chat_read(chat_id)

    async def handle_bluebubbles_reaction(
        self,
        chat_guid: str,
        target_msg_guid: str,
        target_part: int,
        tapback_name: str,
        is_removal: bool,
        is_from_me: bool,
    ):
        """Handle an incoming iMessage tapback and mirror it as a Discord reaction
        on the message it was relayed as."""
        if is_from_me and self.is_recent_bb_reaction_echo(
            (chat_guid, target_msg_guid, tapback_name)
        ):
            # This is just BlueBubbles echoing back a tapback we ourselves sent
            # from Discord a moment ago.
            return

        emoji = TAPBACK_TO_EMOJI.get(tapback_name)
        if not emoji:
            return

        mapping = self.db.get_message_mapping(target_msg_guid)
        if not mapping or not mapping.get("discord_message_id"):
            return

        if is_removal:
            await self.discord_client.remove_reaction_from_message(
                mapping["discord_channel_id"], mapping["discord_message_id"], emoji
            )
        else:
            await self.discord_client.add_reaction_to_message(
                mapping["discord_channel_id"], mapping["discord_message_id"], emoji
            )

    async def handle_discord_reaction(
        self,
        matrix_room_id: str,
        discord_message_id: int,
        emoji: str,
        is_removal: bool,
    ):
        """Handle a user reacting to a bridged Discord message and relay it
        onward as a tapback (currently: BlueBubbles only)."""
        if not matrix_room_id.startswith("bb:") or not self.bluebubbles_client:
            return

        tapback_name = EMOJI_TO_TAPBACK.get(emoji)
        if not tapback_name:
            return

        target_msg_guid = self.db.get_matrix_event_by_discord_message(
            discord_message_id
        )
        if not target_msg_guid:
            return

        chat_guid = matrix_room_id[3:]
        reaction = f"-{tapback_name}" if is_removal else tapback_name
        self.record_recent_bb_reaction_echo((chat_guid, target_msg_guid, tapback_name))
        await self.bluebubbles_client.send_tapback(
            chat_guid=chat_guid,
            target_guid=target_msg_guid,
            target_part=0,
            tapback_name=reaction,
        )

    # ---------------- AIM Phoenix -> Discord Relay ---------------- #

    async def handle_aim_message(
        self,
        sender_screen_name: str,
        text: str,
        is_away: bool = False,
        msg_id: str = "",
        is_self: bool = False,
    ):
        """Handle incoming message from AIM Phoenix and relay to Discord."""
        target_id = f"aim:{sender_screen_name.lower().replace(' ', '')}"
        channel_id = self.db.get_channel_by_matrix_room(target_id)
        channel = None

        if channel_id:
            channel = self.discord_client.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.discord_client.fetch_channel(channel_id)
                except Exception:
                    channel = None

        if not channel:
            clean_name = self.discord_client.clean_channel_name(sender_screen_name)
            category = self.config.aim.category_name
            topic = f"AIM Phoenix chat with {sender_screen_name}"

            channel = await self.discord_client.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                channel_id = channel.id
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel_id,
                    room_name=sender_screen_name,
                    bridge_network="aim",
                    topic=topic,
                    avatar_url="",
                )

        if channel:
            await self._apply_chat_title(channel, sender_screen_name)
            self.db.update_room_activity(target_id, sender_screen_name)
            if is_self:
                sender_display = (
                    self.discord_client.user_display_name
                    or f"{sender_screen_name} (You)"
                )
            else:
                sender_display = (
                    f"{sender_screen_name} [Away]" if is_away else sender_screen_name
                )

            avatar_url = "https://upload.wikimedia.org/wikipedia/commons/thumb/c/cd/AOL_Instant_Messenger_%28running_man_logo%29.svg/512px-AOL_Instant_Messenger_%28running_man_logo%29.svg.png"
            await self.discord_client.relay_matrix_message_to_discord(
                channel=channel,
                sender_name=sender_display,
                text=text,
                avatar_url=avatar_url,
                files=[],
                matrix_event_id=msg_id or f"aim_{uuid.uuid4().hex[:12]}",
            )

    async def handle_aim_buddy_update(
        self, screen_name: str, online: bool, is_away: bool
    ):
        """Handle status notifications for AIM buddies."""
        logger.debug(
            "AIM buddy update: %s (online=%s, away=%s)",
            screen_name,
            online,
            is_away,
        )

    async def handle_slskd_message(
        self,
        chat_id: str,
        chat_title: str,
        sender_name: str,
        text: str,
        is_room: bool = False,
        msg_id: str = "",
        is_self: bool = False,
    ):
        """Handle incoming message from SLSKD (Soulseek) and relay to Discord."""
        if is_self:
            if (
                self.is_recent_discord_send(chat_id, text)
                or self.is_recent_discord_send(f"slsk:{chat_id}", text)
                or (msg_id and self.db.is_message_recorded(msg_id))
            ):
                logger.debug(
                    "Suppressing SLSKD self-echo for Discord message in %s",
                    chat_id,
                )
                return

        target_id = f"slsk:{chat_id}"
        channel_id = self.db.get_channel_by_matrix_room(target_id)
        if not channel_id and not is_room:
            channel_id = self.db.get_channel_by_matrix_room(f"slsk:user:{chat_id}")
        channel = None

        if channel_id:
            channel = self.discord_client.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.discord_client.fetch_channel(channel_id)
                except Exception:
                    channel = None

        if not channel:
            clean_name = self.discord_client.clean_channel_name(chat_title)
            category = self.config.slskd.category_name
            topic = (
                f"Soulseek {'room' if is_room else 'chat'} with"
                f" {chat_title.lstrip('#')}"
            )

            channel = await self.discord_client.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                channel_id = channel.id
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel_id,
                    room_name=chat_title,
                    bridge_network="soulseek",
                    topic=topic,
                    avatar_url="",
                )

        if channel:
            await self._apply_chat_title(channel, chat_title)
            self.db.update_room_activity(target_id, chat_title)
            if is_self:
                sender_display = (
                    self.discord_client.user_display_name or f"{sender_name} (You)"
                )
            else:
                sender_display = sender_name

            avatar_url = "https://raw.githubusercontent.com/walkxcode/dashboard-icons/main/png/soulseek.png"
            await self.discord_client.relay_matrix_message_to_discord(
                channel=channel,
                sender_name=sender_display,
                text=text,
                avatar_url=avatar_url,
                files=[],
                matrix_event_id=msg_id or f"slsk_{uuid.uuid4().hex[:12]}",
            )

    async def handle_email_message(
        self,
        account: str,
        subject: str,
        sender: str,
        sender_addr: str,
        body: str,
        msg_id: str = "",
    ):
        """Handle a new email fetched by the Email bridge and relay to Discord."""
        if msg_id and self.db.is_message_recorded(msg_id):
            return

        target_id = "email:alert"
        channel_id = self.db.get_channel_by_matrix_room(target_id)
        channel = None

        if channel_id:
            channel = self.discord_client.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.discord_client.fetch_channel(channel_id)
                except Exception:
                    channel = None

        if not channel:
            channel = await self.discord_client.get_or_create_channel(
                channel_name=self.config.email.channel_name,
                category_name=self.config.email.category_name,
                topic="Incoming mail alerts",
            )
            if channel:
                channel_id = channel.id
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel_id,
                    room_name=self.config.email.channel_name,
                    bridge_network="email",
                    topic="Incoming mail alerts",
                    avatar_url="",
                )

        if channel:
            self.db.update_room_activity(target_id, self.config.email.channel_name)
            text = f"**{subject}**\nFrom: {sender} <{sender_addr}> (via {account})\n\n{body}"
            avatar_url = "https://raw.githubusercontent.com/walkxcode/dashboard-icons/main/png/gmail.png"
            await self.discord_client.relay_matrix_message_to_discord(
                channel=channel,
                sender_name=sender,
                text=text,
                avatar_url=avatar_url,
                files=[],
                matrix_event_id=msg_id or f"email_{uuid.uuid4().hex[:12]}",
            )

    async def _migrate_xmpp_channel_names(self):
        """Migrate any existing xmpp-* channel names to remove the xmpp- prefix."""
        if not self.discord_client.target_guild:
            return
        for ch in self.discord_client.target_guild.text_channels:
            if ch.name.startswith("xmpp-"):
                new_name = ch.name[5:]
                clean_name = self.discord_client.clean_channel_name(new_name)
                try:
                    await ch.edit(name=clean_name, reason="Remove xmpp- prefix")
                    logger.info("Renamed channel #%s to #%s", ch.name, clean_name)
                except Exception as exc:
                    logger.warning(
                        "Could not rename channel %s (%s): %s", ch.name, ch.id, exc
                    )

    async def handle_teams_auth_prompt(self, uri: str, code: str, message: str):
        """Send device code prompt to Discord status channel."""
        embed = discord.Embed(
            title="🔑 Microsoft Teams Authentication Required",
            description=(
                f"To link your Microsoft Teams chats & DMs to Discord:\n\n"
                f"1. Visit **[{uri}]({uri})**\n"
                f"2. Enter code: **`{code}`**\n"
                f"3. Sign into your Microsoft account (Work/School or Personal)."
            ),
            color=discord.Color.blue(),
        )
        await self.discord_client.send_status_message(embed=embed)

    # ---------------- Discord -> Matrix/XMPP/Teams/Beeper/BlueBubbles/AIM Relay ---------------- #

    async def handle_discord_message(
        self, message: discord.Message, matrix_room_id: str
    ):
        """Handle user message in Discord and send to Matrix, XMPP, Teams, Beeper Desktop, BlueBubbles, or AIM."""
        self.db.update_room_activity(matrix_room_id)
        if message.content:
            self.record_discord_send(matrix_room_id, message.content)

        # Handle AIM Phoenix destination
        if matrix_room_id.startswith("aim:"):
            if self.aim_client and message.content:
                target_sn = matrix_room_id[4:]
                norm_sn = target_sn.lower().replace(" ", "")
                orig_sn = (
                    self.aim_client.buddies.get(norm_sn, {}).get("screen_name")
                    or target_sn
                )
                success = await self.aim_client.send_im(
                    recipient=orig_sn,
                    text=message.content,
                )
                if success:
                    msg_id = f"aim_out_{uuid.uuid4().hex[:12]}"
                    self.db.record_message(
                        matrix_event_id=msg_id,
                        discord_message_id=message.id,
                        channel_id=message.channel.id,
                        sender_id="me",
                    )
                    if self.config.bridge.send_reactions:
                        try:
                            await message.add_reaction("✅")
                        except Exception:
                            pass
            return

        # Handle SLSKD Soulseek destination
        if matrix_room_id.startswith("slsk:"):
            if self.slskd_client and message.content:
                raw_id = matrix_room_id[5:]
                if raw_id.startswith("room:"):
                    room_name = raw_id[5:]
                    msg_id = await self.slskd_client.send_room_message(
                        room_name=room_name,
                        text=message.content,
                    )
                else:
                    username = raw_id.split(":", 1)[-1]
                    msg_id = await self.slskd_client.send_private_message(
                        username=username,
                        text=message.content,
                    )
                if msg_id:
                    self.db.record_message(
                        matrix_event_id=msg_id,
                        discord_message_id=message.id,
                        channel_id=message.channel.id,
                        sender_id="me",
                    )
                    if self.config.bridge.send_reactions:
                        try:
                            await message.add_reaction("✅")
                        except Exception:
                            pass
            return

        # Handle BlueBubbles destination
        if matrix_room_id.startswith("bb:"):
            if self.bluebubbles_client and message.content:
                target_chat_guid = matrix_room_id[3:]
                msg_id = await self.bluebubbles_client.send_message(
                    chat_guid=target_chat_guid,
                    text=message.content,
                )
                if msg_id:
                    self.db.record_message(
                        matrix_event_id=msg_id,
                        discord_message_id=message.id,
                        channel_id=message.channel.id,
                        sender_id="me",
                    )
                if self.config.bridge.send_reactions:
                    try:
                        await message.add_reaction("✅")
                    except Exception:
                        pass
            return

        # Handle Beeper Desktop destination
        if matrix_room_id.startswith("beeper:") or (
            self.beeper_desktop_client and matrix_room_id.startswith("!")
        ):
            if self.beeper_desktop_client and message.content:
                target_chat_id = (
                    matrix_room_id[7:]
                    if matrix_room_id.startswith("beeper:")
                    else matrix_room_id
                )
                res = await self.beeper_desktop_client.send_chat_message(
                    chat_id=target_chat_id,
                    text=message.content,
                )
                if res:
                    self.db.record_message(
                        matrix_event_id=res,
                        discord_message_id=message.id,
                        channel_id=message.channel.id,
                        sender_id="me",
                    )
                    if self.config.bridge.send_reactions:
                        try:
                            await message.add_reaction("✅")
                        except Exception:
                            pass
                    return

        # Handle MS Teams destination
        if matrix_room_id.startswith("teams:"):
            if self.teams_client and message.content:
                target_chat_id = matrix_room_id[6:]
                msg_id = await self.teams_client.send_chat_message(
                    chat_id=target_chat_id,
                    content=message.content,
                )
                if msg_id:
                    self.db.record_message(
                        matrix_event_id=msg_id,
                        discord_message_id=message.id,
                        channel_id=message.channel.id,
                        sender_id="me",
                    )
                if self.config.bridge.send_reactions:
                    try:
                        await message.add_reaction("✅")
                    except Exception:
                        pass
            return

        # Handle XMPP destination
        if matrix_room_id.startswith("xmpp:"):
            if self.xmpp_client and message.content:
                target_jid = matrix_room_id[5:]
                sent = await self.xmpp_client.send_chat_message(
                    recipient_jid=target_jid,
                    body=message.content,
                    is_groupchat=False,
                )
                if sent:
                    if self.config.bridge.send_reactions:
                        try:
                            await message.add_reaction("✅")
                        except Exception:
                            pass
                else:
                    try:
                        await message.add_reaction("⚠️")
                    except Exception:
                        pass
            return

        # Check if reply to a previous message
        reply_to_event_id = None
        if message.reference and message.reference.message_id:
            with self.db._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT matrix_event_id FROM message_mappings WHERE discord_message_id = ?",
                    (message.reference.message_id,),
                )
                row = cursor.fetchone()
                if row:
                    reply_to_event_id = row["matrix_event_id"]

        # Handle Discord file attachments
        if message.attachments:
            if not self._http_session or self._http_session.closed:
                self._http_session = aiohttp.ClientSession()

            for attachment in message.attachments:
                try:
                    async with self._http_session.get(attachment.url) as resp:
                        if resp.status == 200:
                            file_bytes = await resp.read()
                            event_id = await self.matrix_client.upload_and_send_media(
                                room_id=matrix_room_id,
                                file_bytes=file_bytes,
                                filename=attachment.filename,
                                mime_type=attachment.content_type,
                                caption=message.content if not message.content else "",
                            )
                            if event_id:
                                self.db.record_message(
                                    matrix_event_id=event_id,
                                    discord_message_id=message.id,
                                    channel_id=message.channel.id,
                                    sender_id=self.config.matrix.user_id,
                                )
                except Exception as e:
                    logger.error("Failed uploading Discord attachment to Matrix: %s", e)

        # Handle text message
        if message.content:
            event_id = await self.matrix_client.send_text_message(
                room_id=matrix_room_id,
                text=message.content,
                reply_to_event_id=reply_to_event_id,
            )
            if event_id:
                self.db.record_message(
                    matrix_event_id=event_id,
                    discord_message_id=message.id,
                    channel_id=message.channel.id,
                    sender_id=self.config.matrix.user_id,
                )

    async def _cleanup_inactive_channels(self):
        """Delete bridge-created Discord channels after the configured idle period."""
        interval = max(60, self.config.bridge.channel_cleanup_interval_seconds)
        ttl_seconds = self.config.bridge.channel_inactivity_ttl_hours * 3600
        while True:
            try:
                cutoff = time.time() - ttl_seconds
                for mapping in self.db.get_stale_room_mappings(cutoff):
                    channel_id = mapping["discord_channel_id"]
                    channel = self.discord_client.get_channel(channel_id)
                    if channel is None:
                        try:
                            channel = await self.discord_client.fetch_channel(
                                channel_id
                            )
                        except discord.NotFound:
                            channel = None
                        except Exception as exc:
                            logger.warning(
                                "Could not inspect stale channel %s: %s",
                                channel_id,
                                exc,
                            )
                            continue
                    if channel is not None:
                        try:
                            await channel.delete(
                                reason=f"Bridge inactive for {self.config.bridge.channel_inactivity_ttl_hours} hours"
                            )
                        except Exception as exc:
                            logger.warning(
                                "Could not delete stale channel %s: %s", channel_id, exc
                            )
                            continue
                    self.discord_client._webhook_cache.pop(channel_id, None)
                    self.db.delete_channel_state(mapping["matrix_room_id"], channel_id)
                    logger.info("Deleted inactive bridge channel %s", channel_id)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.exception("Inactive-channel cleanup failed: %s", exc)
            await asyncio.sleep(interval)

    # ---------------- Sync Helper ---------------- #

    async def sync_all_rooms(self) -> int:
        """Sync and ensure channels for all joined Matrix rooms."""
        if not self.matrix_client.client:
            return 0

        rooms = self.matrix_client.client.rooms
        count = 0
        for room_id, room in rooms.items():
            created = await self.handle_matrix_room_discovered(
                room_id, room, only_if_named=True
            )
            if created:
                count += 1
                # Small delay to avoid Discord rate limit bursts
                await asyncio.sleep(0.3)
        return count
