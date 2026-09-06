"""Discord bot integration for managing channels, webhooks, and relaying messages."""

import asyncio
import io
import logging
import re
import urllib.parse
from typing import Optional, Callable, Dict, Any, List, Tuple

import discord
from discord.ext import commands

from .config import DiscordConfig, BridgeConfig
from .database import Database
from .text_utils import strip_html_to_discord_text

logger = logging.getLogger("beeper_bridge.discord")


class DiscordBridgeClient(commands.Bot):
    def __init__(
        self,
        config: DiscordConfig,
        bridge_config: BridgeConfig,
        db: Database,
        on_discord_message_callback: Optional[Callable] = None,
        on_manual_sync_callback: Optional[Callable] = None,
        on_teams_token_callback: Optional[Callable] = None,
    ):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.webhooks = True
        intents.members = True

        super().__init__(
            command_prefix=(
                config.command_prefix + " "
                if not config.command_prefix.endswith(" ")
                else config.command_prefix
            ),
            intents=intents,
            help_command=None,
        )

        self.config = config
        self.bridge_config = bridge_config
        self.db = db
        self.on_discord_message_callback = on_discord_message_callback
        self.on_manual_sync_callback = on_manual_sync_callback
        self.on_teams_token_callback = on_teams_token_callback
        self.target_guild: Optional[discord.Guild] = None
        self.status_channel: Optional[discord.TextChannel] = None
        self.user_display_name: str = "You"
        self.user_avatar_url: Optional[str] = None
        self._webhook_cache: Dict[int, discord.Webhook] = {}
        self._category_cache: Dict[str, discord.CategoryChannel] = {}

    async def on_ready(self):
        """Called when Discord bot successfully connects."""
        logger.info(
            "Discord bot logged in as %s (ID: %d)", self.user.name, self.user.id
        )

        # Resolve guild
        if self.config.guild_id != 0:
            self.target_guild = self.get_guild(self.config.guild_id)
        elif len(self.guilds) > 0:
            self.target_guild = self.guilds[0]

        if not self.target_guild:
            logger.error(
                "Could not find Discord guild with ID %s. Available guilds: %s",
                self.config.guild_id,
                [g.name for g in self.guilds],
            )
            return

        logger.info(
            "Bound to target Discord guild: %s (ID: %d)",
            self.target_guild.name,
            self.target_guild.id,
        )

        # Detect primary user identity for self-message puppeting
        admin_id = self.config.admin_user_ids[0] if self.config.admin_user_ids else None
        matched_member = None
        if admin_id:
            matched_member = self.target_guild.get_member(admin_id)
        if not matched_member:
            for m in self.target_guild.members:
                if not m.bot:
                    matched_member = m
                    break
        if matched_member:
            self.user_display_name = (
                matched_member.display_name or matched_member.name or "You"
            )
            self.user_avatar_url = matched_member.display_avatar.url
            logger.info(
                "Detected primary bridge user identity: %s (Avatar: %s)",
                self.user_display_name,
                self.user_avatar_url,
            )

        # Ensure status channel
        await self._ensure_status_channel()

    async def _ensure_status_channel(self):
        """Ensure a status channel exists for system logs and announcements."""
        if not self.target_guild:
            return

        channel_name = self.config.status_channel_name
        channel = discord.utils.get(self.target_guild.text_channels, name=channel_name)
        if not channel:
            try:
                channel = await self.target_guild.create_text_channel(
                    name=channel_name,
                    topic="Beeper <-> Discord Bridge Status & Control",
                )
                logger.info("Created status channel: #%s", channel_name)
            except Exception as e:
                logger.error("Failed to create status channel: %s", e)
                return

        self.status_channel = channel

    async def send_status_message(
        self, message: str = "", embed: Optional[discord.Embed] = None
    ):
        """Send a message to the status channel."""
        if self.status_channel:
            try:
                await self.status_channel.send(
                    content=message if not embed else None, embed=embed
                )
            except Exception as e:
                logger.debug("Could not send status message: %s", e)

    # ---------------- Channel & Category Management ---------------- #

    def sanitize_channel_name(self, name: str, network: str = "") -> str:
        """Sanitize a name to comply with Discord channel name requirements."""
        clean = name.strip().lower()
        # Replace non-alphanumeric with hyphens
        clean = re.sub(r"[^a-z0-9_\-]+", "-", clean)
        clean = re.sub(r"-+", "-", clean).strip("-")

        if not clean:
            clean = "unnamed-chat"

        prefix = ""
        if self.config.channel_prefix:
            prefix = f"{self.config.channel_prefix}-"

        full_name = f"{prefix}{clean}"
        # Limit to 100 characters (Discord text channel limit)
        return full_name[:100].rstrip("-")

    async def get_or_create_channel_for_room(
        self,
        matrix_room_id: str,
        room_name: str,
        network: str,
        topic: str = "",
        avatar_url: str = "",
    ) -> Optional[discord.TextChannel]:
        """Get existing Discord channel or create a new one dynamically for a Matrix room."""
        if not self.target_guild:
            return None

        # Check DB first
        channel_id = self.db.get_channel_by_matrix_room(matrix_room_id)
        if channel_id:
            channel = self.target_guild.get_channel(channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                desired_name = self.sanitize_channel_name(room_name)
                if channel.name != desired_name:
                    try:
                        await channel.edit(
                            name=desired_name, reason="Remove bridge network prefix"
                        )
                    except Exception as exc:
                        logger.warning(
                            "Could not rename channel %s: %s", channel.id, exc
                        )
                return channel

        if not self.bridge_config.auto_create_channels:
            logger.debug(
                "Auto create channels is disabled. Skipping room %s", matrix_room_id
            )
            return None

        # Create new channel
        sanitized_name = self.sanitize_channel_name(room_name, network)
        category = await self.get_or_create_category(
            self.get_category_for_network(network)
        )

        channel_topic = (
            f"Matrix Room: {matrix_room_id} | Network: {network} | {room_name}"
        )
        if len(channel_topic) > 1024:
            channel_topic = channel_topic[:1020] + "..."

        try:
            channel = await self.target_guild.create_text_channel(
                name=sanitized_name,
                category=category,
                topic=channel_topic,
            )
            logger.info(
                "Created Discord channel #%s for Matrix room %s (%s)",
                channel.name,
                matrix_room_id,
                network,
            )

            # Save mapping in database
            self.db.save_room_mapping(
                matrix_room_id=matrix_room_id,
                discord_channel_id=channel.id,
                room_name=room_name,
                bridge_network=network,
                topic=topic,
                avatar_url=avatar_url,
            )

            # Ensure webhook
            if self.config.enable_webhooks:
                await self.get_or_create_webhook(channel)

            return channel
        except Exception as e:
            logger.error(
                "Failed to create Discord channel for Matrix room %s: %s",
                matrix_room_id,
                e,
            )
            return None

    def clean_channel_name(self, name: str) -> str:
        """Sanitize a channel name without network-based prefixing."""
        return self.sanitize_channel_name(name)

    def get_category_for_network(self, network: str) -> str:
        """Resolve the Discord category name to use for a given network."""
        if self.config.category_mode == "single_category":
            return self.config.default_category_name

        platform_titles = {
            "whatsapp": "\U0001f4ac WhatsApp",
            "imessage": "\U0001f4ac iMessage",
            "telegram": "\U0001f4ac Telegram",
            "signal": "\U0001f4ac Signal",
            "googlemessages": "\U0001f4ac SMS / RCS",
            "googlechat": "\U0001f4ac Google Chat on Beeper",
            "gvoice": "\U0001f4ac Google Voice",
            "google voice": "\U0001f4ac Google Voice",
            "linkedin": "\U0001f4ac LinkedIn",
            "instagram": "\U0001f4ac Instagram",
            "twitter": "\U0001f4ac Twitter / X",
            "facebook": "\U0001f4ac Facebook Messenger",
            "slack": "\U0001f4ac Slack",
            "msteams": "\U0001f4ac MS Teams",
            "aim": "\U0001f4ac AIM Phoenix",
            "phoenix": "\U0001f4ac AIM Phoenix",
            "slskd": "\U0001f4ac Soulseek",
            "slsk": "\U0001f4ac Soulseek",
            "soulseek": "\U0001f4ac Soulseek",
            "direct_messages": "\U0001f4ac Direct Messages",
            "matrix": "\U0001f4ac Matrix Rooms",
        }
        return platform_titles.get(
            network.lower(), f"\U0001f4ac {network.capitalize()}"
        )

    async def get_or_create_category(
        self, category_name: str
    ) -> Optional[discord.CategoryChannel]:
        """Get an existing Discord category by name or create it."""
        if (
            not self.target_guild
            or not category_name
            or self.config.category_mode == "none"
        ):
            return None

        if category_name in self._category_cache:
            return self._category_cache[category_name]

        category = discord.utils.get(self.target_guild.categories, name=category_name)
        if not category:
            try:
                category = await self.target_guild.create_category(name=category_name)
                logger.info("Created Discord category: %s", category_name)
            except Exception as e:
                logger.error("Failed to create category '%s': %s", category_name, e)
                category = None

        if category:
            self._category_cache[category_name] = category
        return category

    async def get_or_create_channel(
        self,
        channel_name: str,
        category_name: str = "",
        topic: str = "",
    ) -> Optional[discord.TextChannel]:
        """Get an existing text channel by name or create it under the given category."""
        if not self.target_guild:
            return None

        channel = discord.utils.get(self.target_guild.text_channels, name=channel_name)
        if channel:
            return channel

        category = (
            await self.get_or_create_category(category_name) if category_name else None
        )
        channel_topic = topic[:1024] if topic else None

        try:
            channel = await self.target_guild.create_text_channel(
                name=channel_name,
                category=category,
                topic=channel_topic,
            )
            logger.info("Created Discord channel #%s", channel.name)

            if self.config.enable_webhooks:
                await self.get_or_create_webhook(channel)

            return channel
        except Exception as e:
            logger.error("Failed to create Discord channel #%s: %s", channel_name, e)
            return None

    # ---------------- Webhook Management & Puppeting ---------------- #

    async def get_or_create_webhook(
        self, channel: discord.TextChannel
    ) -> Optional[discord.Webhook]:
        """Get cached webhook or create a new webhook for the channel."""
        if channel.id in self._webhook_cache:
            return self._webhook_cache[channel.id]

        # Check database
        wh_data = self.db.get_webhook(channel.id)
        if wh_data:
            try:
                wh = discord.Webhook.from_url(wh_data["webhook_url"], client=self)
                self._webhook_cache[channel.id] = wh
                return wh
            except Exception as e:
                logger.debug("Failed loading webhook from URL: %s", e)

        # Check channel webhooks
        try:
            webhooks = await channel.webhooks()
            for wh in webhooks:
                if wh.name == "Beeper-Relay":
                    self.db.save_webhook(channel.id, wh.id, wh.token or "", wh.url)
                    self._webhook_cache[channel.id] = wh
                    return wh

            # Create new webhook
            wh = await channel.create_webhook(name="Beeper-Relay")
            self.db.save_webhook(channel.id, wh.id, wh.token or "", wh.url)
            self._webhook_cache[channel.id] = wh
            logger.info("Created new webhook for channel #%s", channel.name)
            return wh
        except Exception as e:
            logger.error("Failed to get/create webhook for #%s: %s", channel.name, e)
            return None

    async def relay_matrix_message_to_discord(
        self,
        channel: discord.TextChannel,
        sender_name: str,
        text: str,
        avatar_url: Optional[str] = None,
        files: Optional[List[Tuple[str, bytes]]] = None,
        matrix_event_id: str = "",
    ):
        """Relay incoming Matrix message to Discord using webhooks or standard channel message."""
        if text:
            text = strip_html_to_discord_text(text)

        # Convert file tuples to discord.File
        discord_files = []
        if files:
            for idx, (filename, data) in enumerate(files):
                clean_fname = re.sub(
                    r"[^\w\.\-_]", "_", filename or f"attachment_{idx+1}.png"
                )
                if not clean_fname or clean_fname == "_":
                    clean_fname = f"attachment_{idx+1}.png"
                # If no extension, add .png
                if "." not in clean_fname:
                    clean_fname = f"{clean_fname}.png"
                bio = io.BytesIO(data)
                bio.seek(0)
                discord_files.append(discord.File(bio, filename=clean_fname))

        # Split text into chunks if > 2000 chars
        chunks = []
        if text:
            while len(text) > 2000:
                split_idx = text.rfind("\n", 0, 2000)
                if split_idx == -1:
                    split_idx = text.rfind(" ", 0, 2000)
                if split_idx == -1:
                    split_idx = 2000
                chunks.append(text[:split_idx])
                text = text[split_idx:].lstrip()
            if text:
                chunks.append(text)
        elif discord_files:
            chunks = [""]

        # Ensure valid sender name
        clean_sender = sender_name.strip()[:80] if sender_name else "Beeper User"

        webhook = None
        if self.config.enable_webhooks:
            webhook = await self.get_or_create_webhook(channel)

        clean_avatar = (
            str(avatar_url).strip()
            if (avatar_url and str(avatar_url).strip().startswith("http"))
            else None
        )
        if not clean_avatar:
            encoded_name = urllib.parse.quote(clean_sender.replace(" (You)", ""))
            clean_avatar = (
                f"https://ui-avatars.com/api/{encoded_name}/256/39D9B7/ffffff"
            )

        sent_msg_ids = []
        try:
            if webhook:
                for i, chunk in enumerate(chunks):
                    # Attach files on the first chunk
                    cur_files = discord_files if i == 0 else []
                    try:
                        msg = await webhook.send(
                            content=chunk if chunk else None,
                            username=clean_sender,
                            avatar_url=clean_avatar,
                            files=cur_files,
                            wait=True,
                        )
                        sent_msg_ids.append(msg.id)
                    except discord.HTTPException as he:
                        if cur_files:
                            logger.warning(
                                "Failed sending files via webhook to #%s (%s), retrying text-only: %s",
                                channel.name,
                                he,
                                he,
                            )
                            # Re-create fresh file objects or retry text
                            fallback_content = (
                                chunk or ""
                            ) + "\n*(Attachment exceeded Discord upload limit)*"
                            msg = await webhook.send(
                                content=fallback_content.strip(),
                                username=clean_sender,
                                avatar_url=clean_avatar,
                                wait=True,
                            )
                            sent_msg_ids.append(msg.id)
                        else:
                            raise
            else:
                for i, chunk in enumerate(chunks):
                    cur_files = discord_files if i == 0 else []
                    header = f"**{clean_sender}**: " if i == 0 else ""
                    try:
                        msg = await channel.send(
                            content=f"{header}{chunk}".strip(),
                            files=cur_files,
                        )
                        sent_msg_ids.append(msg.id)
                    except discord.HTTPException as he:
                        if cur_files:
                            fallback_content = f"{header}{chunk}\n*(Attachment exceeded Discord upload limit)*"
                            msg = await channel.send(content=fallback_content.strip())
                            sent_msg_ids.append(msg.id)
                        else:
                            raise

            if matrix_event_id and sent_msg_ids:
                self.db.record_message(
                    matrix_event_id=matrix_event_id,
                    discord_message_id=sent_msg_ids[0],
                    channel_id=channel.id,
                    sender_id=sender_name,
                )
        except Exception as e:
            logger.error(
                "Error relaying message to Discord channel #%s: %s",
                channel.name,
                e,
                exc_info=True,
            )

    # ---------------- Message Listener & Commands ---------------- #

    def is_authorized_user(self, user: discord.User) -> bool:
        """Check if user has permission to bridge messages."""
        if not self.config.admin_user_ids:
            # If no admin IDs are configured, allow the server owner or everyone in guild
            if self.target_guild and user.id == self.target_guild.owner_id:
                return True
            return True  # Open to server members if not restricted
        return user.id in self.config.admin_user_ids

    async def on_message(self, message: discord.Message):
        """Listen to Discord messages and forward to Matrix or process commands."""
        # Ignore bot and webhook messages
        if message.author.bot or message.webhook_id is not None:
            return

        # Check authorization
        if not self.is_authorized_user(message.author):
            return

        # Check for commands
        content = message.content.strip()
        prefix = self.config.command_prefix
        if content.startswith(prefix):
            await self._handle_bot_command(message, content[len(prefix) :].strip())
            return

        # If not a command, check if channel is mapped to a Matrix room
        matrix_room_id = self.db.get_matrix_room_by_channel(message.channel.id)
        if matrix_room_id and self.on_discord_message_callback:
            try:
                await self.on_discord_message_callback(message, matrix_room_id)
                if self.bridge_config.send_reactions:
                    try:
                        await message.add_reaction("✅")
                    except Exception:
                        pass
            except Exception as e:
                logger.error(
                    "Error forwarding Discord message to Matrix room %s: %s",
                    matrix_room_id,
                    e,
                    exc_info=True,
                )
                if self.bridge_config.send_reactions:
                    try:
                        await message.add_reaction("❌")
                    except Exception:
                        pass

    async def _handle_bot_command(self, message: discord.Message, cmd_str: str):
        """Handle bridge management commands."""
        parts = cmd_str.split()
        if not parts:
            cmd = "help"
            args = []
        else:
            cmd = parts[0].lower()
            args = parts[1:]

        if cmd == "status":
            mappings = self.db.get_all_room_mappings()
            embed = discord.Embed(
                title="⚡ Beeper <-> Discord Bridge Status",
                color=discord.Color.green(),
            )
            embed.add_field(
                name="Target Server",
                value=self.target_guild.name if self.target_guild else "None",
                inline=True,
            )
            embed.add_field(
                name="Active Bridged Channels", value=str(len(mappings)), inline=True
            )
            embed.add_field(
                name="Category Mode", value=self.config.category_mode, inline=True
            )
            embed.add_field(
                name="Webhooks Enabled",
                value=str(self.config.enable_webhooks),
                inline=True,
            )
            embed.set_footer(text="Beeper Matrix Relay Bot")
            await message.channel.send(embed=embed)

        elif cmd == "sync":
            status_msg = await message.channel.send(
                "🔄 Initiating Matrix room sync and channel creation..."
            )
            if self.on_manual_sync_callback:
                count = await self.on_manual_sync_callback()
                await status_msg.edit(
                    content=f"✅ Sync complete! Discovered and mapped **{count}** Beeper rooms."
                )
            else:
                await status_msg.edit(content="⚠️ Manual sync callback not configured.")

        elif cmd == "info":
            room_id = self.db.get_matrix_room_by_channel(message.channel.id)
            if not room_id:
                await message.channel.send(
                    "ℹ️ This Discord channel is not linked to a Matrix/Beeper room."
                )
                return
            mapping = self.db.get_room_mapping(room_id)
            embed = discord.Embed(
                title=f"Channel Info: #{message.channel.name}",
                color=discord.Color.blue(),
            )
            embed.add_field(name="Matrix Room ID", value=f"`{room_id}`", inline=False)
            if mapping:
                embed.add_field(
                    name="Original Name",
                    value=mapping.get("room_name", "N/A"),
                    inline=True,
                )
                embed.add_field(
                    name="Network / Service",
                    value=mapping.get("bridge_network", "N/A").upper(),
                    inline=True,
                )
                embed.add_field(
                    name="Topic",
                    value=mapping.get("topic", "N/A") or "None",
                    inline=False,
                )
            await message.channel.send(embed=embed)

        elif cmd == "link" and args:
            matrix_room_id = args[0].strip()
            self.db.save_room_mapping(
                matrix_room_id=matrix_room_id,
                discord_channel_id=message.channel.id,
                room_name=message.channel.name,
                bridge_network="manual",
            )
            await message.channel.send(
                f"✅ Successfully linked **#{message.channel.name}** to Matrix room `{matrix_room_id}`."
            )

        elif cmd == "xmpp" and args:
            target_jid = args[0].strip()
            clean_nick = target_jid.split("@")[0].replace("/", "-")
            channel_name = clean_nick.lower()
            clean_name = self.clean_channel_name(channel_name)
            category = "💬 XMPP"
            topic = f"XMPP chat with {target_jid}"

            channel = await self.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                target_id = f"xmpp:{target_jid}"
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel.id,
                    room_name=clean_nick,
                    bridge_network="xmpp",
                    topic=topic,
                    avatar_url="",
                )
                if self.config.enable_webhooks:
                    await self.get_or_create_webhook(channel)
                await message.channel.send(
                    f"✅ Created and linked **#{clean_name}** for XMPP contact `{target_jid}`."
                )
            else:
                await message.channel.send(
                    f"❌ Failed creating channel for `{target_jid}`."
                )

        elif cmd == "aim" and args:
            target_sn = " ".join(args).strip()
            clean_name = self.clean_channel_name(target_sn)
            category = "💬 AIM Phoenix"
            topic = f"AIM Phoenix chat with {target_sn}"

            channel = await self.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                target_id = f"aim:{target_sn.lower().replace(' ', '')}"
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel.id,
                    room_name=target_sn,
                    bridge_network="aim",
                    topic=topic,
                    avatar_url="",
                )
                if self.config.enable_webhooks:
                    await self.get_or_create_webhook(channel)
                await message.channel.send(
                    f"✅ Created and linked **#{clean_name}** for AIM contact `{target_sn}`."
                )
            else:
                await message.channel.send(
                    f"❌ Failed creating channel for `{target_sn}`."
                )

        elif cmd in ("slsk", "slskd", "soulseek") and args:
            target_user = " ".join(args).strip()
            clean_name = self.clean_channel_name(target_user)
            category = "💬 Soulseek"
            topic = f"Soulseek (SLSKD) chat with {target_user}"

            channel = await self.get_or_create_channel(
                channel_name=clean_name,
                category_name=category,
                topic=topic,
            )
            if channel:
                target_id = f"slsk:{target_user}"
                self.db.save_room_mapping(
                    matrix_room_id=target_id,
                    discord_channel_id=channel.id,
                    room_name=target_user,
                    bridge_network="soulseek",
                    topic=topic,
                    avatar_url="",
                )
                if self.config.enable_webhooks:
                    await self.get_or_create_webhook(channel)
                await message.channel.send(
                    f"✅ Created and linked **#{clean_name}** for Soulseek user `{target_user}`."
                )
            else:
                await message.channel.send(
                    f"❌ Failed creating channel for `{target_user}`."
                )

        elif cmd == "teams" and args:
            raw_token = args[0].strip()
            if raw_token.lower().startswith("bearer "):
                raw_token = raw_token[7:].strip()
            try:
                await message.delete()
            except Exception:
                pass
            if self.on_teams_token_callback:
                success, resp_text = await self.on_teams_token_callback(raw_token)
                await message.channel.send(resp_text)
            else:
                await message.channel.send("⚠️ Teams token updater is not registered.")

        elif cmd in ("sentry-test", "test-sentry", "sentry"):
            await message.channel.send("🚨 Triggering test Sentry exception...")
            try:
                raise ZeroDivisionError("Sentry test exception from Beeper-Discord bridge")
            except ZeroDivisionError as exc:
                try:
                    import sentry_sdk

                    sentry_sdk.capture_exception(exc)
                    await message.channel.send("✅ Test exception captured and sent to Sentry!")
                except Exception as err:
                    await message.channel.send(f"⚠️ Failed to send exception to Sentry: {err}")

        elif cmd == "unlink":
            room_id = self.db.get_matrix_room_by_channel(message.channel.id)
            if room_id:
                self.db.delete_room_mapping(room_id)
                await message.channel.send(
                    f"✅ Unlinked **#{message.channel.name}** from Matrix room `{room_id}`."
                )
            else:
                await message.channel.send("ℹ️ This channel was not linked.")


        else:
            embed = discord.Embed(
                title="📖 Beeper <-> Discord Bridge Commands",
                description="Commands to manage your Beeper Matrix bridge directly from Discord:",
                color=discord.Color.blurple(),
            )
            prefix = self.config.command_prefix
            embed.add_field(
                name=f"`{prefix} status`",
                value="Show bridge health, stats, and connected channels.",
                inline=False,
            )
            embed.add_field(
                name=f"`{prefix} sync`",
                value="Trigger full sync and channel creation for all Beeper chats.",
                inline=False,
            )
            embed.add_field(
                name=f"`{prefix} info`",
                value="Show Matrix room details for the current channel.",
                inline=False,
            )
            embed.add_field(
                name=f"`{prefix} link <room_id>`",
                value="Manually link current channel to a Matrix room ID.",
                inline=False,
            )
            embed.add_field(
                name=f"`{prefix} xmpp <jid>`",
                value="Create and link a new Discord channel for an XMPP contact JID.",
                inline=False,
            )
            embed.add_field(
                name=f"`{prefix} unlink`",
                value="Unlink the current channel from its Matrix room.",
                inline=False,
            )
            embed.add_field(
                name=f"`{prefix} sentry-test`",
                value="Trigger a test error and send to Sentry.",
                inline=False,
            )
            embed.add_field(
                name=f"`{prefix} help`", value="Display this help menu.", inline=False
            )
            await message.channel.send(embed=embed)

