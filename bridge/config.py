"""Configuration loader for the Beeper-Discord bridge."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
import yaml
from dotenv import load_dotenv


@dataclass
class MatrixConfig:
    homeserver: str = "https://matrix.beeper.com"
    user_id: str = ""
    access_token: str = ""
    device_id: Optional[str] = None
    store_path: str = "./matrix_store"
    encryption_enabled: bool = True
    sync_self_messages: bool = False
    sync_batch_limit: int = 50


@dataclass
class DiscordConfig:
    bot_token: str = ""
    guild_id: int = 0
    admin_user_ids: List[int] = field(default_factory=list)
    category_mode: str = "by_platform"  # "by_platform", "single_category", "none"
    default_category_name: str = "Beeper Chats"
    status_channel_name: str = "beeper-status"
    channel_prefix: str = ""
    enable_webhooks: bool = True
    command_prefix: str = "!beeper"


@dataclass
class BridgeConfig:
    db_path: str = "./beeper_bridge.db"
    download_media: bool = True
    max_attachment_size_mb: int = 25
    auto_create_channels: bool = True
    log_level: str = "INFO"
    send_reactions: bool = True
    channel_inactivity_ttl_hours: int = 24
    channel_cleanup_interval_seconds: int = 300


@dataclass
class XMPPConfig:
    enabled: bool = False
    jid: str = ""
    password: str = ""
    server: Optional[str] = None
    port: int = 5222
    use_tls: bool = True
    use_ssl: bool = False
    status_message: str = "Online via Discord Bridge"
    category_name: str = "💬 XMPP"
    auto_reconnect: bool = True


@dataclass
class TeamsConfig:
    enabled: bool = False
    auth_token: str = ""
    client_id: str = ""
    tenant_id: str = "common"
    poll_interval_seconds: int = 10
    category_name: str = "💬 MS Teams"


@dataclass
class BeeperDesktopConfig:
    enabled: bool = False
    access_token: str = ""
    api_url: str = "http://localhost:23373"
    poll_interval_seconds: int = 5
    sync_self_messages: bool = False


@dataclass
class BlueBubblesConfig:
    enabled: bool = False
    server_url: str = "http://localhost:1234"
    password: str = ""
    poll_interval_seconds: int = 5
    sync_self_messages: bool = False
    category_name: str = "💬 iMessage"
    max_attachment_size_mb: int = 25


@dataclass
class AIMConfig:
    enabled: bool = False
    screen_name: str = ""
    password: str = ""
    server: str = "iwarg.ddns.net"
    port: int = 5190
    category_name: str = "💬 AIM Phoenix"
    away_message: str = ""
    status_message: str = "Online via Discord Bridge"
    auto_reconnect: bool = True
    reconnect_delay_seconds: int = 10


@dataclass
class SLSKDConfig:
    enabled: bool = False
    url: str = "http://localhost:5030"
    api_key: str = ""
    poll_interval_seconds: int = 5
    category_name: str = "💬 Soulseek"
    sync_rooms: bool = True
    sync_private: bool = True
    sync_self_messages: bool = True


@dataclass
class EmailAccount:
    address: str = ""
    app_password: str = ""


@dataclass
class EmailConfig:
    enabled: bool = False
    imap_host: str = "imap.gmail.com"
    imap_port: int = 993
    accounts: List[EmailAccount] = field(default_factory=list)
    poll_interval_seconds: int = 60
    channel_name: str = "alert"
    category_name: str = "Log"


@dataclass
class Config:
    matrix: MatrixConfig = field(default_factory=MatrixConfig)
    discord: DiscordConfig = field(default_factory=DiscordConfig)
    bridge: BridgeConfig = field(default_factory=BridgeConfig)
    xmpp: XMPPConfig = field(default_factory=XMPPConfig)
    teams: TeamsConfig = field(default_factory=TeamsConfig)
    beeper_desktop: BeeperDesktopConfig = field(default_factory=BeeperDesktopConfig)
    bluebubbles: BlueBubblesConfig = field(default_factory=BlueBubblesConfig)
    aim: AIMConfig = field(default_factory=AIMConfig)
    slskd: SLSKDConfig = field(default_factory=SLSKDConfig)
    email: EmailConfig = field(default_factory=EmailConfig)

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> "Config":
        """Load configuration from a YAML file and override with environment variables."""
        load_dotenv()

        data = {}
        path = Path(config_path)
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

        matrix_data = data.get("matrix", {})
        discord_data = data.get("discord", {})
        bridge_data = data.get("bridge", {})

        # Matrix overrides
        matrix_cfg = MatrixConfig(
            homeserver=os.getenv(
                "MATRIX_HOMESERVER",
                matrix_data.get("homeserver", "https://matrix.beeper.com"),
            ),
            user_id=os.getenv("MATRIX_USER_ID", matrix_data.get("user_id", "")),
            access_token=os.getenv(
                "MATRIX_ACCESS_TOKEN", matrix_data.get("access_token", "")
            ),
            device_id=os.getenv("MATRIX_DEVICE_ID", matrix_data.get("device_id", None)),
            store_path=os.getenv(
                "MATRIX_STORE_PATH",
                matrix_data.get("store_path", "./matrix_store"),
            ),
            encryption_enabled=bool(
                os.getenv(
                    "MATRIX_ENCRYPTION_ENABLED",
                    matrix_data.get("encryption_enabled", True),
                )
            ),
            sync_self_messages=bool(
                os.getenv(
                    "MATRIX_SYNC_SELF_MESSAGES",
                    matrix_data.get("sync_self_messages", False),
                )
            ),
            sync_batch_limit=int(
                os.getenv(
                    "MATRIX_SYNC_BATCH_LIMIT",
                    matrix_data.get("sync_batch_limit", 50),
                )
            ),
        )

        # Admin user IDs parsing
        admin_ids_raw = discord_data.get("admin_user_ids", [])
        if isinstance(admin_ids_raw, str):
            admin_ids = [
                int(x.strip()) for x in admin_ids_raw.split(",") if x.strip().isdigit()
            ]
        elif isinstance(admin_ids_raw, list):
            admin_ids = [int(x) for x in admin_ids_raw if str(x).isdigit()]
        else:
            admin_ids = []

        env_admin_ids = os.getenv("DISCORD_ADMIN_USER_IDS")
        if env_admin_ids:
            admin_ids = [
                int(x.strip()) for x in env_admin_ids.split(",") if x.strip().isdigit()
            ]

        discord_cfg = DiscordConfig(
            bot_token=os.getenv("DISCORD_BOT_TOKEN", discord_data.get("bot_token", "")),
            guild_id=int(
                os.getenv("DISCORD_GUILD_ID", discord_data.get("guild_id", 0)) or 0
            ),
            admin_user_ids=admin_ids,
            category_mode=os.getenv(
                "DISCORD_CATEGORY_MODE",
                discord_data.get("category_mode", "by_platform"),
            ),
            default_category_name=os.getenv(
                "DISCORD_DEFAULT_CATEGORY_NAME",
                discord_data.get("default_category_name", "Beeper Chats"),
            ),
            status_channel_name=os.getenv(
                "DISCORD_STATUS_CHANNEL_NAME",
                discord_data.get("status_channel_name", "beeper-status"),
            ),
            channel_prefix=os.getenv(
                "DISCORD_CHANNEL_PREFIX",
                discord_data.get("channel_prefix", ""),
            ),
            enable_webhooks=bool(
                os.getenv(
                    "DISCORD_ENABLE_WEBHOOKS",
                    discord_data.get("enable_webhooks", True),
                )
            ),
            command_prefix=os.getenv(
                "DISCORD_COMMAND_PREFIX",
                discord_data.get("command_prefix", "!beeper"),
            ),
        )

        bridge_cfg = BridgeConfig(
            db_path=os.getenv(
                "BRIDGE_DB_PATH",
                bridge_data.get("db_path", "./beeper_bridge.db"),
            ),
            download_media=bool(
                os.getenv(
                    "BRIDGE_DOWNLOAD_MEDIA",
                    bridge_data.get("download_media", True),
                )
            ),
            max_attachment_size_mb=int(
                os.getenv(
                    "BRIDGE_MAX_ATTACHMENT_SIZE_MB",
                    bridge_data.get("max_attachment_size_mb", 25),
                )
            ),
            auto_create_channels=bool(
                os.getenv(
                    "BRIDGE_AUTO_CREATE_CHANNELS",
                    bridge_data.get("auto_create_channels", True),
                )
            ),
            log_level=os.getenv(
                "BRIDGE_LOG_LEVEL", bridge_data.get("log_level", "INFO")
            ),
            send_reactions=bool(
                os.getenv(
                    "BRIDGE_SEND_REACTIONS",
                    bridge_data.get("send_reactions", True),
                )
            ),
            channel_inactivity_ttl_hours=int(
                os.getenv(
                    "BRIDGE_CHANNEL_INACTIVITY_TTL_HOURS",
                    bridge_data.get("channel_inactivity_ttl_hours", 24),
                )
            ),
            channel_cleanup_interval_seconds=int(
                os.getenv(
                    "BRIDGE_CHANNEL_CLEANUP_INTERVAL_SECONDS",
                    bridge_data.get("channel_cleanup_interval_seconds", 300),
                )
            ),
        )

        xmpp_data = data.get("xmpp", {})
        xmpp_cfg = XMPPConfig(
            enabled=bool(os.getenv("XMPP_ENABLED", xmpp_data.get("enabled", False))),
            jid=os.getenv("XMPP_JID", xmpp_data.get("jid", "")),
            password=os.getenv("XMPP_PASSWORD", xmpp_data.get("password", "")),
            server=os.getenv("XMPP_SERVER", xmpp_data.get("server", None)),
            port=int(os.getenv("XMPP_PORT", xmpp_data.get("port", 5222))),
            use_tls=bool(os.getenv("XMPP_USE_TLS", xmpp_data.get("use_tls", True))),
            use_ssl=bool(os.getenv("XMPP_USE_SSL", xmpp_data.get("use_ssl", False))),
            status_message=os.getenv(
                "XMPP_STATUS_MESSAGE",
                xmpp_data.get("status_message", "Online via Discord Bridge"),
            ),
            category_name=os.getenv(
                "XMPP_CATEGORY_NAME", xmpp_data.get("category_name", "💬 XMPP")
            ),
            auto_reconnect=bool(
                os.getenv("XMPP_AUTO_RECONNECT", xmpp_data.get("auto_reconnect", True))
            ),
        )

        teams_data = data.get("teams", {})
        teams_cfg = TeamsConfig(
            enabled=bool(os.getenv("TEAMS_ENABLED", teams_data.get("enabled", False))),
            auth_token=os.getenv("TEAMS_AUTH_TOKEN", teams_data.get("auth_token", "")),
            client_id=os.getenv("TEAMS_CLIENT_ID", teams_data.get("client_id", "")),
            tenant_id=os.getenv(
                "TEAMS_TENANT_ID", teams_data.get("tenant_id", "common")
            ),
            poll_interval_seconds=int(
                os.getenv(
                    "TEAMS_POLL_INTERVAL_SECONDS",
                    teams_data.get("poll_interval_seconds", 10),
                )
            ),
            category_name=os.getenv(
                "TEAMS_CATEGORY_NAME",
                teams_data.get("category_name", "💬 MS Teams"),
            ),
        )

        beeper_desktop_data = data.get("beeper_desktop", {})
        beeper_desktop_cfg = BeeperDesktopConfig(
            enabled=bool(
                os.getenv(
                    "BEEPER_DESKTOP_ENABLED",
                    beeper_desktop_data.get("enabled", False),
                )
            ),
            access_token=os.getenv(
                "BEEPER_ACCESS_TOKEN",
                beeper_desktop_data.get("access_token", ""),
            ),
            api_url=os.getenv(
                "BEEPER_DESKTOP_API_URL",
                beeper_desktop_data.get("api_url", "http://localhost:23373"),
            ),
            poll_interval_seconds=int(
                os.getenv(
                    "BEEPER_DESKTOP_POLL_INTERVAL",
                    beeper_desktop_data.get("poll_interval_seconds", 5),
                )
            ),
            sync_self_messages=bool(
                os.getenv(
                    "BEEPER_DESKTOP_SYNC_SELF",
                    beeper_desktop_data.get("sync_self_messages", False),
                )
            ),
        )

        bluebubbles_data = data.get("bluebubbles", {})
        bluebubbles_cfg = BlueBubblesConfig(
            enabled=bool(
                os.getenv(
                    "BLUEBUBBLES_ENABLED",
                    bluebubbles_data.get("enabled", False),
                )
            ),
            server_url=os.getenv(
                "BLUEBUBBLES_SERVER_URL",
                bluebubbles_data.get("server_url", "http://localhost:1234"),
            ),
            password=os.getenv(
                "BLUEBUBBLES_PASSWORD",
                bluebubbles_data.get("password", ""),
            ),
            poll_interval_seconds=int(
                os.getenv(
                    "BLUEBUBBLES_POLL_INTERVAL",
                    bluebubbles_data.get("poll_interval_seconds", 5),
                )
            ),
            sync_self_messages=bool(
                os.getenv(
                    "BLUEBUBBLES_SYNC_SELF",
                    bluebubbles_data.get("sync_self_messages", False),
                )
            ),
            category_name=os.getenv(
                "BLUEBUBBLES_CATEGORY_NAME",
                bluebubbles_data.get("category_name", "💬 iMessage"),
            ),
            max_attachment_size_mb=int(
                bluebubbles_data.get(
                    "max_attachment_size_mb", bridge_data.get("max_attachment_size_mb", 25)
                )
            ),
        )

        aim_data = data.get("aim", {})
        aim_cfg = AIMConfig(
            enabled=bool(os.getenv("AIM_ENABLED", aim_data.get("enabled", False))),
            screen_name=os.getenv("AIM_SCREEN_NAME", aim_data.get("screen_name", "")),
            password=os.getenv("AIM_PASSWORD", aim_data.get("password", "")),
            server=os.getenv("AIM_SERVER", aim_data.get("server", "iwarg.ddns.net")),
            port=int(os.getenv("AIM_PORT", aim_data.get("port", 5190))),
            category_name=os.getenv(
                "AIM_CATEGORY_NAME", aim_data.get("category_name", "💬 AIM Phoenix")
            ),
            away_message=os.getenv(
                "AIM_AWAY_MESSAGE", aim_data.get("away_message", "")
            ),
            status_message=os.getenv(
                "AIM_STATUS_MESSAGE",
                aim_data.get("status_message", "Online via Discord Bridge"),
            ),
            auto_reconnect=bool(
                os.getenv("AIM_AUTO_RECONNECT", aim_data.get("auto_reconnect", True))
            ),
            reconnect_delay_seconds=int(
                os.getenv(
                    "AIM_RECONNECT_DELAY_SECONDS",
                    aim_data.get("reconnect_delay_seconds", 10),
                )
            ),
        )

        slskd_data = data.get("slskd", {})
        slskd_cfg = SLSKDConfig(
            enabled=bool(os.getenv("SLSKD_ENABLED", slskd_data.get("enabled", False))),
            url=os.getenv("SLSKD_URL", slskd_data.get("url", "http://localhost:5030")),
            api_key=os.getenv("SLSKD_API_KEY", slskd_data.get("api_key", "")),
            poll_interval_seconds=int(
                os.getenv(
                    "SLSKD_POLL_INTERVAL_SECONDS",
                    slskd_data.get("poll_interval_seconds", 5),
                )
            ),
            category_name=os.getenv(
                "SLSKD_CATEGORY_NAME",
                slskd_data.get("category_name", "💬 Soulseek"),
            ),
            sync_rooms=bool(
                os.getenv("SLSKD_SYNC_ROOMS", slskd_data.get("sync_rooms", True))
            ),
            sync_private=bool(
                os.getenv("SLSKD_SYNC_PRIVATE", slskd_data.get("sync_private", True))
            ),
            sync_self_messages=bool(
                os.getenv(
                    "SLSKD_SYNC_SELF_MESSAGES",
                    slskd_data.get("sync_self_messages", True),
                )
            ),
        )

        email_data = data.get("email", {})
        email_cfg = EmailConfig(
            enabled=bool(os.getenv("EMAIL_ENABLED", email_data.get("enabled", False))),
            imap_host=os.getenv(
                "EMAIL_IMAP_HOST", email_data.get("imap_host", "imap.gmail.com")
            ),
            imap_port=int(
                os.getenv("EMAIL_IMAP_PORT", email_data.get("imap_port", 993))
            ),
            accounts=[
                EmailAccount(
                    address=acct.get("address", ""),
                    app_password=acct.get("app_password", ""),
                )
                for acct in email_data.get("accounts", [])
                if acct.get("address")
            ],
            poll_interval_seconds=int(
                os.getenv(
                    "EMAIL_POLL_INTERVAL_SECONDS",
                    email_data.get("poll_interval_seconds", 60),
                )
            ),
            channel_name=os.getenv(
                "EMAIL_CHANNEL_NAME", email_data.get("channel_name", "alert")
            ),
            category_name=os.getenv(
                "EMAIL_CATEGORY_NAME", email_data.get("category_name", "Log")
            ),
        )

        return cls(
            matrix=matrix_cfg,
            discord=discord_cfg,
            bridge=bridge_cfg,
            xmpp=xmpp_cfg,
            teams=teams_cfg,
            beeper_desktop=beeper_desktop_cfg,
            bluebubbles=bluebubbles_cfg,
            aim=aim_cfg,
            slskd=slskd_cfg,
            email=email_cfg,
        )
