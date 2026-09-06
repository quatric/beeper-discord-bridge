"""Tests for Beeper-Discord bridge components."""

import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from bridge.config import Config, MatrixConfig, DiscordConfig, BridgeConfig
from bridge.database import Database
from bridge.discord_client import DiscordBridgeClient
from bridge.matrix_client import MatrixBridgeClient
from bridge.teams_client import clean_teams_html, _ic3_sender, _valid_display_name


class TestBridgeComponents(unittest.TestCase):
    def setUp(self):
        self.db_path = "/tmp/test_beeper_bridge.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db = Database(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_database_room_mappings(self):
        # Save a room mapping
        self.db.save_room_mapping(
            matrix_room_id="!test_room_1:beeper.local",
            discord_channel_id=123456789,
            room_name="Alice Smith",
            bridge_network="whatsapp",
            topic="Chat with Alice",
        )

        # Retrieve channel by matrix room
        channel_id = self.db.get_channel_by_matrix_room("!test_room_1:beeper.local")
        self.assertEqual(channel_id, 123456789)

        # Retrieve matrix room by channel
        room_id = self.db.get_matrix_room_by_channel(123456789)
        self.assertEqual(room_id, "!test_room_1:beeper.local")

        # Check full mapping
        mapping = self.db.get_room_mapping("!test_room_1:beeper.local")
        self.assertIsNotNone(mapping)
        self.assertEqual(mapping["room_name"], "Alice Smith")
        self.assertEqual(mapping["bridge_network"], "whatsapp")

    def test_database_webhooks(self):
        self.db.save_webhook(
            discord_channel_id=123456789,
            webhook_id=987654321,
            webhook_token="test_token",
            webhook_url="https://discord.com/api/webhooks/987654321/test_token",
        )

        wh = self.db.get_webhook(123456789)
        self.assertIsNotNone(wh)
        self.assertEqual(wh["webhook_id"], 987654321)
        self.assertEqual(wh["webhook_token"], "test_token")

    def test_message_deduplication(self):
        matrix_event_id = "$event_12345:beeper.local"
        self.assertFalse(self.db.is_message_recorded(matrix_event_id))

        self.db.record_message(
            matrix_event_id=matrix_event_id,
            discord_message_id=5555555,
            channel_id=123456789,
            sender_id="@alice:beeper.local",
        )

        self.assertTrue(self.db.is_message_recorded(matrix_event_id))

    def test_channel_name_sanitizer(self):
        d_cfg = DiscordConfig(bot_token="test", guild_id=123)
        b_cfg = BridgeConfig()
        bot = DiscordBridgeClient(d_cfg, b_cfg, self.db)

        # Platform categories make network prefixes redundant.
        name1 = bot.sanitize_channel_name("Alice Smith & Bob!", network="whatsapp")
        self.assertEqual(name1, "alice-smith-bob")

        name2 = bot.sanitize_channel_name("Team Chat [Dev]", network="telegram")
        self.assertEqual(name2, "team-chat-dev")

        name3 = bot.sanitize_channel_name("John Doe (iMessage)", network="imessage")
        self.assertEqual(name3, "john-doe-imessage")

    def test_teams_html_is_discord_safe(self):
        cleaned = clean_teams_html("<div>Cost * 2 &amp; _private_</div>")
        self.assertEqual(cleaned, r"Cost \* 2 & \_private\_")

    def test_teams_wire_id_is_not_a_display_name(self):
        wire_id = "8:orgid:5addaade-a562-4e81-a521-e8be67113e79"
        self.assertEqual(_valid_display_name(wire_id), "")
        sender_id, sender_name = _ic3_sender(
            {"from": wire_id, "imdisplayname": wire_id}
        )
        self.assertEqual(sender_id, wire_id)
        self.assertEqual(sender_name, "Teams User")

    def test_ic3_prefers_token_display_name(self):
        _, sender_name = _ic3_sender(
            {"imdisplayname": "8:orgid:abc", "fromDisplayNameInToken": "Alex Doe"}
        )
        self.assertEqual(sender_name, "Alex Doe")

    def test_stale_room_mappings(self):
        self.db.save_room_mapping("teams:old", 444, "Old chat", "msteams")
        with self.db._get_connection() as conn:
            conn.execute(
                "UPDATE room_mappings SET last_activity = 10 WHERE matrix_room_id = ?",
                ("teams:old",),
            )
        stale = self.db.get_stale_room_mappings(11)
        self.assertEqual([row["matrix_room_id"] for row in stale], ["teams:old"])

    def test_mxc_to_http_conversion(self):
        m_cfg = MatrixConfig(homeserver="https://matrix.beeper.com")
        client = MatrixBridgeClient(m_cfg)

        url = client.mxc_to_http_url("mxc://beeper.com/abcdef123456")
        self.assertEqual(
            url,
            "https://matrix.beeper.com/_matrix/media/r0/download/beeper.com/abcdef123456",
        )

    def test_sentry_config(self):
        b_cfg = BridgeConfig(

            sentry_dsn="https://20cc096076527940901b46de398ebc68@o107347.ingest.us.sentry.io/4512040257585152"
        )
        self.assertEqual(
            b_cfg.sentry_dsn,
            "https://20cc096076527940901b46de398ebc68@o107347.ingest.us.sentry.io/4512040257585152",
        )


class TestEncryptedMatrixEvents(unittest.IsolatedAsyncioTestCase):

    async def test_live_encrypted_event_provisions_room_and_requests_key(self):
        on_room_discovered = AsyncMock()
        client = MatrixBridgeClient(
            MatrixConfig(), on_room_discovered=on_room_discovered
        )
        client._initial_sync_complete = True
        client.client = MagicMock()
        client.client.request_room_key = AsyncMock(return_value=SimpleNamespace())
        room = SimpleNamespace(room_id="!whatsapp:beeper.local")
        event = SimpleNamespace(
            event_id="$encrypted",
            session_id="megolm-session",
        )

        await client._on_megolm_event(room, event)

        on_room_discovered.assert_awaited_once_with(
            room.room_id, room, only_if_named=False
        )
        client.client.request_room_key.assert_awaited_once_with(event)
        self.assertEqual(
            client._pending_megolm_events[event.session_id], [(room, event)]
        )

    async def test_startup_encrypted_event_does_not_mass_provision(self):
        on_room_discovered = AsyncMock()
        client = MatrixBridgeClient(
            MatrixConfig(), on_room_discovered=on_room_discovered
        )
        client.client = MagicMock()
        client.client.request_room_key = AsyncMock()

        await client._on_megolm_event(
            SimpleNamespace(room_id="!old:beeper.local"),
            SimpleNamespace(event_id="$old", session_id="old-session"),
        )

        on_room_discovered.assert_not_awaited()
        client.client.request_room_key.assert_not_awaited()

    async def test_received_room_key_replays_queued_message(self):
        on_message = AsyncMock()
        client = MatrixBridgeClient(MatrixConfig(), on_message_callback=on_message)
        room = SimpleNamespace(room_id="!whatsapp:beeper.local")
        encrypted = SimpleNamespace(event_id="$encrypted", session_id="session")
        decrypted = SimpleNamespace(event_id="$decrypted", sender="@alice:beeper.local")
        client._pending_megolm_events["session"] = [(room, encrypted)]
        client.client = MagicMock()
        client.client.decrypt_event.return_value = decrypted

        await client._on_room_key_event(SimpleNamespace(session_id="session"))

        client.client.decrypt_event.assert_called_once_with(encrypted)
        on_message.assert_awaited_once_with(room, decrypted, is_self=False)
        self.assertNotIn("session", client._pending_megolm_events)


if __name__ == "__main__":
    unittest.main()
