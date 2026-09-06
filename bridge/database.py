"""Database manager for Beeper-Discord bridge using SQLite."""

import sqlite3
import time
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path

logger = logging.getLogger("beeper_bridge.database")


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_db_dir()
        self.init_db()

    def _ensure_db_dir(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        """Initialize SQLite tables and indexes."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Room mapping table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS room_mappings (
                    matrix_room_id TEXT PRIMARY KEY,
                    discord_channel_id INTEGER UNIQUE NOT NULL,
                    room_name TEXT,
                    bridge_network TEXT,
                    topic TEXT DEFAULT '',
                    avatar_url TEXT DEFAULT '',
                    created_at REAL NOT NULL,
                    last_activity REAL NOT NULL
                )
                """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_room_mappings_discord_channel ON room_mappings(discord_channel_id)"
            )

            # Webhooks table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS webhooks (
                    discord_channel_id INTEGER PRIMARY KEY,
                    webhook_id INTEGER NOT NULL,
                    webhook_token TEXT NOT NULL,
                    webhook_url TEXT NOT NULL
                )
                """)

            # Message mapping & deduplication table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS message_mappings (
                    matrix_event_id TEXT PRIMARY KEY,
                    discord_message_id INTEGER,
                    discord_channel_id INTEGER,
                    sender_id TEXT,
                    created_at REAL NOT NULL
                )
                """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_msg_discord_id ON message_mappings(discord_message_id)"
            )

            # Outgoing transaction tracking to prevent echo loops
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS outgoing_transactions (
                    txn_id TEXT PRIMARY KEY,
                    matrix_event_id TEXT DEFAULT '',
                    discord_message_id INTEGER,
                    created_at REAL NOT NULL
                )
                """)

            # Generic key-value store for state (sync tokens, etc.)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS key_value_store (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """)

            conn.commit()
            logger.debug("Database initialized at %s", self.db_path)

    # ---------------- Room Mappings ---------------- #

    def get_channel_by_matrix_room(self, matrix_room_id: str) -> Optional[int]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT discord_channel_id FROM room_mappings WHERE matrix_room_id = ?",
                (matrix_room_id,),
            )
            row = cursor.fetchone()
            return row["discord_channel_id"] if row else None

    def get_matrix_room_by_channel(self, discord_channel_id: int) -> Optional[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT matrix_room_id FROM room_mappings WHERE discord_channel_id = ?",
                (discord_channel_id,),
            )
            row = cursor.fetchone()
            return row["matrix_room_id"] if row else None

    def get_room_mapping(self, matrix_room_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM room_mappings WHERE matrix_room_id = ?",
                (matrix_room_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def save_room_mapping(
        self,
        matrix_room_id: str,
        discord_channel_id: int,
        room_name: str,
        bridge_network: str,
        topic: str = "",
        avatar_url: str = "",
    ):
        now = time.time()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO room_mappings (
                    matrix_room_id, discord_channel_id, room_name, bridge_network, topic, avatar_url, created_at, last_activity
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(matrix_room_id) DO UPDATE SET
                    discord_channel_id = excluded.discord_channel_id,
                    room_name = excluded.room_name,
                    bridge_network = excluded.bridge_network,
                    topic = excluded.topic,
                    avatar_url = excluded.avatar_url,
                    last_activity = excluded.last_activity
                """,
                (
                    matrix_room_id,
                    discord_channel_id,
                    room_name,
                    bridge_network,
                    topic,
                    avatar_url,
                    now,
                    now,
                ),
            )
            conn.commit()

    def update_room_activity(
        self, matrix_room_id: str, room_name: Optional[str] = None
    ):
        now = time.time()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if room_name:
                cursor.execute(
                    "UPDATE room_mappings SET last_activity = ?, room_name = ? WHERE matrix_room_id = ?",
                    (now, room_name, matrix_room_id),
                )
            else:
                cursor.execute(
                    "UPDATE room_mappings SET last_activity = ? WHERE matrix_room_id = ?",
                    (now, matrix_room_id),
                )
            conn.commit()

    def get_all_room_mappings(self) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM room_mappings ORDER BY last_activity DESC")
            return [dict(row) for row in cursor.fetchall()]

    def get_stale_room_mappings(self, inactive_before: float) -> List[Dict[str, Any]]:
        """Return bridge-owned channels whose last activity predates the cutoff."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM room_mappings WHERE last_activity < ? ORDER BY last_activity",
                (inactive_before,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def touch_all_room_mappings(self):
        """Start a fresh activity window when inactivity tracking is first enabled."""
        with self._get_connection() as conn:
            conn.execute("UPDATE room_mappings SET last_activity = ?", (time.time(),))
            conn.commit()

    def delete_room_mapping(self, matrix_room_id: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM room_mappings WHERE matrix_room_id = ?",
                (matrix_room_id,),
            )
            conn.commit()

    def delete_channel_state(self, matrix_room_id: str, discord_channel_id: int):
        """Remove mapping and webhook state after its Discord channel is deleted."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM webhooks WHERE discord_channel_id = ?",
                (discord_channel_id,),
            )
            cursor.execute(
                "DELETE FROM room_mappings WHERE matrix_room_id = ?",
                (matrix_room_id,),
            )
            conn.commit()

    # ---------------- Webhooks ---------------- #

    def save_webhook(
        self,
        discord_channel_id: int,
        webhook_id: int,
        webhook_token: str,
        webhook_url: str,
    ):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO webhooks (discord_channel_id, webhook_id, webhook_token, webhook_url)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(discord_channel_id) DO UPDATE SET
                    webhook_id = excluded.webhook_id,
                    webhook_token = excluded.webhook_token,
                    webhook_url = excluded.webhook_url
                """,
                (discord_channel_id, webhook_id, webhook_token, webhook_url),
            )
            conn.commit()

    def get_webhook(self, discord_channel_id: int) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM webhooks WHERE discord_channel_id = ?",
                (discord_channel_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    # ---------------- Message Tracking & Deduplication ---------------- #

    def record_message(
        self,
        matrix_event_id: str,
        discord_message_id: int,
        channel_id: int,
        sender_id: str,
    ):
        now = time.time()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO message_mappings (
                    matrix_event_id, discord_message_id, discord_channel_id, sender_id, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (matrix_event_id, discord_message_id, channel_id, sender_id, now),
            )
            conn.commit()

    def is_message_recorded(self, matrix_event_id: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM message_mappings WHERE matrix_event_id = ?",
                (matrix_event_id,),
            )
            return cursor.fetchone() is not None

    def record_outgoing_tx(self, txn_id: str, discord_message_id: int = 0):
        now = time.time()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO outgoing_transactions (txn_id, discord_message_id, created_at)
                VALUES (?, ?, ?)
                """,
                (txn_id, discord_message_id, now),
            )
            conn.commit()

    def is_outgoing_tx(self, txn_id: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM outgoing_transactions WHERE txn_id = ?",
                (txn_id,),
            )
            return cursor.fetchone() is not None

    # ---------------- Key-Value Store ---------------- #

    def set_value(self, key: str, value: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO key_value_store (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
            conn.commit()

    def get_value(self, key: str) -> Optional[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM key_value_store WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row["value"] if row else None
