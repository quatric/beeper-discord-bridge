"""AIM Phoenix / OSCAR protocol client for Beeper Discord Bridge."""

import asyncio
import hashlib
import html
import logging
import random
import re
import struct
import uuid
from typing import Optional, Callable, Dict, Any, List

from .config import AIMConfig

logger = logging.getLogger("beeper_bridge.aim")


def clean_aim_html(text: str) -> str:
    """Strip AIM HTML tags and decode HTML entities into readable text."""
    if not text:
        return ""
    # Replace <br> and <p> with newlines
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p>", "\n", text)
    # Remove remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)
    # Decode HTML entities
    return html.unescape(text).strip()


class AIMBridgeClient:
    """Async AIM / OSCAR client for bridging AIM Phoenix conversations with Discord."""

    def __init__(
        self,
        config: AIMConfig,
        on_message_callback: Optional[Callable] = None,
        on_buddy_update: Optional[Callable] = None,
    ):
        self.config = config
        self.on_message_callback = on_message_callback
        self.on_buddy_update = on_buddy_update

        self._seq = 1
        self._req_id = 1
        self._running = False
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._main_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None

        self.is_connected = asyncio.Event()
        self.buddies: Dict[str, Dict[str, Any]] = {}
        self.formatted_screen_name: str = config.screen_name

    def _next_seq(self) -> int:
        s = self._seq
        self._seq = (self._seq + 1) & 0xFFFF
        return s

    def _next_req_id(self) -> int:
        r = self._req_id
        self._req_id = (self._req_id + 1) & 0xFFFFFFFF
        return r

    def _tlv(self, t: int, val: bytes) -> bytes:
        return struct.pack("!HH", t, len(val)) + val

    def _flap(self, channel: int, payload: bytes) -> bytes:
        return (
            struct.pack("!BBHH", 0x2A, channel, self._next_seq(), len(payload))
            + payload
        )

    def _snac(
        self,
        family: int,
        subtype: int,
        flags: int,
        data: bytes,
        req_id: Optional[int] = None,
    ) -> bytes:
        rid = self._next_req_id() if req_id is None else req_id
        return struct.pack("!HHHI", family, subtype, flags, rid) + data

    def _parse_tlvs(self, data: bytes) -> Dict[int, bytes]:
        tlvs = {}
        idx = 0
        while idx + 4 <= len(data):
            t, length = struct.unpack("!HH", data[idx : idx + 4])
            val = data[idx + 4 : idx + 4 + length]
            tlvs[t] = val
            idx += 4 + length
        return tlvs

    async def _read_flap(self) -> tuple[int, int, bytes]:
        if not self._reader:
            raise ConnectionResetError("No active reader stream")
        header = await self._reader.readexactly(6)
        if header[0] != 0x2A:
            raise ValueError(f"Invalid FLAP header magic: {header.hex()}")
        channel, seq, flen = struct.unpack("!BHH", header[1:6])
        payload = await self._reader.readexactly(flen) if flen > 0 else b""
        return channel, seq, payload

    async def start(self):
        """Start the AIM client connection and message loop."""
        if self._running:
            return
        self._running = True
        self._main_task = asyncio.create_task(self._run_loop())

    async def stop(self):
        """Gracefully stop the AIM client."""
        self._running = False
        self.is_connected.clear()
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None
        if self._main_task and not self._main_task.done():
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass
        logger.info("AIM Phoenix client stopped.")

    async def _run_loop(self):
        """Persistent connection loop with automatic reconnect."""
        while self._running:
            try:
                await self._connect_and_process()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("AIM Phoenix connection error: %s", e, exc_info=True)
            finally:
                self.is_connected.clear()
                if self._keepalive_task and not self._keepalive_task.done():
                    self._keepalive_task.cancel()
                if self._writer:
                    try:
                        self._writer.close()
                        await self._writer.wait_closed()
                    except Exception:
                        pass
                    self._writer = None
                    self._reader = None

            if not self._running or not self.config.auto_reconnect:
                break

            delay = max(5, self.config.reconnect_delay_seconds)
            logger.info("Reconnecting to AIM Phoenix in %d seconds...", delay)
            await asyncio.sleep(delay)

    async def _connect_and_process(self):
        """Execute authentication and BOS session lifecycle."""
        # 1. Connect to Auth Server
        logger.info(
            "Connecting to AIM Auth server %s:%d for screen name %s...",
            self.config.server,
            self.config.port,
            self.config.screen_name,
        )
        r_auth, w_auth = await asyncio.open_connection(
            self.config.server, self.config.port
        )
        self._reader = r_auth
        self._writer = w_auth
        self._seq = 1

        # Receive Auth server FLAP 1
        ch, seq, p = await self._read_flap()
        if ch != 1:
            raise ConnectionError(f"Unexpected Auth FLAP channel: {ch}")

        # Send FLAP 1 negotiation
        w_auth.write(self._flap(1, struct.pack("!I", 1)))
        await w_auth.drain()

        # Send BUCP Auth Key Request: SNAC (0x0017, 0x0006)
        w_auth.write(
            self._flap(
                2,
                self._snac(
                    0x0017,
                    0x0006,
                    0,
                    self._tlv(0x0001, self.config.screen_name.encode("latin1")),
                ),
            )
        )
        await w_auth.drain()

        # Receive Auth Key Response: SNAC (0x0017, 0x0007)
        ch, seq, p = await self._read_flap()

        # Compute MD5 password hash
        md5_hash = hashlib.md5(
            self.config.password.encode("latin1") + b"AOL Instant Messenger (SM)"
        ).digest()

        # Send Signon Request: SNAC (0x0017, 0x0002)
        tlvs = bytearray()
        tlvs += self._tlv(0x0001, self.config.screen_name.encode("latin1"))
        tlvs += self._tlv(0x0025, md5_hash)
        tlvs += self._tlv(0x0003, b"AOL Instant Messenger, version 5.9.3861/WIN32")
        tlvs += self._tlv(0x0016, struct.pack("!H", 0x010A))  # client id
        tlvs += self._tlv(0x0017, struct.pack("!H", 0x0005))  # major
        tlvs += self._tlv(0x0018, struct.pack("!H", 0x0009))  # minor
        tlvs += self._tlv(0x0019, struct.pack("!H", 0x0000))  # sub-minor
        tlvs += self._tlv(0x001A, struct.pack("!H", 0x0F15))  # build 3861
        tlvs += self._tlv(0x0014, struct.pack("!I", 0x00000055))  # sub-build
        tlvs += self._tlv(0x000F, b"en")
        tlvs += self._tlv(0x000E, b"us")

        w_auth.write(self._flap(2, self._snac(0x0017, 0x0002, 0, tlvs)))
        await w_auth.drain()

        # Read Signon Response: SNAC (0x0017, 0x0003)
        ch, seq, auth_resp = await self._read_flap()
        try:
            w_auth.close()
            await w_auth.wait_closed()
        except Exception:
            pass

        # SNAC header is 10 bytes (family 2B, subtype 2B, flags 2B, req_id 4B)
        auth_tlvs = self._parse_tlvs(auth_resp[10:])
        if 0x0005 not in auth_tlvs or 0x0006 not in auth_tlvs:
            err_code = auth_tlvs.get(0x0008, b"\x00\x00")
            err_url = auth_tlvs.get(0x0004, b"").decode("latin1", "replace")
            raise ConnectionError(
                f"AIM Auth failed: error={err_code.hex()} url={err_url}"
            )

        bos_server = auth_tlvs[0x0005].decode("latin1")
        cookie = auth_tlvs[0x0006]
        logger.info(
            "AIM Authentication successful for '%s'! Connecting to BOS server: %s",
            self.config.screen_name,
            bos_server,
        )

        # 2. Connect to BOS Server
        bos_host, bos_port_str = bos_server.split(":")
        r_bos, w_bos = await asyncio.open_connection(bos_host, int(bos_port_str))
        self._reader = r_bos
        self._writer = w_bos
        self._seq = 1

        # Receive BOS server FLAP 1
        ch, seq, p = await self._read_flap()

        # Send FLAP 1 with Cookie
        w_bos.write(self._flap(1, struct.pack("!I", 1) + self._tlv(0x0006, cookie)))
        await w_bos.drain()

        # Receive Host Versions SNAC (0x0001, 0x0003)
        ch, seq, p = await self._read_flap()

        # Send Rate Info Request SNAC (0x0001, 0x0006)
        w_bos.write(self._flap(2, self._snac(0x0001, 0x0006, 0, b"")))
        await w_bos.drain()

        # Receive Rate Info SNAC (0x0001, 0x0007)
        ch, seq, p = await self._read_flap()

        # Send Rate Ack SNAC (0x0001, 0x0008)
        rate_ack = struct.pack("!HHHHH", 1, 2, 3, 4, 5)
        w_bos.write(self._flap(2, self._snac(0x0001, 0x0008, 0, rate_ack)))
        await w_bos.drain()

        # Request User Info SNAC (0x0001, 0x000E)
        w_bos.write(self._flap(2, self._snac(0x0001, 0x000E, 0, b"")))
        await w_bos.drain()

        # Set ICBM Parameters SNAC (0x0004, 0x0002)
        icbm_params = struct.pack("!HIHHHI", 0, 0x0000000B, 8000, 999, 999, 0)
        w_bos.write(self._flap(2, self._snac(0x0004, 0x0002, 0, icbm_params)))
        await w_bos.drain()

        # Request SSI (Buddy list) SNAC (0x0013, 0x0004)
        w_bos.write(
            self._flap(2, self._snac(0x0013, 0x0004, 0, struct.pack("!II", 0, 0)))
        )
        await w_bos.drain()

        # Send Client Ready SNAC (0x0001, 0x001E)
        ready_families = struct.pack(
            "!HHHHHHHHHHHHHHHHHHHHHH",
            0x0001,
            0x0004,
            0x0002,
            0x0001,
            0x0003,
            0x0001,
            0x0004,
            0x0001,
            0x0006,
            0x0001,
            0x0008,
            0x0001,
            0x0009,
            0x0001,
            0x000A,
            0x0001,
            0x000B,
            0x0001,
            0x000C,
            0x0001,
            0x0013,
            0x0001,
        )
        w_bos.write(self._flap(2, self._snac(0x0001, 0x001E, 0, ready_families)))
        await w_bos.drain()

        # Send SSI Activate SNAC (0x0013, 0x0007)
        w_bos.write(self._flap(2, self._snac(0x0013, 0x0007, 0, b"")))
        await w_bos.drain()

        # If configured, set away message SNAC (0x0002, 0x0004)
        if self.config.away_message:
            await self.set_away(self.config.away_message)

        self.is_connected.set()
        logger.info(
            "🌟 AIM Phoenix session fully established! Online as '%s'",
            self.config.screen_name,
        )

        # Start background keepalive
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

        # Main packet processing loop
        while self._running:
            ch, seq, payload = await self._read_flap()
            if ch == 2:
                if len(payload) >= 10:
                    fam, sub, flags, rid = struct.unpack("!HHHI", payload[:10])
                    snac_data = payload[10:]
                    await self._handle_snac(fam, sub, flags, rid, snac_data)
            elif ch == 4:
                logger.warning("AIM server sent FLAP Channel 4 Disconnect.")
                break
            elif ch == 5:
                # Keep-alive Ping from server: echo keep-alive back
                if self._writer:
                    self._writer.write(self._flap(5, b""))
                    await self._writer.drain()

    async def _keepalive_loop(self):
        """Send periodic keep-alive pings to prevent idle socket timeouts."""
        while self._running and self.is_connected.is_set():
            try:
                await asyncio.sleep(45)
                if self._writer and not self._writer.is_closing():
                    # Send FLAP Channel 5 Keepalive Frame
                    self._writer.write(self._flap(5, b""))
                    await self._writer.drain()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("AIM keepalive ping failed: %s", e)
                break

    async def _handle_snac(
        self, family: int, subtype: int, flags: int, req_id: int, data: bytes
    ):
        """Route incoming SNAC packets by foodgroup and subtype."""
        # 1. ICBM (Instant Messaging) Foodgroup 0x0004
        if family == 0x0004:
            if subtype == 0x0007:
                # Incoming IM
                await self._handle_incoming_im(data)
            elif subtype == 0x0014:
                # Mini Typing Notification (MTN)
                await self._handle_typing_notification(data)
            elif subtype == 0x000C:
                # IM Ack
                pass

        # 2. SSI (Buddy List / Stored Information) Foodgroup 0x0013
        elif family == 0x0013:
            if subtype == 0x0006:
                self._handle_ssi_response(data)

        # 3. Buddy Services Foodgroup 0x0003
        elif family == 0x0003:
            if subtype == 0x000B:
                # Buddy arrived / online
                self._handle_buddy_arrival(data)
            elif subtype == 0x000C:
                # Buddy departed / offline
                self._handle_buddy_departure(data)

        # 4. Location Services Foodgroup 0x0002
        elif family == 0x0002:
            if subtype == 0x0006:
                # User info response
                pass

        # 5. Generic Service Controls Foodgroup 0x0001
        elif family == 0x0001:
            if subtype == 0x000F:
                # Self User Info
                self._handle_self_info(data)

    def _handle_self_info(self, data: bytes):
        """Parse self screen name formatting and user info."""
        if len(data) >= 1:
            sn_len = data[0]
            if len(data) >= 1 + sn_len:
                self.formatted_screen_name = data[1 : 1 + sn_len].decode(
                    "latin1", "replace"
                )

    def _handle_ssi_response(self, data: bytes):
        """Parse buddy list items from SSI response."""
        if len(data) < 3:
            return
        ssi_ver, num_items = struct.unpack("!BH", data[:3])
        s_idx = 3
        for _ in range(num_items):
            if s_idx + 2 > len(data):
                break
            name_len = struct.unpack("!H", data[s_idx : s_idx + 2])[0]
            s_idx += 2
            item_name = data[s_idx : s_idx + name_len].decode("latin1", "replace")
            s_idx += name_len
            if s_idx + 8 > len(data):
                break
            group_id, item_id, item_type, tlv_len = struct.unpack(
                "!HHHH", data[s_idx : s_idx + 8]
            )
            s_idx += 8 + tlv_len
            if item_type == 0x0000 and item_name:  # Buddy item
                norm_sn = item_name.lower().replace(" ", "")
                self.buddies[norm_sn] = {
                    "screen_name": item_name,
                    "online": False,
                    "idle": 0,
                    "away": False,
                }
        logger.info("Loaded %d buddies from AIM SSI contact list.", len(self.buddies))

    def _handle_buddy_arrival(self, data: bytes):
        """Handle buddy coming online or updating status."""
        if len(data) < 5:
            return
        sn_len = data[0]
        offset = 1
        buddy_sn = data[offset : offset + sn_len].decode("latin1", "replace")
        offset += sn_len
        warning_level, num_tlvs = struct.unpack("!HH", data[offset : offset + 4])
        offset += 4

        tlvs = self._parse_tlvs(data[offset:])
        user_class = struct.unpack("!H", tlvs.get(0x0001, b"\x00\x00"))[0]
        is_away = bool(user_class & 0x0020) or (0x0006 in tlvs)
        idle_time = struct.unpack("!H", tlvs[0x0004])[0] if 0x0004 in tlvs else 0

        norm_sn = buddy_sn.lower().replace(" ", "")
        self.buddies[norm_sn] = {
            "screen_name": buddy_sn,
            "online": True,
            "idle": idle_time,
            "away": is_away,
        }
        logger.info(
            "AIM Buddy Online: %s (Away=%s, Idle=%dm)",
            buddy_sn,
            is_away,
            idle_time,
        )
        if self.on_buddy_update:
            asyncio.create_task(
                self.on_buddy_update(screen_name=buddy_sn, online=True, is_away=is_away)
            )

    def _handle_buddy_departure(self, data: bytes):
        """Handle buddy signing off."""
        if len(data) < 1:
            return
        sn_len = data[0]
        buddy_sn = data[1 : 1 + sn_len].decode("latin1", "replace")
        norm_sn = buddy_sn.lower().replace(" ", "")
        if norm_sn in self.buddies:
            self.buddies[norm_sn]["online"] = False
        logger.info("AIM Buddy Offline: %s", buddy_sn)
        if self.on_buddy_update:
            asyncio.create_task(
                self.on_buddy_update(screen_name=buddy_sn, online=False, is_away=False)
            )

    async def _handle_incoming_im(self, payload: bytes):
        """Parse incoming ICBM instant message packet."""
        if len(payload) < 15:
            return
        cookie = payload[:8]
        channel = struct.unpack("!H", payload[8:10])[0]
        sn_len = payload[10]
        offset = 11
        sender_sn = payload[offset : offset + sn_len].decode("latin1", "replace")
        offset += sn_len
        warning_level, num_sender_tlvs = struct.unpack(
            "!HH", payload[offset : offset + 4]
        )
        offset += 4

        # Skip sender user info TLVs
        for _ in range(num_sender_tlvs):
            if offset + 4 > len(payload):
                break
            t, t_len = struct.unpack("!HH", payload[offset : offset + 4])
            offset += 4 + t_len

        # Parse message block TLVs
        msg_tlvs = {}
        while offset + 4 <= len(payload):
            t, t_len = struct.unpack("!HH", payload[offset : offset + 4])
            val = payload[offset + 4 : offset + 4 + t_len]
            msg_tlvs[t] = val
            offset += 4 + t_len

        is_auto_response = 0x0004 in msg_tlvs
        raw_msg = ""
        if 0x0002 in msg_tlvs:
            block = msg_tlvs[0x0002]
            b_offset = 0
            while b_offset + 4 <= len(block):
                sub_t, sub_len = struct.unpack("!HH", block[b_offset : b_offset + 4])
                sub_val = block[b_offset + 4 : b_offset + 4 + sub_len]
                b_offset += 4 + sub_len
                if sub_t == 0x0101 and len(sub_val) >= 4:
                    charset, sub_charset = struct.unpack("!HH", sub_val[:4])
                    text_bytes = sub_val[4:]
                    if charset == 0x0002:  # UTF-16BE
                        raw_msg = text_bytes.decode("utf-16-be", "replace")
                    elif charset == 0x0000:
                        raw_msg = text_bytes.decode("latin1", "replace")
                    else:
                        try:
                            raw_msg = text_bytes.decode("utf-8")
                        except UnicodeDecodeError:
                            raw_msg = text_bytes.decode("latin1", "replace")

        cleaned_text = clean_aim_html(raw_msg)
        if not cleaned_text:
            return

        msg_id = f"aim_{cookie.hex()}_{uuid.uuid4().hex[:6]}"
        logger.info(
            "Received AIM IM from %s (AutoResp=%s): %s",
            sender_sn,
            is_auto_response,
            cleaned_text[:60],
        )

        if self.on_message_callback:
            try:
                await self.on_message_callback(
                    sender_screen_name=sender_sn,
                    text=cleaned_text,
                    is_away=is_auto_response,
                    msg_id=msg_id,
                )
            except Exception as e:
                logger.error("Error in AIM on_message_callback: %s", e, exc_info=True)

    async def _handle_typing_notification(self, payload: bytes):
        """Handle mini typing notification (MTN)."""
        if len(payload) < 13:
            return
        sn_len = payload[10]
        sender_sn = payload[11 : 11 + sn_len].decode("latin1", "replace")
        event_type = struct.unpack("!H", payload[11 + sn_len : 13 + sn_len])[0]
        # 0x0000 = cleared, 0x0001 = typed, 0x0002 = typing
        logger.debug("AIM typing event from %s: type=%d", sender_sn, event_type)

    async def send_im(self, recipient: str, text: str) -> bool:
        """Send an instant message to an AIM screen name."""
        if not self._writer or not self.is_connected.is_set():
            logger.warning("Cannot send AIM IM: client is not connected.")
            return False

        try:
            cookie = struct.pack("!Q", random.randint(1, 0xFFFFFFFFFFFFFFFF))
            channel = 1  # Standard text IM
            recip_bytes = recipient.encode("latin1", errors="replace")
            recip_header = struct.pack("!B", len(recip_bytes)) + recip_bytes

            # Convert newlines to HTML break tags
            html_text = html.escape(text).replace("\n", "<BR>")
            html_body = f"<HTML><BODY>{html_text}</BODY></HTML>"

            try:
                raw_bytes = html_body.encode("latin1")
                charset = 0x0000
            except UnicodeEncodeError:
                raw_bytes = html_body.encode("utf-16-be")
                charset = 0x0002

            tlv_0101 = (
                struct.pack("!HHHH", 0x0101, len(raw_bytes) + 4, charset, 0x0000)
                + raw_bytes
            )
            tlv_0501 = struct.pack("!HHBBBB", 0x0501, 4, 0x01, 0x01, 0x01, 0x01)
            msg_block = tlv_0501 + tlv_0101

            tlv_0002 = struct.pack("!HH", 0x0002, len(msg_block)) + msg_block
            tlv_0003 = struct.pack("!HH", 0x0003, 0)
            tlv_0006 = struct.pack("!HH", 0x0006, 0)

            icbm_payload = (
                cookie
                + struct.pack("!H", channel)
                + recip_header
                + tlv_0002
                + tlv_0003
                + tlv_0006
            )

            # Send SNAC (0x0004, 0x0006) - Outgoing IM
            packet = self._flap(2, self._snac(0x0004, 0x0006, 0, icbm_payload))
            self._writer.write(packet)
            await self._writer.drain()
            logger.info("Sent AIM message to %s: %s", recipient, text[:60])
            return True
        except Exception as e:
            logger.error("Failed to send AIM message to %s: %s", recipient, e)
            return False

    async def set_away(self, away_text: str = ""):
        """Set or clear AIM Away status message."""
        if not self._writer:
            return
        try:
            tlvs = bytearray()
            if away_text:
                away_html = (
                    f"<HTML><BODY>{html.escape(away_text)}</BODY></HTML>".encode(
                        "latin1", "replace"
                    )
                )
                tlvs += self._tlv(0x0003, away_html)
                tlvs += self._tlv(0x0004, b'text/aolrtf; charset="us-ascii"')
            else:
                tlvs += self._tlv(0x0003, b"")

            # Send SNAC (0x0002, 0x0004) - Set Location / Away Info
            packet = self._flap(2, self._snac(0x0002, 0x0004, 0, tlvs))
            self._writer.write(packet)
            await self._writer.drain()
            logger.info("AIM Away status updated: %r", away_text)
        except Exception as e:
            logger.warning("Failed to update AIM away status: %s", e)
