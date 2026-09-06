"""IMAP email polling client, used to relay alert mailboxes into Discord."""

import asyncio
import email
import imaplib
import logging
from email.header import decode_header
from email.utils import parseaddr
from typing import Any, Callable, Optional

from .config import EmailAccount, EmailConfig
from .database import Database

logger = logging.getLogger("beeper_bridge.email")


def _decode_header_value(raw: Optional[str]) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    out = []
    for text, charset in parts:
        if isinstance(text, bytes):
            try:
                out.append(text.decode(charset or "utf-8", errors="replace"))
            except (LookupError, TypeError):
                out.append(text.decode("utf-8", errors="replace"))
        else:
            out.append(text)
    return "".join(out)


def _extract_plain_body(msg: email.message.Message, max_chars: int = 1500) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition") or "")
            if content_type == "text/plain" and "attachment" not in disposition:
                try:
                    payload = part.get_payload(decode=True) or b""
                    charset = part.get_content_charset() or "utf-8"
                    body = payload.decode(charset, errors="replace")
                    break
                except Exception:
                    continue
    else:
        try:
            payload = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            body = payload.decode(charset, errors="replace")
        except Exception:
            body = ""

    body = body.strip()
    if len(body) > max_chars:
        body = body[:max_chars].rstrip() + "…"
    return body


class EmailBridgeClient:
    """Polls one or more IMAP mailboxes and relays new mail to Discord."""

    def __init__(
        self,
        config: EmailConfig,
        db: Database,
        on_message_callback: Optional[Callable] = None,
    ):
        self.config = config
        self.db = db
        self.on_message_callback = on_message_callback
        self.is_running = False
        self._tasks: list[asyncio.Task] = []

    async def start(self):
        if not self.config.enabled or not self.config.accounts:
            return
        self.is_running = True
        for account in self.config.accounts:
            self._tasks.append(
                asyncio.create_task(self._poll_account_forever(account))
            )
        logger.info(
            "Email bridge started for %d account(s)", len(self.config.accounts)
        )

    async def stop(self):
        self.is_running = False
        for task in self._tasks:
            if not task.done():
                task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks = []
        logger.info("Email bridge stopped")

    async def _poll_account_forever(self, account: EmailAccount):
        poll_interval = max(15, self.config.poll_interval_seconds)
        cursor_key = f"email_last_uid_{account.address}"

        loop = asyncio.get_running_loop()
        while self.is_running:
            try:
                await asyncio.to_thread(self._poll_once, account, cursor_key, loop)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Error polling mailbox %s: %s", account.address, e, exc_info=True
                )
            await asyncio.sleep(poll_interval)

    def _poll_once(
        self, account: EmailAccount, cursor_key: str, loop: asyncio.AbstractEventLoop
    ):
        """Blocking IMAP poll for one account; run via asyncio.to_thread."""
        last_uid_raw = self.db.get_value(cursor_key)
        last_uid = int(last_uid_raw) if last_uid_raw else 0

        conn = imaplib.IMAP4_SSL(self.config.imap_host, self.config.imap_port)
        try:
            conn.login(account.address, account.app_password)
            conn.select("INBOX", readonly=True)

            status, data = conn.uid("search", None, "ALL")
            if status != "OK" or not data or not data[0]:
                return
            all_uids = [int(u) for u in data[0].split()]

            if last_uid == 0:
                # First run for this account: don't replay the whole inbox,
                # just remember the current high-water mark.
                if all_uids:
                    self.db.set_value(cursor_key, str(max(all_uids)))
                return

            new_uids = sorted(u for u in all_uids if u > last_uid)
            if not new_uids:
                return

            for uid in new_uids:
                try:
                    status, msg_data = conn.uid("fetch", str(uid), "(RFC822)")
                    if status != "OK" or not msg_data or not msg_data[0]:
                        continue
                    raw = msg_data[0][1]
                    msg = email.message_from_bytes(raw)
                    subject = _decode_header_value(msg.get("Subject")) or "(no subject)"
                    sender_name, sender_addr = parseaddr(
                        _decode_header_value(msg.get("From"))
                    )
                    sender_display = sender_name or sender_addr or "Unknown sender"
                    body = _extract_plain_body(msg)

                    if self.on_message_callback:
                        asyncio.run_coroutine_threadsafe(
                            self.on_message_callback(
                                account=account.address,
                                subject=subject,
                                sender=sender_display,
                                sender_addr=sender_addr,
                                body=body,
                                msg_id=f"email:{account.address}:{uid}",
                            ),
                            loop,
                        ).result()
                except Exception as e:
                    logger.error(
                        "Error processing email UID %d for %s: %s",
                        uid,
                        account.address,
                        e,
                    )

            self.db.set_value(cursor_key, str(max(new_uids)))
        finally:
            try:
                conn.logout()
            except Exception:
                pass
