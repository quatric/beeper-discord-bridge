"""Shared text/HTML cleanup helpers used by multiple bridge clients."""

import html
import logging
import re

from bs4 import BeautifulSoup

logger = logging.getLogger("beeper_bridge.text_utils")

_TAG_RE = re.compile(r"<[a-zA-Z][^>]*>")


def looks_like_html(text: str) -> bool:
    """Cheap check so plain-text messages skip HTML parsing entirely."""
    return bool(text) and bool(_TAG_RE.search(text))


def strip_html_to_discord_text(raw: str) -> str:
    """Convert stray/embedded HTML (e.g. leaked <a> tags from a source chat
    network) into plain, Discord-safe text. No-op for text with no HTML tags.
    """
    if not looks_like_html(raw):
        return raw
    try:
        decoded = html.unescape(raw)
        soup = BeautifulSoup(decoded, "html.parser")

        for emoji in soup.find_all("emoji"):
            alt = emoji.get("alt") or emoji.get("title") or ""
            emoji.replace_with(alt)

        for at in soup.find_all("at"):
            mention_text = at.get_text()
            if mention_text and not mention_text.startswith("@"):
                mention_text = f"@{mention_text}"
            at.replace_with(mention_text)

        # Render links as "label (url)" plain text so Discord doesn't choke
        # on raw <a> markup, and so the real destination stays visible even
        # when label and href disagree.
        for anchor in soup.find_all("a"):
            label = anchor.get_text(" ", strip=True)
            href = anchor.get("href", "")
            anchor.replace_with(
                f"{label} ({href})" if href and href != label else label or href
            )

        for br in soup.find_all("br"):
            br.replace_with("\n")
        for block in soup.find_all(["p", "div", "li"]):
            if block.name == "li":
                block.insert_before("• ")
            block.append("\n")

        cleaned = soup.get_text("", strip=False).strip()
        cleaned = cleaned.replace("\xa0", " ").replace("&nbsp;", " ")
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()
    except Exception as e:
        logger.debug("Error stripping HTML from message body: %s", e)
        decoded = html.unescape(raw)
        clean = re.sub(r"<br\s*/?>", "\n", decoded, flags=re.IGNORECASE)
        clean = re.sub(r"</p>", "\n", clean, flags=re.IGNORECASE)
        clean = re.sub(r"</div>", "\n", clean, flags=re.IGNORECASE)
        clean = re.sub(r"<[^>]+>", "", clean)
        return clean.strip()
