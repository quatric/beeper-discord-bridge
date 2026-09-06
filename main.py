#!/usr/bin/env python3
"""Main entry point for Beeper-Discord Bridge."""

import argparse
import asyncio
import logging
import signal
import sys

from bridge.config import Config
from bridge.core import BeeperDiscordBridge


def setup_logging(level_name: str):
    numeric_level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


async def async_main():
    parser = argparse.ArgumentParser(
        description="Beeper <-> Discord Bidirectional Dynamic Bridge"
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config.yaml",
        help="Path to configuration YAML file (default: config.yaml)",
    )
    args = parser.parse_args()

    config = Config.load(args.config)
    setup_logging(config.bridge.log_level)

    if config.bridge.sentry_dsn:
        try:
            import sentry_sdk

            sentry_sdk.init(
                dsn=config.bridge.sentry_dsn,
                send_default_pii=True,
            )
            logging.getLogger("beeper_bridge.main").info("Sentry SDK initialized.")
        except ImportError:
            logging.getLogger("beeper_bridge.main").warning(
                "sentry-sdk package not installed, error monitoring disabled."
            )
        except Exception as e:
            logging.getLogger("beeper_bridge.main").warning(
                "Failed to initialize Sentry SDK: %s", e
            )

    logger = logging.getLogger("beeper_bridge.main")
    logger.info("Initializing Beeper <-> Discord Bridge...")


    # Validate essential configurations
    if not config.discord.bot_token:
        logger.error(
            "Discord bot token is missing! Please configure DISCORD_BOT_TOKEN or discord.bot_token in config.yaml"
        )
        sys.exit(1)

    if not config.matrix.access_token or not config.matrix.user_id:
        logger.error(
            "Beeper/Matrix credentials missing! Please configure MATRIX_USER_ID and MATRIX_ACCESS_TOKEN in config.yaml"
        )
        sys.exit(1)

    bridge = BeeperDiscordBridge(config)

    # Set up signal handling
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Termination signal received. Shutting down...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    bridge_task = asyncio.create_task(bridge.start())

    # Wait for either bridge exit or stop signal
    done, pending = await asyncio.wait(
        [bridge_task, asyncio.create_task(stop_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )

    await bridge.stop()

    for task in pending:
        task.cancel()


def main():
    try:
        asyncio.run(async_main())
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
