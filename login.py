#!/usr/bin/env python3
"""Helper script to log into Beeper/Matrix using username and password."""

import argparse
import asyncio
import getpass
import sys
from nio import AsyncClient, LoginResponse
import yaml

from bridge.config import Config


async def login(
    username: str, password: str, homeserver: str = "https://matrix.beeper.com"
):
    print(f"Connecting to {homeserver} as {username}...")
    client = AsyncClient(homeserver=homeserver)

    resp = await client.login(
        password=password,
        user=username,
        device_name="Beeper-Discord-Bridge",
    )

    if isinstance(resp, LoginResponse):
        print("\n Login Successful!")
        print(f"User ID:      {resp.user_id}")
        print(f"Device ID:    {resp.device_id}")
        print(f"Access Token: {resp.access_token[:10]}... (saved to config.yaml)")

        # Update config.yaml
        config_path = "config.yaml"
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            if "matrix" not in data:
                data["matrix"] = {}

            data["matrix"]["homeserver"] = homeserver
            data["matrix"]["user_id"] = resp.user_id
            data["matrix"]["access_token"] = resp.access_token
            data["matrix"]["device_id"] = resp.device_id

            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(data, f, sort_keys=False)

            print(
                f"\n✅ config.yaml has been automatically updated with your Beeper credentials!"
            )
        except Exception as e:
            print(f"Error saving to config.yaml: {e}")

        await client.close()
        return resp.access_token
    else:
        print(f"\n❌ Login failed: {resp}")
        await client.close()
        return None


def main():
    parser = argparse.ArgumentParser(description="Authenticate with Beeper Matrix")
    parser.add_argument(
        "-u", "--user", default=None, help="Matrix User ID or username (e.g. @user:beeper.com)"
    )
    parser.add_argument(
        "-p", "--password", default=None, help="Matrix password (prompted if omitted)"
    )
    parser.add_argument(
        "--homeserver",
        default="https://matrix.beeper.com",
        help="Matrix homeserver URL",
    )
    args = parser.parse_args()

    user = args.user
    if not user:
        user = input("Enter Matrix User ID or username: ").strip()
    password = args.password
    if not password:
        password = getpass.getpass(f"Enter Matrix password for {user}: ")

    asyncio.run(login(user, password, args.homeserver))


if __name__ == "__main__":
    main()
