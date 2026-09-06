# ⚡ Beeper <-> Discord Dynamic Bridge

A bidirectional, dynamic bridge that connects your **Beeper (Matrix)** account to a private **Discord** server. It automatically creates channels for your incoming and ongoing chats (WhatsApp, iMessage, Telegram, Signal, SMS/RCS, etc.), puppets senders with real avatars and display names using Discord Webhooks, and lets you read and reply seamlessly directly from Discord.

---

## 🔍 Why this bridge? (GitHub Ecosystem Comparison)

If you look on GitHub for Matrix and Discord bridging, you will commonly find:
1. **`mautrix-discord`**: This is built to bring Discord *into* Matrix/Beeper (the reverse of what you want).
2. **`matrix-appservice-discord`**: Requires homeserver administrator access to register an application service with Synapse, which standard Beeper users do not have.
3. **`matterbridge`**: Supports simple relays, but requires **manual, static configuration** for every single room and channel—meaning it cannot dynamically auto-create channels for your personal DMs.

**This bridge** connects directly to Beeper as a standard Matrix client, listening to your user sync stream. Whenever you receive or participate in a chat on any platform connected to Beeper, it **automatically provisions and organizes Discord channels** with no manual wiring required.

---

## ✨ Key Features

* **🔄 Dynamic Channel Provisioning**: Automatically creates a dedicated text channel in Discord for each Beeper chat as soon as activity occurs or on initial sync.
* **📂 Smart Category Grouping**: Groups conversations into categories by service (`💬 WhatsApp`, `💬 iMessage`, `💬 Telegram`, `💬 SMS / RCS`, `💬 Signal`, `💬 Direct Messages`, etc.).
* **🎭 Webhook Puppeting**: Relays incoming messages using Discord Webhooks with the sender's actual display name and avatar photo.
* **💬 Bidirectional Chatting**: Type a message in any bridged Discord channel to instantly send it back out through your Beeper account to the other party.
* **📎 Media & Attachment Bridging**: Seamlessly forwards images, videos, audio/voice notes, and file attachments in both directions.
* **🛡️ Deduplication & Echo Prevention**: Prevents feedback loops and echoes for messages sent from Discord or your other Beeper clients.
* **🤖 In-Discord Control Commands**: Manage the bridge directly from Discord (`!beeper status`, `!beeper sync`, `!beeper info`, `!beeper link`).

---

## 🚀 Setup & Installation

### Step 1: Discord Bot Setup
1. Visit the [Discord Developer Portal](https://discord.com/developers/applications).
2. Click **New Application**, give it a name (e.g. `Beeper Relay`), and create it.
3. Under **Bot**:
   * Click **Reset Token** and copy your **Bot Token**.
   * Enable **Privileged Gateway Intents**:
     * ✅ **Server Members Intent**
     * ✅ **Message Content Intent**
4. Under **OAuth2 > URL Generator**:
   * Select Scopes: `bot`, `applications.commands`
   * Select Bot Permissions: `Administrator` (or `Manage Channels`, `Manage Webhooks`, `Send Messages`, `Attach Files`, `Read Message History`, `Add Reactions`).
   * Copy the generated URL and open it in your browser to invite the bot to your private Discord server.
5. In Discord, right-click your Server Name -> **Copy Server ID** (enable Developer Mode in Discord settings if you don't see this).

---

### Step 2: Get Your Beeper Matrix Credentials
Because Beeper runs on Matrix, you need your Beeper Matrix User ID and Access Token:

#### Method A: Beeper Desktop
1. Open the **Beeper Desktop** app.
2. Open Developer Tools (`Ctrl+Shift+I` on Windows/Linux or `Cmd+Option+I` on macOS, or via `Help > Toggle Developer Tools`).
3. Go to the **Console** tab and run:
   ```javascript
   localStorage.getItem("mx_access_token")
   ```
4. Copy the token string (starts with `syt_...` or similar).
5. Your User ID is `@your_username:beeper.com` (found in Beeper Settings).

---

### Step 3: Configure the Bridge
1. Copy the example configuration file:
   ```bash
   cp config.example.yaml config.yaml
   ```
2. Edit `config.yaml` with your credentials:
   ```yaml
   matrix:
     homeserver: "https://matrix.beeper.com"
     user_id: "@your_username:beeper.com"
     access_token: "YOUR_BEEPER_ACCESS_TOKEN"

   discord:
     bot_token: "YOUR_DISCORD_BOT_TOKEN"
     guild_id: 123456789012345678  # Your Discord Server ID
     admin_user_ids:
       - 123456789012345678        # Your Discord User ID
     category_mode: "by_platform"   # Options: "by_platform", "single_category", "none"
   ```

---

### Step 4: Run the Bridge

#### Run Manually
```bash
./venv/bin/python3 main.py -c config.yaml
```

#### Run as a Systemd Service
To keep the bridge running 24/7 in the background:
```bash
cp systemd/beeper-discord-bridge.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now beeper-discord-bridge
systemctl status beeper-discord-bridge
```

---

## 🎮 In-Discord Bot Commands

You can run these commands from any channel in your Discord server:

| Command | Description |
| :--- | :--- |
| `!beeper status` | Shows bridge status, Matrix connection health, and number of bridged channels. |
| `!beeper sync` | Manually triggers full synchronization and auto-creates channels for all Beeper rooms. |
| `!beeper info` | Displays Matrix Room ID, network type, and metadata for the current Discord channel. |
| `!beeper link <room_id>` | Manually associates the current Discord channel with a specific Matrix room ID. |
| `!beeper unlink` | Unlinks the current channel from its Matrix room. |
| `!beeper help` | Shows the help menu with command usage. |

---

## 📁 Codebase Structure

```
beeper-discord-bridge/
├── config.example.yaml          # Sample configuration file
├── requirements.txt             # Python package dependencies
├── main.py                      # Application entrypoint & CLI parser
├── bridge/
│   ├── __init__.py
│   ├── config.py                # Configuration loader & validator
│   ├── database.py              # SQLite storage for mappings, webhooks & deduplication
│   ├── matrix_client.py         # Beeper Matrix client (sync, media download/upload, Olm/E2EE)
│   ├── discord_client.py        # Discord bot (dynamic channel/category management & webhooks)
│   └── core.py                  # Core coordinator handling bidirectional event routing
├── tests/
│   └── test_bridge.py           # Unit and integration test suite
└── systemd/
    └── beeper-discord-bridge.service  # Systemd service unit file
```
