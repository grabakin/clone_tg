# Telegram Chat Mirror

This tool copies messages from one Telegram chat to another using your Telegram account.
It works for channels, groups, and private chats. Even if forwarding is disabled (protected content).

## What You Need
- A Telegram account that:
  - can read the source chat (A)
  - can post in the destination chat (B)
- Telegram API ID and API Hash (free)
- One of the following:
  - Prebuilt app from GitHub Releases (recommended)
  - Docker Desktop (Windows/Mac) or Docker Engine (Linux)

## Get API ID and API Hash (One Time)
1. Open https://my.telegram.org
2. Log in with your phone number
3. Click "API development tools"
4. Create a new app (any name is fine)
5. Copy API ID and API Hash

## Download App (No Docker)
1. Open the GitHub repo and go to the Releases page.
2. Download the file for your OS:
   - Windows: `tg-mirror-windows.zip`
   - macOS: `tg-mirror-macos.zip`
   - Linux: `tg-mirror-linux.zip`
3. Unzip into a normal folder (Desktop is fine).
4. Run:
   - Windows: double-click `tg-mirror.exe` (or run `tg-mirror.exe` in PowerShell)
   - macOS/Linux: run `./tg-mirror` in Terminal

The app creates a `data` folder next to the executable. Keep it.
If macOS blocks the app, go to System Settings -> Privacy & Security and allow it.

## Quick Start (Docker)
Mac/Linux (Terminal):
```bash
docker run -it --rm -v $(pwd)/data:/data vibegrab/tg-mirror:latest
```

Windows PowerShell:
```bash
docker run -it --rm -v ${PWD}\data:/data vibegrab/tg-mirror:latest
```

Or with Docker Compose:
```bash
docker compose run --rm tg-mirror
```

Docker Hub: https://hub.docker.com/r/vibegrab/tg-mirror

## What Happens on First Run
The app will ask you for:
1. API ID and API Hash
2. Login method: qr or phone
3. Run mode (see below)
4. Network check (prints your public IP, optional)
5. Source and destination chat (type `list` to pick from your dialogs)

## Run Modes
- `oneshot` (default): copy history once and exit.
  - Run again later to copy only new messages.
- `links`: update links in already copied messages to point to chat B.
  - Use this after `oneshot` if you had a table of contents.
- `continuous`: keep listening and copy new messages in real time.
  - In this mode you can also mirror edits and deletes.

## Important: Keep the data folder
The folder `data` stores your Telegram session and the message mapping database.
If you delete it, the app will start from scratch and re-copy everything.

## Notes
- Big files are slow because the app must download and re-upload them.
- If the source chat blocks downloads, those media files cannot be copied.
- File names are preserved when possible.
- Secret chats are not supported by the Telegram API.

## Advanced (Optional)
You can run without Docker if you already use Python:
```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python mirror.py
```

You can also set environment variables to skip prompts (`API_ID`, `API_HASH`, `RUN_MODE`, etc).

## License
MIT.
