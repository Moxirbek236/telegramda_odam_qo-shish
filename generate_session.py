from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv
import os


load_dotenv()

api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()

if not api_id_raw or not api_hash:
    raise RuntimeError("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH in environment/.env.")

api_id = int(api_id_raw)

print("=" * 70)
print("Generate Telegram String Session")
print("=" * 70)
print("Login tugagach, pastdagi session string ni Render env ga qo'ying:")
print("TELEGRAM_SESSION_STRING=<copied_value>")
print("=" * 70)

with TelegramClient(StringSession(), api_id, api_hash) as client:
    client.start()
    session_string = client.session.save()
    print("\nSESSION_STRING:")
    print(session_string)
