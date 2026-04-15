from telethon import TelegramClient, utils
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat
import csv
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
from tqdm import tqdm  # Import tqdm for progress bar

# Setup logging
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# Initialize logger
logger = setup_logging()


def read_int_env(key, default):
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default
    return int(raw.strip())

# WARNING MESSAGE
print("=" * 60)
print("IMPORTANT WARNING:")
print("=" * 60)
print("1. This tool is for EDUCATIONAL PURPOSES ONLY.")
print("2. You must have PERMISSION to scrape members from any group.")
print("3. RESPECT user privacy and Telegram's Terms of Service.")
print("4. Make sure to TURN OFF 2-Step Verification in your")
print("   Telegram account settings before using this tool.")
print("=" * 60)

# Confirmation skipped as requested
confirm = 'yes'

load_dotenv()

# Source group configuration
SOURCE_GROUP_ID = read_int_env("SOURCE_GROUP_ID", -1001937901847)  # 1937901847 with Telethon supergroup prefix
SCRAPE_LIMIT = None  # None means scrape as many members as accessible

SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "anonym")
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "").strip()
RUNNING_ON_RENDER = os.getenv("RENDER", "").lower() == "true"

api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
if not api_id_raw or not api_hash:
    raise RuntimeError("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH environment variables.")
api_id = int(api_id_raw)

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), api_id, api_hash)
else:
    client = TelegramClient(SESSION_NAME, api_id, api_hash)


def _build_entity_candidates(target):
    """Build several equivalent references for robust Telethon entity resolution."""
    candidates = [target]
    text = str(target).strip()
    if not text:
        return candidates

    if text.startswith("https://t.me/"):
        text = "@" + text.split("https://t.me/", 1)[1]
        candidates.append(text)
    elif text.startswith("t.me/"):
        text = "@" + text.split("t.me/", 1)[1]
        candidates.append(text)

    if text.startswith("@"):
        candidates.append(text[1:])
        return list(dict.fromkeys(candidates))

    if text.lstrip("-").isdigit():
        raw = int(text)
        raw_abs_text = str(abs(raw))
        candidates.append(raw)

        # For channel/supergroup IDs: 1937901847 <-> -1001937901847
        if raw_abs_text.startswith("100") and len(raw_abs_text) > 10:
            short_id = int(raw_abs_text[3:])
            candidates.extend([short_id, int(f"-100{short_id}")])
        elif len(raw_abs_text) <= 10:
            candidates.extend([int(raw_abs_text), int(f"-100{raw_abs_text}")])

    return list(dict.fromkeys(candidates))


def _candidate_short_ids(candidates):
    short_ids = set()
    for candidate in candidates:
        if not isinstance(candidate, int):
            continue
        value = str(abs(candidate))
        if value.startswith("100") and len(value) > 10:
            short_ids.add(int(value[3:]))
        elif len(value) <= 10:
            short_ids.add(int(value))
    return short_ids


async def resolve_group_entity(client, target):
    candidates = _build_entity_candidates(target)
    last_error = None

    for candidate in candidates:
        try:
            return await client.get_entity(candidate)
        except Exception as exc:
            last_error = exc

    short_ids = _candidate_short_ids(candidates)
    peer_ids = {c for c in candidates if isinstance(c, int)}

    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        if not isinstance(entity, (Channel, Chat)):
            continue
        if entity.id in short_ids:
            return entity
        try:
            if utils.get_peer_id(entity) in peer_ids:
                return entity
        except Exception:
            continue

    if last_error:
        raise last_error
    raise ValueError(f"Could not resolve entity for target: {target}")


async def start_client():
    if SESSION_STRING:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError("TELEGRAM_SESSION_STRING is invalid or expired.")
        return

    if RUNNING_ON_RENDER:
        raise RuntimeError("TELEGRAM_SESSION_STRING is required on Render (non-interactive environment).")

    await client.start()

async def main():
    await start_client()
    
    print(f"\nScraping members from {SOURCE_GROUP_ID}...")
    print("This may take a while depending on group size...")
    
    try:
        source_group = await resolve_group_entity(client, SOURCE_GROUP_ID)
        resolved_source_peer_id = utils.get_peer_id(source_group)
        print(f"Resolved source group: {source_group.title} ({resolved_source_peer_id})")

        # Get all members with progress bar
        logger.info(f"Starting scrape for {SOURCE_GROUP_ID} -> resolved as {resolved_source_peer_id}")
        
        print("\n" + "=" * 60)
        print("FETCHING MEMBERS...")
        print("=" * 60 + "\n")
        
        # Initialize progress bar
        progress_bar = tqdm(
            desc="Scraping Members",
            unit=" members",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]"
        )
        
        # Get members with progress tracking
        members = []
        async for user in client.iter_participants(source_group, limit=SCRAPE_LIMIT):
            members.append(user)
            progress_bar.update(1)
        
        progress_bar.close()
        
        if not members:
            print("\nNo members found or you don't have access to this group.")
            logger.warning(f"No members found for {SOURCE_GROUP_ID}")
            return
            
        print(f"\nFound {len(members)} members.")
        logger.info(f"Found {len(members)} members in {SOURCE_GROUP_ID}")
        
        # Create csv directory if it doesn't exist
        os.makedirs("csv", exist_ok=True)
        
        # Get filename from user
        # Filename automated
        filename = "scraped_members.csv"
        
        csv_path = os.path.join("csv", filename)
        
        print("\n" + "=" * 60)
        print("SAVING DATA TO CSV...")
        print("=" * 60 + "\n")
        
        # Save to CSV with progress bar
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["username", "id", "access_hash"])
            
            # Add progress bar for writing
            with tqdm(total=len(members), desc="Saving to CSV", unit=" rows") as save_bar:
                for user in members:
                    username = user.username if user.username else ""
                    access_hash = user.access_hash if user.access_hash is not None else ""
                    writer.writerow([username, user.id, access_hash])
                    save_bar.update(1)
        
        print(f"\n" + "=" * 60)
        print(f"SUCCESS: {len(members)} members saved to csv/{filename}")
        print("=" * 60)
        logger.info(f"Data saved to {csv_path}")
        
    except Exception as e:
        print(f"\n ERROR: {e}")
        logger.error(f"Error occurred: {e}", exc_info=True)
        print("\nPossible reasons:")
        print("1. You're not a member of the group")
        print("2. The group doesn't exist")
        print("3. You don't have permission to view members")
        print("4. Telegram API restrictions")

# Main execution
try:
    with client:
        client.loop.run_until_complete(main())
        
    # Final warning
    print("\n" + "=" * 60)
    print("REMEMBER: Use this data responsibly and ethically.")
    print("Respect user privacy and Telegram's Terms of Service.")
    print("=" * 60)
    
except KeyboardInterrupt:
    print("\n\n Process interrupted by user.")
    logger.warning("Process interrupted by user")
except Exception as e:
    logger.error(f"Fatal error: {e}", exc_info=True)
    print(f"\n Fatal error occurred. Check logs for details.")
