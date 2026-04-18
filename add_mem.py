from telethon import TelegramClient, errors, utils
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetParticipantRequest, InviteToChannelRequest
from telethon.tl.functions.messages import AddChatUserRequest
from telethon.tl.types import Channel, Chat, InputUser
import csv
import asyncio
import contextlib
from dotenv import load_dotenv
import os
import sys


RUNNING_UNDER_PM2 = "PM2_HOME" in os.environ or "pm_id" in os.environ
DOTENV_OVERRIDE = os.getenv(
    "DOTENV_OVERRIDE",
    "true" if RUNNING_UNDER_PM2 else "false",
).strip().lower() in ("1", "true", "yes", "on")


def read_int_env(key, default):
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default
    return int(raw.strip())


def read_bool_env(key, default):
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def read_group_target_env(key, default):
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default

    value = raw.strip()
    if value.lstrip("-").isdigit():
        return int(value)
    return value


load_dotenv(override=DOTENV_OVERRIDE)

SOURCE_GROUP_ID = read_group_target_env("SOURCE_GROUP_ID", -1001937901847)
TARGET_GROUP_ID = read_group_target_env("TARGET_GROUP_ID", -1002000540919)
MAX_SUCCESSFUL_ADDS = read_int_env("MAX_SUCCESSFUL_ADDS", 0)
INVITE_DELAY_SECONDS = max(0, read_int_env("INVITE_DELAY_SECONDS", 60))
FLOODWAIT_HEARTBEAT_SECONDS = max(5, read_int_env("FLOODWAIT_HEARTBEAT_SECONDS", 60))
AUTO_RAISE_DELAY_ON_FLOODWAIT = read_bool_env("AUTO_RAISE_DELAY_ON_FLOODWAIT", True)
SAFE_DELAY_AFTER_FLOODWAIT = max(10, read_int_env("SAFE_DELAY_AFTER_FLOODWAIT", 60))
DELAY_AFTER_EACH_USER = read_bool_env("DELAY_AFTER_EACH_USER", True)
STOP_ON_PEERFLOOD = read_bool_env("STOP_ON_PEERFLOOD", True)
PEERFLOOD_COOLDOWN_SECONDS = max(60, read_int_env("PEERFLOOD_COOLDOWN_SECONDS", 1800))
MAX_FLOODWAIT_SECONDS = max(0, read_int_env("MAX_FLOODWAIT_SECONDS", 3600))
ENABLE_SKIP_CACHE = read_bool_env("ENABLE_SKIP_CACHE", True)
SKIP_CACHE_FILE = os.getenv("SKIP_CACHE_FILE", "invite_skip_cache.txt")
PRELOAD_TARGET_MEMBER_IDS = read_bool_env("PRELOAD_TARGET_MEMBER_IDS", True)
TARGET_MEMBER_PRELOAD_LIMIT = max(0, read_int_env("TARGET_MEMBER_PRELOAD_LIMIT", 200000))
POST_INVITE_CHECK_ENABLED = read_bool_env("POST_INVITE_CHECK_ENABLED", True)
POST_INVITE_CHECK_ATTEMPTS = max(1, read_int_env("POST_INVITE_CHECK_ATTEMPTS", 3))
POST_INVITE_CHECK_DELAY_SECONDS = max(0, read_int_env("POST_INVITE_CHECK_DELAY_SECONDS", 8))

CSV_DIR = os.getenv("CSV_DIR", "csv")
CSV_FILE = os.getenv("CSV_FILE", "scraped_members.csv")

SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "anonym")
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "").strip()
RUNNING_ON_RENDER = os.getenv("RENDER", "").lower() == "true"
RUN_FOREVER = read_bool_env("RUN_FOREVER", RUNNING_ON_RENDER)
IDLE_SLEEP_SECONDS = max(10, read_int_env("IDLE_SLEEP_SECONDS", 300))
RETRY_ON_FATAL_SECONDS = max(10, read_int_env("RETRY_ON_FATAL_SECONDS", 30))
PM2_HOLD_ON_EXIT = read_bool_env(
    "PM2_HOLD_ON_EXIT",
    RUNNING_UNDER_PM2 and not RUN_FOREVER,
)
PM2_HOLD_HEARTBEAT_SECONDS = max(
    30,
    read_int_env("PM2_HOLD_HEARTBEAT_SECONDS", 300),
)
SUCCESS_LIMIT_ENABLED = MAX_SUCCESSFUL_ADDS > 0
SUCCESS_LIMIT_LABEL = str(MAX_SUCCESSFUL_ADDS) if SUCCESS_LIMIT_ENABLED else "unlimited"

api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
if not api_id_raw or not api_hash:
    raise RuntimeError("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH environment variables.")
api_id = int(api_id_raw)


def resolve_file_session_path(session_name):
    return os.path.abspath(f"{session_name}.session")


FILE_SESSION_PATH = resolve_file_session_path(SESSION_NAME)
SESSION_BACKEND_LABEL = "StringSession" if SESSION_STRING else "FileSession"

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), api_id, api_hash)
else:
    client = TelegramClient(SESSION_NAME, api_id, api_hash)

print("=" * 70)
print("IMPORTANT WARNING & TERMS OF USE:")
print("=" * 70)
print("1. EDUCATIONAL PURPOSE ONLY - This tool is for learning about APIs")
print("2. PERMISSION REQUIRED - You must have admin rights to add members")
print("3. RESPECT PRIVACY - Only add users who want to join the group")
print("4. TELEGRAM TERMS - Violating Telegram's ToS can result in account ban")
print("5. SPAMMING PROHIBITED - Adding users without consent is illegal")
print("6. RATE LIMITS - Respect Telegram's rate limits to avoid bans")
print("=" * 70)

print("\nConfigured source group:", SOURCE_GROUP_ID)
print("Configured target group:", TARGET_GROUP_ID)
print("Dotenv override enabled:", DOTENV_OVERRIDE)
print("Session backend:", SESSION_BACKEND_LABEL)
if SESSION_STRING:
    print("Session source: TELEGRAM_SESSION_STRING")
else:
    print("Session file:", FILE_SESSION_PATH)
    if RUNNING_ON_RENDER or RUNNING_UNDER_PM2 or not sys.stdin.isatty():
        print(
            "Warning: TELEGRAM_SESSION_STRING is empty in a headless environment. "
            "The local .session file must be writable."
        )
print("Success limit:", SUCCESS_LIMIT_LABEL)
print("Invite delay:", INVITE_DELAY_SECONDS, "seconds")
print("Delay after each user:", DELAY_AFTER_EACH_USER)
print("FloodWait heartbeat:", FLOODWAIT_HEARTBEAT_SECONDS, "seconds")
print("Stop on PeerFlood:", STOP_ON_PEERFLOOD)
print("PeerFlood cooldown:", PEERFLOOD_COOLDOWN_SECONDS, "seconds")
if MAX_FLOODWAIT_SECONDS > 0:
    print("Max accepted FloodWait:", MAX_FLOODWAIT_SECONDS, "seconds")
else:
    print("Max accepted FloodWait: disabled (wait any duration)")
print("Skip cache enabled:", ENABLE_SKIP_CACHE)
print("Preload target members:", PRELOAD_TARGET_MEMBER_IDS)
print("Post-invite verification:", POST_INVITE_CHECK_ENABLED)
if POST_INVITE_CHECK_ENABLED:
    print("Post-invite verify attempts:", POST_INVITE_CHECK_ATTEMPTS)
    print("Post-invite verify delay:", POST_INVITE_CHECK_DELAY_SECONDS, "seconds")
print("Run forever:", RUN_FOREVER)
if RUNNING_UNDER_PM2 and not RUN_FOREVER:
    print(
        "Warning: PM2 auto-restarts processes by default. "
        "With RUN_FOREVER=False, this worker can loop unless PM2 autorestart is disabled."
    )
    print("PM2 hold on exit:", PM2_HOLD_ON_EXIT)
    if PM2_HOLD_ON_EXIT:
        print("PM2 hold heartbeat:", PM2_HOLD_HEARTBEAT_SECONDS, "seconds")


def _build_entity_candidates(target):
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
        if raw_abs_text.startswith("100") and len(raw_abs_text) > 10:
            short_id = int(raw_abs_text[3:])
            # If full channel/supergroup ID is already provided (-100...),
            # do not down-convert to short id to avoid accidental PeerUser matches.
            if not text.startswith("-100"):
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
            entity = await client.get_entity(candidate)
            if isinstance(entity, (Channel, Chat)):
                return entity
            last_error = ValueError(
                f"Resolved non-group entity ({type(entity).__name__}) for candidate: {candidate}"
            )
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


def render_progress(processed, total, success):
    if total > 0:
        progress = (processed / total) * 100
    else:
        progress = 100
    filled = int(progress / 5)
    bar = "#" * filled + "-" * (20 - filled)
    return (
        f"Progress: [{bar}] {progress:5.1f}% "
        f"| Processed {processed}/{total} "
        f"| Confirmed {success}/{SUCCESS_LIMIT_LABEL}"
    )


def reached_success_limit(success_count):
    return SUCCESS_LIMIT_ENABLED and success_count >= MAX_SUCCESSFUL_ADDS


def normalize_username(username):
    value = (username or "").strip().lower()
    if value.startswith("@"):
        return value[1:]
    return value


def build_user_keys(username, user_id):
    keys = set()
    normalized_username = normalize_username(username)
    if normalized_username:
        keys.add(f"u:{normalized_username}")

    raw_user_id = (user_id or "").strip()
    if raw_user_id.lstrip("-").isdigit():
        keys.add(f"id:{int(raw_user_id)}")
    return keys


def resolve_skip_cache_path():
    if os.path.isabs(SKIP_CACHE_FILE):
        return SKIP_CACHE_FILE
    return os.path.join(CSV_DIR, SKIP_CACHE_FILE)


def load_skip_keys(cache_path):
    if not ENABLE_SKIP_CACHE:
        return set()

    if not os.path.exists(cache_path):
        return set()

    loaded = set()
    with open(cache_path, encoding="utf-8") as cache_file:
        for line in cache_file:
            key = line.strip()
            if not key:
                continue
            loaded.add(key)
    return loaded


def save_skip_keys(cache_path, skip_keys):
    if not ENABLE_SKIP_CACHE:
        return

    cache_dir = os.path.dirname(cache_path)
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)

    with open(cache_path, "w", encoding="utf-8") as cache_file:
        for key in sorted(skip_keys):
            cache_file.write(key + "\n")


class ExcessiveFloodWaitError(RuntimeError):
    def __init__(self, seconds, user_label):
        self.seconds = int(seconds)
        self.user_label = user_label
        super().__init__(f"FloodWait {self.seconds}s for {self.user_label}")


def ensure_file_session_is_writable():
    if SESSION_STRING:
        return

    session_dir = os.path.dirname(FILE_SESSION_PATH) or os.getcwd()
    if os.path.exists(FILE_SESSION_PATH):
        if os.access(FILE_SESSION_PATH, os.W_OK):
            return
        raise RuntimeError(
            "Telegram session file is not writable: "
            f"{FILE_SESSION_PATH}. Fix file permissions or set TELEGRAM_SESSION_STRING."
        )

    if os.access(session_dir, os.W_OK):
        return

    raise RuntimeError(
        "Telegram session directory is not writable: "
        f"{session_dir}. Fix directory permissions or set TELEGRAM_SESSION_STRING."
    )


async def resolve_input_user(username, user_id, access_hash, user_label):
    # Prefer id+access_hash from CSV to avoid username resolve rate limits.
    if username and username.lstrip("-").isdigit() and not user_id:
        user_id = username
        username = ""

    while True:
        try:
            if user_id and access_hash:
                return InputUser(int(user_id), int(access_hash))
            if user_id:
                return await client.get_input_entity(int(user_id))
            if username:
                lookup_username = username if username.startswith("@") else f"@{username}"
                return await client.get_input_entity(lookup_username)
            raise ValueError("Missing username/user_id in CSV row.")
        except errors.FloodWaitError as e:
            print(f"\nFloodWait while resolving {user_label}: {e.seconds} seconds")
            await wait_with_heartbeat(e.seconds, FLOODWAIT_HEARTBEAT_SECONDS, "Resolve")


async def invite_user(group, user):
    if isinstance(group, Channel):
        await client(InviteToChannelRequest(group, [user]))
    elif isinstance(group, Chat):
        await client(AddChatUserRequest(group.id, user, fwd_limit=0))
    else:
        raise TypeError(f"Unsupported group type: {type(group).__name__}")


async def get_membership_state(group, user, resolved_user_id, user_label, check_label):
    if isinstance(group, Channel):
        while True:
            try:
                await client(GetParticipantRequest(group, user))
                return "member"
            except (
                errors.UserNotParticipantError,
                errors.ParticipantIdInvalidError,
                errors.UserIdInvalidError,
            ):
                return "not_member"
            except errors.ChatAdminRequiredError:
                print(
                    f"\n{check_label} could not verify membership for {user_label}: "
                    "additional admin rights required."
                )
                return "unknown"
            except errors.FloodWaitError as e:
                print(
                    f"\nFloodWait while checking membership for {user_label}: "
                    f"{e.seconds} seconds"
                )
                await wait_with_heartbeat(
                    e.seconds,
                    FLOODWAIT_HEARTBEAT_SECONDS,
                    "Membership check",
                )
            except Exception as e:
                print(
                    f"\n{check_label} membership check failed for {user_label}: "
                    f"{type(e).__name__}: {e}"
                )
                return "unknown"

    if not isinstance(group, Chat):
        print(
            f"\n{check_label} could not verify membership for {user_label}: "
            f"unsupported group type {type(group).__name__}."
        )
        return "unknown"

    if not resolved_user_id or not resolved_user_id.lstrip("-").isdigit():
        print(
            f"\n{check_label} could not verify membership for {user_label}: "
            "missing numeric user_id."
        )
        return "unknown"

    target_user_id = int(resolved_user_id)
    while True:
        try:
            async for participant in client.iter_participants(group):
                if participant.id == target_user_id:
                    return "member"
            return "not_member"
        except errors.ChatAdminRequiredError:
            print(
                f"\n{check_label} could not verify membership for {user_label}: "
                "additional admin rights required."
            )
            return "unknown"
        except errors.FloodWaitError as e:
            print(f"\nFloodWait while checking membership for {user_label}: {e.seconds} seconds")
            await wait_with_heartbeat(e.seconds, FLOODWAIT_HEARTBEAT_SECONDS, "Membership check")
        except Exception as e:
            print(
                f"\n{check_label} membership check failed for {user_label}: "
                f"{type(e).__name__}: {e}"
            )
            return "unknown"


async def is_user_already_in_group(group, user, resolved_user_id, user_label):
    membership_state = await get_membership_state(
        group,
        user,
        resolved_user_id,
        user_label,
        "Pre-invite check",
    )
    return membership_state == "member"


async def verify_post_invite_membership(group, user, resolved_user_id, user_label):
    if not POST_INVITE_CHECK_ENABLED:
        return "confirmed"

    for attempt in range(1, POST_INVITE_CHECK_ATTEMPTS + 1):
        membership_state = await get_membership_state(
            group,
            user,
            resolved_user_id,
            user_label,
            "Post-invite check",
        )
        if membership_state == "member":
            return "confirmed"
        if membership_state == "unknown":
            return "unknown"

        if attempt < POST_INVITE_CHECK_ATTEMPTS:
            print(
                f"\nPost-invite check pending for {user_label}: "
                f"not visible in target group yet "
                f"(attempt {attempt}/{POST_INVITE_CHECK_ATTEMPTS}). "
                f"Retrying in {POST_INVITE_CHECK_DELAY_SECONDS} seconds..."
            )
            await asyncio.sleep(POST_INVITE_CHECK_DELAY_SECONDS)

    return "not_confirmed"


async def preload_target_member_ids(group):
    if not PRELOAD_TARGET_MEMBER_IDS:
        return set()
    if not isinstance(group, (Channel, Chat)):
        return set()

    print("\nPreloading target member IDs to reduce unnecessary invite attempts...")
    member_ids = set()
    loaded = 0
    try:
        async for participant in client.iter_participants(group):
            member_ids.add(participant.id)
            loaded += 1
            if TARGET_MEMBER_PRELOAD_LIMIT > 0 and loaded >= TARGET_MEMBER_PRELOAD_LIMIT:
                print(
                    "Target member preload limit reached: "
                    f"{TARGET_MEMBER_PRELOAD_LIMIT}"
                )
                break
    except errors.ChatAdminRequiredError:
        print(
            "Could not preload target members (admin permission required). "
            "Continuing without preload."
        )
        return set()
    except Exception as e:
        print(
            f"Could not preload target members: {type(e).__name__}: {e}. "
            "Continuing without preload."
        )
        return set()

    print(f"Preloaded target members: {len(member_ids)}")
    return member_ids


async def wait_with_heartbeat(total_seconds, heartbeat_seconds, prefix):
    remaining = int(max(0, total_seconds))
    if remaining == 0:
        return

    print(f"\n{prefix}: waiting {remaining} seconds")
    while remaining > 0:
        sleep_for = min(heartbeat_seconds, remaining)
        await asyncio.sleep(sleep_for)
        remaining -= sleep_for
        if remaining > 0:
            print(f"{prefix}: {remaining} seconds remaining...")


async def wait_between_users(delay_seconds):
    wait_seconds = int(max(0, delay_seconds))
    if wait_seconds <= 0:
        return
    print(f"\nWaiting {wait_seconds} seconds before next user...")
    await asyncio.sleep(wait_seconds)


async def disconnect_client_if_needed():
    if client.is_connected():
        with contextlib.suppress(Exception):
            await client.disconnect()


async def hold_process_for_pm2(reason):
    await disconnect_client_if_needed()
    print(f"\nPM2 hold: {reason}")
    print(
        "Process will stay idle to avoid PM2 immediately restarting "
        "this one-shot worker."
    )
    while True:
        await wait_with_heartbeat(
            PM2_HOLD_HEARTBEAT_SECONDS,
            PM2_HOLD_HEARTBEAT_SECONDS,
            "PM2 idle",
        )


async def invite_with_floodwait_retry(target_group, user, user_label, current_invite_delay):
    while True:
        try:
            await invite_user(target_group, user)
            return current_invite_delay
        except errors.FloodWaitError as e:
            print(f"\nFloodWait received for {user_label}: {e.seconds} seconds")
            if MAX_FLOODWAIT_SECONDS > 0 and e.seconds > MAX_FLOODWAIT_SECONDS:
                raise ExcessiveFloodWaitError(e.seconds, user_label)
            await wait_with_heartbeat(e.seconds, FLOODWAIT_HEARTBEAT_SECONDS, "FloodWait")

            if AUTO_RAISE_DELAY_ON_FLOODWAIT and current_invite_delay < SAFE_DELAY_AFTER_FLOODWAIT:
                current_invite_delay = SAFE_DELAY_AFTER_FLOODWAIT
                print(
                    f"Invite delay auto-updated to {current_invite_delay}s "
                    "to reduce repeated FloodWait blocks."
                )
        except errors.PeerFloodError:
            print(
                f"\nPeerFlood received for {user_label}. "
                "Telegram is rate-limiting this account for invites."
            )
            if STOP_ON_PEERFLOOD:
                raise

            await wait_with_heartbeat(
                PEERFLOOD_COOLDOWN_SECONDS,
                FLOODWAIT_HEARTBEAT_SECONDS,
                "PeerFlood cooldown",
            )
            if AUTO_RAISE_DELAY_ON_FLOODWAIT and current_invite_delay < SAFE_DELAY_AFTER_FLOODWAIT:
                current_invite_delay = SAFE_DELAY_AFTER_FLOODWAIT
                print(
                    f"Invite delay auto-updated to {current_invite_delay}s "
                    "after PeerFlood cooldown."
                )


async def start_client():
    if client.is_connected():
        return

    ensure_file_session_is_writable()

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

    print("\n" + "=" * 60)
    print("LOADING GROUP INFORMATION")
    print("=" * 60)

    try:
        source_group = await resolve_group_entity(client, SOURCE_GROUP_ID)
        target_group = await resolve_group_entity(client, TARGET_GROUP_ID)

        source_peer = utils.get_peer_id(source_group)
        target_peer = utils.get_peer_id(target_group)
        print(f"Source group: {source_group.title} ({source_peer})")
        print(f"Target group: {target_group.title} ({target_peer})")
    except errors.ChannelInvalidError:
        print("Error: Cannot access source or target group.")
        print("Make sure IDs are correct and your account has access.")
        return
    except Exception as e:
        print(f"Error loading groups: {e}")
        if "Could not find the input entity" in str(e):
            print("\nQuick checks:")
            print("1. For supergroups/channels, use full ID format: -100xxxxxxxxxx")
            print("2. Or set TARGET_GROUP_ID as @public_username / https://t.me/public_username")
            print("3. This Telegram account must be a member/admin of target group")
        return

    skip_cache_path = resolve_skip_cache_path()
    skip_keys = load_skip_keys(skip_cache_path)
    skip_cache_loaded_count = len(skip_keys)
    if ENABLE_SKIP_CACHE:
        print(f"Skip cache file: {skip_cache_path}")
        print(f"Skip cache entries loaded: {skip_cache_loaded_count}")

    target_member_ids = await preload_target_member_ids(target_group)

    csv_path = os.path.join(CSV_DIR, CSV_FILE)
    if not os.path.exists(csv_path):
        print(f"\nERROR: CSV file not found: {csv_path}")
        print("Run mem_scrap.py first to scrape source group members.")
        return

    skipped_missing_identity_in_csv = 0
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        rows = []
        for row in reader:
            if not row:
                continue
            username_in_row = row[0].strip() if len(row) > 0 else ""
            user_id_in_row = row[1].strip() if len(row) > 1 else ""
            if not username_in_row and not user_id_in_row.lstrip("-").isdigit():
                skipped_missing_identity_in_csv += 1
                continue
            rows.append(row)

    total_users = len(rows)
    if total_users == 0:
        print("\nERROR: CSV is empty. No users to process.")
        return

    print("\n" + "=" * 60)
    print("STARTING MEMBER ADDITION PROCESS")
    print("=" * 60)
    print(f"CSV file: {csv_path}")
    print(f"Users found in CSV: {total_users}")
    if skipped_missing_identity_in_csv > 0:
        print(
            "Skipped before processing (missing username and user_id in CSV): "
            f"{skipped_missing_identity_in_csv}"
        )
    if SUCCESS_LIMIT_ENABLED:
        print(f"Will stop after {MAX_SUCCESSFUL_ADDS} confirmed additions.")
    else:
        print("No success limit configured. Will process users continuously.")
    current_invite_delay = INVITE_DELAY_SECONDS
    if DELAY_AFTER_EACH_USER:
        print(f"Delay between processed users: {current_invite_delay} seconds")
    else:
        print(f"Delay between invite attempts: {current_invite_delay} seconds")
    print("=" * 60)

    success_count = 0
    fail_count = 0
    already_in_group_count = 0
    not_confirmed_after_invite_count = 0
    unverified_after_invite_count = 0
    skipped_missing_identity_count = 0
    skipped_by_cache_count = 0
    processed_count = 0
    fatal_stop_reason = ""

    print("\n" + render_progress(processed_count, total_users, success_count), end="")

    for index, row in enumerate(rows, start=1):
        if reached_success_limit(success_count):
            print("\n\nSuccess limit reached. Stopping process.")
            break

        processed_count += 1

        username = row[0].strip() if len(row) > 0 else ""
        user_id = row[1].strip() if len(row) > 1 else ""
        access_hash = row[2].strip() if len(row) > 2 else ""
        user_label = username or user_id or f"row-{index}"
        row_keys = build_user_keys(username, user_id)

        if not username and not user_id.lstrip("-").isdigit():
            skipped_missing_identity_count += 1
            print(f"\nSkipped (missing username and user_id) at index {index}")
            print("\r" + render_progress(processed_count, total_users, success_count), end="")
            continue

        if ENABLE_SKIP_CACHE and row_keys and any(key in skip_keys for key in row_keys):
            skipped_by_cache_count += 1
            print(f"\nSkipped by cache: {user_label}")
            print("\r" + render_progress(processed_count, total_users, success_count), end="")
            continue

        if user_id and user_id.lstrip("-").isdigit() and int(user_id) in target_member_ids:
            already_in_group_count += 1
            if ENABLE_SKIP_CACHE:
                skip_keys.update(row_keys)
            print(f"\nAlready in target group (preloaded check), skipped: {user_label}")
            print("\r" + render_progress(processed_count, total_users, success_count), end="")
            continue

        user = None
        invite_attempt_completed = False
        should_wait_before_next_user = DELAY_AFTER_EACH_USER
        try:
            try:
                user = await resolve_input_user(username, user_id, access_hash, user_label)
            except (ValueError, errors.rpcerrorlist.UsernameInvalidError, errors.rpcerrorlist.UserIdInvalidError):
                print(f"\nInvalid user format or not found: {user_label}")
                if ENABLE_SKIP_CACHE:
                    skip_keys.update(row_keys)
                fail_count += 1
                print("\r" + render_progress(processed_count, total_users, success_count), end="")
                continue
            except Exception as e:
                print(f"\nError getting user {user_label}: {e}")
                fail_count += 1
                print("\r" + render_progress(processed_count, total_users, success_count), end="")
                continue

            resolved_user_id = str(getattr(user, "user_id", "") or user_id).strip()
            resolved_keys = build_user_keys(username, resolved_user_id or user_id)

            if await is_user_already_in_group(target_group, user, resolved_user_id, user_label):
                already_in_group_count += 1
                if ENABLE_SKIP_CACHE:
                    skip_keys.update(resolved_keys)
                print(f"\nAlready in target group, skipped: {user_label}")
                print("\r" + render_progress(processed_count, total_users, success_count), end="")
                continue

            current_invite_delay = await invite_with_floodwait_retry(
                target_group,
                user,
                user_label,
                current_invite_delay,
            )
            invite_attempt_completed = True

            verification_result = await verify_post_invite_membership(
                target_group,
                user,
                resolved_user_id,
                user_label,
            )

            if verification_result == "confirmed":
                success_count += 1
                if resolved_user_id and resolved_user_id.lstrip("-").isdigit():
                    target_member_ids.add(int(resolved_user_id))
                print(f"\nConfirmed added: {user_label}")
            elif verification_result == "unknown":
                unverified_after_invite_count += 1
                print(
                    f"\nInvite sent but membership could not be verified: {user_label}"
                )
            else:
                not_confirmed_after_invite_count += 1
                fail_count += 1
                print(
                    f"\nInvite request returned without error, but user is still not "
                    f"in target group: {user_label}"
                )

            print("\r" + render_progress(processed_count, total_users, success_count), end="")

            if reached_success_limit(success_count):
                print("\n\nReached configured success limit. Stopping process.")
                should_wait_before_next_user = False
                break

            if invite_attempt_completed and not DELAY_AFTER_EACH_USER:
                print(f"\nWaiting {current_invite_delay} seconds before next invite...")
                await asyncio.sleep(current_invite_delay)

        except errors.UserPrivacyRestrictedError:
            print(f"\nPrivacy restricted: {user_label}")
            if ENABLE_SKIP_CACHE:
                skip_keys.update(row_keys)
            fail_count += 1
            print("\r" + render_progress(processed_count, total_users, success_count), end="")

        except errors.UserNotMutualContactError:
            print(f"\nCannot add (not mutual contact): {user_label}")
            if ENABLE_SKIP_CACHE:
                skip_keys.update(row_keys)
            fail_count += 1
            print("\r" + render_progress(processed_count, total_users, success_count), end="")

        except errors.UserAlreadyParticipantError:
            already_in_group_count += 1
            if ENABLE_SKIP_CACHE:
                skip_keys.update(row_keys)
            print(f"\nAlready in target group, skipped: {user_label}")
            print("\r" + render_progress(processed_count, total_users, success_count), end="")

        except errors.UserIdInvalidError:
            print(f"\nInvalid ID: {user_label}")
            if ENABLE_SKIP_CACHE:
                skip_keys.update(row_keys)
            fail_count += 1
            print("\r" + render_progress(processed_count, total_users, success_count), end="")

        except errors.PeerFloodError as e:
            fatal_stop_reason = (
                f"PeerFlood detected. Account hit invite limit: "
                f"{type(e).__name__}: {e}"
            )
            print(f"\nFATAL: {fatal_stop_reason}")
            should_wait_before_next_user = False
            break
        except ExcessiveFloodWaitError as e:
            fatal_stop_reason = (
                f"FloodWait too long for {e.user_label}: {e.seconds} seconds "
                f"(MAX_FLOODWAIT_SECONDS={MAX_FLOODWAIT_SECONDS})."
            )
            print(f"\nFATAL: {fatal_stop_reason}")
            print("Stopping this cycle. Retry later or increase INVITE_DELAY_SECONDS.")
            should_wait_before_next_user = False
            break

        except (
            errors.UserBannedInChannelError,
            errors.ChatWriteForbiddenError,
            errors.ChatAdminRequiredError,
        ) as e:
            fatal_stop_reason = (
                f"Cannot invite members to target group with this account: "
                f"{type(e).__name__}: {e}"
            )
            print(f"\nFATAL: {fatal_stop_reason}")
            should_wait_before_next_user = False
            break

        except Exception as e:
            print(f"\nFailed: {user_label} -> {type(e).__name__}: {e}")
            fail_count += 1
            print("\r" + render_progress(processed_count, total_users, success_count), end="")
        finally:
            if should_wait_before_next_user and processed_count < total_users and not reached_success_limit(success_count):
                await wait_between_users(current_invite_delay)

    print("\n")

    if processed_count >= total_users:
        if SUCCESS_LIMIT_ENABLED and not reached_success_limit(success_count):
            print("CSV users finished before reaching the success limit.")
        elif not SUCCESS_LIMIT_ENABLED:
            print("CSV users finished.")
    elif fatal_stop_reason:
        print(f"Stopped early due to fatal invite error: {fatal_stop_reason}")

    skip_cache_final_count = len(skip_keys)
    skip_cache_added_count = max(0, skip_cache_final_count - skip_cache_loaded_count)
    if ENABLE_SKIP_CACHE:
        try:
            save_skip_keys(skip_cache_path, skip_keys)
            if skip_cache_added_count > 0:
                print(
                    f"Skip cache updated: +{skip_cache_added_count} "
                    f"(total {skip_cache_final_count})"
                )
            else:
                print(f"Skip cache unchanged (total {skip_cache_final_count})")
        except Exception as e:
            print(f"Warning: could not save skip cache: {type(e).__name__}: {e}")

    print("\n" + "=" * 60)
    print("PROCESS COMPLETE - SUMMARY")
    print("=" * 60)
    print(f"Users in CSV: {total_users}")
    print(f"Users processed: {processed_count}")
    print(f"Confirmed added to target group: {success_count}")
    print(f"Already in target group (skipped): {already_in_group_count}")
    print(f"Invite sent but not confirmed in group: {not_confirmed_after_invite_count}")
    print(f"Invite sent but could not verify: {unverified_after_invite_count}")
    print(f"Skipped (missing username and user_id): {skipped_missing_identity_count}")
    print(f"Skipped by cache: {skipped_by_cache_count}")
    print(f"Failed to add: {fail_count}")
    print(f"Configured success limit: {SUCCESS_LIMIT_LABEL}")
    if ENABLE_SKIP_CACHE:
        print(f"Skip cache total entries: {skip_cache_final_count}")
    print("=" * 60)

    return {
        "users_in_csv": total_users,
        "users_processed": processed_count,
        "successfully_added": success_count,
        "already_in_target_group": already_in_group_count,
        "not_confirmed_after_invite": not_confirmed_after_invite_count,
        "unverified_after_invite": unverified_after_invite_count,
        "skipped_missing_identity": skipped_missing_identity_count,
        "skipped_by_cache": skipped_by_cache_count,
        "failed_to_add": fail_count,
        "skip_cache_total": skip_cache_final_count,
        "skip_cache_added": skip_cache_added_count,
    }


async def run_service():
    cycle_number = 0
    while True:
        cycle_number += 1
        try:
            print("\n" + "=" * 60)
            print(f"WORKER CYCLE #{cycle_number} STARTED")
            print("=" * 60)
            result = await main()

            if result:
                print(
                    "Cycle summary: "
                    f"confirmed_added={result['successfully_added']}, "
                    f"already_in_group={result['already_in_target_group']}, "
                    f"not_confirmed={result['not_confirmed_after_invite']}, "
                    f"unverified={result['unverified_after_invite']}, "
                    f"skipped_missing_identity={result['skipped_missing_identity']}, "
                    f"skipped_by_cache={result['skipped_by_cache']}, "
                    f"failed={result['failed_to_add']}, "
                    f"processed={result['users_processed']}, "
                    f"skip_cache_total={result['skip_cache_total']}"
                )

            if not RUN_FOREVER:
                print("RUN_FOREVER is disabled. Worker is stopping after this cycle.")
                if PM2_HOLD_ON_EXIT:
                    await hold_process_for_pm2(
                        "Cycle finished while RUN_FOREVER=False."
                    )
                return

            await wait_with_heartbeat(IDLE_SLEEP_SECONDS, FLOODWAIT_HEARTBEAT_SECONDS, "Idle")

        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"\nWorker cycle failed: {type(e).__name__}: {e}")
            if not RUN_FOREVER:
                if PM2_HOLD_ON_EXIT:
                    await hold_process_for_pm2(
                        f"Fatal one-shot worker error: {type(e).__name__}: {e}"
                    )
                raise

            if client.is_connected():
                with contextlib.suppress(Exception):
                    await client.disconnect()

            await wait_with_heartbeat(
                RETRY_ON_FATAL_SECONDS,
                min(FLOODWAIT_HEARTBEAT_SECONDS, RETRY_ON_FATAL_SECONDS),
                "Recovery",
            )


if __name__ == "__main__":
    try:
        client.loop.run_until_complete(run_service())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting cleanly.")
    except Exception as e:
        print(f"\nFatal error: {type(e).__name__}: {e}")
    finally:
        if client.is_connected():
            with contextlib.suppress(Exception):
                client.loop.run_until_complete(client.disconnect())
