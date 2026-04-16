from telethon import TelegramClient, errors, utils
from telethon.sessions import StringSession
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.messages import AddChatUserRequest
from telethon.tl.types import Channel, Chat, InputUser
import csv
import asyncio
import contextlib
from dotenv import load_dotenv
import os


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


load_dotenv()

SOURCE_GROUP_ID = read_int_env("SOURCE_GROUP_ID", -1001937901847)
TARGET_GROUP_ID = read_int_env("TARGET_GROUP_ID", -1002000540919)
MAX_SUCCESSFUL_ADDS = read_int_env("MAX_SUCCESSFUL_ADDS", 0)
INVITE_DELAY_SECONDS = max(0, read_int_env("INVITE_DELAY_SECONDS", 60))
FLOODWAIT_HEARTBEAT_SECONDS = max(5, read_int_env("FLOODWAIT_HEARTBEAT_SECONDS", 60))
AUTO_RAISE_DELAY_ON_FLOODWAIT = read_bool_env("AUTO_RAISE_DELAY_ON_FLOODWAIT", True)
SAFE_DELAY_AFTER_FLOODWAIT = max(10, read_int_env("SAFE_DELAY_AFTER_FLOODWAIT", 60))
DELAY_AFTER_EACH_USER = read_bool_env("DELAY_AFTER_EACH_USER", True)
STOP_ON_PEERFLOOD = read_bool_env("STOP_ON_PEERFLOOD", True)
PEERFLOOD_COOLDOWN_SECONDS = max(60, read_int_env("PEERFLOOD_COOLDOWN_SECONDS", 1800))

CSV_DIR = os.getenv("CSV_DIR", "csv")
CSV_FILE = os.getenv("CSV_FILE", "scraped_members.csv")

SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "anonym")
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "").strip()
RUNNING_ON_RENDER = os.getenv("RENDER", "").lower() == "true"
RUN_FOREVER = read_bool_env("RUN_FOREVER", RUNNING_ON_RENDER)
IDLE_SLEEP_SECONDS = max(10, read_int_env("IDLE_SLEEP_SECONDS", 300))
RETRY_ON_FATAL_SECONDS = max(10, read_int_env("RETRY_ON_FATAL_SECONDS", 30))
SUCCESS_LIMIT_ENABLED = MAX_SUCCESSFUL_ADDS > 0
SUCCESS_LIMIT_LABEL = str(MAX_SUCCESSFUL_ADDS) if SUCCESS_LIMIT_ENABLED else "unlimited"

api_id_raw = os.getenv("TELEGRAM_API_ID", "").strip()
api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
if not api_id_raw or not api_hash:
    raise RuntimeError("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH environment variables.")
api_id = int(api_id_raw)

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
print("Success limit:", SUCCESS_LIMIT_LABEL)
print("Invite delay:", INVITE_DELAY_SECONDS, "seconds")
print("Delay after each user:", DELAY_AFTER_EACH_USER)
print("FloodWait heartbeat:", FLOODWAIT_HEARTBEAT_SECONDS, "seconds")
print("Stop on PeerFlood:", STOP_ON_PEERFLOOD)
print("PeerFlood cooldown:", PEERFLOOD_COOLDOWN_SECONDS, "seconds")
print("Run forever:", RUN_FOREVER)


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
        f"| Added {success}/{SUCCESS_LIMIT_LABEL}"
    )


def reached_success_limit(success_count):
    return SUCCESS_LIMIT_ENABLED and success_count >= MAX_SUCCESSFUL_ADDS


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


async def invite_with_floodwait_retry(target_group, user, user_label, current_invite_delay):
    while True:
        try:
            await invite_user(target_group, user)
            return current_invite_delay
        except errors.FloodWaitError as e:
            print(f"\nFloodWait received for {user_label}: {e.seconds} seconds")
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
        return

    csv_path = os.path.join(CSV_DIR, CSV_FILE)
    if not os.path.exists(csv_path):
        print(f"\nERROR: CSV file not found: {csv_path}")
        print("Run mem_scrap.py first to scrape source group members.")
        return

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        rows = [row for row in reader if row]

    total_users = len(rows)
    if total_users == 0:
        print("\nERROR: CSV is empty. No users to process.")
        return

    print("\n" + "=" * 60)
    print("STARTING MEMBER ADDITION PROCESS")
    print("=" * 60)
    print(f"CSV file: {csv_path}")
    print(f"Users found in CSV: {total_users}")
    if SUCCESS_LIMIT_ENABLED:
        print(f"Will stop after {MAX_SUCCESSFUL_ADDS} successful additions.")
    else:
        print("No success limit configured. Will process users continuously.")
    current_invite_delay = INVITE_DELAY_SECONDS
    if DELAY_AFTER_EACH_USER:
        print(f"Delay between processed users: {current_invite_delay} seconds")
    else:
        print(f"Delay between successful invites: {current_invite_delay} seconds")
    print("=" * 60)

    success_count = 0
    fail_count = 0
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

        if not username and not user_id:
            print(f"\nSkipped empty row at index {index}")
            fail_count += 1
            print("\r" + render_progress(processed_count, total_users, success_count), end="")
            if DELAY_AFTER_EACH_USER and processed_count < total_users and not reached_success_limit(success_count):
                await wait_between_users(current_invite_delay)
            continue

        user = None
        should_wait_before_next_user = DELAY_AFTER_EACH_USER
        try:
            try:
                user = await resolve_input_user(username, user_id, access_hash, user_label)
            except (ValueError, errors.rpcerrorlist.UsernameInvalidError, errors.rpcerrorlist.UserIdInvalidError):
                print(f"\nInvalid user format or not found: {user_label}")
                fail_count += 1
                print("\r" + render_progress(processed_count, total_users, success_count), end="")
                continue
            except Exception as e:
                print(f"\nError getting user {user_label}: {e}")
                fail_count += 1
                print("\r" + render_progress(processed_count, total_users, success_count), end="")
                continue

            current_invite_delay = await invite_with_floodwait_retry(
                target_group,
                user,
                user_label,
                current_invite_delay,
            )
            success_count += 1
            print(f"\nAdded: {user_label}")
            print("\r" + render_progress(processed_count, total_users, success_count), end="")

            if reached_success_limit(success_count):
                print("\n\nReached configured success limit. Stopping process.")
                should_wait_before_next_user = False
                break

            if not DELAY_AFTER_EACH_USER:
                print(f"\nWaiting {current_invite_delay} seconds before next invite...")
                await asyncio.sleep(current_invite_delay)

        except errors.UserPrivacyRestrictedError:
            print(f"\nPrivacy restricted: {user_label}")
            fail_count += 1
            print("\r" + render_progress(processed_count, total_users, success_count), end="")

        except errors.UserNotMutualContactError:
            print(f"\nCannot add (not mutual contact): {user_label}")
            fail_count += 1
            print("\r" + render_progress(processed_count, total_users, success_count), end="")

        except errors.UserIdInvalidError:
            print(f"\nInvalid ID: {user_label}")
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

    print("\n" + "=" * 60)
    print("PROCESS COMPLETE - SUMMARY")
    print("=" * 60)
    print(f"Users in CSV: {total_users}")
    print(f"Users processed: {processed_count}")
    print(f"Successfully added: {success_count}")
    print(f"Failed to add: {fail_count}")
    print(f"Configured success limit: {SUCCESS_LIMIT_LABEL}")
    print("=" * 60)

    return {
        "users_in_csv": total_users,
        "users_processed": processed_count,
        "successfully_added": success_count,
        "failed_to_add": fail_count,
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
                    f"added={result['successfully_added']}, "
                    f"failed={result['failed_to_add']}, "
                    f"processed={result['users_processed']}"
                )

            if not RUN_FOREVER:
                print("RUN_FOREVER is disabled. Worker is stopping after this cycle.")
                return

            await wait_with_heartbeat(IDLE_SLEEP_SECONDS, FLOODWAIT_HEARTBEAT_SECONDS, "Idle")

        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"\nWorker cycle failed: {type(e).__name__}: {e}")
            if not RUN_FOREVER:
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
