import asyncio
import json
import logging
import os
import traceback
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import uvloop
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.functions.account import GetPrivacyRequest, SetPrivacyRequest
from telethon.tl.functions.channels import (
    CreateChannelRequest,
    EditTitleRequest,
    GetAdminedPublicChannelsRequest,
    UpdateUsernameRequest,
)
from telethon.tl.functions.contacts import SearchRequest

#from telethon.tl.functions.messages import EditChatAboutRequest
from telethon.tl.functions.users import GetFullUserRequest, GetUsersRequest
from telethon.tl.types import (
    InputPrivacyKeyStatusTimestamp,
    InputPrivacyValueAllowAll,
    PrivacyValueAllowAll,
    UserStatusEmpty,
    UserStatusOffline,
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

API_ID   = 20011285
API_HASH = '1d4121b8195051979e26f60515396b72'
SESSION_DIR = Path(os.environ.get('SESSION_DIR', 'sniper_sessions13'))

TIMESTAMP_WINDOW = 2592000
WINDOW_LOOP_INTERVAL = 0.0

BATCH_SIZE = 200
LONG_AGO_INTERVAL = 0.0

FROZEN_RECHECK_INTERVAL = 0.0

NOTIFY_SESSION = '+8801979912505' #'+14752822653' # based on DC/phone number/sesson name

LONG_AGO_DC_IDS = {}
WINDOW_DC_IDS = {1, 3}
FROZEN_DC_IDS = {1, 3}

TYPE_DC_SETS = {
    'long_ago': LONG_AGO_DC_IDS,
    'timestamp': WINDOW_DC_IDS,
    'interval_timestamp': WINDOW_DC_IDS,
    'frozen': FROZEN_DC_IDS,
}

# TCP socket settings (must match coordinator.py)
SOCKET_HOST = 'fdaa:a9:2189:a7b:8cfe:e20d:a1b8:3502'
SOCKET_PORT = 8888
COORDINATOR_READ_TIMEOUT = 80  # seconds of silence before assuming connection is dead

CACHE_CONCURRENCY = 30

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)
logging.getLogger('telethon').setLevel(logging.WARNING)

# ─────────────────────────────────────────────────────────────
# GLOBALS
# ─────────────────────────────────────────────────────────────

window_has_peers_event = asyncio.Event()

active_tasks = {}

# ─────────────────────────────────────────────────────────────
# CLIENT DISPATCHER
# ─────────────────────────────────────────────────────────────

class ClientDispatcher:
    """
    Central entity responsible for providing ready Telegram clients.
    Each method has its own deque of available session names.
    - acquire() pops from the left (front)
    - release() appends to the left (front) — same client reused immediately
    - flood_wait() schedules appendleft after sleep via call_later — no tasks
    - Flood-waited clients are physically absent during sleep, re-join front when done
    """

    METHODS = ['GetUsersRequest', 'GetFullUserRequest', 'UpdateUsernameRequest']

    def __init__(self, clients: dict):
        self.clients = clients
        self._deques: dict[str, deque] = {}
        self._events: dict[str, asyncio.Event] = {}
        for method in self.METHODS:
            self._deques[method] = deque(clients.keys())
            event = asyncio.Event()
            event.set()
            self._events[method] = event

    async def acquire(self, method: str, uid: int | None = None):
        """
        Block until a ready client is available for `method`.
        Returns (session_name, profile, peer).
        For uid-specific requests, only sessions with that peer cached are valid.
        """
        d = self._deques[method]
        event = self._events[method]
        passed = []

        while True:
            await event.wait()

            if not d:
                event.clear()
                continue

            session_name = d.popleft()
            profile = self.clients[session_name]

            if method == 'UpdateUsernameRequest' and (not profile.get('channel') or profile['public_channels_count'] >= 10):
                passed.append(session_name)
                if len(passed) >= len(self.clients):
                    # All sessions exhausted — no eligible client exists
                    for s in passed:
                        d.appendleft(s)
                    return None, None, None
                continue

            peer = None
            if uid is not None:
                peer = (
                    next((p for p in profile['frozen']   if getattr(p, 'user_id', None) == uid), None) or
                    next((p for p in profile['window']   if getattr(p, 'user_id', None) == uid), None) or
                    next((p for p in profile['long_ago'] if getattr(p, 'user_id', None) == uid), None)
                )
                if peer is None:
                    passed.append(session_name)
                    if len(passed) >= len(self.clients):
                        # No session has this peer cached — put all back and yield
                        for s in passed:
                            d.appendleft(s)
                        passed = []
                        event.set()
                        await asyncio.sleep(0.01)
                    continue

            # Put any skipped-but-valid sessions back at front before returning
            for s in passed:
                d.appendleft(s)

            if not d:
                event.clear()

            return session_name, profile, peer

    def release(self, method: str, session_name: str):
        """Return client to the front — gets picked again on next acquire."""
        self._deques[method].appendleft(session_name)
        self._events[method].set()

    def flood_wait(self, method: str, session_name: str, seconds: float):
        """
        Client hit FloodWait. Absent from deque during sleep.
        Re-joins the front via call_later — no task created.
        """
        def _reinsert():
            self._deques[method].appendleft(session_name)
            self._events[method].set()
            log.info(f'[dispatcher] {session_name} back in pool for {method} after {seconds:.0f}s')

        asyncio.get_event_loop().call_later(seconds, _reinsert)

    def release_for_claiming(self, session_name: str):
        """Re-add a session to UpdateUsernameRequest pool after a fresh channel is created."""
        self._deques['UpdateUsernameRequest'].appendleft(session_name)
        self._events['UpdateUsernameRequest'].set()

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

async def is_already_everyone(client) -> bool:
    result = await client(GetPrivacyRequest(key=InputPrivacyKeyStatusTimestamp()))
    return len(result.rules) == 1 and isinstance(result.rules[0], PrivacyValueAllowAll)


async def set_last_seen_everyone(clients: dict):
    async def set_one(session_name, profile):
        client = profile['instance']
        try:
            if await is_already_everyone(client):
                log.info(f"[{session_name}] already set to Everyone, skipping")
                return
            await client(SetPrivacyRequest(
                key=InputPrivacyKeyStatusTimestamp(),
                rules=[InputPrivacyValueAllowAll()]
            ))
            log.info(f"[{session_name}] last seen set to Everyone")
        except Exception as e:  # noqa: BLE001
            log.error(f"[{session_name}] failed to set privacy: {e}")

    await asyncio.gather(*(set_one(sn, p) for sn, p in clients.items()))


def calc_end_at(due_at_str: str, seen: str) -> str:
    due_at = datetime.fromisoformat(due_at_str).replace(tzinfo=timezone.utc)
    interval_seconds = int(seen.split(',')[0])
    end_at = due_at + timedelta(seconds=interval_seconds)
    return end_at.isoformat()


async def schedule_window(username: str, info: dict, peers: list, usernames: dict, clients: dict):
    u_type = info['type']
    due_at = datetime.fromisoformat(info['due_at']).replace(tzinfo=timezone.utc)

    if u_type == 'timestamp':
        end_at = due_at + timedelta(seconds=TIMESTAMP_WINDOW)
    else:
        end_at = datetime.fromisoformat(info['end_at']).replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    wait = (due_at - now).total_seconds()
    if wait > 0:
        await asyncio.sleep(wait)

    if username not in usernames:
        active_tasks.pop(username, None)
        return

    for session_name, peer in peers:
        profile = clients.get(session_name)
        if profile and peer not in profile['window']:
            profile['window'].append(peer)
            #log.info(f'[{u_type}] {username} joined batch on {session_name}')

    window_has_peers_event.set()

    now = datetime.now(timezone.utc)
    wait = (end_at - now).total_seconds()
    if wait > 0:
        await asyncio.sleep(wait)

    if username not in usernames:
        active_tasks.pop(username, None)
        return

    uid = info['user_id']
    remove_peer_all_sessions(username, uid, 'window', clients)
    usernames.pop(username, None)
    active_tasks.pop(username, None)
    log.info(f'[{u_type}] {username} window ended, removed')


async def claim_username(username: str, uid: int, u_type: str, usernames: dict, clients: dict, dispatcher: 'ClientDispatcher'):
    while True:
        session_name, profile, _ = await dispatcher.acquire('UpdateUsernameRequest')
        if session_name is None:
            return

        client = profile['instance']
        channel = profile['channel']

        try:
            await client(UpdateUsernameRequest(channel=channel, username=username))
            profile['public_channels_count'] += 1

            me = await client.get_me()
            full_name = f"{me.first_name or ''} {me.last_name or ''}".strip()
            log.info(f'[claim] {username} claimed by {session_name} ({full_name})')

            # 3. Send a message to my main account
            try:
                notify_client = clients[NOTIFY_SESSION]['instance']
                await notify_client.send_message('efficiencybeast', f'📢 @{username} claimed by {full_name} ({session_name})')
                log.info(f'[claim] Notification sent to main account on {session_name}')
            except Exception as e:  # noqa: BLE001
                log.warning(f'[claim] Could not send to main account on {session_name}: {e}')

            # 2. Rename channel
            try:
                await client(EditTitleRequest(channel=channel, title=f'{username}'))
                log.info(f'[claim] Channel renamed to @{username} on {session_name}')
                # Delete the system message about title change
                async for msg in client.iter_messages(channel, limit=1):
                    await msg.delete()
            except Exception as e:  # noqa: BLE001
                log.warning(f'[claim] Could not rename channel on {session_name}: {e}')

            # 3. Post message
            # try:
            #     await client(EditChatAboutRequest(channel=channel, about='📡 @namestream'))
            #     log.info(f'[claim] Message sent for @{username} on {session_name}')
            # except Exception as e:
            #     log.warning(f'[claim] Could not send message on {session_name}: {e}')

            # 4. Cleanup and create fresh channel for next claim
            remove_peer_all_sessions(username, uid, u_type, clients)
            usernames.pop(username, None)
            await create_channel(clients, session_name)
            # Fresh channel ready — session is eligible to claim again
            dispatcher.release_for_claiming(session_name)
            return True

        except FloodWaitError as e:
            log.warning(f'[claim] FloodWait on {session_name} for {e.seconds}s')
            dispatcher.flood_wait('UpdateUsernameRequest', session_name, e.seconds)
            continue

        except Exception as e:  # noqa: BLE001
            log.error(f'[claim] Error claiming {username} on {session_name}: {e}')
            dispatcher.release('UpdateUsernameRequest', session_name)
            return False


def remove_peer_all_sessions(username: str, uid: int, u_type: str, clients: dict):
    removed_count = 0

    for profile in clients.values():
        original_len = len(profile[u_type])
        profile[u_type] = [
            p for p in profile[u_type]
            if getattr(p, 'user_id', None) != uid
        ]
        if len(profile[u_type]) < original_len:
            removed_count += 1

    log.info(f"[cleanup] Removed '{username}' (ID: {uid}) from '{u_type}' across {removed_count} session(s)")





async def process_gur_result(users: list, usernames: dict, clients: dict, dispatcher: 'ClientDispatcher'):
    #log.info(f'[DEBUG] GetUsersRequest called with {len(users)} users')

    uid_to_username = {info['user_id']: uname for uname, info in usernames.items()}

    for user in users:
        matched_username = uid_to_username.get(user.id)
        matched_info = usernames.get(matched_username) if matched_username else None

        if matched_username is None or matched_info is None:
            continue

        uid = user.id
        u_type = matched_info['type']
        peer_u_type = 'window' if u_type in ('timestamp', 'interval_timestamp') else u_type

        if user.deleted:
            claimed = await claim_username(matched_username, uid, peer_u_type, usernames, clients, dispatcher)
            if not claimed:
                # claim failed, check if frozen
                full = None
                while True:
                    session_name, profile, peer = await dispatcher.acquire('GetFullUserRequest', uid=uid)
                    client = profile['instance']
                    try:
                        full = await client(GetFullUserRequest(id=peer))
                        dispatcher.release('GetFullUserRequest', session_name)
                        break
                    except FloodWaitError as e:
                        log.warning(f'[gfur] FloodWait on {session_name} for {e.seconds}s')
                        dispatcher.flood_wait('GetFullUserRequest', session_name, e.seconds)
                        continue
                    except Exception as e:  # noqa: BLE001
                        log.error(f'[gfur] Error on {session_name}: {e}')
                        dispatcher.release('GetFullUserRequest', session_name)
                        break

                if full is None:
                    continue

                full_user = full.full_user
                if full_user.bot_verification is not None:
                    log.info(f'[frozen] {matched_username} is frozen')
                    for profile in clients.values():
                        frozen_peer = (
                            next((p for p in profile['window'] if getattr(p, 'user_id', None) == uid), None) or
                            next((p for p in profile['long_ago'] if getattr(p, 'user_id', None) == uid), None)
                        )
                        if frozen_peer is not None and frozen_peer not in profile['frozen']:
                            profile['frozen'].append(frozen_peer)
                    remove_peer_all_sessions(matched_username, uid, peer_u_type, clients)
                    usernames.pop(matched_username, None)
                    if matched_username not in active_tasks:
                        task = asyncio.create_task(watch_frozen(matched_username, uid, usernames, clients, dispatcher))
                        active_tasks[matched_username] = task
                else:
                    # deleted but claim still failed for some other reason, just drop it
                    log.info(f'[claim] {matched_username} deleted but claim failed, dropping')
                    remove_peer_all_sessions(matched_username, uid, peer_u_type, clients)
                    usernames.pop(matched_username, None)

        else:
            matched_info['last_alive_at'] = datetime.now(timezone.utc).isoformat()
            has_username = False
            if user.username and user.username.lower() == matched_username.lower():
                has_username = True
            elif user.usernames:
                for u in user.usernames:
                    if u.editable and u.username.lower() == matched_username.lower():
                        has_username = True
                        break

            if not has_username:
                remove_peer_all_sessions(matched_username, uid, peer_u_type, clients)
                usernames.pop(matched_username, None)
                remaining = sum(1 for info in usernames.values() if info['type'] == u_type)
                log.info(f'[check] {matched_username} username no longer valid, status {user.status}, remaining {remaining}')
                continue

            seen = matched_info.get('seen')
            status = user.status

            status_valid = (
                status is None or
                isinstance(status, UserStatusEmpty) or
                (
                    isinstance(status, UserStatusOffline)
                    and status.was_online.isoformat() == seen
                )
            )

            if not status_valid:
                remove_peer_all_sessions(matched_username, uid, peer_u_type, clients)
                usernames.pop(matched_username, None)
                remaining = sum(1 for info in usernames.values() if info['type'] == u_type)
                log.info(f'[check] {matched_username} status changed to {user.status}, remaining {remaining}')

# ─────────────────────────────────────────────────────────────
# CHANNEL MANAGEMENT
# ─────────────────────────────────────────────────────────────

async def initialize_sessions(session_dir, api_id, api_hash):
    session_files = [f.stem for f in session_dir.glob("*.session")]
    clients = {}

    async def init_one(session_name):
        session_path = session_dir / session_name
        client_instance = TelegramClient(
            str(session_path),
            api_id,
            api_hash,
            flood_sleep_threshold=0,
            connection_retries=50,
            retry_delay=10
        )

        try:
            await client_instance.connect()
            if not await client_instance.is_user_authorized():
                log.warning(f"Session '{session_name}' not authorized, skipping")
                await client_instance.disconnect()
                return

            result = await client_instance(GetAdminedPublicChannelsRequest())

            clients[session_name] = {
                'instance': client_instance,
                'channel': None,
                'public_channels_count': len(result.chats),
                'window': [],
                'long_ago': [],
                'frozen': [],
                'cooldowns': {
                    'GetUsersRequest': None,
                    'GetFullUserRequest': None,
                    'UpdateUsernameRequest': None,
                    'SearchRequest': None,
                    'get_entity': None,
                }
            }
            log.info(f"[{session_name}] Public channels: {clients[session_name]['public_channels_count']}")

        except Exception as e:  # noqa: BLE001
            log.error(f"Failed to connect session '{session_name}': {e}")
            await client_instance.disconnect()

    await asyncio.gather(*(init_one(s) for s in session_files))

    log.info(f"Successfully connected {len(clients)} out of {len(session_files)} session(s) in '{session_dir}'")
    return clients


async def create_channel(clients: dict, session_name: str):
    client = clients[session_name]['instance']
    try:
        me = await client.get_me()
        full_name = f"{me.first_name or ''} {me.last_name or ''}".strip() or session_name
        result = await client(CreateChannelRequest(
            title=full_name,
            about='',
            megagroup=False,
        ))
        channel = result.chats[0]
        clients[session_name]['channel'] = channel
        log.info(f'[channel] Created channel for {session_name}: {channel.id}')
    except Exception as e:  # noqa: BLE001
        log.error(f'[channel] Failed to create channel for {session_name}: {e}')


async def setup_channels(clients: dict):
    async def setup_one(session_name, profile):
        client = profile['instance']
        channel_entity = None
        async for dialog in client.iter_dialogs():
            if (dialog.is_channel
                    and not dialog.entity.username          # private (no public username)
                    and dialog.entity.participants_count <= 1):  # only owner
                channel_entity = dialog.entity
                break
        if channel_entity:
            clients[session_name]['channel'] = channel_entity
            log.info(f'[channel] Reusing existing private channel for {session_name}')
        else:
            await create_channel(clients, session_name)

    await asyncio.gather(*(setup_one(sn, p) for sn, p in clients.items()))

# ─────────────────────────────────────────────────────────────
# ENTITY CACHING
# ─────────────────────────────────────────────────────────────

async def cache_entity(profile: dict, uid: int, username: str):
    client = profile['instance']
    now = datetime.now(timezone.utc)

    try:
        peer = await client.get_input_entity(uid)
        return peer
    except Exception:  # noqa: BLE001, S110
        pass

    search_cooldown = profile['cooldowns'].get('SearchRequest')
    if search_cooldown is None or now >= search_cooldown:
        try:
            result = await client(SearchRequest(q=username, limit=5))
            for user in result.users:
                if user.id == uid:
                    peer = await client.get_input_entity(uid)
                    return peer
        except FloodWaitError as e:
            profile['cooldowns']['SearchRequest'] = datetime.now(timezone.utc) + timedelta(seconds=e.seconds)
        except Exception:  # noqa: BLE001, S110
            pass

    entity_cooldown = profile['cooldowns'].get('get_entity')
    if entity_cooldown is None or now >= entity_cooldown:
        try:
            entity = await client.get_entity(username)
            if entity.id == uid:
                peer = await client.get_input_entity(uid)
                return peer
        except FloodWaitError as e:
            profile['cooldowns']['get_entity'] = datetime.now(timezone.utc) + timedelta(seconds=e.seconds)
        except Exception:  # noqa: BLE001, S110
            pass

    return None


async def run_caching_phase(usernames: dict, clients: dict, dispatcher: 'ClientDispatcher'):

    for username, info in list(usernames.items()):
        uid = info['user_id']
        u_type = info['type']
        peers = []

        if u_type == 'interval_timestamp' and 'end_at' not in info:
            info['end_at'] = calc_end_at(info['due_at'], info['seen'])

        session_items = list(clients.items())

        for i in range(0, len(session_items), CACHE_CONCURRENCY):
            batch = session_items[i:i + CACHE_CONCURRENCY]

            async def cache_one(session_name, profile, uid=uid, username=username):
                peer = await cache_entity(profile, uid, username)
                return session_name, profile, peer

            results = await asyncio.gather(*(cache_one(sn, p) for sn, p in batch))

            for session_name, profile, peer in results:
                if peer is not None:
                    if u_type in ('timestamp', 'interval_timestamp'):
                        peers.append((session_name, peer))
                    elif u_type == 'long_ago':
                        if peer not in profile['long_ago']:
                            profile['long_ago'].append(peer)
                    elif u_type == 'frozen' and peer not in profile['frozen']:
                        profile['frozen'].append(peer)
                    #log.info(f'[cache] {username} cached on {session_name}')
                else:
                    log.warning(f'[cache] All methods failed for {username} on {session_name}')

        if username in active_tasks:
            continue

        if u_type in ('timestamp', 'interval_timestamp'):
            task = asyncio.create_task(schedule_window(username, info, peers, usernames, clients))
            active_tasks[username] = task
            log.info(f'[cache] Spawned window task for {username} ({u_type})')
        elif u_type == 'frozen':
            task = asyncio.create_task(watch_frozen(username, uid, usernames, clients, dispatcher))
            active_tasks[username] = task
            log.info(f'[cache] Spawned watch_frozen task for {username}')

# ─────────────────────────────────────────────────────────────
# TYPE-SPECIFIC LOOPS
# ─────────────────────────────────────────────────────────────

async def window_loop(usernames: dict, clients: dict, dispatcher: 'ClientDispatcher'):
    while True:
        any_peers = any(profile['window'] for profile in clients.values())
        if not any_peers:
            window_has_peers_event.clear()
            await window_has_peers_event.wait()
            continue

        processed = set()

        while True:
            session_name, profile, _ = await dispatcher.acquire('GetUsersRequest')
            peers = profile['window']

            batch = [p for p in peers if getattr(p, 'user_id', None) not in processed][:BATCH_SIZE]
            if not batch:
                dispatcher.release('GetUsersRequest', session_name)
                break

            client = profile['instance']
            try:
                result = await client(GetUsersRequest(id=batch))
                dispatcher.release('GetUsersRequest', session_name)
                await process_gur_result(result, usernames, clients, dispatcher)
                processed.update(getattr(p, 'user_id', None) for p in batch)
            except ConnectionError:
                log.warning(f'[window] Session {session_name} disconnected, reconnecting...')
                try:
                    await client.disconnect()
                    await client.connect()
                    log.info(f'[window] Session {session_name} reconnected successfully')
                except Exception:  # noqa: BLE001
                    log.error(f'[window] Failed to reconnect {session_name}')
                dispatcher.release('GetUsersRequest', session_name)
                continue
            except FloodWaitError as e:
                #log.warning(f'[window] FloodWait on {session_name} for {e.seconds}s')
                dispatcher.flood_wait('GetUsersRequest', session_name, e.seconds)
                continue
            except Exception as e:  # noqa: BLE001
                log.error(f'[window] Error: {e}')
                dispatcher.release('GetUsersRequest', session_name)
                break

        await asyncio.sleep(WINDOW_LOOP_INTERVAL)


async def long_ago_loop(usernames: dict, clients: dict, dispatcher: 'ClientDispatcher'):
    while True:
        any_peers = any(profile['long_ago'] for profile in clients.values())
        if not any_peers:
            await asyncio.sleep(LONG_AGO_INTERVAL)
            continue

        processed = set()

        while True:
            session_name, profile, _ = await dispatcher.acquire('GetUsersRequest')
            peers = profile['long_ago']

            batch = [p for p in peers if getattr(p, 'user_id', None) not in processed][:BATCH_SIZE]
            if not batch:
                dispatcher.release('GetUsersRequest', session_name)
                break

            client = profile['instance']
            try:
                result = await client(GetUsersRequest(id=batch))
                dispatcher.release('GetUsersRequest', session_name)
                await process_gur_result(result, usernames, clients, dispatcher)
                processed.update(getattr(p, 'user_id', None) for p in batch)
            except ConnectionError:
                log.warning(f'[long_ago] Session {session_name} disconnected, reconnecting...')
                try:
                    await client.disconnect()
                    await client.connect()
                    log.info(f'[long_ago] Session {session_name} reconnected successfully')
                except Exception:  # noqa: BLE001
                    log.error(f'[long_ago] Failed to reconnect {session_name}')
                dispatcher.release('GetUsersRequest', session_name)
                continue
            except FloodWaitError as e:
                #log.warning(f'[long_ago] FloodWait on {session_name} for {e.seconds}s')
                dispatcher.flood_wait('GetUsersRequest', session_name, e.seconds)
                continue
            except Exception as e:  # noqa: BLE001
                log.error(f'[long_ago] Error: {e}\n{traceback.format_exc()}')
                dispatcher.release('GetUsersRequest', session_name)
                break

        await asyncio.sleep(LONG_AGO_INTERVAL)


async def watch_frozen(username: str, uid: int, usernames: dict, clients: dict, dispatcher: 'ClientDispatcher'):

    while True:
        session_name, profile, peer = await dispatcher.acquire('GetFullUserRequest', uid=uid)
        client = profile['instance']

        try:
            full = await client(GetFullUserRequest(id=peer))
            dispatcher.release('GetFullUserRequest', session_name)
        except ConnectionError:
            log.warning(f'[long_ago] Session {session_name} disconnected, reconnecting...')
            try:
                await client.disconnect()
                await client.connect()
                log.info(f'[long_ago] Session {session_name} reconnected successfully')
            except Exception:  # noqa: BLE001
                log.error(f'[long_ago] Failed to reconnect {session_name}')
            dispatcher.release('GetFullUserRequest', session_name)
            continue
        except FloodWaitError as e:
            #log.warning(f'[frozen] FloodWait on {session_name} for {e.seconds}s')
            dispatcher.flood_wait('GetFullUserRequest', session_name, e.seconds)
            continue
        except Exception as e:  # noqa: BLE001
            log.error(f'[frozen] Error checking {username} on {session_name}: {e}')
            dispatcher.release('GetFullUserRequest', session_name)
            await asyncio.sleep(FROZEN_RECHECK_INTERVAL)
            continue

        user_obj = full.users[0] if full.users else None

        if user_obj is None or not user_obj.deleted:
            log.info(f'[frozen] {username} no longer deleted — unfroze, dropping watch')
            remove_peer_all_sessions(username, uid, 'frozen', clients)
            active_tasks.pop(username, None)
            return

        if full.full_user.bot_verification is None:
            log.info(f'[frozen] {username} fully deleted — attempting claim')
            await claim_username(username, uid, 'frozen', usernames, clients, dispatcher)
            active_tasks.pop(username, None)
            return

        #log.info(f'[frozen] {username} still frozen, rechecking in {FROZEN_RECHECK_INTERVAL}s')
        await asyncio.sleep(FROZEN_RECHECK_INTERVAL)

# ─────────────────────────────────────────────────────────────
# COORDINATOR CONNECTION
# ─────────────────────────────────────────────────────────────

async def connect_to_coordinator(usernames: dict, clients: dict, sniper_id: str, dispatcher: 'ClientDispatcher'):
    """Connect to coordinator, receive full data on startup then handle updates."""
    while True:
        try:
            reader, writer = await asyncio.open_connection(SOCKET_HOST, SOCKET_PORT, limit=16 * 1024 * 1024)
            log.info('[coordinator] Connected to coordinator')

            writer.write((json.dumps({'type': 'hello', 'sniper_id': sniper_id}) + '\n').encode())
            await writer.drain()

            while True:
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=COORDINATOR_READ_TIMEOUT)
                except asyncio.TimeoutError:
                    log.warning(f'[coordinator] No data received in {COORDINATOR_READ_TIMEOUT}s, assuming connection dead')
                    break

                if not line:
                    log.warning('[coordinator] Connection closed by coordinator')
                    break

                message = json.loads(line.decode())
                msg_type = message.get('type')
                data     = message.get('data', {})

                if msg_type == 'remove':
                    for username in data:
                        info = usernames.pop(username, None)
                        task = active_tasks.pop(username, None)
                        if task:
                            task.cancel()
                        if info:
                            u_type = info['type']
                            peer_u_type = 'window' if u_type in ('timestamp', 'interval_timestamp') else u_type
                            remove_peer_all_sessions(username, info['user_id'], peer_u_type, clients)
                            log.info(f'[coordinator] {username} removed by coordinator (excluded)')
                    continue

                new_only = {}
                for username, info in data.items():
                    dc_id = info.get('dc_id')
                    u_type = info.get('type')

                    allowed_dc_ids = TYPE_DC_SETS.get(u_type, set())

                    if username in usernames:
                        if dc_id not in allowed_dc_ids:
                            old_info = usernames.pop(username, None)
                            task = active_tasks.pop(username, None)
                            if task:
                                task.cancel()
                            if old_info:
                                old_type = old_info['type']
                                peer_u_type = 'window' if old_type in ('timestamp', 'interval_timestamp') else old_type
                                remove_peer_all_sessions(username, old_info['user_id'], peer_u_type, clients)
                                log.info(f'[coordinator] {username} dropped — dc_id {dc_id} not in {u_type} DC set')
                        continue

                    if dc_id not in allowed_dc_ids:
                        continue

                    usernames[username] = info
                    new_only[username] = info

                if new_only:
                    log.info(f'[coordinator] Received {msg_type}: {len(new_only)} new username(s), caching...')
                    asyncio.create_task(run_caching_phase(new_only, clients, dispatcher))

            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=5)
            except Exception as e:  # noqa: BLE001
                log.warning(f'[coordinator] Error closing old connection: {e}')

        except Exception as e:  # noqa: BLE001
            log.error(f'[coordinator] Connection error: {e}. Retrying in 5s')
            await asyncio.sleep(5)

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

async def main():
    clients = await initialize_sessions(SESSION_DIR, API_ID, API_HASH)
    await set_last_seen_everyone(clients)
    await setup_channels(clients)

    dispatcher = ClientDispatcher(clients)
    log.info(f'[dispatcher] Initialized with {len(clients)} session(s)')

    usernames = {}
    sniper_id = str(uuid.uuid4())
    log.info(f'[sniper] Instance ID: {sniper_id}')

    await asyncio.gather(
        connect_to_coordinator(usernames, clients, sniper_id, dispatcher),
        window_loop(usernames, clients, dispatcher),
        long_ago_loop(usernames, clients, dispatcher),
    )


if __name__ == '__main__':
    uvloop.run(main())
