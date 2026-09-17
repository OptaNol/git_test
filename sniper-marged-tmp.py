import asyncio
import json
import logging
import os
import uuid
from collections import deque
from pathlib import Path
from typing import ClassVar

import uvloop
from telethon import TelegramClient
from telethon.tl.functions.account import GetPrivacyRequest, SetPrivacyRequest
from telethon.tl.functions.channels import (
    CreateChannelRequest,
    GetAdminedPublicChannelsRequest,
)
from telethon.tl.types import (
    InputPrivacyKeyStatusTimestamp,
    InputPrivacyValueAllowAll,
    PrivacyValueAllowAll,
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

API_ID   = 20011285
API_HASH = '1d4121b8195051979e26f60515396b72'
SESSION_DIR = Path(os.environ.get('SESSION_DIR', 'sniper_sessions13'))

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

    METHODS: ClassVar[list[str]] = ['GetUsersRequest', 'GetFullUserRequest', 'UpdateUsernameRequest']

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
                    log.warning(f'[dispatcher] All {len(self.clients)} session(s) exhausted for UpdateUsernameRequest (no eligible channels or limit reached)')
                    for s in passed:
                        d.appendleft(s)
                    return None, None, None
                continue

            peer = None
            if uid is not None:
                peer = (
                    next((p for p in profile['frozen'] if getattr(p, 'user_id', None) == uid), None) or
                    next((p for sublist in profile['window']   for p in sublist if getattr(p, 'user_id', None) == uid), None) or
                    next((p for sublist in profile['long_ago'] for p in sublist if getattr(p, 'user_id', None) == uid), None)
                )
                if peer is None:
                    passed.append(session_name)
                    if len(passed) >= len(self.clients):
                        # No session has this peer cached — put all back and yield
                        log.warning(f'[dispatcher] No session has peer cached for uid={uid} across all {len(self.clients)} session(s). Yielding...')
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

    def release_back(self, method: str, session_name: str):
        """Return client to the back — used after general errors."""
        self._deques[method].append(session_name)
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
        self._deques['UpdateUsernameRequest'].append(session_name)
        self._events['UpdateUsernameRequest'].set()

# ─────────────────────────────────────────────────────────────
# Warmup
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
                'window': [[]],
                'long_ago': [[]],
                'frozen': [],
            }
            log.info(f"[{session_name}] Public channels: {clients[session_name]['public_channels_count']}")

        except Exception as e:  # noqa: BLE001
            log.error(f"Failed to connect session '{session_name}': {e}")
            await client_instance.disconnect()

    await asyncio.gather(*(init_one(s) for s in session_files))

    log.info(f"Successfully connected {len(clients)} out of {len(session_files)} session(s) in '{session_dir}'")
    return clients

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

# ─────────────────────────────────────────────────────────────
# CHANNEL MANAGEMENT
# ─────────────────────────────────────────────────────────────

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
# COORDINATOR CONNECTION
# ─────────────────────────────────────────────────────────────

async def connect_to_coordinator(usernames: dict, clients: dict, sniper_id: str, dispatcher: 'ClientDispatcher', worker_managers: dict):
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
                    asyncio.create_task(run_caching_phase(new_only, clients, dispatcher, worker_managers))

            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=5)
            except Exception as e:  # noqa: BLE001
                log.warning(f'[coordinator] Error closing old connection: {e}')

        except Exception as e:  # noqa: BLE001
            log.error(f'[coordinator] Connection error: {e}. Retrying in 5s')
            await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────
# CONFIGURATION (BATCH)
# ─────────────────────────────────────────────────────────────

BATCH_SIZE           = 200
WINDOW_LOOP_INTERVAL = 0.1   # seconds between window batch cycles
LONG_AGO_INTERVAL    = 60    # seconds between long_ago batch cycles
FROZEN_RECHECK_INTERVAL = 30

# ─────────────────────────────────────────────────────────────
# PEER MANAGEMENT
# ─────────────────────────────────────────────────────────────

def add_peer(clients: dict, key: str, peer, session_name: str | None = None):
    """
    Add peer to the last sublist of profile[key] for each session.
    If last sublist is full (>= BATCH_SIZE), create a new sublist.
    Returns True if a new sublist was created (caller should spawn a new worker).
    session_name: if provided, only add to that session (used when peer is session-specific).
    """
    new_sublist_created = False
    targets = [(session_name, clients[session_name])] if session_name else clients.items()

    for sn, profile in targets:
        sublists = profile[key]
        if not sublists or len(sublists[-1]) >= BATCH_SIZE:
            sublists.append([])
            new_sublist_created = True
        sublists[-1].append(peer)

    return new_sublist_created


def remove_peer_all_sessions(username: str, uid: int, u_type: str, clients: dict):
    """
    Remove peer from all sessions and rebalance sublists.
    For window/long_ago: flatten all sublists, remove peer, rechunk into BATCH_SIZE sublists.
    Returns list of sublist indices that became empty after rebalance (workers to cancel).
    """
    removed_count = 0
    empty_indices = []

    for profile in clients.values():
        if u_type in ('window', 'long_ago'):
            # Flatten, remove, rechunk
            all_peers = [p for sublist in profile[u_type] for p in sublist if getattr(p, 'user_id', None) != uid]
            if len(all_peers) < sum(len(s) for s in profile[u_type]):
                removed_count += 1
            # Rechunk into BATCH_SIZE sublists, keep at least one empty sublist
            profile[u_type] = [all_peers[i:i+BATCH_SIZE] for i in range(0, len(all_peers), BATCH_SIZE)] or [[]]
        else:
            original_len = len(profile[u_type])
            profile[u_type] = [p for p in profile[u_type] if getattr(p, 'user_id', None) != uid]
            if len(profile[u_type]) < original_len:
                removed_count += 1

    log.info(f"[cleanup] Removed '{username}' (uid={uid}) from '{u_type}' across {removed_count} session(s)")

    if u_type in ('window', 'long_ago'):
        # Return how many sublists exist now (first session as reference)
        first_profile = next(iter(clients.values()))
        return len(first_profile[u_type])
    return None

# ─────────────────────────────────────────────────────────────
# CACHING
# ─────────────────────────────────────────────────────────────

CACHE_CONCURRENCY = 30

async def cache_entity(profile: dict, uid: int, username: str):
    """Try to cache entity on a single session. Returns peer or None."""
    client = profile['instance']
    from telethon.errors import FloodWaitError
    from datetime import datetime, timedelta, timezone

    search_cooldown = profile.get('search_cooldown')
    if search_cooldown and datetime.now(timezone.utc) < search_cooldown:
        pass
    else:
        try:
            peer = await client.get_entity(username)
            return peer
        except FloodWaitError as e:
            profile['search_cooldown'] = datetime.now(timezone.utc) + timedelta(seconds=e.seconds)
        except Exception:
            pass

    entity_cooldown = profile.get('entity_cooldown')
    if entity_cooldown and datetime.now(timezone.utc) < entity_cooldown:
        return None
    try:
        peer = await client.get_entity(uid)
        return peer
    except FloodWaitError as e:
        profile['entity_cooldown'] = datetime.now(timezone.utc) + timedelta(seconds=e.seconds)
    except Exception:
        pass

    return None


async def run_caching_phase(usernames: dict, clients: dict, dispatcher: 'ClientDispatcher', worker_managers: dict):
    from telethon.tl.functions.users import GetUsersRequest
    import math

    for username, info in list(usernames.items()):
        uid    = info['user_id']
        u_type = info['type']

        session_items = list(clients.items())

        for i in range(0, len(session_items), CACHE_CONCURRENCY):
            batch = session_items[i:i + CACHE_CONCURRENCY]

            async def cache_one(sn, profile, uid=uid, username=username):
                peer = await cache_entity(profile, uid, username)
                return sn, profile, peer

            results = await asyncio.gather(*(cache_one(sn, p) for sn, p in batch))

            for sn, profile, peer in results:
                if peer is None:
                    log.warning(f'[cache] All methods failed for {username} on {sn}')
                    continue

                if u_type in ('timestamp', 'interval_timestamp'):
                    new_sublist = add_peer(clients, 'window', peer, session_name=sn)
                    if new_sublist:
                        window_has_peers_event.set()
                        # Notify window manager a new sublist was created
                        if 'window' in worker_managers:
                            worker_managers['window'].notify()
                elif u_type == 'long_ago':
                    new_sublist = add_peer(clients, 'long_ago', peer, session_name=sn)
                    if new_sublist and 'long_ago' in worker_managers:
                        worker_managers['long_ago'].notify()
                elif u_type == 'frozen':
                    profile['frozen'].append(peer)

# ─────────────────────────────────────────────────────────────
# WORKERS & MANAGER
# ─────────────────────────────────────────────────────────────

from telethon.errors import FloodWaitError
from telethon.tl.functions.users import GetUsersRequest
import math
import traceback


async def window_worker(sublist_index: int, clients: dict, usernames: dict, dispatcher: 'ClientDispatcher'):
    while True:
        # Collect batch from this sublist index across all sessions
        all_peers = []
        for profile in clients.values():
            if sublist_index < len(profile['window']):
                all_peers.extend(profile['window'][sublist_index])

        if not all_peers:
            await asyncio.sleep(WINDOW_LOOP_INTERVAL)
            continue

        processed = set()
        session_name, profile, _ = await dispatcher.acquire('GetUsersRequest')
        client = profile['instance']

        while True:
            sublist = profile['window'][sublist_index] if sublist_index < len(profile['window']) else []
            batch = [p for p in sublist if getattr(p, 'user_id', None) not in processed][:BATCH_SIZE]
            if not batch:
                dispatcher.release('GetUsersRequest', session_name)
                break

            try:
                result = await client(GetUsersRequest(id=batch))
                await process_gur_result(result, usernames, clients, dispatcher)
                processed.update(getattr(p, 'user_id', None) for p in batch)
            except ConnectionError:
                log.warning(f'[window/{sublist_index}] Session {session_name} disconnected, reconnecting...')
                try:
                    await client.disconnect()
                    await client.connect()
                    log.info(f'[window/{sublist_index}] Reconnected {session_name}')
                except Exception:  # noqa: BLE001
                    log.error(f'[window/{sublist_index}] Failed to reconnect {session_name}')
                    dispatcher.release_back('GetUsersRequest', session_name)
                    break
            except FloodWaitError as e:
                dispatcher.flood_wait('GetUsersRequest', session_name, e.seconds)
                session_name, profile, _ = await dispatcher.acquire('GetUsersRequest')
                client = profile['instance']
            except Exception as e:  # noqa: BLE001
                log.error(f'[window/{sublist_index}] Error: {e}\n{traceback.format_exc()}')
                dispatcher.release_back('GetUsersRequest', session_name)
                break

        await asyncio.sleep(WINDOW_LOOP_INTERVAL)


async def long_ago_worker(sublist_index: int, clients: dict, usernames: dict, dispatcher: 'ClientDispatcher'):
    while True:
        all_peers = []
        for profile in clients.values():
            if sublist_index < len(profile['long_ago']):
                all_peers.extend(profile['long_ago'][sublist_index])

        if not all_peers:
            await asyncio.sleep(LONG_AGO_INTERVAL)
            continue

        processed = set()
        session_name, profile, _ = await dispatcher.acquire('GetUsersRequest')
        client = profile['instance']

        while True:
            sublist = profile['long_ago'][sublist_index] if sublist_index < len(profile['long_ago']) else []
            batch = [p for p in sublist if getattr(p, 'user_id', None) not in processed][:BATCH_SIZE]
            if not batch:
                dispatcher.release('GetUsersRequest', session_name)
                break

            try:
                result = await client(GetUsersRequest(id=batch))
                await process_gur_result(result, usernames, clients, dispatcher)
                processed.update(getattr(p, 'user_id', None) for p in batch)
            except ConnectionError:
                log.warning(f'[long_ago/{sublist_index}] Session {session_name} disconnected, reconnecting...')
                try:
                    await client.disconnect()
                    await client.connect()
                    log.info(f'[long_ago/{sublist_index}] Reconnected {session_name}')
                except Exception:  # noqa: BLE001
                    log.error(f'[long_ago/{sublist_index}] Failed to reconnect {session_name}')
                    dispatcher.release_back('GetUsersRequest', session_name)
                    break
            except FloodWaitError as e:
                dispatcher.flood_wait('GetUsersRequest', session_name, e.seconds)
                session_name, profile, _ = await dispatcher.acquire('GetUsersRequest')
                client = profile['instance']
            except Exception as e:  # noqa: BLE001
                log.error(f'[long_ago/{sublist_index}] Error: {e}\n{traceback.format_exc()}')
                dispatcher.release_back('GetUsersRequest', session_name)
                break

        await asyncio.sleep(LONG_AGO_INTERVAL)


class LoopManager:
    """
    Manages workers for window or long_ago loop type.
    Spawns a worker per sublist index.
    Cancels workers whose sublist is empty after rebalance.
    Call notify() when sublists change.
    """

    def __init__(self, loop_type: str, clients: dict, usernames: dict, dispatcher: 'ClientDispatcher'):
        self.loop_type  = loop_type
        self.clients    = clients
        self.usernames  = usernames
        self.dispatcher = dispatcher
        self.worker_fn  = window_worker if loop_type == 'window' else long_ago_worker
        self.workers: dict[int, asyncio.Task] = {}
        self._event = asyncio.Event()

    def notify(self):
        """Signal that sublists have changed."""
        self._event.set()

    async def run(self):
        while True:
            await self._event.wait()
            self._event.clear()

            # Use first session as reference for sublist count
            first_profile = next(iter(self.clients.values()))
            sublists = first_profile[self.loop_type]
            needed = len(sublists)

            # Spawn missing workers
            for i in range(needed):
                if i not in self.workers or self.workers[i].done():
                    self.workers[i] = asyncio.create_task(
                        self.worker_fn(i, self.clients, self.usernames, self.dispatcher)
                    )
                    log.info(f'[{self.loop_type}] Spawned worker {i} (sublists={needed})')

            # Cancel workers whose sublist is now empty
            for i in list(self.workers.keys()):
                if i >= needed or not sublists[i]:
                    self.workers[i].cancel()
                    del self.workers[i]
                    log.info(f'[{self.loop_type}] Cancelled worker {i} (sublists={needed})')


async def process_gur_result(users: list, usernames: dict, clients: dict, dispatcher: 'ClientDispatcher'):
    from telethon.tl.functions.users import GetFullUserRequest
    from telethon.errors import FloodWaitError

    uid_to_username = {info['user_id']: uname for uname, info in usernames.items()}

    for user in users:
        matched_username = uid_to_username.get(user.id)
        matched_info     = usernames.get(matched_username) if matched_username else None
        if matched_username is None or matched_info is None:
            continue

        uid    = user.id
        u_type = matched_info['type']
        peer_u_type = 'window' if u_type in ('timestamp', 'interval_timestamp') else u_type

        if user.deleted:
            await claim_username(matched_username, uid, peer_u_type, usernames, clients, dispatcher)


async def claim_username(username: str, uid: int, u_type: str, usernames: dict, clients: dict, dispatcher: 'ClientDispatcher'):
    from telethon.tl.functions.account import UpdateUsernameRequest
    from telethon.errors import FloodWaitError, UsernameOccupiedError, UsernameInvalidError

    while True:
        session_name, profile, _ = await dispatcher.acquire('UpdateUsernameRequest')
        if session_name is None:
            return False

        client = profile['instance']
        channel = profile['channel']

        try:
            await client(UpdateUsernameRequest(channel, username))
            log.info(f'[claim] Successfully claimed {username} on {session_name}')
            remove_peer_all_sessions(username, uid, u_type, clients)
            usernames.pop(username, None)
            await create_channel(clients, session_name)
            dispatcher.release_for_claiming(session_name)
            return True
        except FloodWaitError as e:
            log.warning(f'[claim] FloodWait on {session_name} for {e.seconds}s')
            dispatcher.flood_wait('UpdateUsernameRequest', session_name, e.seconds)
            continue
        except (UsernameOccupiedError, UsernameInvalidError):
            log.info(f'[claim] {username} already taken or invalid on {session_name}')
            dispatcher.release('UpdateUsernameRequest', session_name)
            return False
        except Exception as e:  # noqa: BLE001
            log.error(f'[claim] Error claiming {username} on {session_name}: {e}')
            dispatcher.release_back('UpdateUsernameRequest', session_name)
            return False

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

    window_manager   = LoopManager('window',   clients, usernames, dispatcher)
    long_ago_manager = LoopManager('long_ago', clients, usernames, dispatcher)
    worker_managers  = {'window': window_manager, 'long_ago': long_ago_manager}

    await asyncio.gather(
        connect_to_coordinator(usernames, clients, sniper_id, dispatcher, worker_managers),
        window_manager.run(),
        long_ago_manager.run(),
    )


if __name__ == '__main__':
    uvloop.run(main())