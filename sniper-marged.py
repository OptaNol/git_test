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
                    next((p for p in profile['frozen']   if getattr(p, 'user_id', None) == uid), None) or
                    next((p for p in profile['window']   if getattr(p, 'user_id', None) == uid), None) or
                    next((p for p in profile['long_ago'] if getattr(p, 'user_id', None) == uid), None)
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
    )


if __name__ == '__main__':
    uvloop.run(main())