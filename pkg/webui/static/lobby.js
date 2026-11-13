(() => {
  const lobbyDialog = document.getElementById('lobby-dialog');
  const lobbyForm = document.getElementById('lobby-form');
  const inputMatch = document.getElementById('input-match');
  const inputPlayer = document.getElementById('input-player');
  const roomsList = document.getElementById('rooms-list');
  const btnQuickMatch = document.getElementById('btn-quick-match');
  const btnCreateRoom = document.getElementById('btn-create-room');
  const btnRefreshRooms = document.getElementById('btn-refresh-rooms');

  if (!lobbyDialog || !lobbyForm || !roomsList) return;

  function joinSeat(matchId, playerId) {
    inputMatch.value = matchId;
    if (playerId) {
      inputPlayer.value = playerId;
    }
    if (typeof lobbyForm.requestSubmit === 'function') {
      lobbyForm.requestSubmit();
    } else {
      lobbyForm.dispatchEvent(new Event('submit', { cancelable: true, bubbles: true }));
    }
  }

  function renderRooms(rooms) {
    roomsList.textContent = '';
    if (!Array.isArray(rooms) || rooms.length === 0) {
      const empty = document.createElement('p');
      empty.className = 'rooms-empty';
      empty.textContent = 'No active rooms yet. Click "Quick Match" or "Create New Room" to start one.';
      roomsList.appendChild(empty);
      return;
    }

    for (const room of rooms) {
      const row = document.createElement('div');
      row.className = 'room-row';
      row.setAttribute('role', 'listitem');

      const meta = document.createElement('div');
      meta.className = 'room-meta';

      const idEl = document.createElement('span');
      idEl.className = 'room-id';
      idEl.textContent = room.match_id;

      const badges = document.createElement('div');
      badges.className = 'room-badges';

      const statusBadge = document.createElement('span');
      statusBadge.className = `room-badge ${room.status || ''}`;
      statusBadge.textContent = room.status === 'finished' ? 'Finished' : 'Live';

      const seqBadge = document.createElement('span');
      seqBadge.className = 'room-badge';
      seqBadge.textContent = `seq ${room.sequence ?? 0}`;

      badges.appendChild(statusBadge);
      badges.appendChild(seqBadge);
      meta.appendChild(idEl);
      meta.appendChild(badges);

      const seats = document.createElement('div');
      seats.className = 'room-seats';
      const occupiedSet = new Set(room.occupied_seats || []);
      const players = room.players || [];

      for (const seat of players) {
        if (occupiedSet.has(seat)) {
          const taken = document.createElement('span');
          taken.className = 'seat-chip seat-taken';
          taken.textContent = `${seat} · Taken`;
          seats.appendChild(taken);
        } else {
          const btn = document.createElement('button');
          btn.type = 'button';
          btn.className = 'seat-chip seat-open';
          btn.textContent = `${seat} · Join`;
          btn.addEventListener('click', () => joinSeat(room.match_id, seat));
          seats.appendChild(btn);
        }
      }

      row.appendChild(meta);
      row.appendChild(seats);
      roomsList.appendChild(row);
    }
  }

  async function refreshRooms() {
    try {
      const resp = await fetch('/api/rooms', { cache: 'no-store' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      renderRooms(data.rooms || []);
    } catch (err) {
      roomsList.textContent = '';
      const empty = document.createElement('p');
      empty.className = 'rooms-empty';
      empty.textContent = `Unable to load rooms (${err.message}).`;
      roomsList.appendChild(empty);
    }
  }

  async function quickMatch() {
    btnQuickMatch.disabled = true;
    try {
      const resp = await fetch('/api/matchmake', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      joinSeat(data.match_id, data.player_id || 'player1');
    } catch (err) {
      roomsList.textContent = '';
      const empty = document.createElement('p');
      empty.className = 'rooms-empty';
      empty.textContent = `Quick Match failed: ${err.message}`;
      roomsList.appendChild(empty);
    } finally {
      btnQuickMatch.disabled = false;
    }
  }

  async function createNewRoom() {
    btnCreateRoom.disabled = true;
    try {
      const resp = await fetch('/api/rooms', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const room = await resp.json();
      joinSeat(room.match_id, 'player1');
    } catch (err) {
      roomsList.textContent = '';
      const empty = document.createElement('p');
      empty.className = 'rooms-empty';
      empty.textContent = `Create Room failed: ${err.message}`;
      roomsList.appendChild(empty);
    } finally {
      btnCreateRoom.disabled = false;
    }
  }

  if (btnQuickMatch) btnQuickMatch.addEventListener('click', quickMatch);
  if (btnCreateRoom) btnCreateRoom.addEventListener('click', createNewRoom);
  if (btnRefreshRooms) btnRefreshRooms.addEventListener('click', refreshRooms);

  const observer = new MutationObserver(() => {
    if (lobbyDialog.open) {
      refreshRooms();
    }
  });
  observer.observe(lobbyDialog, { attributes: true, attributeFilter: ['open'] });

  if (lobbyDialog.open) {
    refreshRooms();
  }
})();
