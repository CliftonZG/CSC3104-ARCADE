(() => {
  // Feature-detect Popover API; load polyfill only when missing
  if (!Object.hasOwn(HTMLElement.prototype, 'popover')) {
    import('https://unpkg.com/@oddbird/popover-polyfill@latest/dist/popover.min.js').catch(() => {});
  }

  const SYMBOL_MAP = {
    skip: '⊘',
    reverse: '⇄',
    draw2: '+2',
    wild: '★',
    wild_draw4: '+4',
  };

  const state = {
    ws: null,
    matchId: 'room0',
    playerId: 'player1',
    game: null,
    autoBots: true,
    botTimer: null,
    reconnectTimer: null,
    reconnectDelay: 300,
    lastActionSentAt: 0,
    pendingWildIndex: null,
    gameOverShownForSeq: -1,
  };

  // DOM Elements
  const elConnDot = document.getElementById('conn-dot');
  const elConnStatus = document.getElementById('conn-status');
  const elHudMatch = document.getElementById('hud-match');
  const elHudSeat = document.getElementById('hud-seat');
  const elHudSeq = document.getElementById('hud-seq');
  const elHudRtt = document.getElementById('hud-rtt');
  const elAutoBots = document.getElementById('auto-bots-toggle');
  const elBtnStepBot = document.getElementById('btn-step-bot');
  const elBtnNewGame = document.getElementById('btn-new-game');
  const elBtnOpenLobby = document.getElementById('btn-open-lobby');
  const elBtnPlayAgain = document.getElementById('btn-play-again');
  const elOpponents = document.getElementById('opponents-container');
  const elBtnDraw = document.getElementById('btn-draw');
  const elDeckCount = document.getElementById('deck-count');
  const elDiscardCard = document.getElementById('discard-card');
  const elColorBadge = document.getElementById('active-color-badge');
  const elDirBadge = document.getElementById('direction-badge');
  const elBtnPass = document.getElementById('btn-pass');
  const elTurnBanner = document.getElementById('turn-banner');
  const elPlayerHand = document.getElementById('player-hand');

  const lobbyDialog = document.getElementById('lobby-dialog');
  const lobbyForm = document.getElementById('lobby-form');
  const inputMatch = document.getElementById('input-match');
  const inputPlayer = document.getElementById('input-player');
  const inputAutoBots = document.getElementById('input-autobots');

  const wildDialog = document.getElementById('wild-dialog');
  const gameoverDialog = document.getElementById('gameover-dialog');
  const gameoverMsg = document.getElementById('gameover-message');

  const toastRegion = document.getElementById('toast-region');
  const srLive = document.getElementById('sr-announcements');

  function announce(msg) {
    if (typeof document.ariaNotify === 'function') {
      document.ariaNotify(msg);
    } else {
      srLive.textContent = msg;
    }
  }

  function showToast(message, variant = 'info', durationMs = 4000) {
    const toast = document.createElement('div');
    toast.className = `toast toast-${variant}`;
    toast.setAttribute('popover', 'manual');

    const text = document.createElement('span');
    text.textContent = message;

    const closeBtn = document.createElement('button');
    closeBtn.type = 'button';
    closeBtn.className = 'toast-close';
    closeBtn.setAttribute('aria-label', 'Dismiss notification');
    closeBtn.textContent = '✕';
    closeBtn.addEventListener('click', () => {
      if (typeof toast.hidePopover === 'function') {
        try { toast.hidePopover(); } catch {}
      }
      toast.remove();
    });

    toast.append(text, closeBtn);
    toastRegion.appendChild(toast);

    if (typeof toast.showPopover === 'function') {
      try { toast.showPopover(); } catch {}
    } else {
      toast.classList.add(':popover-open');
    }

    announce(message);

    if (durationMs > 0) {
      setTimeout(() => {
        if (!toast.isConnected) return;
        if (toast.contains(document.activeElement)) return;
        if (typeof toast.hidePopover === 'function') {
          try { toast.hidePopover(); } catch {}
        }
        toast.remove();
      }, durationMs);
    }
  }

  function setConnectionState(status, label) {
    elConnDot.className = `status-dot ${status}`;
    elConnStatus.textContent = label;
  }

  function connect() {
    if (state.ws) {
      state.ws.onclose = null;
      state.ws.close();
    }
    clearTimeout(state.reconnectTimer);
    setConnectionState('reconnecting', 'Connecting...');

    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${proto}//${window.location.host}/ws?match=${encodeURIComponent(state.matchId)}&player=${encodeURIComponent(state.playerId)}`;

    const ws = new WebSocket(wsUrl);
    state.ws = ws;

    ws.onopen = () => {
      state.reconnectDelay = 300;
      setConnectionState('connected', 'Connected');
      elHudMatch.textContent = state.matchId;
      elHudSeat.textContent = state.playerId;
    };

    ws.onmessage = (event) => {
      let env;
      try {
        env = JSON.parse(event.data);
      } catch {
        return;
      }

      if (env.type === 'state_broadcast' && env.payload) {
        if (state.lastActionSentAt > 0) {
          const rtt = Date.now() - state.lastActionSentAt;
          elHudRtt.textContent = `${rtt} ms`;
          state.lastActionSentAt = 0;
        }
        state.game = env.payload;
        render();
        scheduleAutoBot();
      } else if (env.type === 'error_response' && env.payload) {
        showToast(env.payload.message || 'Action rejected', 'error');
      }
    };

    ws.onclose = () => {
      setConnectionState('reconnecting', 'Reconnecting...');
      clearTimeout(state.botTimer);
      state.reconnectTimer = setTimeout(() => {
        state.reconnectDelay = Math.min(state.reconnectDelay * 1.5, 3000);
        connect();
      }, state.reconnectDelay);
    };

    ws.onerror = () => {
      // onclose handles reconnect
    };
  }

  function sendEnvelope(type, payload) {
    if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
      showToast('Socket not connected yet', 'warn');
      return;
    }
    state.lastActionSentAt = Date.now();
    state.ws.send(JSON.stringify({
      type,
      match_id: state.matchId,
      timestamp: Date.now(),
      payload,
    }));
  }

  function sendAction(action) {
    sendEnvelope('action_request', action);
  }

  function stepBot() {
    sendEnvelope('bot_step', {});
  }

  function scheduleAutoBot() {
    clearTimeout(state.botTimer);
    if (!state.autoBots || !state.game) return;
    if (state.game.status === 'finished') return;

    const currentPlayer = state.game.players[state.game.current_player_index];
    if (currentPlayer && currentPlayer !== state.playerId) {
      state.botTimer = setTimeout(() => {
        stepBot();
      }, 650);
    }
  }

  function isPlayable(card, game) {
    if (!game || game.status === 'finished') return false;
    const top = game.discard_pile[game.discard_pile.length - 1];
    if (!top) return false;
    if (card.color === 'wild') return true;
    return card.color === game.current_color || card.value === top.value;
  }

  function createCardElement(card) {
    const cardDiv = document.createElement('div');
    cardDiv.className = `card card-${card.color}`;

    const label = SYMBOL_MAP[card.value] || card.value.toUpperCase();

    const tl = document.createElement('span');
    tl.className = 'card-corner tl';
    tl.textContent = label;

    const frame = document.createElement('div');
    frame.className = 'card-frame';
    const center = document.createElement('span');
    center.className = 'card-center';
    center.textContent = label;
    frame.appendChild(center);

    const br = document.createElement('span');
    br.className = 'card-corner br';
    br.textContent = label;

    cardDiv.append(tl, frame, br);
    return cardDiv;
  }

  function render() {
    const g = state.game;
    if (!g) return;

    elHudSeq.textContent = String(g.sequence);
    elDeckCount.textContent = String((g.deck || []).length);

    const currentPlayer = g.players[g.current_player_index];
    const isMyTurn = g.status !== 'finished' && currentPlayer === state.playerId;

    // Opponents
    elOpponents.innerHTML = '';
    for (const p of g.players) {
      if (p === state.playerId) continue;
      const count = (g.hands[p] || []).length;
      const card = document.createElement('div');
      card.className = `opponent-card${p === currentPlayer && g.status !== 'finished' ? ' active-turn' : ''}`;

      const meta = document.createElement('div');
      meta.className = 'opponent-meta';
      const nameEl = document.createElement('span');
      nameEl.className = 'opponent-name';
      nameEl.textContent = p;
      if (count === 1) {
        const uno = document.createElement('span');
        uno.className = 'uno-shout';
        uno.textContent = '1 LEFT!';
        nameEl.appendChild(uno);
      }

      const countEl = document.createElement('span');
      countEl.className = 'opponent-badge';
      countEl.textContent = `${count} ${count === 1 ? 'card' : 'cards'}`;
      meta.append(nameEl, countEl);

      const fan = document.createElement('div');
      fan.className = 'mini-fan';
      fan.setAttribute('aria-hidden', 'true');
      const shown = Math.min(count, 5);
      for (let i = 0; i < shown; i++) {
        const mini = document.createElement('div');
        mini.className = 'mini-card-back';
        fan.appendChild(mini);
      }

      card.append(meta, fan);
      elOpponents.appendChild(card);
    }

    // Discard pile top card
    elDiscardCard.innerHTML = '';
    const topCard = g.discard_pile[g.discard_pile.length - 1];
    if (topCard) {
      elDiscardCard.appendChild(createCardElement(topCard));
    }

    // Active Color & Direction
    elColorBadge.textContent = g.current_color;
    elColorBadge.className = `color-chip color-${g.current_color}`;
    elDirBadge.textContent = g.direction >= 0 ? '↻ Clockwise' : '↺ Counter-CW';

    // Action buttons
    elBtnDraw.disabled = !isMyTurn || g.drew_this_turn;
    elBtnPass.disabled = !isMyTurn || !g.drew_this_turn;
    elBtnStepBot.disabled = g.status === 'finished' || isMyTurn;

    // Turn Banner
    if (g.status === 'finished') {
      elTurnBanner.textContent = `Match Over — Winner: ${g.winner}`;
      elTurnBanner.className = 'turn-banner';
      if (state.gameOverShownForSeq !== g.sequence) {
        state.gameOverShownForSeq = g.sequence;
        gameoverMsg.textContent = g.winner === state.playerId
          ? 'Congratulations! You emptied your hand and won the match.'
          : `${g.winner} won the match at sequence #${g.sequence}.`;
        gameoverDialog.showModal();
      }
    } else if (isMyTurn) {
      elTurnBanner.textContent = g.drew_this_turn
        ? 'Your Turn — Play the drawn card or Pass'
        : 'Your Turn — Play a highlighted card or Draw';
      elTurnBanner.className = 'turn-banner your-turn';
    } else {
      elTurnBanner.textContent = `Waiting for ${currentPlayer}...`;
      elTurnBanner.className = 'turn-banner';
    }

    // Player Hand
    elPlayerHand.innerHTML = '';
    const hand = g.hands[state.playerId] || [];
    hand.forEach((card, idx) => {
      const canPlay = isMyTurn && isPlayable(card, g);
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = `card-btn ${canPlay ? 'playable' : 'unplayable'}`;
      btn.setAttribute('role', 'listitem');
      btn.setAttribute('aria-label', `${card.color} ${card.value}${canPlay ? ', playable' : ''}`);
      btn.disabled = !canPlay;

      btn.appendChild(createCardElement(card));
      btn.addEventListener('click', () => {
        if (card.color === 'wild') {
          state.pendingWildIndex = idx;
          wildDialog.showModal();
        } else {
          sendAction({
            type: 'play',
            player_id: state.playerId,
            card_index: idx,
          });
        }
      });

      elPlayerHand.appendChild(btn);
    });
  }

  // Event Listeners
  elBtnDraw.addEventListener('click', () => {
    sendAction({ type: 'draw', player_id: state.playerId });
  });

  elBtnPass.addEventListener('click', () => {
    sendAction({ type: 'pass', player_id: state.playerId });
  });

  function requestNewGame() {
    state.gameOverShownForSeq = -1;
    if (gameoverDialog.open) {
      gameoverDialog.close();
    }
    sendEnvelope('new_game', {});
  }

  elBtnStepBot.addEventListener('click', () => {
    stepBot();
  });

  elBtnNewGame.addEventListener('click', requestNewGame);
  elBtnPlayAgain.addEventListener('click', requestNewGame);

  elAutoBots.addEventListener('change', (e) => {
    state.autoBots = e.target.checked;
    if (state.autoBots) {
      scheduleAutoBot();
    } else {
      clearTimeout(state.botTimer);
    }
  });

  elBtnOpenLobby.addEventListener('click', () => {
    inputMatch.value = state.matchId;
    inputPlayer.value = state.playerId;
    inputAutoBots.checked = state.autoBots;
    lobbyDialog.showModal();
  });

  lobbyForm.addEventListener('submit', () => {
    state.matchId = inputMatch.value.trim() || 'room0';
    state.playerId = inputPlayer.value;
    state.autoBots = inputAutoBots.checked;
    elAutoBots.checked = state.autoBots;

    const url = new URL(window.location.href);
    url.searchParams.set('match', state.matchId);
    url.searchParams.set('player', state.playerId);
    window.history.replaceState({}, '', url);

    connect();
  });

  lobbyDialog.addEventListener('close', () => {
    lobbyDialog.returnValue = '';
  });

  wildDialog.querySelectorAll('.wild-choice').forEach((btn) => {
    btn.addEventListener('click', () => {
      const color = btn.dataset.color;
      const idx = state.pendingWildIndex;
      state.pendingWildIndex = null;
      wildDialog.close(color);
      if (idx !== null) {
        sendAction({
          type: 'play',
          player_id: state.playerId,
          card_index: idx,
          declared_color: color,
        });
      }
    });
  });

  wildDialog.addEventListener('close', () => {
    if (!wildDialog.returnValue) {
      state.pendingWildIndex = null;
    }
    wildDialog.returnValue = '';
  });

  gameoverDialog.addEventListener('close', () => {
    gameoverDialog.returnValue = '';
  });

  // Boot: read URL query parameters or open lobby dialog
  const params = new URLSearchParams(window.location.search);
  if (params.get('match') && params.get('player')) {
    state.matchId = params.get('match');
    state.playerId = params.get('player');
    elAutoBots.checked = state.autoBots;
    connect();
  } else {
    elAutoBots.checked = state.autoBots;
    lobbyDialog.showModal();
  }
})();
