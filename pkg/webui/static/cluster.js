(() => {
  const btnOpen = document.getElementById('btn-cluster');
  const dialog = document.getElementById('cluster-dialog');
  const btnRefresh = document.getElementById('btn-cluster-refresh');
  const authBox = document.getElementById('cluster-auth-box');
  const authMsg = document.getElementById('cluster-auth-msg');
  const authForm = document.getElementById('cluster-auth-form');
  const tokenInput = document.getElementById('cluster-token-input');
  const contentBox = document.getElementById('cluster-content');

  const cmSessions = document.getElementById('cm-sessions');
  const cmPods = document.getElementById('cm-pods');
  const cmRetargets = document.getElementById('cm-retargets');
  const cmBuffered = document.getElementById('cm-buffered');
  const podsBody = document.getElementById('cluster-pods-body');
  const routesBody = document.getElementById('cluster-routes-body');
  const eventsList = document.getElementById('cluster-events');

  if (!btnOpen || !dialog) return;

  const TOKEN_KEY = 'arcade_cluster_token';
  let pollTimer = null;
  let prevRoutes = new Map();
  let prevPods = new Map();
  let prevRetargets = null;
  const eventLog = [];

  function recordEvent(text) {
    const now = new Date().toLocaleTimeString([], { hour12: false });
    eventLog.unshift({ time: now, text });
    if (eventLog.length > 20) eventLog.pop();
    renderEvents();
  }

  function renderEvents() {
    eventsList.textContent = '';
    if (eventLog.length === 0) {
      const li = document.createElement('li');
      li.className = 'cluster-empty';
      li.textContent = 'Watching for route or server changes.';
      eventsList.appendChild(li);
      return;
    }
    for (const item of eventLog) {
      const li = document.createElement('li');
      const timeSpan = document.createElement('span');
      timeSpan.className = 'event-time';
      timeSpan.textContent = item.time;
      const msgSpan = document.createElement('span');
      msgSpan.textContent = item.text;
      li.appendChild(timeSpan);
      li.appendChild(msgSpan);
      eventsList.appendChild(li);
    }
  }

  function detectChanges(data) {
    const stats = data.stats || {};
    if (prevRetargets !== null && stats.retargets > prevRetargets) {
      const diff = stats.retargets - prevRetargets;
      recordEvent(`Moved ${diff} player session${diff > 1 ? 's' : ''} to a new primary.`);
    }
    prevRetargets = stats.retargets ?? 0;

    const nextRoutes = new Map();
    for (const rt of data.routes || []) {
      nextRoutes.set(rt.match_id, rt);
      const old = prevRoutes.get(rt.match_id);
      if (old && (old.primary_addr !== rt.primary_addr || old.epoch !== rt.epoch)) {
        recordEvent(`${rt.match_id} switched to ${rt.primary_addr} (epoch ${rt.epoch}).`);
      }
    }
    prevRoutes = nextRoutes;

    const nextPods = new Map();
    for (const pod of data.pods || []) {
      nextPods.set(pod.addr, pod);
      const old = prevPods.get(pod.addr);
      if (old) {
        if (old.reachable && !pod.reachable) {
          recordEvent(`Server ${pod.pod || pod.addr} stopped responding.`);
        } else if (!old.reachable && pod.reachable) {
          recordEvent(`Server ${pod.pod || pod.addr} is back online as ${pod.role}.`);
        } else if (old.role !== pod.role) {
          recordEvent(`Server ${pod.pod || pod.addr} changed role: ${old.role} -> ${pod.role}.`);
        }
      }
    }
    prevPods = nextPods;
  }

  function makePill(text, cls) {
    const span = document.createElement('span');
    span.className = `role-pill ${cls}`;
    span.textContent = text;
    return span;
  }

  function renderCluster(data) {
    authBox.hidden = true;
    contentBox.hidden = false;

    const stats = data.stats || {};
    const pods = data.pods || [];
    const routes = data.routes || [];
    const healthyCount = pods.filter((p) => p.reachable).length;

    cmSessions.textContent = String(stats.active_sessions ?? 0);
    cmPods.textContent = `${healthyCount} / ${pods.length}`;
    cmRetargets.textContent = String(stats.retargets ?? 0);
    cmBuffered.textContent = String(stats.buffered_messages ?? 0);

    podsBody.textContent = '';
    if (pods.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 5;
      td.className = 'cluster-empty';
      td.textContent = 'No game servers registered yet.';
      tr.appendChild(td);
      podsBody.appendChild(tr);
    } else {
      for (const pod of pods) {
        const tr = document.createElement('tr');

        const tdName = document.createElement('td');
        tdName.textContent = pod.pod || '—';

        const tdAddr = document.createElement('td');
        tdAddr.className = 'mono-cell';
        tdAddr.textContent = pod.addr;

        const tdRole = document.createElement('td');
        if (pod.reachable) {
          tdRole.appendChild(makePill(pod.role || 'unknown', pod.role === 'primary' ? 'primary' : 'backup'));
        } else {
          tdRole.textContent = '—';
        }

        const tdEpoch = document.createElement('td');
        tdEpoch.className = 'mono-cell';
        tdEpoch.textContent = pod.reachable ? String(pod.epoch ?? 0) : '—';

        const tdStatus = document.createElement('td');
        tdStatus.appendChild(makePill(pod.reachable ? 'Healthy' : 'Offline', pod.reachable ? 'healthy' : 'offline'));

        tr.appendChild(tdName);
        tr.appendChild(tdAddr);
        tr.appendChild(tdRole);
        tr.appendChild(tdEpoch);
        tr.appendChild(tdStatus);
        podsBody.appendChild(tr);
      }
    }

    routesBody.textContent = '';
    if (routes.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 3;
      td.className = 'cluster-empty';
      td.textContent = data.default_upstream
        ? `Using default server (${data.default_upstream}).`
        : 'No active match routes.';
      tr.appendChild(td);
      routesBody.appendChild(tr);
    } else {
      for (const rt of routes) {
        const tr = document.createElement('tr');

        const tdMatch = document.createElement('td');
        tdMatch.className = 'mono-cell';
        tdMatch.textContent = rt.match_id;

        const tdAddr = document.createElement('td');
        tdAddr.className = 'mono-cell';
        tdAddr.textContent = rt.primary_addr;

        const tdEpoch = document.createElement('td');
        tdEpoch.className = 'mono-cell';
        tdEpoch.textContent = String(rt.epoch ?? 0);

        tr.appendChild(tdMatch);
        tr.appendChild(tdAddr);
        tr.appendChild(tdEpoch);
        routesBody.appendChild(tr);
      }
    }

    detectChanges(data);
  }

  async function refreshCluster() {
    const headers = {};
    const saved = sessionStorage.getItem(TOKEN_KEY);
    if (saved) headers.Authorization = `Bearer ${saved}`;

    try {
      const resp = await fetch('/api/cluster', { cache: 'no-store', headers });
      if (resp.status === 401 || resp.status === 403) {
        const errData = await resp.json().catch(() => ({}));
        authMsg.textContent = errData.error || 'Operator token required to view cluster processes.';
        authBox.hidden = false;
        contentBox.hidden = true;
        return;
      }
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      renderCluster(data);
    } catch (err) {
      recordEvent(`Failed to refresh cluster status: ${err.message}`);
    }
  }

  function startPolling() {
    refreshCluster();
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(refreshCluster, 2000);
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  btnOpen.addEventListener('click', () => {
    dialog.showModal();
    startPolling();
  });

  if (btnRefresh) btnRefresh.addEventListener('click', refreshCluster);

  if (authForm) {
    authForm.addEventListener('submit', (e) => {
      e.preventDefault();
      const val = (tokenInput.value || '').trim();
      if (!val) return;
      sessionStorage.setItem(TOKEN_KEY, val);
      tokenInput.value = '';
      refreshCluster();
    });
  }

  dialog.addEventListener('close', () => {
    dialog.returnValue = '';
    stopPolling();
  });
})();
