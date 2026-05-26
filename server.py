"""
SwarmTrain Dashboard Engine — server.py
=======================================
A Streamlit-based UI that mimics a Google Colab / Jupyter Notebook workspace.
It acts as the central coordinator for a two-node pipeline-parallel training demo.

Architecture:
  - This server hosts a WebSocket server (asyncio) in a background thread.
  - Cell 1 pushes tokenised sentences to Node 1 (Attention Layer).
  - Cell 2 forwards Node 1's hidden states to Node 2 (LM Head).
  - Both cells display live streaming logs from their respective nodes.

Run with:
    streamlit run server.py
"""

import asyncio
import json
import threading
import time
import random
import os

import streamlit as st
import websockets
from websockets.server import serve

# ── Tiny mock dataset (no external file needed) ──────────────────────────────
TINYSTORIES = [
    "Once upon a time there was a little cat named Mia who loved to chase butterflies.",
    "The brave rabbit hopped across the meadow to find his lost golden carrot.",
    "Lily the owl stayed up all night watching the stars twinkle in the dark sky.",
    "A small dragon named Pip could only breathe tiny bubbles instead of fire.",
    "Tom the turtle walked very slowly but always arrived right on time.",
    "The kind fairy sprinkled silver dust over the sleeping forest animals.",
    "Every morning the sun smiled at the valley and the flowers woke up to dance.",
    "Billy the bear found a jar of honey hidden under the big mossy rock.",
    "Sophie loved to paint rainbows on rainy days so the clouds would feel happy.",
    "The tiny train puffed and puffed until it finally reached the top of the hill.",
]

# Write tinystories.txt once so the project is self-contained on disk too
_STORIES_PATH = os.path.join(os.path.dirname(__file__), "tinystories.txt")
if not os.path.exists(_STORIES_PATH):
    with open(_STORIES_PATH, "w") as _f:
        _f.write("\n".join(TINYSTORIES))

# ── Shared server state (thread-safe via asyncio.Event / dicts) ───────────────
# We store connected WebSocket clients by their registered node ID.
_CONNECTED_NODES: dict[str, "websockets.WebSocketServerProtocol"] = {}
_LOCK = threading.Lock()

# Queues bridging asyncio ↔ Streamlit (which runs in a normal thread)
_cell1_result_queue: asyncio.Queue = None   # populated after loop starts
_cell2_result_queue: asyncio.Queue = None
_log_queue: asyncio.Queue = None            # general connection logs
_LOOP: asyncio.AbstractEventLoop = None

# ── WebSocket server ──────────────────────────────────────────────────────────

async def _ws_handler(websocket):
    """
    Handle an incoming node connection.

    Protocol (all messages are JSON):
      Node → Server:  {"type": "register", "node": "1" | "2"}
      Server → Node:  {"type": "cell1_payload", "tokens": [...], "sentence": "..."}
                   or {"type": "cell2_payload", "hidden_states": [...]}
      Node → Server:  {"type": "cell1_result",  "hidden_states": [...], "log": "..."}
                   or {"type": "cell2_result",  "predicted_word": "...", "log": "..."}
    """
    node_id = None
    try:
        async for raw in websocket:
            msg = json.loads(raw)

            # ── Registration handshake ──────────────────────────────────────
            if msg["type"] == "register":
                node_id = msg["node"]
                with _LOCK:
                    _CONNECTED_NODES[node_id] = websocket
                await _log_queue.put(f"✅  Node {node_id} connected  [{websocket.remote_address}]")
                await websocket.send(json.dumps({"type": "ack", "message": f"Node {node_id} registered"}))

            # ── Result from Node 1 (Attention Layer) ───────────────────────
            elif msg["type"] == "cell1_result":
                await _log_queue.put(f"📨  Cell 1 result received from Node 1")
                await _cell1_result_queue.put(msg)

            # ── Result from Node 2 (LM Head) ───────────────────────────────
            elif msg["type"] == "cell2_result":
                await _log_queue.put(f"📨  Cell 2 result received from Node 2")
                await _cell2_result_queue.put(msg)

    except websockets.exceptions.ConnectionClosedOK:
        pass
    except Exception as exc:
        await _log_queue.put(f"⚠️  Connection error: {exc}")
    finally:
        if node_id:
            with _LOCK:
                _CONNECTED_NODES.pop(node_id, None)
            await _log_queue.put(f"🔌  Node {node_id} disconnected")


async def _run_ws_server():
    """Start the WebSocket server and initialise shared queues."""
    global _cell1_result_queue, _cell2_result_queue, _log_queue
    _cell1_result_queue = asyncio.Queue()
    _cell2_result_queue = asyncio.Queue()
    _log_queue = asyncio.Queue()
    async with serve(_ws_handler, "localhost", 8765):
        await asyncio.get_event_loop().create_future()  # run forever


def _start_background_loop():
    """Spin up the asyncio event loop in a daemon thread."""
    global _LOOP
    _LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(_LOOP)
    _LOOP.run_until_complete(_run_ws_server())


# ── Helper: run a coroutine from the Streamlit (sync) thread ─────────────────

def _run_coro(coro):
    """Schedule a coroutine on the background loop and block until done."""
    future = asyncio.run_coroutine_threadsafe(coro, _LOOP)
    return future.result(timeout=15)


async def _send_to_node(node_id: str, payload: dict) -> str | None:
    """Send a JSON payload to a connected node; return error string or None."""
    with _LOCK:
        ws = _CONNECTED_NODES.get(node_id)
    if ws is None:
        return f"Node {node_id} is not connected"
    await ws.send(json.dumps(payload))
    return None


async def _drain_queue(q: asyncio.Queue) -> list:
    """Non-blocking drain of all currently queued items."""
    items = []
    while not q.empty():
        items.append(await q.get())
    return items


# ── Streamlit page configuration ──────────────────────────────────────────────
st.set_page_config(
    page_title="SwarmTrain Platform",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS — dark Colab-inspired notebook aesthetic ───────────────────────
st.markdown("""
<style>
  /* ── Google Fonts ── */
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=DM+Sans:wght@300;400;600;700&display=swap');

  /* ── Global reset ── */
  html, body, [class*="css"] {
      font-family: 'DM Sans', sans-serif;
      background-color: #0e1117;
      color: #e0e6f0;
  }

  /* ── Hide Streamlit chrome ── */
  #MainMenu, footer, header { visibility: hidden; }
  .block-container { padding-top: 1.8rem; padding-bottom: 3rem; max-width: 1100px; }

  /* ── Top header bar ── */
  .swarm-header {
      background: linear-gradient(135deg, #0d1b2a 0%, #1a2744 50%, #0f2040 100%);
      border: 1px solid #1e3a5f;
      border-radius: 12px;
      padding: 1.6rem 2.2rem;
      margin-bottom: 1.8rem;
      display: flex;
      align-items: center;
      gap: 1rem;
      box-shadow: 0 4px 32px rgba(0,120,255,0.12), inset 0 1px 0 rgba(255,255,255,0.06);
  }
  .swarm-header h1 {
      margin: 0;
      font-size: 1.9rem;
      font-weight: 700;
      letter-spacing: -0.02em;
      background: linear-gradient(90deg, #4fc3f7, #81d4fa, #b3e5fc);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
  }
  .swarm-header .subtitle {
      font-size: 0.82rem;
      color: #5a8db5;
      margin-top: 0.2rem;
      font-weight: 400;
      letter-spacing: 0.04em;
      text-transform: uppercase;
  }
  .swarm-logo { font-size: 2.4rem; }

  /* ── Status pill ── */
  .status-pill {
      display: inline-flex; align-items: center; gap: 6px;
      background: rgba(0,200,100,0.1);
      border: 1px solid rgba(0,200,100,0.3);
      border-radius: 20px;
      padding: 3px 12px;
      font-size: 0.75rem; font-weight: 600;
      color: #4caf82;
      letter-spacing: 0.05em;
  }
  .status-dot { width: 7px; height: 7px; border-radius: 50%; background: #4caf82;
                animation: pulse 1.8s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }

  /* ── Notebook cell wrapper ── */
  .nb-cell {
      background: #161b27;
      border: 1px solid #1e2d45;
      border-left: 3px solid #1565c0;
      border-radius: 0 8px 8px 0;
      margin-bottom: 0.4rem;
      overflow: hidden;
      box-shadow: 0 2px 12px rgba(0,0,0,0.4);
  }
  .nb-cell-header {
      background: #0d131f;
      padding: 0.45rem 1rem;
      display: flex; align-items: center; gap: 8px;
      border-bottom: 1px solid #1a2540;
      font-size: 0.72rem;
      color: #4a6fa5;
      font-family: 'JetBrains Mono', monospace;
      font-weight: 600;
      letter-spacing: 0.06em;
      text-transform: uppercase;
  }
  .cell-index {
      background: #1a3a6b;
      color: #7ab3e8;
      padding: 1px 8px;
      border-radius: 4px;
      font-size: 0.68rem;
  }

  /* ── Markdown explanation cells ── */
  .md-cell {
      background: #111520;
      border: 1px solid #1a2335;
      border-left: 3px solid #37474f;
      border-radius: 0 8px 8px 0;
      padding: 1rem 1.4rem;
      margin-bottom: 0.5rem;
      font-size: 0.88rem;
      line-height: 1.7;
      color: #b0bec5;
  }
  .md-cell h3 { color: #e3f2fd; font-size: 1rem; margin: 0 0 0.4rem 0; font-weight: 600; }
  .md-cell code {
      background: #1a2a3a; color: #80cbc4;
      padding: 1px 5px; border-radius: 3px;
      font-family: 'JetBrains Mono', monospace; font-size: 0.82em;
  }
  .md-cell .tag {
      display: inline-block;
      background: rgba(21,101,192,0.2); color: #64b5f6;
      border: 1px solid rgba(100,181,246,0.3);
      border-radius: 4px; padding: 1px 7px;
      font-size: 0.73rem; font-weight: 600;
      margin-right: 6px; letter-spacing: 0.04em;
  }

  /* ── Output log box ── */
  .output-box {
      background: #090d14;
      border: 1px solid #1a2540;
      border-top: none;
      border-radius: 0 0 8px 8px;
      padding: 0.8rem 1rem;
      min-height: 60px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.78rem;
      line-height: 1.8;
      color: #78909c;
      white-space: pre-wrap;
  }
  .output-box .log-line { display: block; }
  .output-box .log-node1 { color: #81c784; }
  .output-box .log-node2 { color: #64b5f6; }
  .output-box .log-server { color: #ffd54f; }
  .output-box .log-result { color: #ce93d8; font-weight: 600; }
  .output-box .log-error  { color: #ef9a9a; }
  .output-line-num { color: #263238; margin-right: 10px; user-select: none; }

  /* ── Sidebar ── */
  [data-testid="stSidebar"] {
      background: #0d1117 !important;
      border-right: 1px solid #1a2335;
  }
  .sidebar-section {
      background: #111520;
      border: 1px solid #1a2540;
      border-radius: 8px;
      padding: 0.9rem 1rem;
      margin-bottom: 0.9rem;
      font-size: 0.82rem;
  }
  .sidebar-section h4 { color: #7ab3e8; margin: 0 0 0.6rem; font-size: 0.85rem; font-weight: 600; letter-spacing: 0.04em; }
  .node-badge {
      display: flex; align-items: center; justify-content: space-between;
      padding: 5px 0; border-bottom: 1px solid #1a2335; margin-bottom: 4px;
      font-size: 0.78rem;
  }
  .badge-on  { color: #4caf82; font-weight: 600; }
  .badge-off { color: #455a64; }

  /* ── Divider ── */
  .nb-divider { border: none; border-top: 1px dashed #1a2540; margin: 1.2rem 0; }

  /* ── Code block override ── */
  .stCode { border-radius: 0 !important; border: none !important; }
  pre { background: #0d1320 !important; }
</style>
""", unsafe_allow_html=True)

# ── Start background WebSocket server thread (once per session) ───────────────
if "ws_thread_started" not in st.session_state:
    t = threading.Thread(target=_start_background_loop, daemon=True)
    t.start()
    time.sleep(0.6)  # allow loop to start before we try to use it
    st.session_state.ws_thread_started = True

# ── Session-state initialisation ─────────────────────────────────────────────
for key, default in [
    ("cell1_logs", []),
    ("cell2_logs", []),
    ("cell1_hidden_states", None),
    ("cell1_sentence", None),
    ("server_logs", []),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Helper: drain async queue into session state list ─────────────────────────

def _collect_server_logs():
    if _LOOP is None:
        return
    logs = _run_coro(_drain_queue(_log_queue))
    st.session_state.server_logs.extend(logs)


def _render_log_lines(lines: list[str], key_prefix: str) -> str:
    """Convert log lines to styled HTML spans."""
    html_lines = []
    for i, line in enumerate(lines):
        cls = "log-line"
        if "NODE 1" in line:  cls += " log-node1"
        elif "NODE 2" in line: cls += " log-node2"
        elif "✅" in line or "📨" in line or "🔌" in line: cls += " log-server"
        elif "RESULT" in line.upper() or "predicted" in line.lower(): cls += " log-result"
        elif "error" in line.lower() or "⚠" in line: cls += " log-error"
        num = f'<span class="log-line-num">[{i+1:02d}]</span> '
        html_lines.append(f'<span class="{cls}">{num}{line}</span>')
    return "\n".join(html_lines) if html_lines else '<span class="badge-off">No output yet — run the cell above.</span>'


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:0.6rem 0 1rem">
      <div style="font-size:1.3rem;font-weight:700;color:#4fc3f7;letter-spacing:-0.02em">SwarmTrain</div>
      <div style="font-size:0.7rem;color:#37474f;text-transform:uppercase;letter-spacing:0.08em">Pipeline Coordinator</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section"><h4>🖧 NODE REGISTRY</h4>', unsafe_allow_html=True)
    with _LOCK:
        n1_online = "1" in _CONNECTED_NODES
        n2_online = "2" in _CONNECTED_NODES

    n1_cls = "badge-on" if n1_online else "badge-off"
    n2_cls = "badge-on" if n2_online else "badge-off"
    n1_icon = "●" if n1_online else "○"
    n2_icon = "●" if n2_online else "○"

    st.markdown(f"""
    <div class="node-badge">
      <span>Node 1 — Attention</span>
      <span class="{n1_cls}">{n1_icon} {"ONLINE" if n1_online else "OFFLINE"}</span>
    </div>
    <div class="node-badge" style="border:none">
      <span>Node 2 — LM Head</span>
      <span class="{n2_cls}">{n2_icon} {"ONLINE" if n2_online else "OFFLINE"}</span>
    </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section"><h4>📡 TRANSPORT</h4>', unsafe_allow_html=True)
    st.markdown("""
    <div style="font-size:0.78rem;color:#546e7a;line-height:1.8">
      Protocol &nbsp; <b style="color:#80cbc4">WebSocket</b><br>
      Host &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <b style="color:#80cbc4">localhost:8765</b><br>
      Encoding &nbsp; <b style="color:#80cbc4">JSON / UTF-8</b><br>
      Mode &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; <b style="color:#80cbc4">Pipeline Parallel</b>
    </div>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🔄  Refresh Node Status", use_container_width=True):
        _collect_server_logs()
        st.rerun()

    st.markdown('<div class="sidebar-section"><h4>📋 SERVER LOG</h4>', unsafe_allow_html=True)
    _collect_server_logs()
    for l in st.session_state.server_logs[-8:]:
        colour = "#4caf82" if "✅" in l else ("#ef9a9a" if "⚠" in l else "#546e7a")
        st.markdown(f'<div style="font-size:0.72rem;font-family:\'JetBrains Mono\',monospace;color:{colour};line-height:1.7">{l}</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("🗑  Clear All Logs", use_container_width=True):
        st.session_state.cell1_logs = []
        st.session_state.cell2_logs = []
        st.session_state.server_logs = []
        st.session_state.cell1_hidden_states = None
        st.session_state.cell1_sentence = None
        st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN CONTENT
# ══════════════════════════════════════════════════════════════════════════════

# ── Top header ────────────────────────────────────────────────────────────────
st.markdown("""
<div class="swarm-header">
  <div class="swarm-logo">🧠</div>
  <div>
    <h1>SwarmTrain Platform</h1>
    <div class="subtitle">Decentralised Pipeline-Parallel Training · Notebook Interface · v0.1-alpha</div>
  </div>
  <div style="margin-left:auto">
    <div class="status-pill"><div class="status-dot"></div>COORDINATOR ONLINE</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Intro markdown cell ───────────────────────────────────────────────────────
st.markdown("""
<div class="md-cell">
  <h3>📓 SwarmTrain — Interactive Pipeline Demo</h3>
  This notebook coordinates a two-stage <code>Pipeline Parallelism</code> pass over
  the <em>TinyStories</em> corpus. Each cell dispatches work to a registered
  <strong>SwarmTrain Node</strong> over a raw WebSocket connection.
  <br><br>
  <span class="tag">CELL 1</span> Tokenise → Attention Layer &nbsp;|&nbsp;
  <span class="tag">CELL 2</span> Hidden States → LM Head → Predicted Token
  <br><br>
  Start both nodes first: &nbsp;<code>python node.py</code> &nbsp;(run twice, choose Node 1 then Node 2).
</div>
""", unsafe_allow_html=True)

st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  CELL 1 — ATTENTION LAYER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="md-cell">
  <h3>🔍 Stage 1 — Tokenisation &amp; Attention Layer</h3>
  Reads a random sentence from <code>tinystories.txt</code>, converts it to a
  simple integer token sequence, and ships the payload to <strong>Node 1</strong>.
  Node 1 simulates the <em>multi-head self-attention</em> computation and returns
  a mock <strong>hidden-state vector</strong> to this dashboard.
</div>
""", unsafe_allow_html=True)

# Code block
cell1_code = '''\
# ── Cell 1: Attention Layer (executed on Node 1) ──────────────────────────────
import random, hashlib

def tokenise(sentence: str) -> list[int]:
    """Minimal char-level tokeniser for demo purposes."""
    return [ord(c) % 512 for c in sentence.lower() if c.strip()]

def attention_forward(tokens: list[int]) -> list[float]:
    """
    Mock multi-head self-attention.
    In a real model this would be:  softmax(QK^T / sqrt(d_k)) · V
    Here we return a deterministic-ish 8-dim hidden state vector.
    """
    seed = sum(tokens) % (2**32)
    rng  = random.Random(seed)
    return [round(rng.gauss(0, 1), 4) for _ in range(8)]

# ── Dispatcher (runs server-side, payload sent via WebSocket) ─────────────────
sentence = random.choice(TINYSTORIES)
tokens   = tokenise(sentence)
payload  = {"type": "cell1_payload", "sentence": sentence, "tokens": tokens}
# → dispatched to ws://localhost:8765  →  Node 1 processes  →  returns hidden states
'''

st.markdown('<div class="nb-cell"><div class="nb-cell-header"><span class="cell-index">In [1]</span> attention_layer.py — Node 1 Dispatch</div>', unsafe_allow_html=True)
st.code(cell1_code, language="python")
st.markdown('</div>', unsafe_allow_html=True)

# Run button + output
col_btn1, col_status1 = st.columns([1, 4])
with col_btn1:
    run_cell1 = st.button("▶  Run Cell 1", key="run_c1", use_container_width=True, type="primary")
with col_status1:
    if not n1_online:
        st.warning("⚠️  Node 1 is offline — start `python node.py` and register as Node 1")

cell1_out = st.empty()

if run_cell1:
    if not n1_online:
        st.session_state.cell1_logs.append("❌  ERROR — Node 1 not connected. Launch node.py first.")
    else:
        sentence = random.choice(TINYSTORIES)
        tokens   = [ord(c) % 512 for c in sentence.lower() if c.strip()]
        st.session_state.cell1_sentence = sentence
        st.session_state.cell1_logs.append(f"[SERVER] Dispatching Cell 1 payload...")
        st.session_state.cell1_logs.append(f"[SERVER] Sentence  : \"{sentence}\"")
        st.session_state.cell1_logs.append(f"[SERVER] Token seq : {tokens[:12]}{'…' if len(tokens)>12 else ''}")

        err = _run_coro(_send_to_node("1", {
            "type": "cell1_payload",
            "sentence": sentence,
            "tokens": tokens,
        }))
        if err:
            st.session_state.cell1_logs.append(f"❌  ERROR — {err}")
        else:
            # Wait for result (up to 10 s)
            try:
                result = asyncio.run_coroutine_threadsafe(
                    asyncio.wait_for(_cell1_result_queue.get(), timeout=10.0),
                    _LOOP
                ).result(timeout=12)
                hs = result.get("hidden_states", [])
                log = result.get("log", "")
                st.session_state.cell1_hidden_states = hs
                st.session_state.cell1_logs.append(f"[NODE 1] {log}")
                st.session_state.cell1_logs.append(f"[NODE 1] Hidden states (8-dim): {hs}")
                st.session_state.cell1_logs.append(f"✅  Cell 1 complete — hidden states cached for Cell 2")
            except Exception as exc:
                st.session_state.cell1_logs.append(f"❌  Timeout / error waiting for Node 1: {exc}")

# Render output box
cell1_out.markdown(
    f'<div class="output-box">{_render_log_lines(st.session_state.cell1_logs, "c1")}</div>',
    unsafe_allow_html=True
)

st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  CELL 2 — LANGUAGE MODEL HEAD
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="md-cell">
  <h3>🎯 Stage 2 — Language Model Head &amp; Token Prediction</h3>
  Takes the <strong>hidden states</strong> returned by Node 1 and forwards them to
  <strong>Node 2</strong>. Node 2 simulates a linear projection over the vocabulary
  (logits) followed by an <em>argmax</em> to produce the most likely <strong>next
  token</strong> — completing the forward pass.
</div>
""", unsafe_allow_html=True)

cell2_code = '''\
# ── Cell 2: Language Model Head (executed on Node 2) ─────────────────────────
import math

VOCAB = ["the","a","and","to","of","in","was","she","he",
         "little","big","old","new","happy","sad","ran",
         "walked","found","loved","said","time","day","night"]

def lm_head_forward(hidden_states: list[float]) -> dict:
    """
    Mock LM-head: linear(hidden) → logits → softmax → argmax.
    Real version:  W_vocab @ h  where W_vocab is (vocab_size × d_model).
    """
    # Project 8-dim hidden state onto each vocab token (dot with hash-seeded weights)
    logits = []
    for i, word in enumerate(VOCAB):
        weight = sum(h * math.sin(i * 0.7 + j) for j, h in enumerate(hidden_states))
        logits.append((weight, word))

    # Softmax denominator
    max_l   = max(l for l, _ in logits)
    exp_sum = sum(math.exp(l - max_l) for l, _ in logits)
    probs   = [(math.exp(l - max_l) / exp_sum, w) for l, w in logits]

    predicted = max(probs, key=lambda x: x[0])
    return {"predicted_word": predicted[1], "confidence": round(predicted[0], 4)}

# ── Dispatcher (receives hidden_states from Cell 1 result) ────────────────────
payload = {"type": "cell2_payload", "hidden_states": hidden_states_from_cell1}
# → dispatched to ws://localhost:8765  →  Node 2 processes  →  returns predicted word
'''

st.markdown('<div class="nb-cell"><div class="nb-cell-header"><span class="cell-index">In [2]</span> lm_head.py — Node 2 Dispatch</div>', unsafe_allow_html=True)
st.code(cell2_code, language="python")
st.markdown('</div>', unsafe_allow_html=True)

col_btn2, col_status2 = st.columns([1, 4])
with col_btn2:
    run_cell2 = st.button("▶  Run Cell 2", key="run_c2", use_container_width=True, type="primary")
with col_status2:
    if not n2_online:
        st.warning("⚠️  Node 2 is offline — start `python node.py` and register as Node 2")
    elif st.session_state.cell1_hidden_states is None:
        st.info("ℹ️  Run Cell 1 first to generate hidden states.")

cell2_out = st.empty()

if run_cell2:
    if not n2_online:
        st.session_state.cell2_logs.append("❌  ERROR — Node 2 not connected.")
    elif st.session_state.cell1_hidden_states is None:
        st.session_state.cell2_logs.append("❌  ERROR — No hidden states available. Run Cell 1 first.")
    else:
        hs = st.session_state.cell1_hidden_states
        st.session_state.cell2_logs.append(f"[SERVER] Forwarding hidden states to Node 2...")
        st.session_state.cell2_logs.append(f"[SERVER] Hidden states (8-dim): {hs}")

        err = _run_coro(_send_to_node("2", {
            "type": "cell2_payload",
            "hidden_states": hs,
            "source_sentence": st.session_state.cell1_sentence or "",
        }))
        if err:
            st.session_state.cell2_logs.append(f"❌  ERROR — {err}")
        else:
            try:
                result = asyncio.run_coroutine_threadsafe(
                    asyncio.wait_for(_cell2_result_queue.get(), timeout=10.0),
                    _LOOP
                ).result(timeout=12)
                word       = result.get("predicted_word", "?")
                confidence = result.get("confidence", 0.0)
                log        = result.get("log", "")
                st.session_state.cell2_logs.append(f"[NODE 2] {log}")
                st.session_state.cell2_logs.append(
                    f"[NODE 2] RESULT — Predicted next token: \"{word}\"  (confidence: {confidence:.4f})"
                )
                st.session_state.cell2_logs.append(
                    f"✅  Full forward pass complete: \"{st.session_state.cell1_sentence}\" → \"{word}\""
                )
            except Exception as exc:
                st.session_state.cell2_logs.append(f"❌  Timeout / error waiting for Node 2: {exc}")

cell2_out.markdown(
    f'<div class="output-box">{_render_log_lines(st.session_state.cell2_logs, "c2")}</div>',
    unsafe_allow_html=True
)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)
st.markdown("""
<div style="text-align:center;font-size:0.72rem;color:#263238;padding-bottom:1rem;font-family:'JetBrains Mono',monospace">
  SwarmTrain Platform · Pipeline Parallelism Demo · WebSocket Transport · TinyStories Corpus
</div>
""", unsafe_allow_html=True)
