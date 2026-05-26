"""
SwarmTrain Dashboard Engine - server.py
=======================================
A Streamlit-based UI that mimics a notebook workspace and coordinates a
two-node pipeline-parallel training demo over WebSockets.

Run with:
    streamlit run server.py
"""

import asyncio
import json
import os
import queue
import random
import threading
import time
from pathlib import Path

import streamlit as st
import websockets
from websockets.server import serve

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

APP_DIR = Path(__file__).resolve().parent
STORIES_PATH = APP_DIR / "tinystories.txt"
CSS_PATH = APP_DIR / "styles.css"

if not STORIES_PATH.exists():
    STORIES_PATH.write_text("\n".join(TINYSTORIES), encoding="utf-8")


class SwarmCoordinator:
    """Persisted WebSocket coordinator that survives Streamlit reruns."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8765):
        self.host = host
        self.port = port
        self.server_uri = f"ws://{host}:{port}"
        self.connected_nodes: dict[str, "websockets.WebSocketServerProtocol"] = {}
        self.lock = threading.Lock()
        self.cell1_result_queue: queue.Queue = queue.Queue()
        self.cell2_result_queue: queue.Queue = queue.Queue()
        self.log_queue: queue.Queue = queue.Queue()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.started = threading.Event()
        self.start_error: Exception | None = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return

        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

        if not self.started.wait(timeout=3):
            raise RuntimeError("WebSocket server did not start in time.")
        if self.start_error:
            raise RuntimeError(f"WebSocket server failed to start: {self.start_error}") from self.start_error

    def _run_loop(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run_ws_server())
        except Exception as exc:
            self.start_error = exc
            self.started.set()
            raise

    async def _run_ws_server(self) -> None:
        async with serve(self._ws_handler, self.host, self.port):
            self.log_queue.put(f"Server listening on {self.server_uri}")
            self.started.set()
            await asyncio.get_running_loop().create_future()

    async def _ws_handler(self, websocket) -> None:
        node_id = None
        try:
            async for raw in websocket:
                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == "register":
                    node_id = str(msg.get("node", ""))
                    if node_id not in {"1", "2"}:
                        await websocket.send(
                            json.dumps({"type": "error", "message": f"Invalid node id: {node_id}"})
                        )
                        continue

                    with self.lock:
                        self.connected_nodes[node_id] = websocket
                    self.log_queue.put(f"Node {node_id} connected [{websocket.remote_address}]")
                    await websocket.send(json.dumps({"type": "ack", "message": f"Node {node_id} registered"}))

                elif msg_type == "cell1_result":
                    self.log_queue.put("Cell 1 result received from Node 1")
                    self.cell1_result_queue.put(msg)

                elif msg_type == "cell2_result":
                    self.log_queue.put("Cell 2 result received from Node 2")
                    self.cell2_result_queue.put(msg)

        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as exc:
            self.log_queue.put(f"Connection error: {exc}")
        finally:
            if node_id:
                with self.lock:
                    self.connected_nodes.pop(node_id, None)
                self.log_queue.put(f"Node {node_id} disconnected")

    def is_node_online(self, node_id: str) -> bool:
        with self.lock:
            return node_id in self.connected_nodes

    def send_to_node(self, node_id: str, payload: dict) -> str | None:
        with self.lock:
            ws = self.connected_nodes.get(node_id)

        if ws is None:
            return f"Node {node_id} is not connected"

        future = asyncio.run_coroutine_threadsafe(ws.send(json.dumps(payload)), self.loop)
        try:
            future.result(timeout=10)
        except Exception as exc:
            return str(exc)
        return None

    def wait_for_cell1_result(self, timeout: float = 10.0) -> dict:
        return self.cell1_result_queue.get(timeout=timeout)

    def wait_for_cell2_result(self, timeout: float = 10.0) -> dict:
        return self.cell2_result_queue.get(timeout=timeout)

    def drain_logs(self) -> list[str]:
        logs = []
        while True:
            try:
                logs.append(self.log_queue.get_nowait())
            except queue.Empty:
                break
        return logs


@st.cache_resource
def get_coordinator() -> SwarmCoordinator:
    coordinator = SwarmCoordinator()
    coordinator.start()
    return coordinator


def load_styles() -> None:
    st.markdown(f"<style>{CSS_PATH.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def collect_server_logs(coordinator: SwarmCoordinator) -> None:
    st.session_state.server_logs.extend(coordinator.drain_logs())


def render_log_lines(lines: list[str]) -> str:
    html_lines = []
    for i, line in enumerate(lines):
        cls = "log-line"
        if "NODE 1" in line:
            cls += " log-node1"
        elif "NODE 2" in line:
            cls += " log-node2"
        elif "connected" in line.lower() or "disconnected" in line.lower() or "received" in line.lower():
            cls += " log-server"
        elif "RESULT" in line.upper() or "predicted" in line.lower():
            cls += " log-result"
        elif "error" in line.lower() or "timeout" in line.lower():
            cls += " log-error"
        num = f'<span class="log-line-num">[{i + 1:02d}]</span> '
        html_lines.append(f'<span class="{cls}">{num}{line}</span>')
    return "\n".join(html_lines) if html_lines else '<span class="badge-off">No output yet - run the cell above.</span>'


st.set_page_config(
    page_title="SwarmTrain Platform",
    page_icon="ST",
    layout="wide",
    initial_sidebar_state="expanded",
)

load_styles()
coordinator = get_coordinator()

for key, default in [
    ("cell1_logs", []),
    ("cell2_logs", []),
    ("cell1_hidden_states", None),
    ("cell1_sentence", None),
    ("server_logs", []),
]:
    if key not in st.session_state:
        st.session_state[key] = default

collect_server_logs(coordinator)

n1_online = coordinator.is_node_online("1")
n2_online = coordinator.is_node_online("2")
n1_cls = "badge-on" if n1_online else "badge-off"
n2_cls = "badge-on" if n2_online else "badge-off"
n1_icon = "●" if n1_online else "○"
n2_icon = "●" if n2_online else "○"

with st.sidebar:
    st.markdown(
        """
        <div class="sidebar-brand">
          <div class="sidebar-title">SwarmTrain</div>
          <div class="sidebar-subtitle">Pipeline Coordinator</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="sidebar-section"><h4>Node Registry</h4>', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="node-badge">
          <span>Node 1 - Attention</span>
          <span class="{n1_cls}">{n1_icon} {"ONLINE" if n1_online else "OFFLINE"}</span>
        </div>
        <div class="node-badge node-badge-last">
          <span>Node 2 - LM Head</span>
          <span class="{n2_cls}">{n2_icon} {"ONLINE" if n2_online else "OFFLINE"}</span>
        </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="sidebar-section"><h4>Transport</h4>', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="transport-details">
          Protocol <b>WebSocket</b><br>
          Host <b>{coordinator.host}:{coordinator.port}</b><br>
          Encoding <b>JSON / UTF-8</b><br>
          Mode <b>Pipeline Parallel</b>
        </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("Refresh Node Status", use_container_width=True):
        collect_server_logs(coordinator)
        st.rerun()

    st.markdown('<div class="sidebar-section"><h4>Server Log</h4>', unsafe_allow_html=True)
    for line in st.session_state.server_logs[-8:]:
        css_class = "server-log-entry"
        if "connected" in line.lower():
            css_class += " server-log-good"
        elif "error" in line.lower():
            css_class += " server-log-bad"
        st.markdown(f'<div class="{css_class}">{line}</div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("Clear All Logs", use_container_width=True):
        st.session_state.cell1_logs = []
        st.session_state.cell2_logs = []
        st.session_state.server_logs = []
        st.session_state.cell1_hidden_states = None
        st.session_state.cell1_sentence = None
        st.rerun()

st.markdown(
    """
    <div class="swarm-header">
      <div class="swarm-logo">ST</div>
      <div>
        <h1>SwarmTrain Platform</h1>
        <div class="subtitle">Decentralised Pipeline-Parallel Training · Notebook Workspace · v0.1-alpha</div>
      </div>
      <div class="header-status">
        <div class="status-pill"><div class="status-dot"></div>Coordinator online</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="notebook-toolbar">
      <div class="toolbar-group">
        <span class="toolbar-chip toolbar-chip-active">Python 3</span>
        <span class="toolbar-chip">Pipeline demo</span>
        <span class="toolbar-chip">WebSocket runtime</span>
      </div>
      <div class="toolbar-runtime">Connected to local runtime</div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="md-cell">
      <h3>SwarmTrain - Interactive Pipeline Demo</h3>
      This notebook coordinates a two-stage <code>Pipeline Parallelism</code> pass over
      the <em>TinyStories</em> corpus. Each cell dispatches work to a registered
      <strong>SwarmTrain Node</strong> over a raw WebSocket connection.
      <br><br>
      <span class="tag">CELL 1</span> Tokenise -> Attention Layer |
      <span class="tag">CELL 2</span> Hidden States -> LM Head -> Predicted Token
      <br><br>
      Start both nodes first: <code>python node.py</code> (run twice, choose Node 1 then Node 2).
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)

st.markdown(
    """
    <div class="md-cell">
      <h3>Stage 1 - Tokenisation &amp; Attention Layer</h3>
      Reads a random sentence from <code>tinystories.txt</code>, converts it to a
      simple integer token sequence, and ships the payload to <strong>Node 1</strong>.
      Node 1 simulates the <em>multi-head self-attention</em> computation and returns
      a mock <strong>hidden-state vector</strong> to this dashboard.
    </div>
    """,
    unsafe_allow_html=True,
)

cell1_code = """\
# Cell 1: Attention Layer (executed on Node 1)
import random, hashlib

def tokenise(sentence: str) -> list[int]:
    return [ord(c) % 512 for c in sentence.lower() if c.strip()]

def attention_forward(tokens: list[int]) -> list[float]:
    seed = sum(tokens) % (2**32)
    rng  = random.Random(seed)
    return [round(rng.gauss(0, 1), 4) for _ in range(8)]

sentence = random.choice(TINYSTORIES)
tokens   = tokenise(sentence)
payload  = {"type": "cell1_payload", "sentence": sentence, "tokens": tokens}
"""

st.markdown(
    '<div class="nb-cell"><div class="nb-cell-header"><span class="cell-index">In [1]</span> attention_layer.py - Node 1 Dispatch</div>',
    unsafe_allow_html=True,
)
st.code(cell1_code, language="python")
st.markdown("</div>", unsafe_allow_html=True)

col_btn1, col_status1 = st.columns([1, 4])
with col_btn1:
    run_cell1 = st.button("Run Cell 1", key="run_c1", use_container_width=True, type="primary")
with col_status1:
    if not n1_online:
        st.warning("Node 1 is offline - start `python node.py` and register as Node 1")

cell1_out = st.empty()

if run_cell1:
    if not n1_online:
        st.session_state.cell1_logs.append("ERROR - Node 1 not connected. Launch node.py first.")
    else:
        sentence = random.choice(TINYSTORIES)
        tokens = [ord(c) % 512 for c in sentence.lower() if c.strip()]
        st.session_state.cell1_sentence = sentence
        st.session_state.cell1_logs.append("[SERVER] Dispatching Cell 1 payload...")
        st.session_state.cell1_logs.append(f'[SERVER] Sentence  : "{sentence}"')
        st.session_state.cell1_logs.append(
            f"[SERVER] Token seq : {tokens[:12]}{'...' if len(tokens) > 12 else ''}"
        )

        err = coordinator.send_to_node(
            "1",
            {
                "type": "cell1_payload",
                "sentence": sentence,
                "tokens": tokens,
            },
        )
        if err:
            st.session_state.cell1_logs.append(f"ERROR - {err}")
        else:
            try:
                result = coordinator.wait_for_cell1_result(timeout=10.0)
                hs = result.get("hidden_states", [])
                log = result.get("log", "")
                st.session_state.cell1_hidden_states = hs
                st.session_state.cell1_logs.append(f"[NODE 1] {log}")
                st.session_state.cell1_logs.append(f"[NODE 1] Hidden states (8-dim): {hs}")
                st.session_state.cell1_logs.append("Cell 1 complete - hidden states cached for Cell 2")
            except queue.Empty:
                st.session_state.cell1_logs.append("Timeout waiting for Node 1.")
            collect_server_logs(coordinator)

cell1_out.markdown(
    f'<div class="output-box">{render_log_lines(st.session_state.cell1_logs)}</div>',
    unsafe_allow_html=True,
)

st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)

st.markdown(
    """
    <div class="md-cell">
      <h3>Stage 2 - Language Model Head &amp; Token Prediction</h3>
      Takes the <strong>hidden states</strong> returned by Node 1 and forwards them to
      <strong>Node 2</strong>. Node 2 simulates a linear projection over the vocabulary
      followed by an <em>argmax</em> to produce the most likely <strong>next token</strong>.
    </div>
    """,
    unsafe_allow_html=True,
)

cell2_code = """\
# Cell 2: Language Model Head (executed on Node 2)
import math

VOCAB = ["the","a","and","to","of","in","was","she","he",
         "little","big","old","new","happy","sad","ran",
         "walked","found","loved","said","time","day","night"]

def lm_head_forward(hidden_states: list[float]) -> dict:
    logits = []
    for i, word in enumerate(VOCAB):
        weight = sum(h * math.sin(i * 0.7 + j) for j, h in enumerate(hidden_states))
        logits.append((weight, word))

    max_l   = max(l for l, _ in logits)
    exp_sum = sum(math.exp(l - max_l) for l, _ in logits)
    probs   = [(math.exp(l - max_l) / exp_sum, w) for l, w in logits]

    predicted = max(probs, key=lambda x: x[0])
    return {"predicted_word": predicted[1], "confidence": round(predicted[0], 4)}

payload = {"type": "cell2_payload", "hidden_states": hidden_states_from_cell1}
"""

st.markdown(
    '<div class="nb-cell"><div class="nb-cell-header"><span class="cell-index">In [2]</span> lm_head.py - Node 2 Dispatch</div>',
    unsafe_allow_html=True,
)
st.code(cell2_code, language="python")
st.markdown("</div>", unsafe_allow_html=True)

col_btn2, col_status2 = st.columns([1, 4])
with col_btn2:
    run_cell2 = st.button("Run Cell 2", key="run_c2", use_container_width=True, type="primary")
with col_status2:
    if not n2_online:
        st.warning("Node 2 is offline - start `python node.py` and register as Node 2")
    elif st.session_state.cell1_hidden_states is None:
        st.info("Run Cell 1 first to generate hidden states.")

cell2_out = st.empty()

if run_cell2:
    if not n2_online:
        st.session_state.cell2_logs.append("ERROR - Node 2 not connected.")
    elif st.session_state.cell1_hidden_states is None:
        st.session_state.cell2_logs.append("ERROR - No hidden states available. Run Cell 1 first.")
    else:
        hs = st.session_state.cell1_hidden_states
        st.session_state.cell2_logs.append("[SERVER] Forwarding hidden states to Node 2...")
        st.session_state.cell2_logs.append(f"[SERVER] Hidden states (8-dim): {hs}")

        err = coordinator.send_to_node(
            "2",
            {
                "type": "cell2_payload",
                "hidden_states": hs,
                "source_sentence": st.session_state.cell1_sentence or "",
            },
        )
        if err:
            st.session_state.cell2_logs.append(f"ERROR - {err}")
        else:
            try:
                result = coordinator.wait_for_cell2_result(timeout=10.0)
                word = result.get("predicted_word", "?")
                confidence = result.get("confidence", 0.0)
                log = result.get("log", "")
                st.session_state.cell2_logs.append(f"[NODE 2] {log}")
                st.session_state.cell2_logs.append(
                    f'[NODE 2] RESULT - Predicted next token: "{word}" (confidence: {confidence:.4f})'
                )
                st.session_state.cell2_logs.append(
                    f'Full forward pass complete: "{st.session_state.cell1_sentence}" -> "{word}"'
                )
            except queue.Empty:
                st.session_state.cell2_logs.append("Timeout waiting for Node 2.")
            collect_server_logs(coordinator)

cell2_out.markdown(
    f'<div class="output-box">{render_log_lines(st.session_state.cell2_logs)}</div>',
    unsafe_allow_html=True,
)

st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)
st.markdown(
    """
    <div class="footer-note">
      SwarmTrain Platform · Pipeline Parallelism Demo · WebSocket Transport · TinyStories Corpus
    </div>
    """,
    unsafe_allow_html=True,
)
