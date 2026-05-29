"""
SwarmTrain Dashboard Engine - server.py
=======================================
A Streamlit-based UI that mimics a notebook workspace and coordinates a
two-node pipeline-parallel training demo over WebSockets.

Run with:
    streamlit run server.py
"""

import asyncio
import base64
import json
import os
import queue
import random
import threading
import time
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
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
POLICY_PLANNER_PATH = APP_DIR / "client_policy_planner.html"

NODE_LOCATION_METADATA = {
    "1": {
        "name": "Node 1",
        "coords": [51.5072, -0.1276],
        "role": "Attention layer candidate",
        "region": "London edge",
    },
    "2": {
        "name": "Node 2",
        "coords": [50.1109, 8.6821],
        "role": "LM head candidate",
        "region": "Frankfurt core",
    },
    "3": {
        "name": "Node 3",
        "coords": [64.1466, -21.9426],
        "role": "Reserve sustainable pool",
        "region": "Reykjavik hydro",
    },
    "4": {
        "name": "Node 4",
        "coords": [39.0438, -77.4874],
        "role": "Burst compute pool",
        "region": "Virginia cloud",
    },
}

PLANNER_FALLBACK_NODES = [
    {
        "id": "fallback-london",
        "nodeId": "LDN",
        "name": "London Edge",
        "coords": [51.5072, -0.1276],
        "role": "Latency-optimized inference hub",
        "region": "United Kingdom",
        "performance": 78,
        "efficiency": 66,
        "carbon": 33,
    },
    {
        "id": "fallback-frankfurt",
        "nodeId": "FRA",
        "name": "Frankfurt Core",
        "coords": [50.1109, 8.6821],
        "role": "Balanced training pool",
        "region": "Germany",
        "performance": 82,
        "efficiency": 71,
        "carbon": 29,
    },
    {
        "id": "fallback-reykjavik",
        "nodeId": "REK",
        "name": "Reykjavik Hydro",
        "coords": [64.1466, -21.9426],
        "role": "Low-carbon reserve cluster",
        "region": "Iceland",
        "performance": 64,
        "efficiency": 94,
        "carbon": 8,
    },
    {
        "id": "fallback-virginia",
        "nodeId": "IAD",
        "name": "Virginia Cloud",
        "coords": [39.0438, -77.4874],
        "role": "Burst compute pool",
        "region": "United States East",
        "performance": 88,
        "efficiency": 62,
        "carbon": 47,
    },
    {
        "id": "fallback-quebec",
        "nodeId": "YUL",
        "name": "Quebec Hydro",
        "coords": [45.5017, -73.5673],
        "role": "Hydro-backed batch cluster",
        "region": "Canada",
        "performance": 73,
        "efficiency": 89,
        "carbon": 14,
    },
    {
        "id": "fallback-saopaulo",
        "nodeId": "GRU",
        "name": "Sao Paulo Grid",
        "coords": [-23.5505, -46.6333],
        "role": "South America routing edge",
        "region": "Brazil",
        "performance": 69,
        "efficiency": 74,
        "carbon": 26,
    },
    {
        "id": "fallback-nairobi",
        "nodeId": "NBO",
        "name": "Nairobi Solar Edge",
        "coords": [-1.2921, 36.8219],
        "role": "East Africa clean edge",
        "region": "Kenya",
        "performance": 61,
        "efficiency": 83,
        "carbon": 18,
    },
    {
        "id": "fallback-mumbai",
        "nodeId": "BOM",
        "name": "Mumbai Compute Port",
        "coords": [19.076, 72.8777],
        "role": "High-demand regional pool",
        "region": "India",
        "performance": 84,
        "efficiency": 54,
        "carbon": 58,
    },
    {
        "id": "fallback-singapore",
        "nodeId": "SIN",
        "name": "Singapore Exchange",
        "coords": [1.3521, 103.8198],
        "role": "Global traffic exchange node",
        "region": "Singapore",
        "performance": 80,
        "efficiency": 68,
        "carbon": 41,
    },
    {
        "id": "fallback-sydney",
        "nodeId": "SYD",
        "name": "Sydney Coastal Node",
        "coords": [-33.8688, 151.2093],
        "role": "Oceania distributed trainer",
        "region": "Australia",
        "performance": 72,
        "efficiency": 76,
        "carbon": 24,
    },
]

if not STORIES_PATH.exists():
    STORIES_PATH.write_text("\n".join(TINYSTORIES), encoding="utf-8")


class SwarmCoordinator:
    """Persisted WebSocket coordinator that survives Streamlit reruns."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8765):
        self.host = host
        self.port = port
        self.server_uri = f"ws://{host}:{port}"
        self.connected_nodes: dict[str, dict] = {}
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
                    profile = msg.get("profile", {})
                    if node_id not in {"1", "2", "3", "4"}:
                        await websocket.send(
                            json.dumps({"type": "error", "message": f"Invalid node id: {node_id}"})
                        )
                        continue

                    with self.lock:
                        self.connected_nodes[node_id] = {
                            "websocket": websocket,
                            "profile": {
                                "compute_capacity": int(profile.get("compute_capacity", 50)),
                                "clean_energy_level": int(profile.get("clean_energy_level", 50)),
                                "carbon_footprint": int(profile.get("carbon_footprint", 50)),
                            },
                        }
                    self.log_queue.put(
                        f"Node {node_id} connected [{websocket.remote_address}] "
                        f"cap={profile.get('compute_capacity', 50)} "
                        f"clean={profile.get('clean_energy_level', 50)} "
                        f"carbon={profile.get('carbon_footprint', 50)}"
                    )
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

    def get_connected_nodes_snapshot(self) -> dict[str, dict]:
        with self.lock:
            return {
                node_id: {
                    "profile": node_data["profile"].copy(),
                }
                for node_id, node_data in self.connected_nodes.items()
            }

    def send_to_node(self, node_id: str, payload: dict) -> str | None:
        with self.lock:
            node = self.connected_nodes.get(node_id)

        if node is None:
            return f"Node {node_id} is not connected"
        ws = node["websocket"]

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


def get_scheduler_summary(compute_priority: int, efficiency_priority: int, carbon_priority: int) -> tuple[str, str]:
    if compute_priority >= max(efficiency_priority, carbon_priority):
        return (
            "Performance-first routing",
            "Jobs prefer the fastest available node pool. Energy and carbon checks are minimized.",
        )
    if carbon_priority >= compute_priority and carbon_priority >= efficiency_priority:
        return (
            "Low-carbon routing",
            "Jobs prefer regions with cleaner grid mix and better renewable availability.",
        )
    return (
        "Balanced efficiency routing",
        "Jobs aim for good throughput while favoring efficient nodes and cleaner regions.",
    )


def rank_connected_nodes(node_snapshot: dict[str, dict], compute_priority: int, efficiency_priority: int, carbon_priority: int) -> list[dict]:
    ranked = []
    for node_id, node_data in node_snapshot.items():
        profile = node_data["profile"]
        carbon_preference = 100 - profile["carbon_footprint"]
        score = (
            profile["compute_capacity"] * (compute_priority / 100)
            + profile["clean_energy_level"] * (efficiency_priority / 100)
            + carbon_preference * (carbon_priority / 100)
        )
        ranked.append(
            {
                "node_id": node_id,
                "name": f"Node {node_id}",
                "role": "Generic compute worker",
                "region": "Live demo node",
                "compute_capacity": profile["compute_capacity"],
                "clean_energy_level": profile["clean_energy_level"],
                "carbon_footprint": profile["carbon_footprint"],
                "score": round(score, 1),
            }
        )
    return sorted(ranked, key=lambda item: item["score"], reverse=True)


def build_planner_nodes(node_snapshot: dict[str, dict]) -> list[dict]:
    planner_nodes = [node.copy() for node in PLANNER_FALLBACK_NODES]
    fallback_index = {node["nodeId"]: idx for idx, node in enumerate(planner_nodes)}
    for node_id, node_data in sorted(node_snapshot.items(), key=lambda item: item[0]):
        profile = node_data["profile"]
        meta = NODE_LOCATION_METADATA.get(
            node_id,
            {
                "name": f"Node {node_id}",
                "coords": [20, 0],
                "role": "Generic compute worker",
                "region": "Live demo node",
            },
        )
        live_node = {
            "id": f"node-{node_id}",
            "nodeId": node_id,
            "name": meta["name"],
            "coords": meta["coords"],
            "role": meta["role"],
            "region": meta["region"],
            "performance": profile["compute_capacity"],
            "efficiency": profile["clean_energy_level"],
            "carbon": profile["carbon_footprint"],
        }
        if node_id in fallback_index:
            planner_nodes[fallback_index[node_id]] = live_node
        else:
            planner_nodes.append(live_node)
    return planner_nodes


def build_policy_planner_html(
    compute_priority: int,
    efficiency_priority: int,
    carbon_priority: int,
    node_snapshot: dict[str, dict],
    embed_mode: bool,
) -> str:
    html = POLICY_PLANNER_PATH.read_text(encoding="utf-8")
    html = html.replace("__INITIAL_COMPUTE__", str(compute_priority))
    html = html.replace("__INITIAL_EFFICIENCY__", str(efficiency_priority))
    html = html.replace("__INITIAL_CARBON__", str(carbon_priority))
    html = html.replace("__NODE_DATA__", json.dumps(build_planner_nodes(node_snapshot)))
    html = html.replace("__EMBED_MODE__", "true" if embed_mode else "false")
    return html


def get_policy_planner_url(
    compute_priority: int,
    efficiency_priority: int,
    carbon_priority: int,
    node_snapshot: dict[str, dict],
) -> str:
    html = build_policy_planner_html(
        compute_priority=compute_priority,
        efficiency_priority=efficiency_priority,
        carbon_priority=carbon_priority,
        node_snapshot=node_snapshot,
        embed_mode=False,
    )
    encoded = base64.b64encode(html.encode("utf-8")).decode("ascii")
    return f"data:text/html;base64,{encoded}"


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
    ("compute_priority", 80),
    ("efficiency_priority", 55),
    ("carbon_priority", 45),
]:
    if key not in st.session_state:
        st.session_state[key] = default

collect_server_logs(coordinator)

connected_nodes_snapshot = coordinator.get_connected_nodes_snapshot()

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
    for node_id in ["1", "2", "3", "4"]:
        node_data = connected_nodes_snapshot.get(node_id)
        online = node_data is not None
        badge_cls = "badge-on" if online else "badge-off"
        icon = "[*]" if online else "[*]‹"
        profile_line = ""
        if online:
            profile = node_data["profile"]
            profile_line = (
                f'<div class="node-profile-line">cap {profile["compute_capacity"]} - '
                f'clean {profile["clean_energy_level"]} - carbon {profile["carbon_footprint"]}</div>'
            )
        st.markdown(
            f"""
            <div class="node-badge-card">
              <div class="node-badge">
                <span>Node {node_id}</span>
                <span class="{badge_cls}">{icon} {"ONLINE" if online else "OFFLINE"}</span>
              </div>
              {profile_line}
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)

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
        <div class="subtitle">Decentralised Pipeline-Parallel Training - Notebook Workspace - v0.1-alpha</div>
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

policy_col, map_col = st.columns([0.92, 1.3], gap="large")

with policy_col:
    st.markdown(
        """
        <div class="scheduler-panel">
          <div class="panel-eyebrow">Client policy</div>
          <h3>Workload routing controls</h3>
          <p>
            Adjust the policy below to decide whether this run should prioritize
            compute throughput, energy efficiency, or lower carbon routing.
            Setting efficiency or carbon to <code>0</code> makes them optional
            for the demo scheduler.
          </p>
        """,
        unsafe_allow_html=True,
    )

    compute_priority = st.slider(
        "Compute priority",
        min_value=0,
        max_value=100,
        value=st.session_state.compute_priority,
        key="compute_priority",
        help="Higher values push the demo toward the fastest available compute.",
    )
    efficiency_priority = st.slider(
        "Clean energy priority",
        min_value=0,
        max_value=100,
        value=st.session_state.efficiency_priority,
        key="efficiency_priority",
        help="Higher values prefer nodes that deliver more work per watt.",
    )
    carbon_priority = st.slider(
        "Carbon footprint priority",
        min_value=0,
        max_value=100,
        value=st.session_state.carbon_priority,
        key="carbon_priority",
        help="Higher values prefer cleaner-energy regions for dispatch.",
    )

    summary_title, summary_text = get_scheduler_summary(
        compute_priority,
        efficiency_priority,
        carbon_priority,
    )
    ranked_nodes = rank_connected_nodes(
        connected_nodes_snapshot,
        compute_priority,
        efficiency_priority,
        carbon_priority,
    )
    top_node = ranked_nodes[0] if ranked_nodes else None

    if top_node:
        st.markdown(
            f"""
                <div class="policy-summary">
                  <div class="policy-summary-title">{summary_title}</div>
                  <div class="policy-summary-text">{summary_text}</div>
                  <div class="policy-metric-grid">
                    <div class="policy-metric-card">
                      <div class="policy-metric-label">Stage 1 candidate</div>
                      <div class="policy-metric-value">Node {ranked_nodes[0]["node_id"]}</div>
                    </div>
                    <div class="policy-metric-card">
                      <div class="policy-metric-label">Stage 2 candidate</div>
                      <div class="policy-metric-value">Node {ranked_nodes[1]["node_id"] if len(ranked_nodes) > 1 else "Waiting for more nodes"}</div>
                    </div>
                    <div class="policy-metric-card">
                      <div class="policy-metric-label">Top policy score</div>
                      <div class="policy-metric-value">{top_node["score"]}</div>
                    </div>
                  </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
                <div class="policy-summary">
                  <div class="policy-summary-title">Waiting for nodes</div>
                  <div class="policy-summary-text">
                    Start at least two nodes and enter their compute, clean-energy,
                    and carbon settings to enable automatic routing.
                  </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    planner_url = get_policy_planner_url(
        compute_priority,
        efficiency_priority,
        carbon_priority,
        connected_nodes_snapshot,
    )
    st.markdown(
        f"""
        <a class="planner-link" href="{planner_url}" target="_blank" rel="noopener noreferrer">
          Open planner in a new tab
        </a>
        <div class="planner-link-note">
          The map on the right is the same real planner, embedded directly in this
          dashboard and seeded from these policy values.
        </div>
        """,
        unsafe_allow_html=True,
    )

with map_col:
    st.markdown(
        """
        <div class="map-panel">
          <div class="panel-eyebrow">Grid awareness</div>
          <h3>Interactive clean-energy map</h3>
          <p>
            The selected policy values drive the highlighted compute node,
            region weighting, and sustainability recommendation.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    components.html(
        build_policy_planner_html(
            compute_priority=compute_priority,
            efficiency_priority=efficiency_priority,
            carbon_priority=carbon_priority,
            node_snapshot=connected_nodes_snapshot,
            embed_mode=True,
        ),
        height=860,
        scrolling=False,
    )

st.markdown(
    """
    <div class="md-cell">
      <h3>SwarmTrain - Interactive Pipeline Demo</h3>
      This notebook coordinates a two-stage <code>Pipeline Parallelism</code> pass over
      the <em>TinyStories</em> corpus. The scheduler ranks all connected nodes using
      the selected policy and automatically chooses the best two nodes for Stage 1
      and Stage 2.
      <br><br>
      <span class="tag">STAGE 1</span> Tokenise -> Attention Layer |
      <span class="tag">STAGE 2</span> Hidden States -> LM Head -> Predicted Token
      <br><br>
      Start any two or more nodes first: <code>python node.py</code>, then enter each node's
      profile at startup.
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)

st.markdown(
    """
    <div class="md-cell">
      <h3>Auto-Routed Distributed Pipeline</h3>
      A single run now selects the best two connected nodes based on your
      policy preferences and each node's registered <code>compute capacity</code>,
      <code>clean energy level</code>, and <code>carbon footprint</code>.
    </div>
    """,
    unsafe_allow_html=True,
)

cell1_code = """\
# Auto pipeline selection
ranked_nodes = rank_connected_nodes(connected_nodes, policy)
stage1_node, stage2_node = ranked_nodes[:2]

sentence = random.choice(TINYSTORIES)
tokens = tokenise(sentence)

send(stage1_node, {"type": "cell1_payload", "sentence": sentence, "tokens": tokens})
hidden_states = wait_for_stage1_result()

send(stage2_node, {"type": "cell2_payload", "hidden_states": hidden_states})
prediction = wait_for_stage2_result()
"""

st.markdown(
    '<div class="nb-cell"><div class="nb-cell-header"><span class="cell-index">Run</span> pipeline_router.py - Dynamic Node Selection</div>',
    unsafe_allow_html=True,
)
st.code(cell1_code, language="python")
st.markdown("</div>", unsafe_allow_html=True)

run_pipeline = st.button("Run Distributed Pipeline", use_container_width=True, type="primary")

if len(ranked_nodes) < 2:
    st.warning("Start at least two connected nodes to run the pipeline.")

cell1_out = st.empty()
cell2_out = st.empty()

if run_pipeline:
    st.session_state.cell1_logs = []
    st.session_state.cell2_logs = []

    if len(ranked_nodes) < 2:
        st.session_state.cell1_logs.append("ERROR - Need at least two connected nodes.")
    else:
        stage1_node = ranked_nodes[0]
        stage2_node = ranked_nodes[1]
        sentence = random.choice(TINYSTORIES)
        tokens = [ord(c) % 512 for c in sentence.lower() if c.strip()]
        policy = {
            "compute_priority": compute_priority,
            "efficiency_priority": efficiency_priority,
            "carbon_priority": carbon_priority,
        }
        st.session_state.cell1_sentence = sentence

        st.session_state.cell1_logs.append(
            f'[SCHEDULER] Stage 1 -> Node {stage1_node["node_id"]} | '
            f'cap={stage1_node["compute_capacity"]} clean={stage1_node["clean_energy_level"]} '
            f'carbon={stage1_node["carbon_footprint"]} score={stage1_node["score"]}'
        )
        st.session_state.cell1_logs.append(
            f'[SCHEDULER] Stage 2 -> Node {stage2_node["node_id"]} | '
            f'cap={stage2_node["compute_capacity"]} clean={stage2_node["clean_energy_level"]} '
            f'carbon={stage2_node["carbon_footprint"]} score={stage2_node["score"]}'
        )
        st.session_state.cell1_logs.append(f'[SERVER] Sentence  : "{sentence}"')
        st.session_state.cell1_logs.append(
            f"[SERVER] Token seq : {tokens[:12]}{'...' if len(tokens) > 12 else ''}"
        )

        err = coordinator.send_to_node(
            stage1_node["node_id"],
            {
                "type": "cell1_payload",
                "sentence": sentence,
                "tokens": tokens,
                "policy": policy,
            },
        )
        if err:
            st.session_state.cell1_logs.append(f"ERROR - {err}")
        else:
            try:
                result = coordinator.wait_for_cell1_result(timeout=10.0)
                hs = result.get("hidden_states", [])
                stage1_result_node = result.get("node_id", stage1_node["node_id"])
                st.session_state.cell1_hidden_states = hs
                st.session_state.cell1_logs.append(f'[NODE {stage1_result_node}] {result.get("log", "")}')
                st.session_state.cell1_logs.append(f"[NODE {stage1_result_node}] Hidden states (8-dim): {hs}")
                st.session_state.cell1_logs.append("Stage 1 complete - forwarding to Stage 2.")

                st.session_state.cell2_logs.append(
                    f'[SCHEDULER] Forwarding Stage 1 output from Node {stage1_result_node} '
                    f'to Node {stage2_node["node_id"]}.'
                )
                st.session_state.cell2_logs.append(f"[SERVER] Hidden states (8-dim): {hs}")

                err = coordinator.send_to_node(
                    stage2_node["node_id"],
                    {
                        "type": "cell2_payload",
                        "hidden_states": hs,
                        "source_sentence": sentence,
                        "policy": policy,
                    },
                )
                if err:
                    st.session_state.cell2_logs.append(f"ERROR - {err}")
                else:
                    result2 = coordinator.wait_for_cell2_result(timeout=10.0)
                    stage2_result_node = result2.get("node_id", stage2_node["node_id"])
                    word = result2.get("predicted_word", "?")
                    confidence = result2.get("confidence", 0.0)
                    st.session_state.cell2_logs.append(f'[NODE {stage2_result_node}] {result2.get("log", "")}')
                    st.session_state.cell2_logs.append(
                        f'[NODE {stage2_result_node}] RESULT - Predicted next token: "{word}" '
                        f"(confidence: {confidence:.4f})"
                    )
                    st.session_state.cell2_logs.append(f"[POLICY] {summary_title}")
                    st.session_state.cell2_logs.append(
                        f'Full forward pass complete: "{sentence}" -> "{word}"'
                    )
            except queue.Empty:
                st.session_state.cell1_logs.append("Timeout waiting for Stage 1 node.")
            collect_server_logs(coordinator)

cell1_out.markdown(
    f'<div class="output-box">{render_log_lines(st.session_state.cell1_logs)}</div>',
    unsafe_allow_html=True,
)

st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)

cell2_out.markdown(
    f'<div class="output-box">{render_log_lines(st.session_state.cell2_logs)}</div>',
    unsafe_allow_html=True,
)

st.markdown('<hr class="nb-divider">', unsafe_allow_html=True)
st.markdown(
    """
    <div class="footer-note">
      SwarmTrain Platform - Pipeline Parallelism Demo - WebSocket Transport - TinyStories Corpus
    </div>
    """,
    unsafe_allow_html=True,
)

