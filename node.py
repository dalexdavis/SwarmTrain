"""
SwarmTrain Client Worker - node.py
==================================
Run this script up to four times in separate terminals.

Usage:
    python node.py

Each node registers:
  - Node ID (1-4)
  - A built-in demo profile for compute, clean energy, and carbon footprint

After registration, the node can execute either stage of the demo pipeline.
"""

import asyncio
import json
import math
import random
import sys
import time

import websockets

SERVER_URI = "ws://127.0.0.1:8765"

VOCAB = [
    "the", "a", "and", "to", "of", "in", "was", "she", "he",
    "little", "big", "old", "new", "happy", "sad", "ran",
    "walked", "found", "loved", "said", "time", "day", "night",
]

RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RED = "\033[91m"
DIM = "\033[2m"

NODE_PROFILES = {
    "1": {
        "compute_capacity": 78,
        "clean_energy_level": 42,
        "carbon_footprint": 58,
    },
    "2": {
        "compute_capacity": 92,
        "clean_energy_level": 64,
        "carbon_footprint": 44,
    },
    "3": {
        "compute_capacity": 68,
        "clean_energy_level": 93,
        "carbon_footprint": 12,
    },
    "4": {
        "compute_capacity": 84,
        "clean_energy_level": 57,
        "carbon_footprint": 39,
    },
}


def banner(node_id: str, profile: dict) -> None:
    colour = GREEN if node_id in {"1", "3"} else BLUE
    bar = "-" * 58
    print(f"\n{colour}{BOLD}")
    print(f"  +{bar}+")
    print(f"  |   SwarmTrain - Node {node_id:<2} - Generic Compute Worker          |")
    print(f"  |   Connected to {SERVER_URI:<40}|")
    print(
        "  |   Capacity {compute_capacity:>3} - Clean {clean_energy_level:>3} - Carbon {carbon_footprint:>3}        |".format(
            **profile
        )
    )
    print(f"  +{bar}+{RESET}\n")


def log(node_id: str, msg: str, level: str = "info") -> None:
    ts = time.strftime("%H:%M:%S")
    colour = GREEN if node_id in {"1", "3"} else BLUE
    icons = {"info": "[.]", "recv": "[v]", "send": "[^]", "ok": "[OK]", "err": "[X]"}
    icon = icons.get(level, "[.]")
    print(f"  {DIM}{ts}{RESET}  {colour}{BOLD}[SwarmTrain - NODE {node_id}]{RESET}  {icon}  {msg}")


def attention_forward(tokens: list[int]) -> list[float]:
    seed = (sum(tokens) * 31 + len(tokens) * 97) % (2**32)
    rng = random.Random(seed)
    hidden = []
    for head in range(8):
        attn_score = sum(t * math.sin(head * 0.9 + i * 0.3) for i, t in enumerate(tokens[:16]))
        attn_score = math.tanh(attn_score / max(len(tokens), 1))
        hidden.append(round(attn_score + rng.gauss(0, 0.05), 6))
    return hidden


def lm_head_forward(hidden_states: list[float]) -> tuple[str, float]:
    logits = []
    for i, _word in enumerate(VOCAB):
        weight = sum(h * math.sin(i * 0.73 + j * 1.17 + 0.5) for j, h in enumerate(hidden_states))
        logits.append(weight)

    max_l = max(logits)
    exp_l = [math.exp(l - max_l) for l in logits]
    exp_sum = sum(exp_l)
    probs = [e / exp_sum for e in exp_l]
    best_idx = probs.index(max(probs))
    return VOCAB[best_idx], round(probs[best_idx], 6)


async def worker_handler(websocket, node_id: str, profile: dict) -> None:
    log(node_id, "Ready for stage dispatch from SwarmTrain dashboard.", "info")

    async for raw in websocket:
        msg = json.loads(raw)
        msg_type = msg.get("type")

        if msg_type == "ack":
            log(node_id, f"Server ACK: {msg.get('message', '')}", "ok")
            continue

        if msg_type == "cell1_payload":
            sentence = msg.get("sentence", "")
            tokens = msg.get("tokens", [])
            policy = msg.get("policy", {})

            log(node_id, "Executing Stage 1 attention payload...", "recv")
            log(node_id, f"Sentence  : \"{sentence}\"", "info")
            log(node_id, f"Policy    : {policy}", "info")
            log(node_id, f"Token seq : {tokens[:12]}{'...' if len(tokens) > 12 else ''} ({len(tokens)} tokens)", "info")

            time.sleep(random.uniform(0.25, 0.65))
            hidden_states = attention_forward(tokens)
            log(node_id, f"Hidden states (8-dim): {hidden_states}", "ok")

            response = json.dumps(
                {
                    "type": "cell1_result",
                    "hidden_states": hidden_states,
                    "log": f"Stage 1 complete on Node {node_id} [{len(tokens)} tokens -> 8-dim hidden state]",
                    "node_id": node_id,
                }
            )
            await websocket.send(response)
            log(node_id, "Stage 1 result sent back to SwarmTrain.", "send")

        elif msg_type == "cell2_payload":
            hidden_states = msg.get("hidden_states", [])
            source_sentence = msg.get("source_sentence", "")
            policy = msg.get("policy", {})

            log(node_id, "Executing Stage 2 vocabulary projection...", "recv")
            log(node_id, f"Source sentence : \"{source_sentence}\"", "info")
            log(node_id, f"Policy          : {policy}", "info")
            log(node_id, f"Hidden states   : {hidden_states}", "info")

            time.sleep(random.uniform(0.2, 0.5))
            predicted_word, confidence = lm_head_forward(hidden_states)
            log(node_id, f"Predicted token -> \"{predicted_word}\" (p={confidence:.4f})", "ok")

            response = json.dumps(
                {
                    "type": "cell2_result",
                    "predicted_word": predicted_word,
                    "confidence": confidence,
                    "log": (
                        f"Stage 2 complete on Node {node_id} "
                        f"[{len(hidden_states)}-dim hidden -> {len(VOCAB)}-token vocab]"
                    ),
                    "node_id": node_id,
                }
            )
            await websocket.send(response)
            log(node_id, "Stage 2 result sent back to SwarmTrain.", "send")


async def main() -> None:
    print(f"\n  {CYAN}{BOLD}SwarmTrain Node Registration{RESET}")
    print(f"  {DIM}{'-' * 40}{RESET}")

    while True:
        node_id = input("\n  Register as SwarmTrain Node 1, 2, 3, or 4?  > ").strip()
        if node_id in {"1", "2", "3", "4"}:
            break
        print(f"  {RED}Please enter 1, 2, 3, or 4.{RESET}")

    profile = NODE_PROFILES[node_id].copy()
    print(f"\n  {YELLOW}Loaded built-in demo profile for Node {node_id}{RESET}")
    print(
        "  "
        f"Capacity {profile['compute_capacity']} - "
        f"Clean {profile['clean_energy_level']} - "
        f"Carbon {profile['carbon_footprint']}"
    )

    banner(node_id, profile)
    print(f"  Connecting to {SERVER_URI} ...")

    try:
        async with websockets.connect(SERVER_URI) as websocket:
            reg_msg = json.dumps(
                {
                    "type": "register",
                    "node": node_id,
                    "profile": profile,
                }
            )
            await websocket.send(reg_msg)
            log(node_id, f"Registration sent with profile {profile}", "send")
            await worker_handler(websocket, node_id, profile)

    except ConnectionRefusedError:
        print(f"\n  {RED}{BOLD}[X]  Could not connect to {SERVER_URI}{RESET}")
        print(f"  {DIM}Make sure server.py is running: streamlit run server.py{RESET}\n")
        sys.exit(1)
    except websockets.exceptions.ConnectionClosedOK:
        print(f"\n  {DIM}Connection closed by server. Exiting.{RESET}\n")
    except KeyboardInterrupt:
        print(f"\n  {DIM}Interrupted. Goodbye.{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
