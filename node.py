"""
SwarmTrain Client Worker — node.py
===================================
Run this script twice in two separate terminal windows.

Usage:
    python node.py

On launch you will be prompted to register as Node 1 or Node 2.

Node 1 — Attention Layer
  • Receives a tokenised sentence from the SwarmTrain dashboard (Cell 1).
  • Simulates a multi-head self-attention computation.
  • Returns an 8-dimensional mock hidden-state vector.

Node 2 — Language Model Head
  • Receives the hidden states produced by Node 1 (Cell 2).
  • Projects them onto a small mock vocabulary (logits + softmax).
  • Returns the predicted next token and confidence score.

Protocol (all messages are JSON strings over WebSocket):
  Server → Node:  {"type": "cell1_payload",  "tokens": [...], "sentence": "..."}
                  {"type": "cell2_payload",  "hidden_states": [...]}
  Node → Server:  {"type": "cell1_result",   "hidden_states": [...], "log": "..."}
                  {"type": "cell2_result",   "predicted_word": "...",
                                             "confidence": 0.xx, "log": "..."}
"""

import asyncio
import json
import math
import random
import sys
import time

import websockets

# ── Server address ─────────────────────────────────────────────────────────────
SERVER_URI = "ws://127.0.0.1:8765"

# ── Small mock vocabulary for Node 2 ─────────────────────────────────────────
VOCAB = [
    "the", "a", "and", "to", "of", "in", "was", "she", "he",
    "little", "big", "old", "new", "happy", "sad", "ran",
    "walked", "found", "loved", "said", "time", "day", "night",
]

# ── ANSI colour helpers (makes terminal output more legible) ──────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
MAGENTA= "\033[95m"
RED    = "\033[91m"
DIM    = "\033[2m"

def banner(node_id: str):
    """Print a styled startup banner."""
    colour = GREEN if node_id == "1" else BLUE
    role   = "Attention Layer" if node_id == "1" else "Language Model Head"
    bar    = "─" * 54
    print(f"\n{colour}{BOLD}")
    print(f"  ┌{bar}┐")
    print(f"  │   SwarmTrain · Node {node_id} — {role:<28}│")
    print(f"  │   Connected to {SERVER_URI:<36}│")
    print(f"  └{bar}┘{RESET}\n")

def log(node_id: str, msg: str, level: str = "info"):
    """Formatted log line with timestamp."""
    ts     = time.strftime("%H:%M:%S")
    colour = GREEN if node_id == "1" else BLUE
    icons  = {"info": "·", "recv": "↓", "send": "↑", "ok": "✓", "err": "✗"}
    icon   = icons.get(level, "·")
    print(f"  {DIM}{ts}{RESET}  {colour}{BOLD}[SwarmTrain - NODE {node_id}]{RESET}  {icon}  {msg}")


# ══════════════════════════════════════════════════════════════════════════════
#  NODE 1 — ATTENTION LAYER PROCESSING
# ══════════════════════════════════════════════════════════════════════════════

def attention_forward(tokens: list[int]) -> list[float]:
    """
    Simulate multi-head self-attention for a token sequence.

    Real operation:  Attention(Q,K,V) = softmax(QK^T / sqrt(d_k)) · V
    Here we produce a deterministic 8-dim vector seeded from the token sum so
    that the same sentence always yields the same hidden states.
    """
    seed = (sum(tokens) * 31 + len(tokens) * 97) % (2 ** 32)
    rng  = random.Random(seed)

    # Simulate 8 attention heads each computing one output dimension
    hidden = []
    for head in range(8):
        # Fake Q·K score (dot product stand-in)
        attn_score = sum(t * math.sin(head * 0.9 + i * 0.3) for i, t in enumerate(tokens[:16]))
        attn_score = math.tanh(attn_score / max(len(tokens), 1))   # normalise
        # Add Gaussian noise for realism
        hidden.append(round(attn_score + rng.gauss(0, 0.05), 6))

    return hidden


async def node1_handler(websocket):
    """
    Node 1 main receive loop.
    Waits for cell1_payload messages and responds with hidden states.
    """
    node_id = "1"
    log(node_id, "Waiting for Cell 1 payloads from SwarmTrain dashboard…", "info")

    async for raw in websocket:
        msg = json.loads(raw)

        # ── Ignore non-cell1 messages ──────────────────────────────────────
        if msg.get("type") == "ack":
            log(node_id, f"Server ACK: {msg.get('message', '')}", "ok")
            continue

        if msg.get("type") != "cell1_payload":
            continue

        # ── Process the attention layer ────────────────────────────────────
        sentence = msg.get("sentence", "")
        tokens   = msg.get("tokens",   [])

        log(node_id, f"Executing attention matrix payload…", "recv")
        log(node_id, f"Sentence  : \"{sentence}\"", "info")
        log(node_id, f"Token seq : {tokens[:12]}{'…' if len(tokens) > 12 else ''} ({len(tokens)} tokens)", "info")

        # Simulate processing delay (realistic)
        time.sleep(random.uniform(0.3, 0.7))

        # Run mock attention forward pass
        hidden_states = attention_forward(tokens)

        log(node_id, f"Hidden states (8-dim): {hidden_states}", "ok")

        # Build log string to send back for dashboard display
        result_log = (
            f"Executing attention matrix payload... "
            f"[{len(tokens)} tokens → 8-dim hidden state]"
        )

        # ── Send result back to server ─────────────────────────────────────
        response = json.dumps({
            "type":          "cell1_result",
            "hidden_states": hidden_states,
            "log":           result_log,
        })
        await websocket.send(response)
        log(node_id, "Result dispatched to SwarmTrain dashboard ✓", "send")
        log(node_id, "Ready for next Cell 1 payload…", "info")


# ══════════════════════════════════════════════════════════════════════════════
#  NODE 2 — LANGUAGE MODEL HEAD
# ══════════════════════════════════════════════════════════════════════════════

def lm_head_forward(hidden_states: list[float]) -> tuple[str, float]:
    """
    Simulate the language model head:
      logits = W_vocab @ hidden_states   (dot product approximation)
      probs  = softmax(logits)
      token  = argmax(probs)

    Returns (predicted_word, confidence_probability).
    """
    logits = []
    for i, word in enumerate(VOCAB):
        # Dot product of hidden vector with a hash-derived weight vector per word
        weight = sum(
            h * math.sin(i * 0.73 + j * 1.17 + 0.5)
            for j, h in enumerate(hidden_states)
        )
        logits.append(weight)

    # Numerically stable softmax
    max_l   = max(logits)
    exp_l   = [math.exp(l - max_l) for l in logits]
    exp_sum = sum(exp_l)
    probs   = [e / exp_sum for e in exp_l]

    best_idx  = probs.index(max(probs))
    return VOCAB[best_idx], round(probs[best_idx], 6)


async def node2_handler(websocket):
    """
    Node 2 main receive loop.
    Waits for cell2_payload messages and responds with a predicted next token.
    """
    node_id = "2"
    log(node_id, "Waiting for Cell 2 payloads (hidden states) from SwarmTrain dashboard…", "info")

    async for raw in websocket:
        msg = json.loads(raw)

        if msg.get("type") == "ack":
            log(node_id, f"Server ACK: {msg.get('message', '')}", "ok")
            continue

        if msg.get("type") != "cell2_payload":
            continue

        # ── Process the LM head ────────────────────────────────────────────
        hidden_states   = msg.get("hidden_states",   [])
        source_sentence = msg.get("source_sentence", "")

        log(node_id, f"Calculating vocabulary logits…", "recv")
        log(node_id, f"Source sentence : \"{source_sentence}\"", "info")
        log(node_id, f"Hidden states   : {hidden_states}", "info")

        time.sleep(random.uniform(0.2, 0.5))

        # Run mock LM-head forward pass
        predicted_word, confidence = lm_head_forward(hidden_states)

        log(node_id, f"Vocab logits computed over {len(VOCAB)} tokens", "ok")
        log(node_id, f"Softmax argmax  → \"{predicted_word}\"  (p={confidence:.4f})", "ok")

        result_log = (
            f"Calculating vocabulary logits... "
            f"[{len(hidden_states)}-dim hidden → {len(VOCAB)}-token vocab] "
            f"→ predicted: \"{predicted_word}\" (confidence: {confidence:.4f})"
        )

        # ── Send result back to server ─────────────────────────────────────
        response = json.dumps({
            "type":           "cell2_result",
            "predicted_word": predicted_word,
            "confidence":     confidence,
            "log":            result_log,
        })
        await websocket.send(response)
        log(node_id, f"Prediction \"{predicted_word}\" dispatched to SwarmTrain dashboard ✓", "send")
        log(node_id, "Ready for next Cell 2 payload…", "info")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    # ── Node selection prompt ──────────────────────────────────────────────
    print(f"\n  {CYAN}{BOLD}SwarmTrain Node Registration{RESET}")
    print(f"  {DIM}{'─'*40}{RESET}")

    while True:
        choice = input(f"\n  Register as SwarmTrain Node {YELLOW}1{RESET} or {YELLOW}2{RESET}?  › ").strip()
        if choice in ("1", "2"):
            node_id = choice
            break
        print(f"  {RED}Please enter 1 or 2.{RESET}")

    # ── Print banner ───────────────────────────────────────────────────────
    banner(node_id)

    # ── Connect and register with the server ──────────────────────────────
    print(f"  Connecting to {SERVER_URI} …")

    try:
        async with websockets.connect(SERVER_URI) as websocket:
            # Send registration message
            reg_msg = json.dumps({"type": "register", "node": node_id})
            await websocket.send(reg_msg)
            log(node_id, f"Registration message sent (Node {node_id})", "send")

            # Dispatch to the correct handler
            if node_id == "1":
                await node1_handler(websocket)
            else:
                await node2_handler(websocket)

    except ConnectionRefusedError:
        print(f"\n  {RED}{BOLD}✗  Could not connect to {SERVER_URI}{RESET}")
        print(f"  {DIM}Make sure server.py is running: streamlit run server.py{RESET}\n")
        sys.exit(1)
    except websockets.exceptions.ConnectionClosedOK:
        print(f"\n  {DIM}Connection closed by server. Exiting.{RESET}\n")
    except KeyboardInterrupt:
        print(f"\n  {DIM}Interrupted. Goodbye.{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
