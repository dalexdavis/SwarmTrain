# SwarmTrain Policy Scoring & Node Selection Summary

## Overview
SwarmTrain uses a **weighted policy-based scoring system** to automatically select the best two nodes for a distributed pipeline. The system ranks all connected nodes based on three user-controlled priorities, then picks the top two scorers for Stage 1 and Stage 2 execution.

---

## How the Policy Score is Calculated

### The Scoring Formula
Each node receives a composite score based on three weighted factors:

```
score = (compute_capacity × compute_priority/100) 
       + (clean_energy_level × efficiency_priority/100) 
       + (carbon_preference × carbon_priority/100)
```

Where:
- **compute_capacity**: Node's raw compute power (0-100 scale)
- **clean_energy_level**: Node's energy efficiency rating (0-100 scale)
- **carbon_preference**: Inverse of carbon footprint = `100 - carbon_footprint`
- **compute_priority**: User slider (0-100) - weight for compute
- **efficiency_priority**: User slider (0-100) - weight for clean energy
- **carbon_priority**: User slider (0-100) - weight for low carbon

### Example Calculation: Score of 145.2

Let's say you have **Node 3** with these registered metrics:
- compute_capacity: 68
- clean_energy_level: 93
- carbon_footprint: 12

And you set these policy sliders:
- compute_priority: 80
- efficiency_priority: 55
- carbon_priority: 45

**Calculation:**
```
carbon_preference = 100 - 12 = 88

score = (68 × 80/100) + (93 × 55/100) + (88 × 45/100)
      = (68 × 0.80) + (93 × 0.55) + (88 × 0.45)
      = 54.4 + 51.15 + 39.6
      = 145.15
      ≈ 145.2 (rounded to 1 decimal place)
```

---

## How the Two Nodes Are Chosen

### Step 1: Score All Connected Nodes
The `rank_connected_nodes()` function iterates through all connected nodes and calculates a score for each using the formula above.

### Step 2: Sort by Score (Descending)
All nodes are sorted in descending order by their calculated score:
```python
return sorted(ranked, key=lambda item: item["score"], reverse=True)
```

### Step 3: Select Top 2
The pipeline automatically selects:
- **Stage 1 Node** = `ranked_nodes[0]` (highest score)
- **Stage 2 Node** = `ranked_nodes[1]` (second highest score)

### Example Selection
If you have 4 nodes connected with these scores:
- Node 3: **145.2** ← Stage 1 (highest)
- Node 1: **138.5** ← Stage 2 (second highest)
- Node 2: 125.3
- Node 4: 112.1

---

## Policy Priority Modes

The system automatically detects your priority intent:

### 1. Performance-First Routing
**Triggered when:** `compute_priority >= max(efficiency_priority, carbon_priority)`
- Favors nodes with highest compute capacity
- Energy and carbon checks are minimized
- Best for: Speed-critical workloads

### 2. Low-Carbon Routing
**Triggered when:** `carbon_priority >= compute_priority AND carbon_priority >= efficiency_priority`
- Favors nodes with lower carbon footprint (higher carbon_preference)
- Prefers cleaner-energy regions
- Best for: Sustainability-focused workloads

### 3. Balanced Efficiency Routing (Default)
**Triggered when:** Neither of the above conditions apply
- Balances throughput with efficiency and carbon
- Aims for good performance while favoring efficient nodes
- Best for: General-purpose workloads

---

## Key Design Insights

1. **Weighted Combination**: The score is a linear combination of three independent metrics, allowing flexible policy expression.

2. **Carbon Inversion**: Carbon footprint is inverted (`100 - carbon_footprint`) so that lower carbon is better, matching the direction of other metrics.

3. **Normalization**: All priorities are divided by 100, converting the 0-100 slider range into 0-1 weights.

4. **Deterministic Selection**: Given the same nodes and policy, the selection is always the same—no randomness.

5. **Real-Time Adaptation**: As nodes connect/disconnect or you adjust sliders, the ranking updates immediately.

---

## Data Flow

```
User adjusts sliders
    ↓
Policy values: compute_priority, efficiency_priority, carbon_priority
    ↓
rank_connected_nodes() called with:
  - connected_nodes_snapshot (all online nodes)
  - policy values
    ↓
For each node:
  - Calculate score using weighted formula
  - Store in ranked list
    ↓
Sort ranked list by score (descending)
    ↓
Display:
  - Top policy score (ranked_nodes[0]["score"])
  - Stage 1 candidate (ranked_nodes[0]["node_id"])
  - Stage 2 candidate (ranked_nodes[1]["node_id"])
    ↓
When "Run Pipeline" clicked:
  - Send Stage 1 payload to ranked_nodes[0]
  - Send Stage 2 payload to ranked_nodes[1]
```

---

## Summary

The **145.2 score** is a weighted sum of three node metrics multiplied by their respective policy priorities. The two nodes are chosen by calculating this score for all connected nodes, sorting them in descending order, and selecting the top two. This creates a flexible, policy-driven scheduler that adapts to your sustainability and performance goals.
