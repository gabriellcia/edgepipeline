# 🏭 Integrated Edge AI Pipeline

> **Data Engineering Course — Lab 4 + Lab 5 + Lab 7**

A fully integrated edge computing pipeline that combines **Visual Quality Control**, **efficient binary serialization**, and **fault-tolerant cloud upload** into a single end-to-end workflow.

---

## 📐 Architecture

```
[Edge Camera]
     │
     ▼
┌──────────────────────────────────────────────────────┐
│  STAGE 1 ─ Lab 7: Visual QC Gatekeeper               │
│  • Blur Detection    (Laplacian Variance)             │
│  • Exposure Check    (Mean Brightness)                │
│  • Sort → dataset/passed/ or dataset/rejected/        │
└──────────────────────┬───────────────────────────────┘
                       │ Passed images only
                       ▼
┌──────────────────────────────────────────────────────┐
│  STAGE 2 ─ Lab 5: Serialization Benchmark            │
│  • JSON payload   → ~220 bytes (verbose)             │
│  • Binary payload →  ~50 bytes (compact, ~78% saved) │
│  • MQTT publish to 'factory/visual_qc/results' QoS=1 │
└──────────────────────┬───────────────────────────────┘
                       │ Binary payload
                       ▼
┌──────────────────────────────────────────────────────┐
│  STAGE 3 ─ Lab 4: Fault-Tolerant Upload              │
│  • Exponential Backoff + Jitter                      │
│  • Max 5 retries per payload                         │
│  • DLQ fallback → dead_letter_queue.json             │
└──────────────────────────────────────────────────────┘
```

---

## 🛠️ Setup & Run

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/edge-pipeline-project.git
cd edge-pipeline-project
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the full pipeline
```bash
python integrated_edge_pipeline.py
```

---

## 📊 Sample Output

```
╔══════════════════════════════════════════════════════════╗
║    🏭  INTEGRATED EDGE AI PIPELINE                      ║
╚══════════════════════════════════════════════════════════╝

STAGE 1 — Visual Quality Control (Lab 7)
  ✅ PASS   | 01_good_image.jpg        | PASS ✔
  ❌ REJECT | 02_blurry_image.jpg      | Blurry (score=1.8 < 100.0)
  ❌ REJECT | 03_dark_image.jpg        | Blurry (score=53.1 < 100.0)
  ✅ PASS   | 04_overexposed_image.jpg | PASS ✔
  ✅ PASS   | 05_noisy_image.jpg       | PASS ✔

STAGE 2 — Serialization Benchmark (Lab 5)
  → Average bandwidth saving with binary: 77.8%
  📡  MQTT published to 'factory/visual_qc/results' QoS=1

STAGE 3 — Fault-Tolerant Upload (Lab 4)
  ✅ 01_good_image.jpg        → [200 OK] attempt 1
  ✅ 04_overexposed_image.jpg → [200 OK] attempt 1
  ✅ 05_noisy_image.jpg       → [200 OK] attempt 1
```

---

## 🗂️ Project Structure

```
edge-pipeline-project/
├── integrated_edge_pipeline.py   # Main pipeline (Lab 4+5+7)
├── requirements.txt
├── .gitignore
└── README.md
```

> **Note:** The `dataset/` folder and `dead_letter_queue.json` are generated at runtime and excluded from version control via `.gitignore`.

---

## 🔬 Labs Reference

| Lab | Topic | Key Concept |
|-----|-------|-------------|
| Lab 4 | Fault Tolerance | Exponential Backoff, Jitter, Dead Letter Queue |
| Lab 5 | Edge Serialization | Protobuf vs JSON, MQTT QoS=1 |
| Lab 7 | Visual QC | Laplacian Blur Detection, Brightness Thresholding |

---

## 🧠 Key Design Decisions

- **QC before upload**: Reject bad images *at the edge* to save bandwidth — don't waste cloud compute sorting garbage data.  
- **Binary over JSON**: ~78% smaller payloads = lower MQTT costs on constrained networks.  
- **DLQ pattern**: No data is ever silently dropped; failed payloads are queued for replay once connectivity is restored.
