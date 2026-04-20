"""
============================================================
  Edge AI Pipeline - Integrated Lab (Lab 4 + Lab 5 + Lab 7)
  Data Engineering Course
  
  PIPELINE FLOW:
  [Image Capture] → [Lab 7: Visual QC] → [Lab 5: Serialization]
                 → [Lab 4: Fault-Tolerant Upload] → [Cloud / DLQ]
============================================================
"""

import cv2
import numpy as np
import os
import shutil
import time
import random
import json
import struct
import sys

# ─────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────
# Lab 7 – Visual QC Thresholds
BLUR_THRESHOLD   = 100.0
MIN_BRIGHTNESS   = 40.0
MAX_BRIGHTNESS   = 220.0

# Lab 4 – Fault-Tolerance Config
MAX_RETRIES      = 5
BASE_DELAY       = 1.0          # seconds
DLQ_FILE         = "dead_letter_queue.json"

# Directories
RAW_DIR          = "dataset/raw_images"
PASS_DIR         = "dataset/passed"
REJECT_DIR       = "dataset/rejected"


# ═══════════════════════════════════════════════════════════════
#  STAGE 0 ─ Dataset Generator (helper from generate_dataset.py)
# ═══════════════════════════════════════════════════════════════

def generate_mock_images():
    """Creates 5 synthetic test images simulating edge-camera conditions."""
    os.makedirs(RAW_DIR, exist_ok=True)
    print("📷  Generating mock images …")

    base = np.zeros((300, 300), dtype=np.uint8)
    cv2.rectangle(base, (50, 50), (250, 250), 200, -1)
    cv2.circle(base, (150, 150), 50, 100, -1)
    cv2.putText(base, "QC TEST", (80, 150), cv2.FONT_HERSHEY_SIMPLEX, 1, 255, 2)

    images = {
        "01_good_image.jpg":       base,
        "02_blurry_image.jpg":     cv2.GaussianBlur(base, (25, 25), 0),
        "03_dark_image.jpg":       (base * 0.2).astype(np.uint8),
        "04_overexposed_image.jpg": cv2.add(base, 150),
        "05_noisy_image.jpg":      cv2.add(base, np.random.randint(0, 50, (300, 300), dtype=np.uint8)),
    }

    for name, img in images.items():
        cv2.imwrite(os.path.join(RAW_DIR, name), img)

    print(f"    ✔  {len(images)} images written to '{RAW_DIR}'\n")
    return list(images.keys())


# ═══════════════════════════════════════════════════════════════
#  STAGE 1 ─ Lab 7: Visual Quality Control
# ═══════════════════════════════════════════════════════════════

def variance_of_laplacian(image):
    """Focus measure – lower variance = blurrier image."""
    return cv2.Laplacian(image, cv2.CV_64F).var()


def visual_qc_check(image_path):
    """
    Evaluates a single image against QC thresholds.
    Returns (is_valid: bool, reason: str, metrics: dict)
    """
    image = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    if image is None:
        return False, "Read Error", {}

    focus_measure   = variance_of_laplacian(image)
    mean_brightness = float(image.mean())

    metrics = {
        "focus_score": round(focus_measure, 2),
        "brightness":  round(mean_brightness, 2),
        "resolution":  f"{image.shape[1]}x{image.shape[0]}",
    }

    if focus_measure < BLUR_THRESHOLD:
        return False, f"Blurry (score={focus_measure:.1f} < {BLUR_THRESHOLD})", metrics
    if mean_brightness < MIN_BRIGHTNESS:
        return False, f"Too Dark (brightness={mean_brightness:.1f} < {MIN_BRIGHTNESS})", metrics
    if mean_brightness > MAX_BRIGHTNESS:
        return False, f"Too Bright (brightness={mean_brightness:.1f} > {MAX_BRIGHTNESS})", metrics

    return True, f"PASS ✔", metrics


def run_visual_qc():
    """
    Iterates over raw images, sorts them into passed/rejected,
    and returns a list of QC result records for further processing.
    """
    print("=" * 60)
    print("  STAGE 1 — Visual Quality Control (Lab 7)")
    print("=" * 60)

    for d in [PASS_DIR, REJECT_DIR]:
        if os.path.exists(d): shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    qc_results = []

    for filename in sorted(os.listdir(RAW_DIR)):
        if not filename.lower().endswith(('.jpg', '.jpeg', '.png')):
            continue

        image_path = os.path.join(RAW_DIR, filename)
        is_valid, reason, metrics = visual_qc_check(image_path)

        status_icon = "✅ PASS  " if is_valid else "❌ REJECT"
        print(f"  {status_icon} | {filename:<30} | {reason}")

        dest_dir = PASS_DIR if is_valid else REJECT_DIR
        shutil.copy(image_path, os.path.join(dest_dir, filename))

        qc_results.append({
            "filename":  filename,
            "status":    "pass" if is_valid else "reject",
            "reason":    reason,
            "metrics":   metrics,
            "timestamp": int(time.time()),
        })

    passed  = sum(1 for r in qc_results if r["status"] == "pass")
    rejected = len(qc_results) - passed
    print(f"\n  Summary: {passed} passed | {rejected} rejected | "
          f"{len(qc_results)} total\n")

    return qc_results


# ═══════════════════════════════════════════════════════════════
#  STAGE 2 ─ Lab 5: Serialization  (JSON vs Compact Binary)
# ═══════════════════════════════════════════════════════════════

def serialize_compact_binary(record):
    """
    Simulates Protobuf-style compact binary serialization.
    Packs key numeric fields using Python's struct module.
    Format: [focus_score(f) | brightness(f) | timestamp(q) | status_flag(B)]
    """
    metrics    = record.get("metrics", {})
    focus      = metrics.get("focus_score", 0.0)
    brightness = metrics.get("brightness", 0.0)
    ts         = record.get("timestamp", 0)
    flag       = 1 if record["status"] == "pass" else 0

    # Pack: float(4) + float(4) + int64(8) + uint8(1) = 17 bytes
    binary = struct.pack(">ffqB", focus, brightness, ts, flag)
    return binary


def run_serialization_comparison(qc_results):
    """
    For each QC result, compares JSON payload size vs compact binary.
    Mirrors the Lab 5 comparison between JSON and Protobuf.
    """
    print("=" * 60)
    print("  STAGE 2 — Serialization Benchmark (Lab 5)")
    print("=" * 60)
    print(f"  {'Filename':<30} {'JSON':>10} {'Binary':>10} {'Saved':>8}")
    print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*8}")

    payloads = []

    for record in qc_results:
        json_bytes   = json.dumps(record).encode("utf-8")
        binary_bytes = serialize_compact_binary(record)

        json_size   = sys.getsizeof(json_bytes)
        binary_size = sys.getsizeof(binary_bytes)
        saved_pct   = (1 - binary_size / json_size) * 100

        print(f"  {record['filename']:<30} {json_size:>8} B  {binary_size:>8} B  "
              f"{saved_pct:>6.1f}%")

        payloads.append({
            "record":        record,
            "json_bytes":    json_bytes,
            "binary_bytes":  binary_bytes,
            "json_size":     json_size,
            "binary_size":   binary_size,
        })

    avg_saving = (1 - sum(p["binary_size"] for p in payloads) /
                      sum(p["json_size"]   for p in payloads)) * 100
    print(f"\n  → Average bandwidth saving with binary: {avg_saving:.1f}%\n")
    print("  📡  MQTT simulation: binary payload published to")
    print("      topic 'factory/visual_qc/results' with QoS=1\n")
    time.sleep(0.3)

    return payloads


# ═══════════════════════════════════════════════════════════════
#  STAGE 3 ─ Lab 4: Fault-Tolerant Upload with Backoff + DLQ
# ═══════════════════════════════════════════════════════════════

def cloud_api_mock(payload):
    """Simulates a flaky cloud API: 70% success, 30% failure."""
    roll = random.random()
    if   roll < 0.70: return True,  "200 OK"
    elif roll < 0.90: return False, "429 Too Many Requests"
    else:             return False, "500 Internal Server Error"


def save_to_dlq(record):
    """Appends a failed record to the local Dead Letter Queue JSON file."""
    dlq = []
    if os.path.exists(DLQ_FILE):
        with open(DLQ_FILE, "r") as f:
            dlq = json.load(f)

    dlq.append({
        "queued_at": time.time(),
        "payload":   record,
    })

    with open(DLQ_FILE, "w") as f:
        json.dump(dlq, f, indent=2)

    print(f"      💾 [DLQ] Payload saved to '{DLQ_FILE}' for later retry.")


def upload_with_backoff(record, label="payload"):
    """
    Attempts to upload a record with Exponential Backoff + Jitter (Lab 4).
    Falls back to DLQ if all retries are exhausted.
    """
    print(f"\n  ↑  Uploading: {label}")

    for attempt in range(1, MAX_RETRIES + 1):
        success, status = cloud_api_mock(record)

        if success:
            print(f"      ✅ [{status}] on attempt {attempt}")
            return True

        print(f"      ⚠️  [{status}] attempt {attempt}/{MAX_RETRIES}")

        if attempt < MAX_RETRIES:
            backoff    = BASE_DELAY * (2 ** (attempt - 1))
            jitter     = random.uniform(0, 0.5)
            sleep_time = backoff + jitter
            print(f"      ⏳ Backoff: {sleep_time:.2f}s "
                  f"(base={backoff:.1f}s + jitter={jitter:.2f}s)")
            time.sleep(sleep_time)

    print(f"      ☠️  Max retries reached — routing to DLQ.")
    save_to_dlq(record)
    return False


def run_fault_tolerant_upload(qc_results):
    """
    Uploads only the PASSED QC records using fault-tolerant retry logic.
    """
    print("=" * 60)
    print("  STAGE 3 — Fault-Tolerant Upload (Lab 4)")
    print("=" * 60)

    passed_records = [r for r in qc_results if r["status"] == "pass"]
    print(f"  Uploading {len(passed_records)} passed record(s) to Cloud …")

    success_count = 0
    dlq_count     = 0

    for record in passed_records:
        ok = upload_with_backoff(record, label=record["filename"])
        if ok:
            success_count += 1
        else:
            dlq_count += 1

    print(f"\n  Summary: {success_count} uploaded | {dlq_count} sent to DLQ")

    if dlq_count > 0:
        print(f"  ℹ️  Check '{DLQ_FILE}' — a background worker should")
        print(f"      replay these once the network is stable.\n")
    else:
        print()


# ═══════════════════════════════════════════════════════════════
#  MAIN ─ Orchestrate the Full Pipeline
# ═══════════════════════════════════════════════════════════════

def print_banner():
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║    🏭  INTEGRATED EDGE AI PIPELINE                      ║")
    print("║    Lab 4 (Fault Tolerance) +                            ║")
    print("║    Lab 5 (Serialization)   +                            ║")
    print("║    Lab 7 (Visual QC)                                    ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()


def print_final_summary(qc_results):
    passed  = [r for r in qc_results if r["status"] == "pass"]
    rejected = [r for r in qc_results if r["status"] == "reject"]

    print("=" * 60)
    print("  PIPELINE COMPLETE — Final Report")
    print("=" * 60)
    print(f"  Total images processed : {len(qc_results)}")
    print(f"  ✅ Passed QC            : {len(passed)}")
    print(f"  ❌ Rejected by QC       : {len(rejected)}")

    if rejected:
        print("\n  Rejected images (not uploaded):")
        for r in rejected:
            print(f"    • {r['filename']} → {r['reason']}")

    dlq_exists = os.path.exists(DLQ_FILE)
    print(f"\n  DLQ file exists : {'Yes ⚠️' if dlq_exists else 'No ✔'}")
    if dlq_exists:
        with open(DLQ_FILE) as f:
            dlq = json.load(f)
        print(f"  Items in DLQ    : {len(dlq)}")

    print()
    print("  Output directories:")
    print(f"    📁 Passed images  → {PASS_DIR}/")
    print(f"    📁 Rejected images→ {REJECT_DIR}/")
    if dlq_exists:
        print(f"    📄 Dead Letter Q  → {DLQ_FILE}")
    print()


if __name__ == "__main__":
    random.seed(42)        # Reproducible demo run
    print_banner()

    # Clean up previous DLQ
    if os.path.exists(DLQ_FILE):
        os.remove(DLQ_FILE)

    # ── Stage 0: Generate mock images ──────────────────────────
    generate_mock_images()

    # ── Stage 1: Visual QC (Lab 7) ─────────────────────────────
    qc_results = run_visual_qc()

    # ── Stage 2: Serialization benchmark (Lab 5) ───────────────
    run_serialization_comparison(qc_results)

    # ── Stage 3: Fault-tolerant upload (Lab 4) ─────────────────
    run_fault_tolerant_upload(qc_results)

    # ── Final report ───────────────────────────────────────────
    print_final_summary(qc_results)
