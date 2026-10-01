#!/usr/bin/env python3
"""
==============================================================================
ARDUINO STRUCTURAL HEALTH MONITORING — DAQ BRIDGE (daq_bridge.py)
==============================================================================
Authoritative Serial Data Acquisition Bridge from Arduino UNO (UNO-01)
Ingests continuous 1 Hz serial telemetry stream into MongoDB Atlas (Primary Database).
Synchronizes rolling 10-minute (600 readings) batches to Google Sheets every 2 minutes.

Data Pipeline Architecture:
    Arduino UNO (UNO-01)
        │
        │ USB Serial (115200 baud, 1 Hz reading)
        ▼
    daq_bridge.py
        │
        │ Ingest (Low-latency batch / persistent queue retry)
        ▼
    MongoDB Atlas (PRIMARY DATABASE — Full Permanent History)
        │
        │ Every 120 seconds (Batch of ~120 readings)
        ▼
    Google Sheet / Code.gs (SECONDARY DATABASE — Rolling 10-Min Cache)
        │
        ▼
    Index.html (Live Graphs & Real-time Status Dashboard)

Export Path:
    Index.html -> query.py / query_api.py -> MongoDB Atlas -> CSV Export

ABSOLUTE DATA INTEGRITY RULE:
    NEVER GENERATE OR FABRICATE SENSOR DATA.
"""

import os
import sys
import time
import json
import logging
import signal
import threading
import collections
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

# ------------------------------------------------------------------------------
# 1. ENVIRONMENT & CONFIGURATION LOADER
# ------------------------------------------------------------------------------
def load_env_file():
    """Loads environment variables from .env file."""
    for base_dir in [os.path.dirname(os.path.abspath(__file__)), os.getcwd()]:
        env_file = os.path.join(base_dir, ".env")
        if os.path.exists(env_file):
            try:
                from dotenv import load_dotenv
                load_dotenv(env_file)
                return
            except ImportError:
                try:
                    with open(env_file, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if line and not line.startswith("#") and "=" in line:
                                k, v = line.split("=", 1)
                                k = k.strip()
                                v = v.strip().strip("'\"")
                                if k and k not in os.environ:
                                    os.environ[k] = v
                except Exception as e:
                    print(f"[ENV] Warning: Failed to parse .env file: {e}")

load_env_file()

# Environment Defaults
SERIAL_PORT = os.getenv("SERIAL_PORT", "COM6")
BAUD_RATE = int(os.getenv("BAUD_RATE", 115200))
ARDUINO_ID = os.getenv("ARDUINO_ID", "UNO-01")
MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "shm_bridge_db").strip()
WEB_APP_URL = os.getenv("WEB_APP_URL", "").strip()
QUERY_API_PORT = int(os.getenv("QUERY_API_PORT", 5000))
QUERY_API_KEY = os.getenv("QUERY_API_KEY", "SHM_SECURE_READ_KEY_2026").strip()

RECONNECT_DELAY_SEC = 3
HTTP_TIMEOUT_SEC = (5, 15)

# Primary Ingestion & Queue Settings
BATCH_SIZE = 1                    # Low-latency primary write per packet
BATCH_TIMEOUT_SEC = 1.0           # Max wait time for low-latency batch commit
MAX_BATCH_SIZE = 50               # Max drain count during queue recovery
QUEUE_FILE = "pending_queue.json"

# 2-Minute Sync to Google Sheets Requirement
SYNC_INTERVAL_SEC = 120           # 2-minute (120s) synchronization cycle
ROLLING_WINDOW_MINUTES = 10       # 10-minute rolling window retention in Google Sheets (~600 rows)
MAX_SYNC_PACKETS_PER_BATCH = 120  # ~120 readings for 2 minutes of 1Hz acquisition

# PyMongo Dynamic Loading
PYMONGO_AVAILABLE = False
MongoClient = None
UpdateOne = None
IndexModel = None
ASCENDING = 1
DESCENDING = -1
PyMongoError = Exception

try:
    import pymongo
    from pymongo import MongoClient, UpdateOne, IndexModel, ASCENDING, DESCENDING
    from pymongo.errors import PyMongoError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False

try:
    import serial
    import serial.tools.list_ports
except ImportError:
    print("[ARDUINO] [ERROR] 'pyserial' package is required. Install using: pip install pyserial")

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry
except ImportError:
    print("[HTTP] [ERROR] 'requests' package is required. Install using: pip install requests")

# Persistent HTTP session with connection pooling for Google Apps Script
http_session = requests.Session()
retry_strategy = Retry(total=2, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
http_session.mount("https://", adapter)
http_session.mount("http://", adapter)

# ------------------------------------------------------------------------------
# GLOBAL STATE & QUEUE LOCKS
# ------------------------------------------------------------------------------
is_running = True
last_received_seq = None

def create_new_session_id():
    """Generates a unique telemetry session ID upon bridge launch or Arduino sequence reset."""
    return f"BOOT-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"

current_session_id = create_new_session_id()

telemetry_queue = collections.deque()
queue_lock = threading.Lock()
last_disk_save_time = 0.0
dirty_packet_count = 0

# ------------------------------------------------------------------------------
# SENSOR METADATA INFERENCE HELPER
# ------------------------------------------------------------------------------
def infer_sensor_metadata(sensor_id: str) -> dict:
    """Maps physical sensor ID to category type and engineering unit."""
    s = sensor_id.upper()
    if "FORCE" in s or "LOAD" in s or s.startswith("F-"):
        return {"type": "force", "unit": "N"}
    elif "TEMP" in s or "THERM" in s or s.startswith("T-"):
        return {"type": "temp", "unit": "°C"}
    elif "HUMID" in s or s.startswith("H-"):
        return {"type": "humidity", "unit": "%"}
    else:
        prefix = s.split("-")[0].lower() if "-" in s else s.lower()
        return {"type": prefix, "unit": "raw"}

# ==============================================================================
# MONGODB ATLAS DATABASE MANAGER (PRIMARY DATABASE SOURCE OF TRUTH)
# ==============================================================================
class MongoDBManager:
    """
    Manages MongoDB Atlas connection, index initialization, and telemetry storage.
    MongoDB Atlas is the PRIMARY database storing permanent 100% complete history.
    """
    def __init__(self, uri: str, db_name: str):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None
        self.is_connected = False
        self.last_connection_attempt = 0.0
        self.lock = threading.Lock()

    def connect(self) -> bool:
        """Initializes connection to MongoDB Atlas with connection pooling."""
        if not PYMONGO_AVAILABLE:
            print("[MONGODB] Notice: 'pymongo' library not installed. MongoDB connection disabled.")
            return False

        if not self.uri:
            print("[MONGODB] Notice: MONGODB_URI not configured in .env file.")
            return False

        now = time.time()
        if now - self.last_connection_attempt < 5.0 and not self.is_connected:
            return False

        self.last_connection_attempt = now

        try:
            with self.lock:
                if self.client:
                    try:
                        self.client.admin.command('ping')
                        self.is_connected = True
                        return True
                    except Exception:
                        self.is_connected = False
                        try:
                            self.client.close()
                        except Exception:
                            pass

                print(f"[MONGODB] Connecting to MongoDB Atlas cluster (Database: {self.db_name})...")
                self.client = MongoClient(
                    self.uri,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=10000,
                    maxPoolSize=20,
                    minPoolSize=2,
                    retryWrites=True,
                    retryReads=True
                )
                self.client.admin.command('ping')
                self.db = self.client[self.db_name]
                self.is_connected = True
                print(f"[MONGODB] Connected successfully to MongoDB Atlas ✓ (Database: {self.db_name})")
                
                self.ensure_indexes()
                return True

        except Exception as e:
            self.is_connected = False
            print(f"[MONGODB] Connection failed: {e}")
            return False

    def ensure_indexes(self):
        """Creates optimized indexes for long-term telemetry storage and range queries."""
        try:
            if self.db is None:
                return

            telemetry_col = self.db["telemetry"]
            sensors_col = self.db["sensors"]
            devices_col = self.db["devices"]
            logs_col = self.db["ingestion_logs"]
            sync_col = self.db["sync_metadata"]

            # Unique packet index
            telemetry_col.create_index(
                [("arduino_id", ASCENDING), ("session_id", ASCENDING), ("sequence", ASCENDING)],
                unique=True,
                name="uniq_packet_key"
            )

            # Query performance indexes
            telemetry_col.create_indexes([
                IndexModel([("timestamp", DESCENDING)], name="idx_timestamp_desc"),
                IndexModel([("arduino_id", ASCENDING), ("timestamp", DESCENDING)], name="idx_arduino_time"),
                IndexModel([("synced_to_sheets", ASCENDING), ("ingested_at", ASCENDING)], name="idx_sync_ingest"),
                IndexModel([("sequence", ASCENDING)], name="idx_sequence")
            ])

            sensors_col.create_index([("sensor_id", ASCENDING)], unique=True, name="uniq_sensor_id")
            devices_col.create_index([("arduino_id", ASCENDING)], unique=True, name="uniq_arduino_id")
            logs_col.create_index([("timestamp", DESCENDING)], name="idx_log_timestamp")
            
            print("[MONGODB] Database indexes verified successfully ✓")
        except Exception as e:
            print(f"[MONGODB] Index initialization notice: {e}")

    def ingest_batch(self, batch_packets: list) -> tuple:
        """
        Idempotently inserts telemetry packets into MongoDB Atlas.
        Each packet corresponds to 1 reading/second DAQ acquisition from Arduino.
        Returns: (success: bool, inserted_count: int, duplicate_count: int)
        """
        if not batch_packets:
            return True, 0, 0

        if not self.is_connected and not self.connect():
            return False, 0, 0

        try:
            telemetry_requests = []
            sensor_updates = {}
            now_dt = datetime.now(timezone.utc)
            seq_numbers = []

            for pkt in batch_packets:
                arduino_id = pkt.get("arduino_id", ARDUINO_ID)
                session_id = pkt.get("session_id", current_session_id)
                sequence = pkt.get("sequence")
                ts_str = pkt.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S"))
                
                if sequence is not None:
                    seq_numbers.append(sequence)

                try:
                    ts_dt = (
                        datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                        .replace(tzinfo=IST)
                        .astimezone(timezone.utc)
                    )
                except ValueError:
                    ts_dt = now_dt

                doc_id = f"{arduino_id}_{session_id}_{sequence}"
                sensors = pkt.get("sensors", {})

                doc = {
                    "_id": doc_id,
                    "arduino_id": arduino_id,
                    "session_id": session_id,
                    "sequence": sequence,
                    "timestamp": ts_dt,
                    "timestamp_ist": ts_str,
                    "sensors": sensors,
                    "ingested_at": now_dt,
                    "synced_to_sheets": False
                }

                telemetry_requests.append(
                    UpdateOne({"_id": doc_id}, {"$setOnInsert": doc}, upsert=True)
                )

                for sensor_id, info in sensors.items():
                    sensor_id_norm = str(sensor_id).strip().upper()
                    status = info.get("status", "OFFLINE")
                    meta = infer_sensor_metadata(sensor_id_norm)
                    sensor_updates[sensor_id_norm] = {
                        "sensor_id": sensor_id_norm,
                        "sensor_type": meta["type"],
                        "unit": meta["unit"],
                        "status": status,
                        "last_seen_at": ts_dt
                    }

            if not telemetry_requests:
                return True, 0, 0

            telemetry_col = self.db["telemetry"]
            result = telemetry_col.bulk_write(telemetry_requests, ordered=False)

            inserted_cnt = result.upserted_count
            duplicate_cnt = len(telemetry_requests) - inserted_cnt

            # Update sensors metadata collection
            sensors_col = self.db["sensors"]
            for s_id, s_info in sensor_updates.items():
                sensors_col.update_one(
                    {"sensor_id": s_id},
                    {
                        "$set": {
                            "sensor_name": s_id,
                            "sensor_type": s_info["sensor_type"],
                            "unit": s_info["unit"],
                            "status": s_info["status"],
                            "last_seen_at": s_info["last_seen_at"],
                            "enabled": True
                        },
                        "$setOnInsert": {
                            "sensor_id": s_id,
                            "created_at": now_dt
                        }
                    },
                    upsert=True
                )

            # Update devices metadata collection
            devices_col = self.db["devices"]
            max_seq = max(seq_numbers) if seq_numbers else None
            devices_col.update_one(
                {"arduino_id": ARDUINO_ID},
                {
                    "$set": {
                        "last_seen_at": now_dt,
                        "last_sequence": max_seq,
                        "status": "ONLINE"
                    },
                    "$setOnInsert": {
                        "arduino_id": ARDUINO_ID,
                        "device_name": f"Arduino UNO ({ARDUINO_ID}) Hardware Node",
                        "first_seen_at": now_dt
                    }
                },
                upsert=True
            )

            seq_str = f"Seq {min(seq_numbers)}..{max(seq_numbers)}" if seq_numbers else f"{len(batch_packets)} packets"
            print(f"[MONGODB] Primary Ingest: {inserted_cnt} new packet(s), {duplicate_cnt} duplicate(s) prevented ({seq_str}) ✓")
            return True, inserted_cnt, duplicate_cnt

        except PyMongoError as pe:
            print(f"[MONGODB] Database insertion failed: {pe}")
            self.is_connected = False
            return False, 0, 0
        except Exception as e:
            print(f"[MONGODB] Ingestion error: {e}")
            return False, 0, 0

    def fetch_unsynced_telemetry_packets(self, max_packets: int = MAX_SYNC_PACKETS_PER_BATCH) -> tuple:
        """
        Queries MongoDB telemetry collection for unsynced records (synced_to_sheets = False).
        Returns a tuple of (unsynced_packets: list, doc_ids: list).
        """
        if not self.is_connected and not self.connect():
            return [], []

        try:
            query = {
                "$or": [
                    {"synced_to_sheets": False},
                    {"synced_to_sheets": {"$exists": False}}
                ]
            }

            telemetry_col = self.db["telemetry"]
            cursor = (
                telemetry_col
                .find(query)
                .sort("ingested_at", ASCENDING)
                .limit(max_packets)
            )

            unsynced_packets = []
            doc_ids = []

            for doc in cursor:
                doc_id = doc.get("_id")
                seq = doc.get("sequence")
                if seq is None or doc_id is None:
                    continue

                packet = {
                    "arduino_id": doc.get("arduino_id", ARDUINO_ID),
                    "session_id": doc.get("session_id", "LEGACY"),
                    "sequence": seq,
                    "timestamp": doc.get("timestamp_ist", time.strftime("%Y-%m-%d %H:%M:%S")),
                    "sensors": doc.get("sensors", {})
                }
                unsynced_packets.append(packet)
                doc_ids.append(doc_id)

            return unsynced_packets, doc_ids

        except Exception as e:
            print(f"[MONGODB SYNC] Error fetching unsynced records from MongoDB: {e}")
            return [], []

    def mark_telemetry_synced_to_sheets(self, doc_ids: list) -> bool:
        """Updates exact MongoDB documents by _id to synced_to_sheets = True."""
        if not doc_ids or not (self.is_connected or self.connect()):
            return False

        try:
            telemetry_col = self.db["telemetry"]
            result = telemetry_col.update_many(
                {"_id": {"$in": doc_ids}},
                {"$set": {"synced_to_sheets": True}}
            )
            print(f"[MONGODB SYNC] Marked {result.modified_count} exact MongoDB document(s) as synced_to_sheets=True ✓")
            return True
        except Exception as e:
            print(f"[MONGODB SYNC] Error marking telemetry records as synced: {e}")
            return False

    def update_sync_state(self, last_seq: int, count: int, pruned: int, status: str, err_msg: str = None):
        """Updates durable synchronization metadata in MongoDB sync_metadata collection."""
        if not self.is_connected and not self.connect():
            return
        try:
            now_dt = datetime.now(timezone.utc)
            self.db["sync_metadata"].update_one(
                {"_id": "google_sheets_sync"},
                {
                    "$set": {
                        "last_synced_sequence": last_seq,
                        "last_sync_timestamp": now_dt,
                        "last_sync_ist": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "records_synced": count,
                        "records_pruned_in_sheets": pruned,
                        "status": status,
                        "error_message": err_msg,
                        "updated_at": now_dt
                    }
                },
                upsert=True
            )
        except Exception as e:
            print(f"[MONGODB SYNC] Warning: Failed to update sync_metadata: {e}")

# Global MongoDB Manager Instance
mongo_manager = MongoDBManager(MONGODB_URI, MONGODB_DB_NAME)

# ------------------------------------------------------------------------------
# PERSISTENT LOCAL QUEUE MANAGEMENT
# ------------------------------------------------------------------------------
def save_queue_to_disk(force: bool = False):
    """Persists pending queue to disk JSON file safely for 100% zero data loss."""
    global last_disk_save_time, dirty_packet_count
    try:
        now = time.time()
        with queue_lock:
            q_len = len(telemetry_queue)
            packets = list(telemetry_queue)

        dirty_packet_count += 1

        if force or q_len <= 10 or (now - last_disk_save_time >= 3.0) or (dirty_packet_count >= 5):
            tmp_file = QUEUE_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(packets, f)
            if os.path.exists(tmp_file):
                os.replace(tmp_file, QUEUE_FILE)
            last_disk_save_time = now
            dirty_packet_count = 0
    except Exception as e:
        print(f"[QUEUE] Warning: Failed to save queue to disk: {e}")

def load_queue_from_disk():
    """Restores pending queue from disk on startup."""
    if not os.path.exists(QUEUE_FILE):
        return
    try:
        with open(QUEUE_FILE, "r", encoding="utf-8") as f:
            packets = json.load(f)
        if isinstance(packets, list) and packets:
            with queue_lock:
                for pkt in packets:
                    if isinstance(pkt, dict) and "sequence" in pkt and pkt.get("sensors"):
                        telemetry_queue.append(pkt)
            print(f"[QUEUE] Restored {len(packets)} pending packet(s) from {QUEUE_FILE} ✓")
    except Exception as e:
        print(f"[QUEUE] Warning: Failed to load queue from disk: {e}")

# ==============================================================================
# ARDUINO PARSER & TELEMETRY PROCESSING
# ==============================================================================
def parse_arduino_line(line: str):
    """
    Parses exact output from professor-provided Arduino UNO sketch:
    DAQ:ONLINE,SEQ:142,FORCE-01:ONLINE:12.345,TEMP-01:ONLINE:25.40,HUMIDITY-01:ONLINE:60.50
    Offline format:
    DAQ:ONLINE,SEQ:143,FORCE-01:OFFLINE:NULL,TEMP-01:ONLINE:25.40,HUMIDITY-01:OFFLINE:NULL
    """
    global last_received_seq, current_session_id
    line = line.replace('\x00', '').strip()
    if not line:
        return None

    tokens = line.split(",")
    if not tokens:
        return None

    header = tokens[0].strip()

    if header.startswith("DAQ:STARTUP"):
        startup_info = {}
        for token in tokens[1:]:
            parts = token.strip().split(":", 1)
            if len(parts) == 2:
                startup_info[parts[0]] = parts[1]
        print(f"[ARDUINO] Startup packet received from hardware: {startup_info}")
        return None

    if not header.startswith("DAQ:ONLINE"):
        print(f"[ARDUINO] Unrecognized serial line skipped: raw='{line}'")
        return None

    packet_data = {
        "arduino_id": ARDUINO_ID,
        "session_id": current_session_id,
        "sequence": None,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sensors": {}
    }

    for token in tokens[1:]:
        token = token.strip()
        parts = token.split(":")
        if len(parts) == 2 and parts[0] == "SEQ":
            try:
                packet_data["sequence"] = int(parts[1])
            except ValueError:
                return None
        elif len(parts) == 3:
            sensor_id = parts[0].strip().upper()
            raw_status = parts[1].strip().upper()
            raw_val = parts[2].strip()

            # Resilient status parsing
            if "ON" in raw_status or "LINE" in raw_status:
                status = "ONLINE"
            elif "OFF" in raw_status or "WAIT" in raw_status:
                status = "OFFLINE"
            else:
                status = "OFFLINE"

            if status == "OFFLINE" or raw_val.upper() in ("NULL", "INVALID", "NONE", ""):
                val = None
            else:
                try:
                    val = float(raw_val)
                except ValueError:
                    val = None
                    status = "OFFLINE"

            packet_data["sensors"][sensor_id] = {
                "status": status,
                "value": val
            }

    if packet_data["sequence"] is None or not packet_data["sensors"]:
        return None

    seq = packet_data["sequence"]

    # Sequence gap & reset detection
    if last_received_seq is not None:
        if seq > last_received_seq + 1:
            missing_count = seq - (last_received_seq + 1)
            missing_range = f"{last_received_seq + 1}" if missing_count == 1 else f"{last_received_seq + 1}..{seq - 1}"
            print(f"[ARDUINO] [SEQUENCE GAP] Missing sequence: {missing_range} ({missing_count} missing packet(s))")
        elif seq <= last_received_seq:
            print(f"[ARDUINO] [SEQUENCE RESET] Hardware sequence reset: SEQ {seq} after SEQ {last_received_seq}")
            old_session_id = current_session_id
            current_session_id = create_new_session_id()
            packet_data["session_id"] = current_session_id
            print(f"[ARDUINO] Started new telemetry session: OLD={old_session_id} -> NEW={current_session_id}")
            last_received_seq = None

    last_received_seq = seq
    return packet_data

# ==============================================================================
# WORKER THREAD: PRIMARY MONGODB ATLAS INGESTION WORKER
# ==============================================================================
def batch_uploader_worker():
    """
    Worker thread: Continuous immediate ingestion into MongoDB Atlas PRIMARY DATABASE.
    Google Sheets is NOT written here.
    """
    last_upload_attempt = time.time()
    was_offline = False
    retry_delay = 2.0
    max_retry_delay = 15.0

    while is_running:
        try:
            pending_buffer = []
            now = time.time()

            with queue_lock:
                q_len = len(telemetry_queue)
                is_full = q_len >= BATCH_SIZE
                is_timed_out = (q_len > 0 and (now - last_upload_attempt >= BATCH_TIMEOUT_SEC))

                if is_full or is_timed_out:
                    while telemetry_queue and len(pending_buffer) < MAX_BATCH_SIZE:
                        pending_buffer.append(telemetry_queue.popleft())

            if pending_buffer:
                pending_buffer.sort(key=lambda p: p.get("sequence", 0) if p.get("sequence") is not None else 0)

                mongo_ok, mongo_inserted, mongo_duplicates = mongo_manager.ingest_batch(pending_buffer)
                last_upload_attempt = time.time()

                if mongo_ok or (not MONGODB_URI):
                    if was_offline:
                        was_offline = False
                        print("[MONGODB] Primary database connection restored! Flushing queue...")
                    retry_delay = 2.0
                    pending_buffer.clear()
                    save_queue_to_disk(force=True)
                else:
                    if not was_offline:
                        was_offline = True
                        print(f"[RETRY] Primary database temporarily unreachable. Buffering locally... [QUEUE: {len(telemetry_queue) + len(pending_buffer)} pending]")
                    
                    with queue_lock:
                        for pkt in reversed(pending_buffer):
                            telemetry_queue.appendleft(pkt)
                    save_queue_to_disk(force=True)

                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_retry_delay)
            else:
                time.sleep(0.5)

        except Exception as e:
            print(f"[MONGODB] Ingestion worker error: {e}")
            time.sleep(retry_delay)

# ==============================================================================
# WORKER THREAD: 2-MINUTE MONGODB → GOOGLE SHEETS SYNCHRONIZER WORKER
# ==============================================================================
def mongo_to_sheets_sync_worker():
    """
    Worker thread: 2-minute batch synchronization from MongoDB Atlas -> Google Sheets.
    Pushes a batch of ~120 actual readings every 120 seconds.
    Enforces rolling 10-minute window retention in Google Sheets (~600 rows).
    """
    time.sleep(5.0)

    while is_running:
        try:
            if MONGODB_URI and WEB_APP_URL:
                unsynced_packets, fetched_doc_ids = mongo_manager.fetch_unsynced_telemetry_packets(
                    max_packets=MAX_SYNC_PACKETS_PER_BATCH
                )

                if unsynced_packets:
                    seqs = [p.get("sequence") for p in unsynced_packets if p.get("sequence") is not None]
                    max_seq = max(seqs) if seqs else None
                    seq_str = f"Seq {min(seqs)}..{max_seq}" if seqs else f"{len(unsynced_packets)} packets"

                    print(f"[MONGODB SYNC] Starting 2-minute sync: {len(unsynced_packets)} packet(s) ({seq_str}) -> Google Sheets...")

                    payload = {
                        "action": "sync_telemetry_batch",
                        "arduino_id": ARDUINO_ID,
                        "batch": unsynced_packets,
                        "rollingWindowMinutes": ROLLING_WINDOW_MINUTES
                    }

                    headers = {"Content-Type": "application/json"}
                    response = http_session.post(
                        WEB_APP_URL,
                        data=json.dumps(payload),
                        headers=headers,
                        allow_redirects=True,
                        timeout=(10, 30)
                    )

                    if response.status_code in (200, 201):
                        try:
                            res_json = response.json()
                            if res_json.get("status") == "success":
                                count = res_json.get("count", len(unsynced_packets))
                                pruned = res_json.get("pruned", 0)
                                retained = res_json.get("retainedRows", 0)

                                if fetched_doc_ids:
                                    mongo_manager.mark_telemetry_synced_to_sheets(fetched_doc_ids)

                                if max_seq is not None:
                                    mongo_manager.update_sync_state(max_seq, count, pruned, "SUCCESS")

                                print(f"[MONGODB SYNC] 2-Minute Sync Complete: {count} packet(s) synced to Google Sheets. Pruned: {pruned}, Retained: {retained} rows ✓")
                            else:
                                print(f"[MONGODB SYNC] Google Sheets sync rejected payload: {res_json}")
                        except Exception as pe:
                            print(f"[MONGODB SYNC] Response parse error: {pe}")
                    else:
                        print(f"[MONGODB SYNC] Google Apps Script returned HTTP {response.status_code}")
                else:
                    print("[MONGODB SYNC] No unsynced MongoDB records waiting for Google Sheets sync.")
            else:
                if not MONGODB_URI:
                    print("[MONGODB SYNC] Notice: MONGODB_URI not configured.")
                if not WEB_APP_URL:
                    print("[MONGODB SYNC] Notice: WEB_APP_URL not configured.")

            time.sleep(SYNC_INTERVAL_SEC)

        except Exception as e:
            print(f"[MONGODB SYNC] Synchronizer worker error: {e}")
            time.sleep(30.0)

# ==============================================================================
# WORKER THREAD: SERIAL PORT READER WORKER
# ==============================================================================
def serial_reader_thread():
    """Worker thread: Continuous non-blocking reading from Arduino serial line."""
    global is_running
    while is_running:
        try:
            print(f"[ARDUINO] Opening serial port {SERIAL_PORT} @ {BAUD_RATE} baud...")
            with serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1) as ser:
                print(f"[ARDUINO] Connected to {SERIAL_PORT} @ {BAUD_RATE} baud ✓")
                ser.reset_input_buffer()

                last_data_time = time.time()
                last_warning_time = time.time()

                while is_running:
                    raw_bytes = ser.readline()
                    now = time.time()

                    if not raw_bytes:
                        idle_sec = int(now - last_data_time)
                        if idle_sec >= 15 and (now - last_warning_time >= 10):
                            print(f"[ARDUINO] Waiting for Arduino data... ({idle_sec}s idle on {SERIAL_PORT})")
                            last_warning_time = now
                        if idle_sec >= 30:
                            print(f"[ARDUINO] Serial idle for 30s. Re-initializing {SERIAL_PORT}...")
                            break
                        continue

                    last_data_time = now
                    last_warning_time = now

                    try:
                        line = raw_bytes.decode('utf-8', errors='replace')
                    except Exception:
                        continue

                    packet = parse_arduino_line(line)
                    if packet is None:
                        continue

                    seq = packet.get("sequence")
                    ts = packet.get("timestamp")
                    print(f"[ARDUINO] Packet received @ {ts} - Sequence: {seq}")
                    for sensor_id, info in packet.get("sensors", {}).items():
                        print(f"          -> {sensor_id}: {info['status']} ({info['value']})")

                    with queue_lock:
                        telemetry_queue.append(packet)
                    save_queue_to_disk()

        except serial.SerialException as se:
            if is_running:
                print(f"[ARDUINO] Serial connection lost on {SERIAL_PORT}: {se}")
                print(f"[ARDUINO] Retrying serial connection in {RECONNECT_DELAY_SEC}s...")
                time.sleep(RECONNECT_DELAY_SEC)
        except Exception as ex:
            if is_running:
                print(f"[ARDUINO] Unexpected serial error: {ex}")
                time.sleep(RECONNECT_DELAY_SEC)

# ==============================================================================
# SECURE READ-ONLY MONGODB QUERY API THREAD
# ==============================================================================
def start_query_api_thread():
    """Launches secure read-only MongoDB Query API server on background thread."""
    try:
        import query_api
        api_thread = threading.Thread(target=query_api.run_query_api_server, daemon=True)
        api_thread.start()
        print(f"[QUERY API] Read-Only MongoDB Query Layer running on port {QUERY_API_PORT} ✓")
    except Exception as e:
        print(f"[QUERY API] Query API thread start error: {e}")

# ==============================================================================
# GRACEFUL SHUTDOWN HANDLER
# ==============================================================================
def signal_handler(sig, frame):
    global is_running
    print("\n[DAQ BRIDGE] Shutdown initiated. Flushing pending queues & closing resources...")
    is_running = False

    # Flush queue
    save_queue_to_disk(force=True)

    if mongo_manager.client:
        try:
            mongo_manager.client.close()
        except Exception:
            pass

    print("[DAQ BRIDGE] Stopped cleanly.")
    sys.exit(0)

def run_bridge():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("==================================================================")
    print("   ARDUINO STRUCTURAL HEALTH MONITORING — DAQ PYTHON BRIDGE      ")
    print("==================================================================")
    print(f"Target Serial Port : {SERIAL_PORT} @ {BAUD_RATE} baud")
    print(f"Target Mongo DB    : {MONGODB_DB_NAME}")
    print(f"Target Web App URL : {WEB_APP_URL}")
    print(f"Data Flow          : Arduino -> Python -> MongoDB Atlas (Primary DB)")
    print(f"                   : MongoDB Atlas -> 2-Min Sync -> Google Sheets (Secondary)")
    print(f"Sync Interval      : Every {SYNC_INTERVAL_SEC}s (Rolling {ROLLING_WINDOW_MINUTES}-Min Window)")
    print("==================================================================")

    if MONGODB_URI:
        mongo_manager.connect()
    else:
        print("[MONGODB] Notice: MONGODB_URI is not set in environment or .env file.")

    load_queue_from_disk()

    # Start uploader thread (MongoDB primary)
    uploader_thread = threading.Thread(target=batch_uploader_worker, daemon=True)
    uploader_thread.start()

    # Start 2-minute MongoDB to Google Sheets synchronizer worker thread
    sync_thread = threading.Thread(target=mongo_to_sheets_sync_worker, daemon=True)
    sync_thread.start()

    # Start Query API thread
    start_query_api_thread()

    # Run serial reader worker
    serial_reader_thread()

if __name__ == "__main__":
    run_bridge()
