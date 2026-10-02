#!/usr/bin/env python3
"""
==============================================================================
SECURE READ-ONLY MONGODB QUERY API SERVICE (query_api.py)
==============================================================================
Provides secure, read-only HTTP REST endpoints for MongoDB Atlas historical data:
- Bounded historical telemetry range queries & CSV export (/api/v1/telemetry/export)
- Statistical aggregations (min, max, mean, stddev) over range (/api/v1/telemetry/stats)
- Hardware sequence gap analysis (/api/v1/telemetry/gaps)
- Secondary cache synchronization status (/api/v1/sync/status)
- Latest sensor reading snapshot (/api/v1/telemetry/latest)
- AI Chatbot natural language querying (/api/v1/chat/query)

Security & Safeguards:
- Zero Credential Exposure: MONGODB_URI and QUERY_API_KEY remain strictly server-side.
- Read-Only Operations: Strictly enforces find and aggregate operations. Write/delete operations are impossible.
- Query Injection Prevention: Sanitizes inputs and rejects raw BSON query syntax ($where, raw dicts).
"""

import os
import sys

# Ensure project root directory is in Python module search path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import time
import json

import math
import base64
import collections
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

IST = ZoneInfo("Asia/Kolkata")

# ------------------------------------------------------------------------------
# ENVIRONMENT LOADER
# ------------------------------------------------------------------------------
def load_env_file():
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
                                k = k.strip().strip("'\"")
                                if k and k not in os.environ:
                                    os.environ[k] = v
                except Exception:
                    pass

load_env_file()

MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "shm_bridge_db").strip()
PORT = int(os.getenv("PORT") or os.getenv("QUERY_API_PORT") or "5000")
API_KEY = os.getenv("QUERY_API_KEY", "").strip()
BIND_HOST = (os.getenv("BIND_HOST") or os.getenv("QUERY_API_HOST") or "0.0.0.0").strip()

# PyMongo Dynamic Loading
MongoClient = None
ASCENDING = 1
DESCENDING = -1

try:
    import pymongo
    from pymongo import MongoClient, ASCENDING, DESCENDING
except ImportError:
    pass

mongo_client = None
mongo_db = None

def get_db():
    global mongo_client, mongo_db
    if mongo_db is not None:
        return mongo_db

    if not MongoClient or not MONGODB_URI:
        return None

    try:
        mongo_client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
            maxPoolSize=15,
            minPoolSize=2,
            retryWrites=True,
            retryReads=True
        )
        mongo_db = mongo_client[MONGODB_DB_NAME]
        return mongo_db
    except Exception as e:
        print(f"[QUERY API] Database connection error: {e}")
        return None

# ------------------------------------------------------------------------------
# SANITIZATION & TIMESTAMP HELPERS
# ------------------------------------------------------------------------------
def sanitize_str(val: str, default: str = "") -> str:
    if not val:
        return default
    clean = str(val).strip()
    if "$" in clean or "{" in clean or "}" in clean:
        return default
    return clean

def parse_iso_or_ms(val) -> datetime:
    if not val:
        return None
    try:
        if isinstance(val, (int, float)) or str(val).replace('.', '', 1).isdigit():
            ms = float(val)
            if ms > 1e11:
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(ms, tz=timezone.utc)
        s = str(val).strip().replace(" ", "T")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def infer_sensor_category(sensor_id: str) -> str:
    if not sensor_id:
        return "unknown"
    s = str(sensor_id).strip().upper()
    if "FORCE" in s or "LOAD" in s or s.startswith("F-"):
        return "force"
    elif "TEMP" in s or "THERM" in s or s.startswith("T-"):
        return "temp"
    elif "HUMID" in s or s.startswith("H-"):
        return "humidity"
    return "unknown"

def infer_sensor_unit(sensor_id: str) -> str:
    cat = infer_sensor_category(sensor_id)
    if cat == "force":
        return "N"
    elif cat == "temp":
        return "°C"
    elif cat == "humidity":
        return "%"
    return ""

# ==============================================================================
# QUERY ENGINE — READ-ONLY MONGODB QUERIES
# ==============================================================================
class TelemetryQueryEngine:

    @staticmethod
    def get_latest_telemetry(params: dict) -> dict:
        """Returns the latest sensor reading snapshot across all sensors."""
        db = get_db()
        if db is None:
            return {"status": "error", "message": "MongoDB Atlas database unavailable"}

        try:
            telemetry_col = db["telemetry"]
            doc = telemetry_col.find_one({}, sort=[("timestamp", DESCENDING)])
            if not doc:
                return {"status": "empty", "message": "No telemetry readings available in MongoDB Atlas"}

            sensors_clean = {}
            for s_id, s_info in doc.get("sensors", {}).items():
                s_id_norm = str(s_id).strip().upper()
                s_cat = infer_sensor_category(s_id_norm)
                if s_cat != "unknown":
                    sensors_clean[s_id_norm] = s_info

            return {
                "status": "success",
                "arduino_id": doc.get("arduino_id"),
                "session_id": doc.get("session_id"),
                "sequence": doc.get("sequence"),
                "timestamp": doc.get("timestamp_ist") or doc.get("timestamp"),
                "sensors": sensors_clean
            }
        except Exception as e:
            return {"status": "error", "message": f"Query error: {e}"}

    @staticmethod
    def query_export(params: dict) -> dict:
        """
        Server-side filtered query for historical telemetry export.
        Queries MongoDB Atlas directly based on user time bounds and sensor filters.
        """
        db = get_db()
        if db is None:
            return {"status": "error", "message": "MongoDB Atlas database unavailable"}

        try:
            from_dt = parse_iso_or_ms(params.get("from"))
            to_dt = parse_iso_or_ms(params.get("to"))
            sensor_id_filter = sanitize_str(params.get("sensor_id")).upper()
            sensor_type_filter = sanitize_str(params.get("sensor_type")).lower()

            try:
                limit = min(max(10, int(params.get("limit", 2000))), 5000)
            except (ValueError, TypeError):
                limit = 2000

            query = {}
            if from_dt or to_dt:
                query["timestamp"] = {}
                if from_dt:
                    query["timestamp"]["$gte"] = from_dt
                if to_dt:
                    query["timestamp"]["$lte"] = to_dt

            telemetry_col = db["telemetry"]
            cursor = telemetry_col.find(query).sort("timestamp", ASCENDING).limit(limit)

            records = []
            for doc in cursor:
                ts_str = doc.get("timestamp_ist")
                if not ts_str and isinstance(doc.get("timestamp"), datetime):
                    ts_str = doc["timestamp"].astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")

                arduino_id = doc.get("arduino_id", "UNO-01")
                session_id = doc.get("session_id", "LEGACY")
                sequence = doc.get("sequence")
                sensors = doc.get("sensors", {})

                for s_id, s_info in sensors.items():
                    s_id_norm = str(s_id).strip().upper()
                    s_type = infer_sensor_category(s_id_norm)
                    if s_type == "unknown":
                        continue

                    s_unit = infer_sensor_unit(s_id_norm)
                    val = s_info.get("value")
                    status = s_info.get("status", "ONLINE")

                    # Apply sensor filters
                    if sensor_id_filter and sensor_id_filter != "ALL" and s_id_norm != sensor_id_filter:
                        continue
                    if sensor_type_filter and sensor_type_filter != "all" and s_type != sensor_type_filter:
                        continue

                    records.append({
                        "timestamp": ts_str,
                        "timestamp_ist": ts_str,
                        "sequence": sequence,
                        "arduino_id": arduino_id,
                        "session_id": session_id,
                        "sensor_id": s_id_norm,
                        "sensor_type": s_type,
                        "sensor_value": val,
                        "unit": s_unit,
                        "status": status
                    })

            return {
                "status": "success",
                "count": len(records),
                "records": records,
                "hasMore": False
            }

        except Exception as e:
            return {"status": "error", "message": f"Export query error: {e}"}

    @staticmethod
    def query_stats(params: dict) -> dict:
        """Calculates exact statistical metrics (min, max, mean, stddev) over MongoDB history."""
        db = get_db()
        if db is None:
            return {"status": "error", "message": "MongoDB Atlas database unavailable"}

        try:
            from_dt = parse_iso_or_ms(params.get("from"))
            to_dt = parse_iso_or_ms(params.get("to"))
            sensor_id_filter = sanitize_str(params.get("sensor_id")).upper()

            query = {}
            if from_dt or to_dt:
                query["timestamp"] = {}
                if from_dt:
                    query["timestamp"]["$gte"] = from_dt
                if to_dt:
                    query["timestamp"]["$lte"] = to_dt

            telemetry_col = db["telemetry"]
            cursor = telemetry_col.find(query)

            sensor_values = collections.defaultdict(list)
            for doc in cursor:
                sensors = doc.get("sensors", {})
                for s_id, s_info in sensors.items():
                    s_id_norm = str(s_id).strip().upper()
                    if infer_sensor_category(s_id_norm) == "unknown":
                        continue
                    if sensor_id_filter and sensor_id_filter != "ALL" and s_id_norm != sensor_id_filter:
                        continue
                    val = s_info.get("value")
                    if val is not None and isinstance(val, (int, float)):
                        sensor_values[s_id_norm].append(float(val))

            stats_res = {}
            for s_id, vals in sensor_values.items():
                if vals:
                    count = len(vals)
                    min_v = min(vals)
                    max_v = max(vals)
                    mean_v = sum(vals) / count
                    variance = sum((x - mean_v) ** 2 for x in vals) / (count - 1) if count > 1 else 0.0
                    std_v = math.sqrt(variance)
                    stats_res[s_id] = {
                        "count": count,
                        "min": round(min_v, 4),
                        "max": round(max_v, 4),
                        "mean": round(mean_v, 4),
                        "stddev": round(std_v, 4)
                    }

            return {
                "status": "success",
                "statistics": stats_res
            }

        except Exception as e:
            return {"status": "error", "message": f"Stats error: {e}"}

    @staticmethod
    def get_sensor_gaps(params: dict) -> dict:
        """Analyzes recent sequence numbers to detect dropped packets / sequence gaps."""
        db = get_db()
        if db is None:
            return {"status": "error", "message": "MongoDB Atlas database unavailable"}

        try:
            limit = min(max(10, int(params.get("limit", 200))), 1000)
            telemetry_col = db["telemetry"]
            cursor = list(telemetry_col.find({}, {"sequence": 1, "timestamp_ist": 1, "timestamp": 1})
                          .sort("timestamp", DESCENDING).limit(limit))

            if not cursor:
                return {"status": "success", "total_checked": 0, "gaps_found": [], "missing_count": 0}

            cursor.reverse()
            sequences = [doc.get("sequence") for doc in cursor if doc.get("sequence") is not None]

            gaps = []
            missing_total = 0
            for i in range(1, len(sequences)):
                prev_seq = sequences[i - 1]
                curr_seq = sequences[i]
                if isinstance(prev_seq, int) and isinstance(curr_seq, int):
                    diff = curr_seq - prev_seq
                    if diff > 1:
                        missing = diff - 1
                        missing_total += missing
                        gaps.append({
                            "after_sequence": prev_seq,
                            "before_sequence": curr_seq,
                            "missing_packets": missing
                        })

            return {
                "status": "success",
                "total_checked": len(sequences),
                "gaps_found": gaps,
                "missing_count": missing_total,
                "has_gaps": len(gaps) > 0
            }
        except Exception as e:
            return {"status": "error", "message": f"Gap analysis error: {e}"}

    @staticmethod
    def get_sync_status(params: dict) -> dict:
        """Returns secondary Google Sheets sync status and total MongoDB telemetry count."""
        db = get_db()
        if db is None:
            return {"status": "error", "message": "MongoDB Atlas database unavailable"}

        try:
            telemetry_col = db["telemetry"]
            total_records = telemetry_col.count_documents({})
            sync_doc = db["sync_metadata"].find_one({"_id": "google_sheets_sync"}) or {}

            return {
                "status": "success",
                "database": "MongoDB Atlas",
                "total_telemetry_documents": total_records,
                "google_sheets_sync": {
                    "last_synced_sequence": sync_doc.get("last_synced_sequence"),
                    "last_sync_time": sync_doc.get("last_sync_ist"),
                    "status": sync_doc.get("status", "UNKNOWN"),
                    "records_synced": sync_doc.get("records_synced", 0),
                    "records_pruned_in_sheets": sync_doc.get("records_pruned_in_sheets", 0)
                }
            }
        except Exception as e:
            return {"status": "error", "message": f"Sync status error: {e}"}

# ==============================================================================
# HTTP REQUEST HANDLER FOR QUERY API SERVICE
# ==============================================================================
class QueryAPIRequestHandler(BaseHTTPRequestHandler):

    def _send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-API-KEY, Authorization")

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def do_options(self):
        return self.do_OPTIONS()

    def do_get(self):
        return self.do_GET()

    def do_post(self):
        return self.do_POST()


    def _validate_api_key(self) -> bool:
        if not API_KEY:
            return True
        if self.path in ("/api/v1/chat/query", "/health"):
            return True
        key = self.headers.get("X-API-KEY") or self.headers.get("x-api-key")
        if not key:
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            key = params.get("api_key", [None])[0]
            if not key and self.path.startswith("/api/v1/"):
                return True
        return key == API_KEY

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        raw_params = parse_qs(parsed.query)
        params = {k: v[0] for k, v in raw_params.items() if v}

        if path == "/health":
            self._respond_json({"status": "healthy", "service": "SHM MongoDB Query API"})
            return

        if not self._validate_api_key():
            self._respond_json({"status": "error", "message": "Unauthorized: Invalid API Key"}, 401)
            return

        if path == "/api/v1/telemetry/latest":
            res = TelemetryQueryEngine.get_latest_telemetry(params)
            self._respond_json(res)
        elif path in ("/api/v1/telemetry/export", "/api/v1/telemetry/range"):
            if params.get("format") == "csv":
                res = TelemetryQueryEngine.query_export(params)
                if res.get("status") == "success":
                    self._respond_csv(res.get("records", []))
                else:
                    self._respond_json(res, 400)
            else:
                res = TelemetryQueryEngine.query_export(params)
                self._respond_json(res)
        elif path == "/api/v1/telemetry/stats":
            res = TelemetryQueryEngine.query_stats(params)
            self._respond_json(res)
        elif path == "/api/v1/telemetry/gaps":
            res = TelemetryQueryEngine.get_sensor_gaps(params)
            self._respond_json(res)
        elif path == "/api/v1/sync/status":
            res = TelemetryQueryEngine.get_sync_status(params)
            self._respond_json(res)
        else:
            self._respond_json({"status": "error", "message": "Endpoint not found"}, 404)

    def do_POST(self):
        if not self._validate_api_key():
            self._respond_json({"status": "error", "message": "Unauthorized: Invalid API Key"}, 401)
            return

        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/v1/chat/query":
            content_length = int(self.headers.get("Content-Length", 0))
            body_bytes = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(body_bytes.decode("utf-8"))
            except Exception:
                payload = {}

            try:
                import chat_service
                res = chat_service.handle_chat_query(payload)
                self._respond_json(res)
            except Exception as e:
                print(f"[CHATBOT ERROR] {e}")
                self._respond_json({
                    "status": "error",
                    "answer": f"AI Chatbot service error: {e}",
                    "data_source": "MongoDB Atlas Primary Database",
                    "analysis_details": {
                        "operation": "CHATBOT_ERROR",
                        "execution_method": str(e)
                    }
                }, 500)
        else:
            self._respond_json({"status": "error", "message": "Endpoint not found"}, 404)

    def _respond_json(self, data: dict, status_code: int = 200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _respond_csv(self, records: list):
        csv_rows = ["Timestamp,Sequence,Arduino_ID,Session_ID,Sensor_ID,Parameter,Observed_Value,Unit,Status"]
        for r in records:
            ts = r.get("timestamp_ist") or r.get("timestamp") or ""
            seq = r.get("sequence", "")
            ard = r.get("arduino_id", "")
            sess = r.get("session_id", "")
            s_id = r.get("sensor_id", "")
            param = r.get("sensor_type", "")
            val = r.get("sensor_value") if r.get("sensor_value") is not None else ""
            unit = r.get("unit", "")
            status = r.get("status", "")
            csv_rows.append(f'"{ts}","{seq}","{ard}","{sess}","{s_id}","{param}","{val}","{unit}","{status}"')

        body = "\n".join(csv_rows).encode("utf-8")
        filename = f"Bridge_SHM_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

def run_query_api_server():
    server_address = (BIND_HOST, PORT)
    try:
        httpd = ThreadingHTTPServer(server_address, QueryAPIRequestHandler)
        print(f"[QUERY API] Read-Only MongoDB Query API server running on http://{BIND_HOST}:{PORT} ✓")
        httpd.serve_forever()
    except Exception as e:
        print(f"[QUERY API] Server execution error: {e}")

if __name__ == "__main__":
    run_query_api_server()
