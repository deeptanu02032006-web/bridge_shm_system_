#!/usr/bin/env python3
"""
==============================================================================
SHM AI CHATBOT SERVICE — PROVIDER-INDEPENDENT MONGODB TELEMETRY ASSISTANT
==============================================================================
Provides a strictly read-only, MongoDB-grounded AI assistant layer for the Bridge SHM
system. Queries MongoDB Atlas as the SINGLE AUTHORITATIVE SOURCE OF TRUTH.

Architecture:
Browser -> POST /api/v1/chat/query -> query_api.py -> chat_service.py -> LLM -> MongoDB tools -> LLM -> Natural Answer

Features:
- OpenAI Responses API & Chat Completions API with Function / Tool Calling
- Grounded read-only MongoDB Atlas tools (get_latest_sensor_data, get_sensor_statistics,
  get_sensor_history, get_record_count, get_sensor_gaps, get_active_sensors)
- Multi-turn conversation memory
- Natural language time range handling (IST timezone)
- Sensor alias mapping & physical deployment constraints (FORCE-01, TEMP-01, HUMIDITY-01)
- Zero fake/sample telemetry, zero secret exposure
"""

import os
import sys
import time
import json
import re
import math
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import requests

from query_api import get_db, TelemetryQueryEngine, parse_iso_or_ms, IST

# ------------------------------------------------------------------------------
# CANONICAL SENSOR DEFINITIONS & HARDWARE CONSTRAINTS
# ------------------------------------------------------------------------------
CANONICAL_SENSORS = {
    "FORCE-01": {"id": "FORCE-01", "name": "Force Sensor 01", "type": "force", "unit": "N"},
    "TEMP-01": {"id": "TEMP-01", "name": "Temperature Sensor 01", "type": "temp", "unit": "°C"},
    "HUMIDITY-01": {"id": "HUMIDITY-01", "name": "Humidity Sensor 01", "type": "humidity", "unit": "%"}
}

FICTIONAL_SENSORS = ["STRAIN", "PRESSURE", "VIBRATION", "ACCELERATION", "DISPLACEMENT"]

SYSTEM_INSTRUCTION = """You are the official AI assistant for a Structural Health Monitoring (SHM) telemetry workstation.

You can answer general engineering, structural physics, data analysis, and software questions naturally.

When a question requires actual bridge telemetry, sequence numbers, record counts, sensor statistics, historical observations, packet gaps, or active sensor verification, you MUST obtain the information by invoking the provided read-only MongoDB tools.

MongoDB Atlas is the authoritative source of telemetry.
Never fabricate, estimate, interpolate, simulate, or invent sensor readings, timestamps, record counts, statistics, sequence numbers, or historical observations.

If MongoDB does not contain the required data for a requested period, explicitly state that no data is available in MongoDB for that period.

The physical deployment currently contains exactly three active sensors:
- FORCE-01 — Force (N)
- TEMP-01 — Temperature (°C)
- HUMIDITY-01 — Humidity (%)
Microcontroller: Arduino UNO-01.

Sensors like Strain, Pressure, Vibration, Acceleration, or Displacement are not active physical hardware nodes in this bridge deployment.

Google Sheets is a secondary cache/presentation layer and MUST NEVER be used as the chatbot telemetry source.

Never modify, insert, update, create, or delete telemetry data.

Use clear, professional, natural engineering language.

Do not expose internal tool function names, API keys, database credentials, or backend connection details to the user unless requested."""

# ------------------------------------------------------------------------------
# SENSOR ALIAS RESOLUTION
# ------------------------------------------------------------------------------
def resolve_sensor_alias(sensor_str: str) -> str:
    """Resolves natural language sensor names and aliases to canonical hardware IDs."""
    if not sensor_str:
        return "ALL"
    s = str(sensor_str).strip().upper()
    if any(k in s for k in ["FORCE", "LOAD", "WEIGHT", "F-01", "F1"]) or s == "FORCE-01":
        return "FORCE-01"
    if any(k in s for k in ["TEMP", "THERMAL", "THERM", "T-01", "T1", "TEMPERATURE"]) or s == "TEMP-01":
        return "TEMP-01"
    if any(k in s for k in ["HUMID", "RH", "H-01", "H1", "HUMIDITY"]) or s == "HUMIDITY-01":
        return "HUMIDITY-01"
    if s in ("ALL", "ANY", "*", "EVERY"):
        return "ALL"
    return s

# ------------------------------------------------------------------------------
# TIME RANGE PARSER
# ------------------------------------------------------------------------------
NUMBER_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
    "twenty-four": 24, "twenty four": 24, "thirty": 30
}

def parse_natural_time_range(query_text: str) -> tuple:
    """
    Parses natural language time queries into UTC ISO string time bounds and human label.
    Returns: (from_iso, to_iso, label_str)
    """
    q = query_text.lower()
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)

    # Convert word numbers to digits in query text
    for word, num in NUMBER_WORDS.items():
        q = re.sub(rf"\b{word}\b", str(num), q)

    if any(k in q for k in ["latest", "current", "now", "realtime", "real-time", "present"]):
        return None, None, "Latest Snapshot"

    if "today" in q:
        start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_ist.astimezone(timezone.utc).isoformat(), now_utc.isoformat(), "Today (IST)"

    if "yesterday" in q:
        yesterday_ist = now_ist - timedelta(days=1)
        start_ist = yesterday_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        end_ist = yesterday_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start_ist.astimezone(timezone.utc).isoformat(), end_ist.astimezone(timezone.utc).isoformat(), "Yesterday (IST)"

    if any(k in q for k in ["this week", "past week", "last week", "7 days", "7 d"]):
        from_dt = now_utc - timedelta(days=7)
        return from_dt.isoformat(), now_utc.isoformat(), "Last 7 days"

    if any(k in q for k in ["this month", "past month", "last month", "30 days", "30 d"]):
        from_dt = now_utc - timedelta(days=30)
        return from_dt.isoformat(), now_utc.isoformat(), "Last 30 days"

    # Match time regex: "X hours", "X hr", "X mins", "X min", "X days", "X d"
    time_match = re.search(r"(\d+)\s*(hours?|hrs?|h|minutes?|mins?|min|days?|d)\b", q)
    if time_match:
        val = int(time_match.group(1))
        unit = time_match.group(2).lower()
        if any(u in unit for u in ["min", "minute"]):
            from_dt = now_utc - timedelta(minutes=val)
            return from_dt.isoformat(), now_utc.isoformat(), f"Last {val} minutes"
        elif any(u in unit for u in ["day", "d"]):
            from_dt = now_utc - timedelta(days=val)
            return from_dt.isoformat(), now_utc.isoformat(), f"Last {val} days"
        else:
            from_dt = now_utc - timedelta(hours=val)
            return from_dt.isoformat(), now_utc.isoformat(), f"Last {val} hours"

    return None, None, "Latest Snapshot"

# ------------------------------------------------------------------------------
# SAFE READ-ONLY MONGODB QUERY TOOLS (AUTHORITATIVE SOURCE OF TRUTH)
# ------------------------------------------------------------------------------
def mongodb_tool_get_latest(sensor_id: str = None) -> dict:
    """Retrieves the latest sensor reading snapshot across active sensors from MongoDB Atlas."""
    sensor_norm = resolve_sensor_alias(sensor_id) if sensor_id else "ALL"
    res = TelemetryQueryEngine.get_latest_telemetry({})
    if res.get("status") != "success":
        return {
            "status": "empty",
            "message": "No telemetry readings are currently stored in MongoDB Atlas.",
            "sensors": ["FORCE-01", "TEMP-01", "HUMIDITY-01"]
        }

    doc_sensors = res.get("sensors", {})
    if sensor_norm != "ALL" and sensor_norm in CANONICAL_SENSORS:
        filtered_sensors = {sensor_norm: doc_sensors.get(sensor_norm)} if sensor_norm in doc_sensors else {}
    else:
        filtered_sensors = {k: v for k, v in doc_sensors.items() if k in CANONICAL_SENSORS}

    return {
        "status": "success",
        "arduino_id": res.get("arduino_id", "UNO-01"),
        "session_id": res.get("session_id"),
        "sequence": res.get("sequence"),
        "timestamp_ist": res.get("timestamp"),
        "sensors": filtered_sensors
    }

def mongodb_tool_get_stats(sensor_id: str = "ALL", from_time: str = None, to_time: str = None) -> dict:
    """Calculates exact min, max, mean, stddev metrics over MongoDB Atlas telemetry."""
    sensor_norm = resolve_sensor_alias(sensor_id)
    params = {}
    if sensor_norm != "ALL":
        params["sensor_id"] = sensor_norm
    if from_time:
        params["from"] = from_time
    if to_time:
        params["to"] = to_time

    res = TelemetryQueryEngine.query_stats(params)
    return res

def mongodb_tool_get_history(sensor_id: str = "ALL", from_time: str = None, to_time: str = None, limit: int = 100) -> dict:
    """Queries chronologically sorted historical telemetry range from MongoDB Atlas."""
    sensor_norm = resolve_sensor_alias(sensor_id)
    try:
        limit_val = min(max(1, int(limit)), 200)
    except (ValueError, TypeError):
        limit_val = 100

    params = {"limit": limit_val}
    if sensor_norm != "ALL":
        params["sensor_id"] = sensor_norm
    if from_time:
        params["from"] = from_time
    if to_time:
        params["to"] = to_time

    res = TelemetryQueryEngine.query_export(params)
    return res

def mongodb_tool_get_count(from_time: str = None, to_time: str = None) -> dict:
    """Counts total telemetry documents stored in MongoDB Atlas."""
    db = get_db()
    if db is None:
        return {"status": "error", "message": "MongoDB database unavailable"}

    query = {}
    from_dt = parse_iso_or_ms(from_time) if from_time else None
    to_dt = parse_iso_or_ms(to_time) if to_time else None

    if from_dt or to_dt:
        query["timestamp"] = {}
        if from_dt:
            query["timestamp"]["$gte"] = from_dt
        if to_dt:
            query["timestamp"]["$lte"] = to_dt

    try:
        telemetry_col = db["telemetry"]
        count = telemetry_col.count_documents(query)
        latest_doc = telemetry_col.find_one({}, sort=[("timestamp", -1)]) or {}
        max_seq = latest_doc.get("sequence")

        return {
            "status": "success",
            "total_documents": count,
            "latest_sequence": max_seq,
            "time_filter": {"from": from_time, "to": to_time}
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def mongodb_tool_get_gaps(limit: int = 200) -> dict:
    """Analyzes sequence gaps and missing packets in recent telemetry from MongoDB Atlas."""
    try:
        limit_val = min(max(10, int(limit)), 1000)
    except (ValueError, TypeError):
        limit_val = 200

    res = TelemetryQueryEngine.get_sensor_gaps({"limit": limit_val})
    return res

def mongodb_tool_get_active_sensors() -> dict:
    """Returns authoritative active physical sensor list."""
    return {
        "status": "success",
        "arduino_id": "UNO-01",
        "active_sensors": [
            {"id": "FORCE-01", "name": "Force Sensor 01", "parameter": "Force", "unit": "N"},
            {"id": "TEMP-01", "name": "Temperature Sensor 01", "parameter": "Temperature", "unit": "°C"},
            {"id": "HUMIDITY-01", "name": "Humidity Sensor 01", "parameter": "Humidity", "unit": "%"}
        ]
    }

# Tool execution dispatcher
READONLY_TOOLS_MAP = {
    "get_latest_sensor_data": mongodb_tool_get_latest,
    "get_sensor_statistics": mongodb_tool_get_stats,
    "get_sensor_history": mongodb_tool_get_history,
    "get_record_count": mongodb_tool_get_count,
    "get_sensor_gaps": mongodb_tool_get_gaps,
    "get_active_sensors": mongodb_tool_get_active_sensors
}

def execute_read_only_tool(tool_name: str, arguments: dict, default_from_iso: str = None, default_to_iso: str = None) -> dict:
    """Executes a named read-only MongoDB tool with sanitized arguments."""
    if tool_name not in READONLY_TOOLS_MAP:
        return {"status": "error", "message": f"Unknown tool: '{tool_name}'"}

    func = READONLY_TOOLS_MAP[tool_name]
    kwargs = dict(arguments or {})

    # Parameter normalization
    if "sensor_id" in kwargs:
        kwargs["sensor_id"] = resolve_sensor_alias(kwargs["sensor_id"])

    # Alias 'from' and 'to' parameter names
    if "from" in kwargs and "from_time" not in kwargs:
        kwargs["from_time"] = kwargs.pop("from")
    if "to" in kwargs and "to_time" not in kwargs:
        kwargs["to_time"] = kwargs.pop("to")

    # Supply natural time bounds if tool accepts time parameters and model omitted them
    if tool_name in ("get_sensor_statistics", "get_sensor_history", "get_record_count"):
        if default_from_iso and not kwargs.get("from_time"):
            kwargs["from_time"] = default_from_iso
        if default_to_iso and not kwargs.get("to_time"):
            kwargs["to_time"] = default_to_iso

    try:
        res = func(**kwargs)
        return res
    except TypeError:
        # Retry with filtered valid kwargs
        import inspect
        sig = inspect.signature(func)
        valid_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        return func(**valid_kwargs)
    except Exception as e:
        return {"status": "error", "message": f"Tool execution error in '{tool_name}': {e}"}

# OPENAI TOOL SCHEMAS
OPENAI_TOOLS_SCHEMA = [
    {
        "type": "function",
        "name": "get_latest_sensor_data",
        "description": "Returns the latest actual telemetry snapshot across active sensors (FORCE-01, TEMP-01, HUMIDITY-01) from MongoDB Atlas.",
        "parameters": {
            "type": "object",
            "properties": {
                "sensor_id": {
                    "type": "string",
                    "description": "Optional canonical sensor ID: FORCE-01, TEMP-01, HUMIDITY-01, or ALL."
                }
            }
        }
    },
    {
        "type": "function",
        "name": "get_sensor_statistics",
        "description": "Calculates exact document count, min, max, mean (average), and standard deviation metrics for bridge sensors over a specified time period in MongoDB Atlas.",
        "parameters": {
            "type": "object",
            "properties": {
                "sensor_id": {
                    "type": "string",
                    "description": "Canonical sensor ID: FORCE-01, TEMP-01, HUMIDITY-01, or ALL."
                },
                "from_time": {
                    "type": "string",
                    "description": "ISO 8601 formatted start timestamp (e.g. 2026-10-02T00:00:00+05:30)."
                },
                "to_time": {
                    "type": "string",
                    "description": "ISO 8601 formatted end timestamp (e.g. 2026-10-02T23:59:59+05:30)."
                }
            },
            "required": ["sensor_id"]
        }
    },
    {
        "type": "function",
        "name": "get_sensor_history",
        "description": "Retrieves chronologically sorted historical telemetry records from MongoDB Atlas within specified time bounds.",
        "parameters": {
            "type": "object",
            "properties": {
                "sensor_id": {
                    "type": "string",
                    "description": "Canonical sensor ID: FORCE-01, TEMP-01, HUMIDITY-01, or ALL."
                },
                "from_time": {
                    "type": "string",
                    "description": "ISO 8601 formatted start timestamp."
                },
                "to_time": {
                    "type": "string",
                    "description": "ISO 8601 formatted end timestamp."
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of records to return (1 to 200)."
                }
            }
        }
    },
    {
        "type": "function",
        "name": "get_record_count",
        "description": "Returns the exact count of telemetry documents stored in MongoDB Atlas, optionally filtered by time bounds.",
        "parameters": {
            "type": "object",
            "properties": {
                "from_time": {
                    "type": "string",
                    "description": "Optional ISO 8601 formatted start timestamp."
                },
                "to_time": {
                    "type": "string",
                    "description": "Optional ISO 8601 formatted end timestamp."
                }
            }
        }
    },
    {
        "type": "function",
        "name": "get_sensor_gaps",
        "description": "Analyzes recent sequence numbers to detect dropped telemetry packets or sequence gaps in MongoDB Atlas.",
        "parameters": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of sequence documents to inspect (default 200)."
                }
            }
        }
    },
    {
        "type": "function",
        "name": "get_active_sensors",
        "description": "Returns the authoritative list of active physical hardware sensors (FORCE-01, TEMP-01, HUMIDITY-01) deployed on the bridge DAQ node.",
        "parameters": {
            "type": "object",
            "properties": {}
        }
    }
]

# ------------------------------------------------------------------------------
# PROVIDER-INDEPENDENT AI SERVICE ARCHITECTURE
# ------------------------------------------------------------------------------
class BaseAIProvider:
    """Abstract base class for AI Providers (OpenAI, Gemini, Fallback Engine)."""
    def generate_response(self, user_query: str, chat_history: list, default_from_iso: str = None, default_to_iso: str = None) -> tuple:
        """
        Returns: (answer_text: str, executed_tools_meta: list)
        """
        raise NotImplementedError

class OpenAIProvider(BaseAIProvider):
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model or "gpt-4o-mini"

    def _call_responses_api(self, input_items: list, tools: list) -> dict:
        url = "https://api.openai.com/v1/responses"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model,
            "instructions": SYSTEM_INSTRUCTION,
            "input": input_items,
            "tools": tools
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        raise RuntimeError(f"Responses API error (HTTP {resp.status_code}): {resp.text[:300]}")

    def _call_chat_completions(self, messages: list, tools: list) -> dict:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": self.model,
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": 0.1,
            "max_tokens": 600
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        raise RuntimeError(f"Chat Completions API error (HTTP {resp.status_code}): {resp.text[:300]}")

    def generate_response(self, user_query: str, chat_history: list, default_from_iso: str = None, default_to_iso: str = None) -> tuple:
        # Build Chat Completions messages list
        messages = [{"role": "system", "content": SYSTEM_INSTRUCTION}]

        # Append bounded multi-turn conversation history
        for h in (chat_history or [])[-6:]:
            if isinstance(h, dict) and "user" in h and "bot" in h:
                messages.append({"role": "user", "content": h["user"]})
                messages.append({"role": "assistant", "content": h["bot"]})

        messages.append({"role": "user", "content": user_query})

        executed_tools_meta = []
        max_turns = 5

        # Execute Tool Calling Loop via OpenAI API
        for _ in range(max_turns):
            res_json = self._call_chat_completions(messages, OPENAI_TOOLS_SCHEMA)
            choice = res_json["choices"][0]
            msg = choice["message"]

            tool_calls = msg.get("tool_calls")
            if not tool_calls:
                # Model finished generation with final text response
                content = msg.get("content") or ""
                return content.strip(), executed_tools_meta

            # Append assistant message containing tool calls
            messages.append(msg)

            # Execute requested tool function(s) against MongoDB Atlas
            for tc in tool_calls:
                fn_info = tc.get("function", {})
                call_id = tc.get("id")
                fn_name = fn_info.get("name")

                try:
                    fn_args = json.loads(fn_info.get("arguments", "{}"))
                except Exception:
                    fn_args = {}

                tool_res = execute_read_only_tool(fn_name, fn_args, default_from_iso, default_to_iso)
                executed_tools_meta.append({
                    "tool": fn_name,
                    "args": fn_args,
                    "records_analyzed": tool_res.get("records_analyzed") or tool_res.get("total_documents") or (1 if tool_res.get("status") == "success" else 0)
                })

                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": json.dumps(tool_res, default=str)
                })

        return "Completed telemetry analysis.", executed_tools_meta

class GeminiProvider(BaseAIProvider):
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash"):
        self.api_key = api_key
        self.model = model or "gemini-2.5-flash"

    def generate_response(self, user_query: str, chat_history: list, default_from_iso: str = None, default_to_iso: str = None) -> tuple:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
        headers = {"Content-Type": "application/json"}

        # Perform grounded MongoDB telemetry tool lookup for Gemini context
        latest_res = mongodb_tool_get_latest()
        context_str = json.dumps(latest_res, indent=2, default=str)

        prompt_text = f"{SYSTEM_INSTRUCTION}\n\nAUTHORITATIVE MONGODB ATLAS TELEMETRY:\n{context_str}\n\nUser Question: {user_query}"

        payload = {
            "contents": [{"parts": [{"text": prompt_text}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 600}
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if resp.status_code == 200:
            res_json = resp.json()
            candidates = res_json.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if parts:
                    return parts[0].get("text", "").strip(), [{"tool": "get_latest_sensor_data", "records_analyzed": 1}]
            raise RuntimeError("Gemini returned empty candidate response")
        else:
            raise RuntimeError(f"Gemini API error (HTTP {resp.status_code}): {resp.text[:300]}")

class DeterministicMongoDBEngine(BaseAIProvider):
    """
    Emergency rule-based grounded telemetry formatter. Used ONLY when no external AI API key
    is configured or if AI provider connection fails. Clearly flags execution mode as fallback.
    """
    def generate_response(self, user_query: str, chat_history: list, default_from_iso: str = None, default_to_iso: str = None) -> tuple:
        q_lower = user_query.lower()
        sensor_filter = resolve_sensor_alias(user_query)

        is_active_query = any(k in q_lower for k in ["active sensor", "active sensors", "what sensors", "available sensor"])
        is_gap_query = any(k in q_lower for k in ["gap", "missing", "packet loss", "dropped"])
        is_count_query = any(k in q_lower for k in ["count", "how many", "stored", "total record", "number of record"])
        is_stat_query = any(k in q_lower for k in ["max", "maximum", "min", "minimum", "avg", "average", "mean", "std", "stddev", "statistic"])
        is_history_query = any(k in q_lower for k in ["history", "historical", "readings", "list", "show", "export"]) or default_from_iso is not None

        if is_active_query:
            tool_name = "get_active_sensors"
            res = mongodb_tool_get_active_sensors()
        elif is_gap_query:
            tool_name = "get_sensor_gaps"
            res = mongodb_tool_get_gaps()
        elif is_count_query:
            tool_name = "get_record_count"
            res = mongodb_tool_get_count(default_from_iso, default_to_iso)
        elif is_stat_query:
            tool_name = "get_sensor_statistics"
            res = mongodb_tool_get_stats(sensor_filter, default_from_iso, default_to_iso)
        elif is_history_query:
            tool_name = "get_sensor_history"
            res = mongodb_tool_get_history(sensor_filter, default_from_iso, default_to_iso)
        else:
            tool_name = "get_latest_sensor_data"
            res = mongodb_tool_get_latest(sensor_filter if sensor_filter != "ALL" else None)

        meta = [{"tool": tool_name, "records_analyzed": res.get("total_checked") or res.get("total_documents") or 1}]

        if res.get("status") != "success":
            return f"No telemetry data is currently available in MongoDB Atlas. ({res.get('message', '')})", meta

        if tool_name == "get_active_sensors":
            lines = ["The physical DAQ node (UNO-01) currently operates exactly three active sensors in MongoDB Atlas:\n"]
            for s in res.get("active_sensors", []):
                lines.append(f"• {s['id']} — {s['name']} ({s['parameter']}, {s['unit']})")
            return "\n".join(lines), meta

        elif tool_name == "get_latest_sensor_data":
            ts = res.get("timestamp_ist", "N/A")
            seq = res.get("sequence", "N/A")
            sensors = res.get("sensors", {})
            lines = [f"Latest MongoDB Atlas telemetry snapshot (Sequence {seq}, Time: {ts}):\n"]
            for s_id, s_info in sensors.items():
                val = s_info.get("value")
                meta_s = CANONICAL_SENSORS.get(s_id, {"unit": ""})
                val_str = f"{val:.2f} {meta_s['unit']}" if isinstance(val, (int, float)) else "N/A"
                lines.append(f"• {s_id}: {val_str} [{s_info.get('status', 'OFFLINE')}]")
            return "\n".join(lines), meta

        elif tool_name == "get_sensor_statistics":
            stats = res.get("statistics", {})
            if not stats:
                return "No telemetry data is available in MongoDB Atlas for that period.", meta
            lines = ["MongoDB Atlas telemetry statistical summary:\n"]
            for s_id, s_stats in stats.items():
                meta_s = CANONICAL_SENSORS.get(s_id, {"unit": ""})
                unit = meta_s.get("unit", "")
                lines.append(
                    f"{s_id}:\n"
                    f"  • Maximum: {s_stats.get('max')} {unit}\n"
                    f"  • Average: {s_stats.get('mean')} {unit}\n"
                    f"  • Minimum: {s_stats.get('min')} {unit}\n"
                    f"  • Std Deviation: {s_stats.get('stddev')} {unit}\n"
                    f"  • Records Analyzed: {s_stats.get('count', 0)}"
                )
            return "\n".join(lines), meta

        elif tool_name == "get_record_count":
            cnt = res.get("total_documents", 0)
            seq = res.get("latest_sequence", "N/A")
            return f"MongoDB Atlas Primary Database Status:\n• Total Stored Records: {cnt:,}\n• Latest Hardware Sequence: {seq}\n• Active Sensors: FORCE-01, TEMP-01, HUMIDITY-01", meta

        elif tool_name == "get_sensor_gaps":
            gaps = res.get("gaps_found", [])
            missing = res.get("missing_count", 0)
            total = res.get("total_checked", 0)
            if not gaps:
                return f"Sequence Continuity Check: Analyzed the last {total} sequence records in MongoDB Atlas. No packet gaps detected. Continuity is 100%.", meta
            return f"Sequence Continuity Check: Analyzed {total} sequence records in MongoDB Atlas. Found {len(gaps)} gap event(s) with {missing} missing packet(s).", meta

        return "MongoDB Atlas telemetry query executed.", meta

# ------------------------------------------------------------------------------
# PROVIDER ROUTER
# ------------------------------------------------------------------------------
def get_ai_provider() -> tuple:
    """
    Selects AI Provider based on AI_PROVIDER, OPENAI_API_KEY, and GEMINI_API_KEY env variables.
    Returns: (provider_instance: BaseAIProvider, provider_name: str, model_name: str)
    """
    provider_type = os.getenv("AI_PROVIDER", "openai").strip().lower()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"

    if provider_type == "openai" and openai_key:
        return OpenAIProvider(openai_key, openai_model), "OpenAI", openai_model
    elif provider_type == "gemini" and gemini_key:
        return GeminiProvider(gemini_key, gemini_model), "Google Gemini", gemini_model
    elif provider_type in ("auto", "any"):
        if openai_key:
            return OpenAIProvider(openai_key, openai_model), "OpenAI", openai_model
        elif gemini_key:
            return GeminiProvider(gemini_key, gemini_model), "Google Gemini", gemini_model

    return DeterministicMongoDBEngine(), "MongoDB Atlas Engine (Fallback)", "Read-Only Query Layer"

# ------------------------------------------------------------------------------
# MAIN CHATBOT ENTRY POINT
# ------------------------------------------------------------------------------
def handle_chat_query(payload: dict) -> dict:
    """
    Main entry point for POST /api/v1/chat/query.
    Processes natural language question, executes safe MongoDB tools, grounds answer in MongoDB data.
    """
    if not isinstance(payload, dict):
        payload = {}

    user_message = str(payload.get("message") or payload.get("query") or "").strip()
    chat_history = payload.get("history") or []

    if not user_message:
        return {
            "status": "error",
            "answer": "Please provide a valid question regarding bridge telemetry (e.g. 'What is the current force?' or 'What was the maximum temperature today?').",
            "data_source": "MongoDB Atlas Primary Database",
            "analysis_details": {"operation": "INVALID_INPUT", "execution_method": "Input Sanitizer"}
        }

    print(f"[CHATBOT] User Query: '{user_message}'")

    # Check for fictional sensor questions
    q_upper = user_message.upper()
    if any(fake in q_upper for fake in FICTIONAL_SENSORS):
        ans = (
            "The physical hardware DAQ node (UNO-01) currently operates exactly three active sensors in MongoDB Atlas:\n"
            "• FORCE-01 — Force (N)\n"
            "• TEMP-01 — Temperature (°C)\n"
            "• HUMIDITY-01 — Humidity (%)\n\n"
            "Sensors such as Strain, Pressure, Vibration, Acceleration, or Displacement are not active physical hardware nodes in this bridge deployment."
        )
        return {
            "status": "success",
            "answer": ans,
            "data_source": "MongoDB Atlas Primary Database",
            "analysis_details": {
                "operation": "get_active_sensors",
                "sensors": ["FORCE-01", "TEMP-01", "HUMIDITY-01"],
                "time_range": "N/A",
                "records_analyzed": 0,
                "execution_method": "Authoritative Hardware Registry Check"
            }
        }

    # Extract natural time bounds and sensor filter
    from_iso, to_iso, time_label = parse_natural_time_range(user_message)
    sensor_filter = resolve_sensor_alias(user_message)

    # Obtain AI Provider or Fallback Engine
    provider_inst, provider_name, model_name = get_ai_provider()
    print(f"[CHATBOT] Selected AI Provider: {provider_name} ({model_name})")

    try:
        answer, tools_meta = provider_inst.generate_response(user_message, chat_history, from_iso, to_iso)
        op_name = tools_meta[0]["tool"] if tools_meta else "general_assistant_query"
        records_cnt = sum(t.get("records_analyzed", 0) for t in tools_meta) if tools_meta else 0
        exec_method = f"{provider_name} ({model_name})"
    except Exception as e:
        print(f"[CHATBOT ERROR] AI Provider '{provider_name}' error: {e}. Falling back to Deterministic MongoDB Engine.")
        fallback = DeterministicMongoDBEngine()
        answer, tools_meta = fallback.generate_response(user_message, chat_history, from_iso, to_iso)
        op_name = tools_meta[0]["tool"] if tools_meta else "mongodb_fallback_query"
        records_cnt = sum(t.get("records_analyzed", 0) for t in tools_meta) if tools_meta else 0
        exec_method = "AI Provider Unavailable / Fallback Mode (DeterministicMongoDBEngine)"

    return {
        "status": "success",
        "answer": answer,
        "data_source": "MongoDB Atlas Primary Database",
        "analysis_details": {
            "operation": op_name,
            "sensors": [sensor_filter] if sensor_filter != "ALL" else ["FORCE-01", "TEMP-01", "HUMIDITY-01"],
            "time_range": time_label,
            "records_analyzed": records_cnt,
            "execution_method": exec_method
        }
    }

if __name__ == "__main__":
    print("Testing chat_service.py...")
    test_res = handle_chat_query({"message": "What is the current force?"})
    print("Test Response:", json.dumps(test_res, indent=2))
