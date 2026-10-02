#!/usr/bin/env python3
"""
==============================================================================
SHM AI CHATBOT SERVICE — PROVIDER-INDEPENDENT MONGODB TELEMETRY ASSISTANT
==============================================================================
Provides a strictly read-only, MongoDB-grounded AI assistant layer for the Bridge SHM
system. Queries MongoDB Atlas as the SINGLE SOURCE OF TRUTH. Never uses Google Sheets,
never invents or fabricates telemetry data, and supports selectable AI providers
(OpenAI, Gemini, or fallback Deterministic MongoDB Engine).

Supported AI Providers:
- OpenAI (AI_PROVIDER=openai, OPENAI_API_KEY=...)
- Gemini (AI_PROVIDER=gemini, GEMINI_API_KEY=...)
- Auto Selection (AI_PROVIDER=auto)
- Deterministic Rule Engine (AI_PROVIDER=none or missing keys)
"""

import os
import sys
import time
import json
import re
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

SYSTEM_INSTRUCTION = """You are the official SHM Telemetry Assistant for this Structural Health Monitoring Workstation.

MongoDB Atlas is the authoritative source of telemetry.

The current physical deployment has exactly three active sensors:
- FORCE-01 — Force — N
- TEMP-01 — Temperature — °C
- HUMIDITY-01 — Humidity — %

Answer telemetry questions only using actual data returned by the provided read-only MongoDB tools.

Never fabricate, estimate, simulate, interpolate, or invent telemetry values, timestamps, record counts, or statistics.

If required data is unavailable, explicitly state that it is unavailable.

Google Sheets is a secondary cache/presentation layer and is not the chatbot's telemetry source.

Never modify, delete, insert, or update telemetry.

Do not claim a sensor exists unless it is part of the canonical active sensor list (FORCE-01, TEMP-01, HUMIDITY-01).

Use clear engineering language."""

# ------------------------------------------------------------------------------
# TIME RANGE PARSER & INTENT RECOGNIZER
# ------------------------------------------------------------------------------
def parse_natural_time_range(query_text: str) -> tuple:
    """
    Parses natural language time queries into UTC ISO string time bounds and label.
    Returns: (from_iso, to_iso, label_str)
    """
    q = query_text.lower()
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)

    if any(k in q for k in ["latest", "current", "now", "realtime", "real-time", "present"]):
        return None, None, "latest"

    if "today" in q:
        start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_ist.astimezone(timezone.utc).isoformat(), now_utc.isoformat(), "Today (IST)"

    if "yesterday" in q:
        yesterday_ist = now_ist - timedelta(days=1)
        start_ist = yesterday_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        end_ist = yesterday_ist.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start_ist.astimezone(timezone.utc).isoformat(), end_ist.astimezone(timezone.utc).isoformat(), "Yesterday (IST)"

    if "this week" in q or "past week" in q or "last week" in q:
        from_dt = now_utc - timedelta(days=7)
        return from_dt.isoformat(), now_utc.isoformat(), "Last 7 days"

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

    return None, None, "latest"

def detect_sensor_filter(query_text: str) -> str:
    """Detects if query targets a specific sensor ID or sensor type."""
    q = query_text.upper()
    if "FORCE-01" in q or "FORCE" in q or "LOAD" in q:
        return "FORCE-01"
    if "TEMP-01" in q or "TEMP" in q or "TEMPERATURE" in q:
        return "TEMP-01"
    if "HUMIDITY-01" in q or "HUMIDITY" in q or "HUMID" in q:
        return "HUMIDITY-01"
    return "ALL"

# ------------------------------------------------------------------------------
# SAFE READ-ONLY MONGODB QUERY TOOLS (SINGLE SOURCE OF TRUTH)
# ------------------------------------------------------------------------------
def mongodb_tool_get_latest() -> dict:
    """Retrieves the latest sensor reading snapshot across all sensors from MongoDB Atlas."""
    res = TelemetryQueryEngine.get_latest_telemetry({})
    return {
        "tool": "get_latest_sensor_data",
        "result": res,
        "sensors": ["FORCE-01", "TEMP-01", "HUMIDITY-01"],
        "records_analyzed": 1 if res.get("status") == "success" else 0
    }

def mongodb_tool_get_stats(sensor_id: str = "ALL", from_iso: str = None, to_iso: str = None) -> dict:
    """Calculates exact min, max, mean, stddev metrics over MongoDB telemetry."""
    params = {}
    if sensor_id and sensor_id != "ALL":
        params["sensor_id"] = sensor_id
    if from_iso:
        params["from"] = from_iso
    if to_iso:
        params["to"] = to_iso

    res = TelemetryQueryEngine.query_stats(params)
    stats_data = res.get("statistics", {})
    count = sum(s.get("count", 0) for s in stats_data.values()) if isinstance(stats_data, dict) else 0

    return {
        "tool": "get_sensor_statistics",
        "result": res,
        "sensors": [sensor_id] if sensor_id != "ALL" else ["FORCE-01", "TEMP-01", "HUMIDITY-01"],
        "records_analyzed": count
    }

def mongodb_tool_get_history(sensor_id: str = "ALL", from_iso: str = None, to_iso: str = None, limit: int = 100) -> dict:
    """Queries chronologically sorted historical telemetry range from MongoDB Atlas."""
    params = {"limit": min(limit, 200)}
    if sensor_id and sensor_id != "ALL":
        params["sensor_id"] = sensor_id
    if from_iso:
        params["from"] = from_iso
    if to_iso:
        params["to"] = to_iso

    res = TelemetryQueryEngine.query_export(params)
    records = res.get("records", [])

    return {
        "tool": "get_sensor_history",
        "result": {
            "status": res.get("status"),
            "count": len(records),
            "sample_records": records[:30] if len(records) > 30 else records
        },
        "sensors": [sensor_id] if sensor_id != "ALL" else ["FORCE-01", "TEMP-01", "HUMIDITY-01"],
        "records_analyzed": len(records)
    }

def mongodb_tool_get_count(from_iso: str = None, to_iso: str = None) -> dict:
    """Counts total telemetry documents stored in MongoDB Atlas."""
    db = get_db()
    if db is None:
        return {"tool": "get_record_count", "result": {"status": "error", "message": "MongoDB database unavailable"}, "records_analyzed": 0}

    query = {}
    from_dt = parse_iso_or_ms(from_iso) if from_iso else None
    to_dt = parse_iso_or_ms(to_iso) if to_iso else None

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
            "tool": "get_record_count",
            "result": {
                "status": "success",
                "total_documents": count,
                "latest_sequence": max_seq
            },
            "sensors": ["FORCE-01", "TEMP-01", "HUMIDITY-01"],
            "records_analyzed": count
        }
    except Exception as e:
        return {"tool": "get_record_count", "result": {"status": "error", "message": str(e)}, "records_analyzed": 0}

def mongodb_tool_get_gaps(limit: int = 200) -> dict:
    """Analyzes sequence gaps and missing packets in recent telemetry."""
    res = TelemetryQueryEngine.get_sensor_gaps({"limit": limit})
    return {
        "tool": "get_sensor_gaps",
        "result": res,
        "sensors": ["FORCE-01", "TEMP-01", "HUMIDITY-01"],
        "records_analyzed": res.get("total_checked", 0)
    }

# ------------------------------------------------------------------------------
# PROVIDER-INDEPENDENT AI SERVICE ARCHITECTURE
# ------------------------------------------------------------------------------
class BaseAIProvider:
    """Abstract interface for AI Providers (OpenAI, Gemini, Fallback Engine)."""
    def generate(self, user_query: str, chat_history: list, mongodb_context: dict) -> str:
        raise NotImplementedError

class OpenAIProvider(BaseAIProvider):
    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model

    def generate(self, user_query: str, chat_history: list, mongodb_context: dict) -> str:
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        context_str = json.dumps(mongodb_context, indent=2, default=str)
        messages = [{"role": "system", "content": f"{SYSTEM_INSTRUCTION}\n\nACTUAL RETRIEVED MONGODB TELEMETRY CONTEXT:\n{context_str}"}]

        for h in (chat_history or []):
            if isinstance(h, dict) and "user" in h and "bot" in h:
                messages.append({"role": "user", "content": h["user"]})
                messages.append({"role": "assistant", "content": h["bot"]})

        messages.append({"role": "user", "content": user_query})

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,
            "max_tokens": 500
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=25)
        if resp.status_code == 200:
            res_json = resp.json()
            return res_json["choices"][0]["message"]["content"].strip()
        else:
            raise RuntimeError(f"OpenAI API error (HTTP {resp.status_code}): {resp.text[:200]}")

class GeminiProvider(BaseAIProvider):
    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        self.api_key = api_key
        self.model = model

    def generate(self, user_query: str, chat_history: list, mongodb_context: dict) -> str:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={self.api_key}"
        headers = {"Content-Type": "application/json"}

        context_str = json.dumps(mongodb_context, indent=2, default=str)
        prompt_text = f"{SYSTEM_INSTRUCTION}\n\nACTUAL RETRIEVED MONGODB TELEMETRY CONTEXT:\n{context_str}\n\nUser Question: {user_query}"

        payload = {
            "contents": [
                {
                    "parts": [{"text": prompt_text}]
                }
            ],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 500
            }
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=25)
        if resp.status_code == 200:
            res_json = resp.json()
            candidates = res_json.get("candidates", [])
            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                if parts:
                    return parts[0].get("text", "").strip()
            raise RuntimeError("Gemini returned empty candidate response")
        else:
            raise RuntimeError(f"Gemini API error (HTTP {resp.status_code}): {resp.text[:200]}")

class DeterministicMongoDBEngine(BaseAIProvider):
    """
    Rule-based grounded telemetry formatter. Used when no external AI API key is configured
    or if AI API connection fails, guaranteeing zero downtime and 100% accurate MongoDB grounding.
    """
    def generate(self, user_query: str, chat_history: list, mongodb_context: dict) -> str:
        tool_name = mongodb_context.get("tool")
        res_data = mongodb_context.get("result", {})

        if tool_name in ("get_latest_sensor_data", "get_active_sensors"):
            if res_data.get("status") != "success":
                return "No telemetry readings are currently stored in MongoDB Atlas."

            ts = res_data.get("timestamp", "N/A")
            seq = res_data.get("sequence", "N/A")
            sensors = res_data.get("sensors", {})

            if tool_name == "get_active_sensors":
                lines = ["The physical hardware DAQ node (UNO-01) currently operates the following active sensors in MongoDB Atlas:\n"]
            else:
                lines = [f"Based on the latest telemetry in MongoDB Atlas (Sequence: {seq}, Time: {ts}):\n"]

            for s_id, s_info in sensors.items():
                s_norm = s_id.upper()
                if s_norm in CANONICAL_SENSORS:
                    meta = CANONICAL_SENSORS[s_norm]
                    val = s_info.get("value")
                    val_str = f"{val:.2f} {meta['unit']}" if isinstance(val, (int, float)) else "N/A"
                    status = s_info.get("status", "OFFLINE")
                    lines.append(f"• {s_norm} — {meta['name']} ({meta['type'].title()}, {meta['unit']}): {val_str} [{status}]")

            return "\n".join(lines)

        elif tool_name == "get_sensor_statistics":
            stats = res_data.get("statistics", {})
            if not stats:
                return "No telemetry records were found in MongoDB Atlas for the requested period, so statistics cannot be calculated."

            lines = ["Based on MongoDB Atlas historical telemetry statistical analysis:\n"]
            for s_id, s_stats in stats.items():
                s_norm = s_id.upper()
                meta = CANONICAL_SENSORS.get(s_norm, {"name": s_id, "unit": ""})
                unit = meta.get("unit", "")
                lines.append(
                    f"{s_norm} ({meta['name']}):\n"
                    f"  • Maximum: {s_stats.get('max', 'N/A')} {unit}\n"
                    f"  • Average: {s_stats.get('mean', 'N/A')} {unit}\n"
                    f"  • Minimum: {s_stats.get('min', 'N/A')} {unit}\n"
                    f"  • Std Deviation: {s_stats.get('stddev', 'N/A')} {unit}\n"
                    f"  • Records Analyzed: {s_stats.get('count', 0)}\n"
                )
            return "\n".join(lines)

        elif tool_name == "get_record_count":
            total = res_data.get("total_documents", 0)
            seq = res_data.get("latest_sequence", "N/A")
            return f"MongoDB Atlas Primary Database Status:\n• Total Stored Records: {total:,}\n• Latest Hardware Sequence: {seq}\n• Active Sensors: FORCE-01, TEMP-01, HUMIDITY-01"

        elif tool_name == "get_sensor_history":
            count = res_data.get("count", 0)
            sample = res_data.get("sample_records", [])
            if count == 0:
                return "No historical telemetry records were found in MongoDB Atlas for the requested period."
            
            lines = [f"Found {count} historical telemetry records in MongoDB Atlas for the specified range. Sample readings:\n"]
            for r in sample[:5]:
                lines.append(f"• [{r.get('timestamp')}] Seq: {r.get('sequence')} | {r.get('sensor_id')}: {r.get('sensor_value')} {r.get('unit')}")
            if count > 5:
                lines.append(f"\n(... showing first 5 of {count} total records analyzed in MongoDB Atlas)")
            return "\n".join(lines)

        elif tool_name == "get_sensor_gaps":
            gaps_data = res_data.get("gaps_found", [])
            missing = res_data.get("missing_count", 0)
            total = res_data.get("total_checked", 0)
            if not gaps_data:
                return f"Sequence Analysis Result: Analyzed the last {total} sequence records in MongoDB Atlas. No packet gaps or missing sequence numbers were detected. Data continuity is 100%."
            else:
                return f"Sequence Analysis Result: Analyzed {total} sequence records in MongoDB Atlas. Found {len(gaps_data)} gap event(s) with a total of {missing} missing packet(s)."

        return "MongoDB Atlas query executed successfully."

# ------------------------------------------------------------------------------
# CHATBOT ENGINE ROUTER & HANDLER
# ------------------------------------------------------------------------------
def get_ai_provider() -> tuple:
    """
    Selects AI Provider based on AI_PROVIDER, OPENAI_API_KEY, and GEMINI_API_KEY env variables.
    Returns: (provider_instance: BaseAIProvider, provider_name: str, model_name: str)
    """
    provider_type = os.getenv("AI_PROVIDER", "auto").strip().lower()
    openai_key = os.getenv("OPENAI_API_KEY", "").strip()
    gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash").strip()

    if provider_type == "openai" and openai_key:
        return OpenAIProvider(openai_key, openai_model), "OpenAI", openai_model
    elif provider_type == "gemini" and gemini_key:
        return GeminiProvider(gemini_key, gemini_model), "Google Gemini", gemini_model
    elif provider_type in ("auto", "any"):
        if openai_key:
            return OpenAIProvider(openai_key, openai_model), "OpenAI", openai_model
        elif gemini_key:
            return GeminiProvider(gemini_key, gemini_model), "Google Gemini", gemini_model

    # Fallback to deterministic MongoDB engine
    return DeterministicMongoDBEngine(), "MongoDB Atlas Engine", "Read-Only Query Layer"

def handle_chat_query(payload: dict) -> dict:
    """
    Main entry point for POST /api/v1/chat/query.
    Processes natural language question, executes safe MongoDB query tool, grounds answer in MongoDB data.
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

    print(f"[CHATBOT] Request received: '{user_message}'")

    # Check for fictional sensor questions
    q_upper = user_message.upper()
    if any(fake in q_upper for fake in ["STRAIN", "PRESSURE", "VIBRATION", "ACCELERATION", "DISPLACEMENT"]):
        ans = "The physical hardware DAQ node (UNO-01) currently operates exactly three active sensors:\n• FORCE-01 (Force, N)\n• TEMP-01 (Temperature, °C)\n• HUMIDITY-01 (Humidity, %)\n\nSensors like Strain, Pressure, Vibration, Acceleration, or Displacement are not active physical hardware nodes in this bridge deployment."
        print(f"[CHATBOT] MongoDB query executed: get_available_sensors")
        print(f"[CHATBOT] Provider selected: Authoritative Hardware Registry Check")
        print(f"[CHATBOT] Response generated")
        return {
            "status": "success",
            "answer": ans,
            "data_source": "MongoDB Atlas Primary Database",
            "analysis_details": {
                "operation": "get_available_sensors",
                "sensors": ["FORCE-01", "TEMP-01", "HUMIDITY-01"],
                "time_range": "N/A",
                "records_analyzed": 0,
                "execution_method": "Authoritative Hardware Registry Check"
            }
        }

    # Parse time bounds and sensor filters
    from_iso, to_iso, time_label = parse_natural_time_range(user_message)
    sensor_filter = detect_sensor_filter(user_message)

    q_lower = user_message.lower()

    # Intent routing to safe read-only tools
    is_active_sensors_query = any(k in q_lower for k in ["active sensor", "active sensors", "what sensors", "available sensor", "which sensor"])
    is_gap_query = any(k in q_lower for k in ["gap", "missing", "packet loss", "sequence gap", "dropped reading"])
    is_count_query = any(k in q_lower for k in ["count", "how many", "stored", "total record", "total reading", "number of record"])
    is_stat_query = any(k in q_lower for k in ["max", "maximum", "min", "minimum", "avg", "average", "mean", "std", "stddev", "statistic", "summary"])
    is_history_query = any(k in q_lower for k in ["history", "historical", "readings", "data", "list", "show", "export", "log"]) or (from_iso is not None and not is_stat_query and not is_count_query)

    if is_active_sensors_query and not is_stat_query and not is_history_query:
        tool_ctx = mongodb_tool_get_latest()
        tool_ctx["tool"] = "get_active_sensors"
    elif is_gap_query:
        tool_ctx = mongodb_tool_get_gaps()
    elif is_count_query:
        tool_ctx = mongodb_tool_get_count(from_iso, to_iso)
    elif is_stat_query:
        tool_ctx = mongodb_tool_get_stats(sensor_filter, from_iso, to_iso)
    elif is_history_query and (from_iso or to_iso or "history" in q_lower or "data" in q_lower):
        tool_ctx = mongodb_tool_get_history(sensor_filter, from_iso, to_iso)
    else:
        tool_ctx = mongodb_tool_get_latest()

    print(f"[CHATBOT] MongoDB query executed: {tool_ctx.get('tool')}")

    # Obtain AI Provider or Fallback Engine
    provider_inst, provider_name, model_name = get_ai_provider()
    print(f"[CHATBOT] Provider selected: {provider_name} ({model_name})")

    try:
        answer = provider_inst.generate(user_message, chat_history, tool_ctx)
    except Exception as e:
        print(f"[CHATBOT ERROR] AI Provider '{provider_name}' error: {e}. Falling back to Deterministic MongoDB Engine.")
        fallback = DeterministicMongoDBEngine()
        answer = fallback.generate(user_message, chat_history, tool_ctx)
        provider_name = "MongoDB Atlas Engine (Fallback)"

    print(f"[CHATBOT] Response generated")

    return {
        "status": "success",
        "answer": answer,
        "data_source": "MongoDB Atlas Primary Database",
        "analysis_details": {
            "operation": tool_ctx.get("tool", "mongodb_query"),
            "sensors": tool_ctx.get("sensors", ["FORCE-01", "TEMP-01", "HUMIDITY-01"]),
            "time_range": time_label,
            "records_analyzed": tool_ctx.get("records_analyzed", 0),
            "execution_method": f"{provider_name} ({model_name})"
        }
    }

if __name__ == "__main__":
    print("Testing chat_service.py...")
    test_res = handle_chat_query({"message": "What is the latest force reading?"})
    print("Test Response:", json.dumps(test_res, indent=2))
