#!/usr/bin/env python3
import json
from chat_service import handle_chat_query

test_messages = [
    # Conversational tests
    "Hi",
    "Hello",
    "Hey",
    "Good morning",
    "How are you?",
    "Thanks",
    "Bye",
    "What can you do?",
    "Help",
    # Telemetry tests
    "What is the current force?",
    "What is the current temperature?",
    "What is the current humidity?",
    "What is the maximum force today?",
    "How many records are stored?",
    "Are there any packet gaps?",
    "What sensors are active?",
    "Show the last 2 hours of temperature data",
    # General engineering tests
    "What is structural health monitoring?",
    "What is a load cell?"
]

results = []
for msg in test_messages:
    res = handle_chat_query({"message": msg})
    details = res.get("analysis_details", {})
    results.append({
        "message": msg,
        "status": res.get("status"),
        "operation": details.get("operation"),
        "records_analyzed": details.get("records_analyzed"),
        "execution_method": details.get("execution_method"),
        "answer_snippet": res.get("answer", "")[:120].replace("\n", " ")
    })

with open("test_output.json", "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2)

print("Test suite execution complete. Results saved to test_output.json.")
