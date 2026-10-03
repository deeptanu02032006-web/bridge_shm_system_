import py_compile
import json
import sys

print("Compiling chat_service.py...")
py_compile.compile("chat_service.py", doraise=True)
print("chat_service.py compiled successfully!")

print("Compiling query_api.py...")
py_compile.compile("query_api.py", doraise=True)
print("query_api.py compiled successfully!")

from chat_service import handle_chat_query

test_cases = [
    ("Hi", "CONVERSATIONAL_RESPONSE", 0),
    ("Hello", "CONVERSATIONAL_RESPONSE", 0),
    ("Hey", "CONVERSATIONAL_RESPONSE", 0),
    ("Good morning", "CONVERSATIONAL_RESPONSE", 0),
    ("How are you?", "CONVERSATIONAL_RESPONSE", 0),
    ("Thanks", "CONVERSATIONAL_RESPONSE", 0),
    ("Bye", "CONVERSATIONAL_RESPONSE", 0),
    ("What can you do?", "CONVERSATIONAL_RESPONSE", 0),
    ("Help", "CONVERSATIONAL_RESPONSE", 0),
    ("What is structural health monitoring?", "GENERAL_ENGINEERING_FALLBACK", 0),
    ("What is a load cell?", "GENERAL_ENGINEERING_FALLBACK", 0),
    ("What is the current force?", "get_latest_sensor_data", 1),
    ("What is the current temperature?", "get_latest_sensor_data", 1),
    ("What is the current humidity?", "get_latest_sensor_data", 1),
    ("What is the maximum force today?", "get_sensor_statistics", None),
    ("How many records are stored?", "get_record_count", None),
    ("Are there any packet gaps?", "get_sensor_gaps", None),
    ("What sensors are active?", "get_active_sensors", None),
    ("Show the last 2 hours of temperature data", "get_sensor_history", None),
]

summary = []
all_passed = True
for msg, expected_op, expected_records in test_cases:
    res = handle_chat_query({"message": msg})
    details = res.get("analysis_details", {})
    op = details.get("operation")
    records = details.get("records_analyzed")
    ans = res.get("answer", "")
    
    op_ok = (op == expected_op)
    rec_ok = (expected_records is None or records == expected_records)
    passed = op_ok and rec_ok
    if not passed:
        all_passed = False
        
    summary.append({
        "query": msg,
        "passed": passed,
        "operation": op,
        "expected_op": expected_op,
        "records_analyzed": records,
        "expected_records": expected_records,
        "answer_preview": ans[:100].replace("\n", " ")
    })

output_report = {
    "all_passed": all_passed,
    "summary": summary
}

with open("validation_report.json", "w", encoding="utf-8") as f:
    json.dump(output_report, f, indent=2)

print(f"Validation finished. All passed: {all_passed}")
