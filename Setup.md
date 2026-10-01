# MongoDB Atlas Setup & Query API Configuration Guide

This document provides step-by-step instructions for configuring **MongoDB Atlas** as the Primary Database and operating the **Secure Read-Only MongoDB Query Layer (`query_api.py`)** for historical analytics and the future AI chatbot.

---

## 1. Required Manual Configuration Steps in MongoDB Atlas

Follow these steps to provision and configure your MongoDB Atlas cluster:

1. **Log in to MongoDB Atlas**:
   - Go to [MongoDB Atlas](https://cloud.mongodb.com/) and log in (or create an account).

2. **Create / Select Database User**:
   - Navigate to **Security** → **Database Access**.
   - Click **Add New Database User**.
   - Select **Password Authentication**.
   - Set a Username (e.g. `shm_admin`) and a secure Password.
   - Under **Database User Privileges**, select **Read and write to any database** (or restrict specifically to `shm_bridge_db`).
   - Click **Add User**.

3. **Configure Network Access (IP Whitelist)**:
   - Navigate to **Security** → **Network Access**.
   - Click **Add IP Address**.
   - Add the public IP address of the gateway machine running `daq_bridge.py` and `query_api.py` (or click **Allow Access from Anywhere** `0.0.0.0/0` for dynamic IP testing).
   - Click **Confirm**.

4. **Obtain MongoDB Atlas Connection String**:
   - Navigate to **Deployment** → **Database**.
   - Click **Connect** next to your cluster.
   - Select **Drivers** (Python).
   - Copy the standard `mongodb+srv://` connection string.
     - Example format: `mongodb+srv://<username>:<password>@cluster0.mongodb.net/?retryWrites=true&w=majority`

---

## 2. Local Environment Configuration (`.env`)

In the project directory (`d:\NEW_BRIDGE_SHM`), copy [.env.example](file:///d:/NEW_BRIDGE_SHM/.env.example) to [.env](file:///d:/NEW_BRIDGE_SHM/.env) and set credentials:

```env
# MongoDB Atlas Connection URI
MONGODB_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority

# Database Name
MONGODB_DB_NAME=shm_bridge_db

# Query API Configuration (Round 5)
QUERY_API_PORT=5000
QUERY_API_KEY=SHM_SECURE_READ_KEY_2026

# Hardware & Serial Port Configuration
ARDUINO_ID=UNO-01
SERIAL_PORT=COM6
BAUD_RATE=115200

# Google Apps Script Web App URL (Secondary Cache)
WEB_APP_URL=https://script.google.com/macros/s/AKfycbxGnbuwnPdagDayubkzzBSqzNsfPq4FEP_eAWLGzE-FtJjDeqDpYwFHI6HTTOkH5TuQ_A/exec
```

## 3. Secure Read-Only MongoDB Query API Endpoints (Round 5)

The query layer runs on port `5000` (or standalone via `python query_api.py`).

| Endpoint | Method | Allowed Query Parameters | Description |
| :--- | :--- | :--- | :--- |
| `/api/v1/telemetry/latest` | `GET` | None | Returns latest sensor readings across active sensors. |
| `/api/v1/telemetry/range` | `GET` | `from`, `to`, `sensor_id`, `sensor_type`, `limit` (max 5000) | Bounded historical telemetry query. |
| `/api/v1/telemetry/stats` | `GET` | `from`, `to`, `sensor_id` | Full-range statistical aggregations (min, max, mean, stddev, count). |
| `/api/v1/telemetry/gaps` | `GET` | `arduino_id`, `session_id`, `limit` | Session-aware hardware sequence gap detection (`arduino_id + session_id`). |
| `/api/v1/telemetry/correlation`| `GET` | `sensor1`, `sensor2`, `from`, `to` | Calculates Pearson correlation ($r$) and covariance over MongoDB history. |
| `/api/v1/sync/status` | `GET` | `arduino_id` | Returns primary DB status and secondary cache sync state for configured Arduino. |

### Security & Safeguards
- **Zero Credential Exposure**: Credentials exist exclusively in `.env` on the server/gateway machine. The browser NEVER receives `QUERY_API_KEY`.
- **Session Authentication Proxy**: Google Apps Script `proxyMongoApi()` validates the user's `currentUserSession.token` before forwarding queries to `query_api.py`.
- **Read-Only Access**: Enforces read operations (`find`, `aggregate`). Write or delete commands are strictly impossible over the API.
- **Query Injection Prevention**: Sanitizes input strings; rejects BSON injection syntax (`$where`, raw queries).
- **Result Limits**: Mandatory query bounds and standard max limit of 5,000 raw documents per request.

---

## 4. Complete Website Feature Dependency Map

| Website Feature | Data Source | Function / API Endpoint | Database / Cache Target | Retention / Window |
| :--- | :--- | :--- | :--- | :--- |
| **Overview Dashboard** | Secondary Cache | `getTelemetry` / `doGet` | Google Sheets (`TelemetryData`) | Rolling 10 Minutes (~600 rows) |
| **Live Sensors View** | Secondary Cache | `getTelemetry` / `doGet` | Google Sheets (`TelemetryData`) | Live / Rolling 10 Minutes |
| **Sensor Registry** | Primary & Cache | `fetchSensorRegistry` / `syncAllSensors` | Sheets (`SensorRegistry`) & MongoDB (`sensors`) | Permanent Registry (Registry metadata prioritized) |
| **Real-Time Polling** | Secondary Cache | `fetchTelemetry()` every 5s | Google Sheets (`TelemetryData`) | Rolling 10 Minutes |
| **Historical Analytics** | Primary Database | `GET /api/v1/telemetry/stats` | MongoDB Atlas (`telemetry`) | 100% Complete History |
| **Sequence Gap Audit** | Primary Database | `GET /api/v1/telemetry/gaps` | MongoDB Atlas (`telemetry`) | Session-Aware Sequence History (`arduino_id + session_id`) |
| **Alerts & Thresholds** | Client & Cache | `evaluateAlertsAndEvents()` | Local Storage & Sheets (`TelemetryData`) | Active Live Window (kN unit matching) |
| **Event History Audit** | Primary & Cache | `fetchEventHistoryFromSheet` | Sheets (`EventHistoryLog`) & MongoDB (`events`) | Permanent Log |
| **User Auth & Sessions** | Primary & Cache | `loginUser`, `validateSession` | Sheets (`UserRegistry`, `SessionRegistry`) | Durable Sessions |
| **Sync Status Banner** | Primary & Cache | `getSyncStatus` / `GET /api/v1/sync/status` | MongoDB (`sync_metadata`) | Real-time 120s Sync State |

---

## 5. Install Dependencies & Launch

```bash
pip install pymongo dnspython python-dotenv pyserial requests
```

Launch the DAQ Bridge (automatically launches serial reader, MongoDB uploader, 120-second (2-minute) sync worker, and Query API):

```bash
python daq_bridge.py
```

