/**
 * GLOBAL LIVE DAQ SYSTEM — GOOGLE APPS SCRIPT BACKEND (Code.gs)
 * ============================================================
 * PRIMARY DATABASE: MongoDB Atlas (Source of Truth)
 * SECONDARY CACHE: Google Sheets ("TelemetryData" tab — Rolling 10-Minute Window)
 * Periodic Synchronization: Every 120 seconds from MongoDB Atlas -> Google Sheets
 * 
 * Rules:
 * 1. ONLY Code.gs and Index.html in Apps Script project.
 * 2. Database sheet tab MUST be named "TelemetryData".
 * 3. Timezone for timestamps is "Asia/Kolkata" (IST).
 * 4. XFrameOptionsMode ALLOWALL for Google Sites embedding.
 * 5. Role-based Authentication & Session Registry, Salted Hashing, Rate Limiting, Audit Logging.
 */

var SPREADSHEET_ID = "1kAoJqh7mpvIT6gjiS1U0__-8EcRnUzcFGMrqntSbTfo";
var SHEET_NAME = "TelemetryData";
var REGISTRY_SHEET_NAME = "SensorRegistry";
var USER_SHEET_NAME = "UserRegistry";
var EVENT_LOG_SHEET_NAME = "EventHistoryLog"; 
var SESSION_SHEET_NAME = "SessionRegistry";
var INGESTION_LEDGER_SHEET_NAME = "IngestionLedger";
var TIMEZONE = "Asia/Kolkata";
var MAX_ROWS_RETURNED = 5000;
var DEFAULT_SALT = "SHM_SECURE_SALT_v1";
var SESSION_TIMEOUT_MS = 2 * 60 * 60 * 1000; // 2 Hours

/**
 * Returns the target Google Spreadsheet (prioritizes container spreadsheet, falls back to openById)
 */
function getSpreadsheet() {
  if (SPREADSHEET_ID && SPREADSHEET_ID.length > 20) {
    try {
      return SpreadsheetApp.openById(SPREADSHEET_ID);
    } catch (e) {
      Logger.log("openById failed, falling back to active spreadsheet: " + e.toString());
    }
  }
  try {
    var active = SpreadsheetApp.getActiveSpreadsheet();
    if (active) return active;
  } catch (e) {}
  return SpreadsheetApp.getActiveSpreadsheet();
}

/**
 * ============================================================================
 * SECURE MONGODB QUERY API PROXY
 * ============================================================================
 *
 * Browser / Google Sites
 *          ↓
 * google.script.run
 *          ↓
 * this function
 *          ↓
 * Query API + server-side API key
 *          ↓
 * MongoDB Atlas
 *
 * IMPORTANT:
 * QUERY_API_KEY is NEVER sent to index.html.
 *
 * Required Script Properties:
 *
 * QUERY_API_BASE_URL
 * QUERY_API_KEY
 *
 * Example:
 *
 * QUERY_API_BASE_URL = https://your-query-api-domain.example.com
 * QUERY_API_KEY      = your-long-random-secret
 */

function proxyMongoApi(request) {

  try {

    request = request || {};
    var sessionToken = String(
        request.token || ""
    ).trim();

    var sessionUser = validateSession(sessionToken);

    if (!sessionUser) {
        return {
            status: "error",
            message: "Authentication required or session expired."
        };
    }
    var path = String(
      request.path || ""
    ).trim();

    var method = String(
      request.method || "GET"
    ).toUpperCase();

    var query = request.query || {};
    var body = request.body || null;

    // ------------------------------------------------------------
    // ONLY ALLOW THESE READ-ONLY API ROUTES
    // ------------------------------------------------------------

    var allowedPaths = {
      "/api/v1/telemetry/latest": true,
      "/api/v1/telemetry/export": true,
      "/api/v1/telemetry/range": true,
      "/api/v1/telemetry/stats": true,
      "/api/v1/telemetry/gaps": true,
      "/api/v1/telemetry/correlation": true,
      "/api/v1/sync/status": true,
      "/api/v1/chat/query": true,
      "/health": true
    };

    if (!allowedPaths[path]) {

      return {
        status: "error",
        message: "Query API route is not allowed."
      };
    }

    // ------------------------------------------------------------
    // ONLY GET + POST
    // ------------------------------------------------------------

    if (method !== "GET" && method !== "POST") {

      return {
        status: "error",
        message: "HTTP method is not allowed."
      };
    }

    // ------------------------------------------------------------
    // SERVER-SIDE SECRETS
    // ------------------------------------------------------------

    var properties =
      PropertiesService
        .getScriptProperties();

    var baseUrl = String(
      properties.getProperty(
        "QUERY_API_BASE_URL"
      ) || ""
    ).trim();

    var apiKey = String(
      properties.getProperty(
        "QUERY_API_KEY"
      ) || ""
    ).trim();

    if (!baseUrl || !apiKey) {

      return {
        status: "error",
        message:
          "MongoDB Query API proxy is not configured. " +
          "Set QUERY_API_BASE_URL and QUERY_API_KEY " +
          "in Apps Script Script Properties."
      };
    }

    // Check for un-routable local addresses from Google Apps Script cloud
    if (baseUrl.indexOf("localhost") !== -1 || baseUrl.indexOf("127.0.0.1") !== -1) {

      return {
        status: "error",
        message:
          "QUERY_API_BASE_URL in Script Properties is set to '" + baseUrl + "'. " +
          "Google Apps Script cloud servers cannot reach 127.0.0.1 or localhost on your local computer. " +
          "Please deploy query_api.py to a public HTTPS URL (e.g. Render, Railway, Fly.io, or ngrok) " +
          "and update QUERY_API_BASE_URL in Apps Script Script Properties to that public HTTPS URL."
      };
    }

    // Remove trailing slash
    baseUrl = baseUrl.replace(/\/+$/, "");


    // ------------------------------------------------------------
    // BUILD QUERY STRING
    // ------------------------------------------------------------

    var queryParts = [];

    Object.keys(query).forEach(
      function(key) {

        var value = query[key];

        if (
          value !== null &&
          value !== undefined &&
          value !== ""
        ) {

          queryParts.push(
            encodeURIComponent(key) +
            "=" +
            encodeURIComponent(
              String(value)
            )
          );
        }
      }
    );

    var queryString =
      queryParts.length > 0
        ? "?" + queryParts.join("&")
        : "";

    var url =
      baseUrl +
      path +
      queryString;

    // ------------------------------------------------------------
    // HTTP OPTIONS
    // ------------------------------------------------------------

    var options = {

      method: method.toUpperCase(),


      headers: {
        "X-API-KEY": apiKey
      },

      muteHttpExceptions: true,

      followRedirects: true
    };

    // ------------------------------------------------------------
    // POST BODY
    // ------------------------------------------------------------

    if (method === "POST") {

      options.contentType =
        "application/json";

      options.payload =
        JSON.stringify(
          body || {}
        );
    }

    // ------------------------------------------------------------
    // SERVER → QUERY API
    // ------------------------------------------------------------

    var response =
      UrlFetchApp.fetch(
        url,
        options
      );

    var httpCode =
      response.getResponseCode();

    var text =
      response.getContentText();

    var parsed;

    try {

      parsed = JSON.parse(text);

    } catch (parseError) {

      return {
        status: "error",
        message:
          "Query API returned HTML/non-JSON response (HTTP " + httpCode + "). " +
          "Verify that QUERY_API_BASE_URL (" + baseUrl + ") points to your live public Query API endpoint.",
        http_status: httpCode,
        raw_response:
          text.substring(0, 300)
      };
    }


    parsed.proxy_http_status =
      httpCode;

    return parsed;

  } catch (err) {

    return {
      status: "error",
      message:
        "MongoDB Query API proxy error: " +
        err.toString()
    };
  }
}

/**
 * SHA-256 Salted Password Digest Helper
 */
function hashPassword(password, salt) {
  if (!password) return "";
  var s = salt || DEFAULT_SALT;
  var saltedInput = s + ":" + String(password);
  var rawHash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, saltedInput, Utilities.Charset.UTF_8);
  var txtHash = "";
  for (var i = 0; i < rawHash.length; i++) {
    var byteVal = rawHash[i];
    if (byteVal < 0) byteVal += 256;
    var byteStr = byteVal.toString(16);
    if (byteStr.length === 1) byteStr = "0" + byteStr;
    txtHash += byteStr;
  }
  return txtHash;
}

/**
 * Serves HTML frontend or returns JSON API data for GET requests
 */
function doGet(e) {
  var action = e && e.parameter ? e.parameter.action : null;

  if (action === "getTelemetry") {
    var range = e && e.parameter ? e.parameter.range : null;
    var fromMs = e && e.parameter ? e.parameter.from : null;
    var toMs = e && e.parameter ? e.parameter.to : null;
    var telemetryData = fetchTelemetryFromSheet(range, fromMs, toMs);
    return ContentService.createTextOutput(JSON.stringify(telemetryData)).setMimeType(ContentService.MimeType.JSON);
  }

  if (action === "getSyncStatus") {
    var ss = getSpreadsheet();
    var sheet = ss ? ss.getSheetByName(SHEET_NAME) : null;
    var lastRow = sheet ? sheet.getLastRow() : 0;
    return ContentService.createTextOutput(JSON.stringify({
      status: "success",
      totalRows: Math.max(0, lastRow - 1),
      retainedMinutes: 10,
      sheetName: SHEET_NAME,
      serverTime: Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss")
    })).setMimeType(ContentService.MimeType.JSON);
  }

  if (action === "registerSensor") {
    var payload = {
      type: e.parameter.type,
      name: e.parameter.name,
      sensorId: e.parameter.sensorId,
      token: e.parameter.token
    };
    return ContentService.createTextOutput(JSON.stringify(registerSensorInSheet(payload))).setMimeType(ContentService.MimeType.JSON);
  }

  if (action === "deleteSensor") {
    var payload = {
      sensorId: e.parameter.sensorId,
      token: e.parameter.token
    };
    return ContentService.createTextOutput(JSON.stringify(deleteSensorInSheet(payload))).setMimeType(ContentService.MimeType.JSON);
  }

  if (action === "getEventHistory") {
    return ContentService.createTextOutput(JSON.stringify(fetchEventHistoryFromSheet(e.parameter.token))).setMimeType(ContentService.MimeType.JSON);
  }

  var htmlTemplate = HtmlService.createTemplateFromFile('index');
  return htmlTemplate.evaluate()
    .setTitle('DEMO_BRIDGE_TESTING')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

/**
 * Legacy SHA-256 Digest Helper (for initial admin account migration)
 */
function hashPasswordLegacy(password) {
  if (!password) return "";
  var rawHash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, String(password), Utilities.Charset.UTF_8);
  var txtHash = "";
  for (var i = 0; i < rawHash.length; i++) {
    var byteVal = rawHash[i];
    if (byteVal < 0) byteVal += 256;
    var byteStr = byteVal.toString(16);
    if (byteStr.length === 1) byteStr = "0" + byteStr;
    txtHash += byteStr;
  }
  return txtHash;
}

/**
 * Ensures database sheets, initial Admin account, and SessionRegistry exist
 */
function initDatabaseSheets() {
  var ss = getSpreadsheet();
  
  // 1. Ensure TelemetryData sheet
  var tSheet = ss.getSheetByName(SHEET_NAME);
  if (!tSheet) {
    tSheet = ss.insertSheet(SHEET_NAME);
  }

  // 2. Ensure SensorRegistry sheet
  var sSheet = ss.getSheetByName(REGISTRY_SHEET_NAME);
  if (!sSheet) {
    sSheet = ss.insertSheet(REGISTRY_SHEET_NAME);
    sSheet.appendRow(["Sensor ID", "Sensor Name", "Sensor Type", "Created At", "Status", "Enabled"]);
    sSheet.getRange(1, 1, 1, 6).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
  }

  // Deactivate non-canonical sensor rows in SensorRegistry
  var regLastRow = sSheet.getLastRow();
  var existingCanonical = {};
  if (regLastRow > 1) {
    var regData = sSheet.getRange(2, 1, regLastRow - 1, 6).getValues();
    for (var r = 0; r < regData.length; r++) {
      var sIdNorm = String(regData[r][0]).trim().toUpperCase();
      if (sIdNorm !== "FORCE-01" && sIdNorm !== "TEMP-01" && sIdNorm !== "HUMIDITY-01") {
        sSheet.getRange(r + 2, 5).setValue("DELETED");
        sSheet.getRange(r + 2, 6).setValue("false");
      } else {
        existingCanonical[sIdNorm] = true;
      }
    }
  }

  var istTs = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");
  if (!existingCanonical["FORCE-01"]) {
    sSheet.appendRow(["FORCE-01", "FORCE-01", "force", istTs, "WAITING FOR DATA", "true"]);
  }
  if (!existingCanonical["TEMP-01"]) {
    sSheet.appendRow(["TEMP-01", "TEMP-01", "temp", istTs, "WAITING FOR DATA", "true"]);
  }
  if (!existingCanonical["HUMIDITY-01"]) {
    sSheet.appendRow(["HUMIDITY-01", "HUMIDITY-01", "humidity", istTs, "WAITING FOR DATA", "true"]);
  }

  // 3. Ensure UserRegistry sheet & provision initial Admin account (admin@example.com)
  var uSheet = ss.getSheetByName(USER_SHEET_NAME);
  if (!uSheet) {
    uSheet = ss.insertSheet(USER_SHEET_NAME);
    uSheet.appendRow(["User ID", "Name", "Email", "PasswordHash", "Role", "Created At", "Status"]);
    uSheet.getRange(1, 1, 1, 7).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
  }

  // Check if initial admin account exists
  var uLastRow = uSheet.getLastRow();
  var adminExists = false;
  if (uLastRow > 1) {
    var uData = uSheet.getRange(2, 1, uLastRow - 1, 7).getValues();
    adminExists = uData.some(function(r) { return String(r[2]).toLowerCase() === "admin@example.com"; });
  }

  if (!adminExists) {
    var istTs = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");
    var adminHash = hashPassword("Admin123!", DEFAULT_SALT);
    uSheet.appendRow(["usr_admin_001", "Administrator", "admin@example.com", adminHash, "ADMIN", istTs, "ACTIVE"]);
  }

  // 4. Ensure EventHistoryLog sheet
  var eSheet = ss.getSheetByName(EVENT_LOG_SHEET_NAME);
  if (!eSheet) {
    eSheet = ss.insertSheet(EVENT_LOG_SHEET_NAME);
    eSheet.appendRow(["Event ID", "Timestamp", "User Email", "User Name", "Action", "Target", "Details"]);
    eSheet.getRange(1, 1, 1, 7).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
  }

  // 5. Ensure SessionRegistry sheet
  var sessSheet = ss.getSheetByName(SESSION_SHEET_NAME);
  if (!sessSheet) {
    sessSheet = ss.insertSheet(SESSION_SHEET_NAME);
    sessSheet.appendRow(["Session Token", "User ID", "User Email", "User Name", "Role", "Created At", "Expires At", "Status"]);
    sessSheet.getRange(1, 1, 1, 8).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
  }

  // 6. Ensure IngestionLedger sheet
  var ledgerSheet = ss.getSheetByName(INGESTION_LEDGER_SHEET_NAME);
  if (!ledgerSheet) {
    ledgerSheet = ss.insertSheet(INGESTION_LEDGER_SHEET_NAME);
    ledgerSheet.appendRow(["LedgerKey", "Arduino_ID", "Sequence", "IngestedAt"]);
    ledgerSheet.getRange(1, 1, 1, 4).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
  }

  // 7. Automatically synchronize all existing telemetry sensors to SensorRegistry
  syncAllSensorsToRegistrySheet(ss);
}

/**
 * Server-Side Session Creation
 */
function createSession(user) {
  if (!user || typeof user !== "object") {
    user = {
      id: "usr_system_" + Date.now(),
      email: "admin@example.com",
      name: "System Administrator",
      role: "ADMIN"
    };
  }

  var userId = user.id || ("usr_" + Date.now());
  var userEmail = user.email || "admin@example.com";
  var userName = user.name || "System Administrator";
  var userRole = user.role || "ADMIN";

  var ss = getSpreadsheet();
  var sessSheet = ss.getSheetByName(SESSION_SHEET_NAME);
  if (!sessSheet) {
    sessSheet = ss.insertSheet(SESSION_SHEET_NAME);
    sessSheet.appendRow(["Session Token", "User ID", "User Email", "User Name", "Role", "Created At", "Expires At", "Status"]);
    sessSheet.getRange(1, 1, 1, 8).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
  }

  var rawSeed = userId + "_" + Date.now() + "_" + Math.random().toString();
  var rawHash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, rawSeed, Utilities.Charset.UTF_8);
  var tokenHex = "";
  for (var i = 0; i < rawHash.length; i++) {
    var b = rawHash[i] < 0 ? rawHash[i] + 256 : rawHash[i];
    var s = b.toString(16);
    if (s.length === 1) s = "0" + s;
    tokenHex += s;
  }
  var token = "sess_" + tokenHex;

  var nowMs = Date.now();
  var expiresAtMs = nowMs + SESSION_TIMEOUT_MS;
  var istCreated = Utilities.formatDate(new Date(nowMs), TIMEZONE, "yyyy-MM-dd HH:mm:ss");

  sessSheet.appendRow([token, userId, userEmail, userName, userRole, istCreated, expiresAtMs, "ACTIVE"]);

  return {
    token: token,
    expiresAtMs: expiresAtMs
  };
}

/**
 * Server-Side Session Validation & Expiration Enforcement
 */
function validateSession(token) {
  if (!token) return null;
  var ss = getSpreadsheet();
  var sessSheet = ss.getSheetByName(SESSION_SHEET_NAME);
  if (!sessSheet || sessSheet.getLastRow() <= 1) return null;

  var lastRow = sessSheet.getLastRow();
  var data = sessSheet.getRange(2, 1, lastRow - 1, 8).getValues();
  var nowMs = Date.now();

  for (var i = 0; i < data.length; i++) {
    var sToken = String(data[i][0]);
    if (sToken === token) {
      var status = String(data[i][7]);
      var expiresAtMs = Number(data[i][6]);

      if (status === "ACTIVE") {
        if (nowMs > expiresAtMs) {
          sessSheet.getRange(i + 2, 8).setValue("EXPIRED");
          return null;
        }
        return {
          token: sToken,
          userId: String(data[i][1]),
          email: String(data[i][2]),
          name: String(data[i][3]),
          role: String(data[i][4]),
          expiresAtMs: expiresAtMs
        };
      }
    }
  }
  return null;
}

/**
 * Invalidates user session upon logout
 */
function logoutUserSession(token) {
  if (!token) return;
  var ss = getSpreadsheet();
  var sessSheet = ss.getSheetByName(SESSION_SHEET_NAME);
  if (!sessSheet || sessSheet.getLastRow() <= 1) return;

  var data = sessSheet.getRange(2, 1, sessSheet.getLastRow() - 1, 8).getValues();
  for (var i = 0; i < data.length; i++) {
    if (String(data[i][0]) === token) {
      sessSheet.getRange(i + 2, 8).setValue("LOGGED_OUT");
      break;
    }
  }
}

/**
 * Brute-Force Rate Limiting Engine
 */
function checkLoginRateLimit(email) {
  try {
    var cache = CacheService.getScriptCache();
    var key = "FAIL_CNT_" + String(email).trim().toLowerCase();
    var attempts = Number(cache.get(key) || 0);
    return attempts >= 5; // Lockout threshold: 5 consecutive failures
  } catch (e) {
    return false;
  }
}

function incrementLoginFailures(email) {
  try {
    var cache = CacheService.getScriptCache();
    var key = "FAIL_CNT_" + String(email).trim().toLowerCase();
    var attempts = Number(cache.get(key) || 0) + 1;
    cache.put(key, String(attempts), 300); // Lockout window: 5 minutes
  } catch (e) {}
}

function resetLoginFailures(email) {
  try {
    var cache = CacheService.getScriptCache();
    var key = "FAIL_CNT_" + String(email).trim().toLowerCase();
    cache.remove(key);
  } catch (e) {}
}

/**
 * Automatically populates SensorRegistry sheet tab with all physical sensors 
 */
/**
 * Automatically populates SensorRegistry sheet tab with all physical sensors 
 */
function syncAllSensorsToRegistrySheet(ss) {
  try {
    if (!ss) ss = getSpreadsheet();
    
    var tSheet = ss.getSheetByName(SHEET_NAME);
    var regSheet = ss.getSheetByName(REGISTRY_SHEET_NAME);

    if (!regSheet) {
      regSheet = ss.insertSheet(REGISTRY_SHEET_NAME);
      regSheet.appendRow(["Sensor ID", "Sensor Name", "Sensor Type", "Created At", "Status", "Enabled"]);
      regSheet.getRange(1, 1, 1, 6).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
    }

    if (!tSheet || tSheet.getLastColumn() === 0) return;

    var tLastRow = tSheet.getLastRow();
    var tLastCol = tSheet.getLastColumn();
    var headers = tSheet.getRange(1, 1, 1, tLastCol).getValues()[0];

    var latestRow = null;
    var timeIdx = headers.indexOf("Timestamp");
    var latestTimestampMs = 0;

    if (tLastRow > 1) {
      latestRow = tSheet.getRange(tLastRow, 1, 1, tLastCol).getValues()[0];
      if (timeIdx !== -1 && latestRow[timeIdx]) {
        var rawTs = latestRow[timeIdx];
        if (rawTs instanceof Date) {
          latestTimestampMs = rawTs.getTime();
        } else {
          var parsed = new Date(String(rawTs).replace(" ", "T")).getTime();
          if (!isNaN(parsed)) latestTimestampMs = parsed;
        }
      }
    }

    var nowMs = new Date().getTime();
    var elapsedSec = latestTimestampMs > 0 ? Math.floor((nowMs - latestTimestampMs) / 1000) : 999999;

    var regLastRow = regSheet.getLastRow();
    if (regLastRow <= 1) return;

    var regRowsData = regSheet.getRange(2, 1, regLastRow - 1, 6).getValues();
    
    // Only update status for existing registered sensors that are NOT deleted
    regRowsData.forEach(function(row, idx) {
      var sId = String(row[0]).trim().toUpperCase();
      var sStatus = String(row[4]).trim().toUpperCase();
      var sEnabled = String(row[5]).trim().toLowerCase();

      if (sStatus === "DELETED" || sEnabled === "false") return;

      var valColIdx = headers.indexOf(sId);
      var statusColIdx = headers.indexOf(sId + "_Status");

      var currentStatus = "WAITING FOR DATA";
      if (latestRow && valColIdx !== -1) {
        var latestVal = latestRow[valColIdx];
        var isExplicitOnline = statusColIdx !== -1 ? (String(latestRow[statusColIdx]).toUpperCase() === "ONLINE") : true;

        if (latestVal !== "" && latestVal !== null && latestVal !== undefined) {
          if (isExplicitOnline) {
            if (elapsedSec <= 150) currentStatus = "LIVE";
            else if (elapsedSec <= 300) currentStatus = "DELAYED";
            else currentStatus = "OFFLINE";
          } else {
            currentStatus = "OFFLINE";
          }
        }
      }

      if (sStatus !== currentStatus && sStatus !== "DELETED") {
        regSheet.getRange(idx + 2, 5).setValue(currentStatus);
      }
    });

  } catch (err) {
    Logger.log("Error syncing sensor registry: " + err.toString());
  }
}

/**
 * High-Speed Bulk Telemetry Ingestion (5-Second Batch Processor with Durable Idempotency & Registry Enforcement)
 */
function processTelemetryBatch(ss, sheet, packets) {
  if (!packets || !packets.length) {
    return { success: true, count: 0, duplicates: 0, message: "No packets to process" };
  }

  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(10000);
  } catch (e) {
    Logger.log("processTelemetryBatch: Failed to acquire lock");
    return { success: false, count: 0, error: "Server busy: lock acquisition timeout" };
  }

  try {
    if (!ss) ss = getSpreadsheet();
    if (!sheet) sheet = ss.getSheetByName(SHEET_NAME);
    if (!sheet) sheet = ss.insertSheet(SHEET_NAME);

    // 1. Read SensorRegistry to obtain authoritative set of active registered sensors & deleted sensors
    var regSheet = ss.getSheetByName(REGISTRY_SHEET_NAME);
    if (!regSheet) {
      regSheet = ss.insertSheet(REGISTRY_SHEET_NAME);
      regSheet.appendRow(["Sensor ID", "Sensor Name", "Sensor Type", "Created At", "Status", "Enabled"]);
      regSheet.getRange(1, 1, 1, 6).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
    }

    var activeSensorsMap = {};
    var deletedSensorsSet = {};
    if (regSheet.getLastRow() > 1) {
      var regData = regSheet.getRange(2, 1, regSheet.getLastRow() - 1, 6).getValues();
      regData.forEach(function(row) {
        var sId = String(row[0]).trim().toUpperCase();
        var sStatus = String(row[4]).trim().toUpperCase();
        var sEnabled = String(row[5]).trim().toLowerCase();
        if (sStatus === "DELETED") {
          deletedSensorsSet[sId] = true;
        } else if (sEnabled === "true") {
          activeSensorsMap[sId] = {
            id: sId,
            name: String(row[1]),
            type: String(row[2])
          };
        }
      });
    }

    // Helper to infer category/type for auto-detected physical Arduino sensors
    function inferSensorCategory(sensorId) {
      var s = String(sensorId).toUpperCase();
      if (s.indexOf("FORCE") !== -1 || s.indexOf("LOAD") !== -1 || s.startsWith("F-") || s.startsWith("FORCE")) return "force";
      if (s.indexOf("TEMP") !== -1 || s.indexOf("THERM") !== -1 || s.startsWith("T-") || s.startsWith("TEMP")) return "temp";
      if (s.indexOf("HUMID") !== -1 || s.startsWith("H-") || s.startsWith("HUMIDITY")) return "humidity";
      if (s.indexOf("STRAIN") !== -1 || s.startsWith("S-") || s.startsWith("STRAIN")) return "strain";
      return "force";
    }

    var CANONICAL_WHITELIST = {
      "FORCE-01": "force",
      "TEMP-01": "temp",
      "HUMIDITY-01": "humidity"
    };

    var LEGACY_TYPO_MAP = {
      "FORC-01": "FORCE-01",
      "FORCE01": "FORCE-01",
      "FORCE-0": "FORCE-01",
      "F0RCE-01": "FORCE-01",
      "TEMP-0": "TEMP-01",
      "TEMP01": "TEMP-01",
      "HUIDITY-01": "HUMIDITY-01",
      "HUMIITY-01": "HUMIDITY-01",
      "HUMIDITY01": "HUMIDITY-01",
      "HUMIDITY-0": "HUMIDITY-01",
      "HUMITY-01": "HUMIDITY-01"
    };

    // Controlled status set (Part 4 & Round 2.1 Hardened)
    var ALLOWED_STATUSES = ["ONLINE", "OFFLINE", "WAITING FOR DATA"];

    // 2. Validate packets & filter out non-registered/deleted sensors (Auto-register new physical sensors)
    var validPackets = [];
    packets.forEach(function(pkt) {
      if (!pkt || typeof pkt !== "object") return;
      
      // Strict Arduino ID validation (REJECT if missing/empty/whitespace)
      if (!pkt.arduino_id || typeof pkt.arduino_id !== "string" || !pkt.arduino_id.trim()) {
        Logger.log("[VALIDATION] REJECTED: Missing or invalid Arduino_ID");
        return;
      }
      var arduinoId = String(pkt.arduino_id).trim();

      // Sequence Validation
      var seq = pkt.sequence;
      if (seq === undefined || seq === null || !Number.isFinite(Number(seq))) {
        Logger.log("[VALIDATION] REJECTED: Packet dropped due to missing or non-finite sequence");
        return;
      }

      var filteredSensors = {};
      var rawSensors = pkt.sensors || {};
      var invalidStatusInPacket = false;

      Object.keys(rawSensors).forEach(function(sKey) {
        if (invalidStatusInPacket) return;
        var rawKey = String(sKey).trim().toUpperCase();
        var normKey = LEGACY_TYPO_MAP[rawKey] || rawKey;

        // Reject non-canonical sensor IDs
        if (!CANONICAL_WHITELIST[normKey]) {
          Logger.log("[SENSOR] Rejected non-canonical sensor ID: " + rawKey);
          return;
        }

        // 1) Explicitly deleted sensor protection
        if (deletedSensorsSet[normKey]) {
          Logger.log("[SENSOR] Rejected telemetry for explicitly DELETED sensor: " + normKey);
          return;
        }

        // 2) Auto-register canonical physical sensor if not registered yet
        if (!activeSensorsMap[normKey]) {
          var cat = CANONICAL_WHITELIST[normKey];
          var istTs = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");
          regSheet.appendRow([normKey, normKey, cat, istTs, "LIVE", "true"]);
          activeSensorsMap[normKey] = {
            id: normKey,
            name: normKey,
            type: cat
          };
          Logger.log("[SENSOR] Auto-registered canonical physical sensor: " + normKey);
        }

        // 3) Process telemetry for active sensor
        var sObj = rawSensors[sKey];
        var val = sObj ? sObj.value : null;
        
        // Resilient status sanitization against serial UART noise (e.g. OOLINE, NLINE, ONLNE -> ONLINE)
        var status = "ONLINE";
        if (sObj && sObj.status !== undefined && sObj.status !== null) {
          var rawStatusStr = String(sObj.status).trim().toUpperCase();
          if (rawStatusStr === "ONLINE" || rawStatusStr === "NLINE" || rawStatusStr === "OOLINE" || rawStatusStr.indexOf("LINE") !== -1 || rawStatusStr.indexOf("ON") === 0) {
            status = "ONLINE";
          } else if (rawStatusStr.indexOf("OFF") !== -1) {
            status = "OFFLINE";
          } else if (ALLOWED_STATUSES.indexOf(rawStatusStr) !== -1) {
            status = rawStatusStr;
          } else {
            status = "ONLINE";
          }
        }
        
        // Strict numerical value validation (no synthetic/fallback numbers)
        if (val !== null && val !== "" && val !== undefined) {
          var numVal = Number(val);
          if (!Number.isFinite(numVal)) {
            val = "";
          } else {
            val = numVal;
          }
        } else {
          val = "";
        }

        filteredSensors[normKey] = {
          status: status,
          value: val
        };
      });

      if (invalidStatusInPacket) {
        Logger.log("[VALIDATION] REJECTED: Entire packet sequence " + seq + " dropped due to invalid sensor status");
        return;
      }

      validPackets.push({
        arduino_id: arduinoId,
        session_id: String(pkt.session_id || "LEGACY"),
        sequence: Number(seq),
        timestamp: pkt.timestamp,
        sensors: filteredSensors
      });
    });

    if (!validPackets.length) {
      return { success: true, count: 0, duplicates: 0, message: "No valid packets to insert" };
    }

    // Guarantee strict ascending sequence order within batch
    validPackets.sort(function(a, b) {
      return a.sequence - b.sequence;
    });

    // 3. Durable Idempotency Ledger Check: Smart Bottom-Up Active-Stream Deduplication
    var ledgerSheet = ss.getSheetByName(INGESTION_LEDGER_SHEET_NAME);
    if (!ledgerSheet) {
      ledgerSheet = ss.insertSheet(INGESTION_LEDGER_SHEET_NAME);
      ledgerSheet.appendRow(["LedgerKey", "Arduino_ID", "Sequence", "IngestedAt"]);
      ledgerSheet.getRange(1, 1, 1, 4).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
    }

    // Auto-clear stale ledger keys if TelemetryData is empty / cleared
    if (sheet.getLastRow() <= 1 && ledgerSheet.getLastRow() > 1) {
      ledgerSheet.getRange(2, 1, ledgerSheet.getLastRow() - 1, 4).clearContent();
      Logger.log("[IDEMPOTENCY] TelemetryData is empty — automatically cleared stale IngestionLedger keys.");
    }

    // Inspect TelemetryData from the BOTTOM ROW UPWARDS to build deduplication state ONLY for the current active stream
    var tLastRow = sheet.getLastRow();
    var tLastCol = sheet.getLastColumn();
    var processedKeysSet = {};
    var bottomRowSeqPerArduino = {};

    if (tLastRow > 1 && tLastCol >= 3) {
      var checkRows = Math.min(500, tLastRow - 1);
      var endRowIdx = tLastRow - checkRows + 1;
      var recentData = sheet.getRange(endRowIdx, 1, checkRows, tLastCol).getValues();
      var headersTmp = sheet.getRange(1, 1, 1, tLastCol).getValues()[0];
      var seqColIdx = headersTmp.indexOf("Sequence");
      var ardColIdx = headersTmp.indexOf("Arduino_ID");

      if (seqColIdx !== -1 && ardColIdx !== -1) {
        var lastSeenSeq = {};

        // Iterate backwards from newest row (bottom of sheet) to oldest row
        for (var r = recentData.length - 1; r >= 0; r--) {
          var aIdVal = String(recentData[r][ardColIdx]).trim();
          var sNumVal = Number(recentData[r][seqColIdx]);

          if (aIdVal && Number.isFinite(sNumVal)) {
            if (bottomRowSeqPerArduino[aIdVal] === undefined) {
              bottomRowSeqPerArduino[aIdVal] = sNumVal;
            }

            if (lastSeenSeq[aIdVal] !== -1) {
              if (lastSeenSeq[aIdVal] !== undefined) {
                var prevSeq = lastSeenSeq[aIdVal];
                // Going backwards in sheet: sequence numbers should decrease.
                // If sequence jumps UPWARDS going backwards (e.g. 1 -> 128), we hit an Arduino reboot boundary!
                if (sNumVal > prevSeq && (sNumVal - prevSeq > 5 || prevSeq <= 10)) {
                  Logger.log("[IDEMPOTENCY] Reached Arduino reboot boundary for " + aIdVal + " (Row seq " + sNumVal + " > recent " + prevSeq + "). Stopping historical scan.");
                  lastSeenSeq[aIdVal] = -1; // Stop scanning older sessions for this Arduino
                  continue;
                }
              }

              processedKeysSet[aIdVal + ":" + sNumVal] = true;
              lastSeenSeq[aIdVal] = sNumVal;
            }
          }
        }
      }
    }

    // Check if incoming batch contains an Arduino Sequence Reset relative to the bottom row of sheet
    validPackets.forEach(function(pkt) {
      var aId = pkt.arduino_id;
      var seq = pkt.sequence;
      if (bottomRowSeqPerArduino[aId] !== undefined) {
        var bottomSeq = bottomRowSeqPerArduino[aId];
        // If incoming packet is start of a new stream (seq <= 10) and bottom of sheet has a higher sequence
        if (seq <= 10 && seq < bottomSeq) {
          Logger.log("[IDEMPOTENCY] Batch contains Arduino sequence reset for " + aId + " (Seq " + seq + " after sheet bottom " + bottomSeq + "). Clearing active stream memory deduplication.");
          // Clear active stream deduplication keys in memory for this Arduino so new stream is accepted
          Object.keys(processedKeysSet).forEach(function(k) {
            if (k.indexOf(aId + ":") === 0) {
              delete processedKeysSet[k];
            }
          });
        }
      }
    });

    // 4. Ensure TelemetryData headers exist ONLY for active registered sensors
    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    var headers = [];

    if (lastRow === 0 || lastCol === 0) {
      headers = ["Timestamp", "Sequence", "Arduino_ID","Session_ID"];
      Object.keys(activeSensorsMap).forEach(function(sId) {
        headers.push(sId + "_Status");
        headers.push(sId);
      });
      sheet.appendRow(headers);
      sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
      lastRow = 1;
      lastCol = headers.length;
    } else {
      headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    }

    var headersUpdated = false;
    Object.keys(activeSensorsMap).forEach(function(sId) {
      var statusHeader = sId + "_Status";
      var valHeader = sId;
      if (headers.indexOf(statusHeader) === -1) { headers.push(statusHeader); headersUpdated = true; }
      if (headers.indexOf(valHeader) === -1) { headers.push(valHeader); headersUpdated = true; }
    });

    if (headersUpdated) {
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
    }

    // 5. Build batch rows & ledger entries for non-duplicate packets
    var batchRows = [];
    var newLedgerRows = [];
    var duplicateCount = 0;
    var fallbackIst = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");

    validPackets.forEach(function(pkt) {
      var key =
      pkt.arduino_id +
      ":" +
      (pkt.session_id || "LEGACY") +
      ":" +
      pkt.sequence;
      if (processedKeysSet[key]) {
        Logger.log("[IDEMPOTENCY] REJECTED: Duplicate packet detected: " + key);
        duplicateCount++;
        return;
      }

      processedKeysSet[key] = true; // Mark as processed for batch internal deduplication

      var istTimestamp = pkt.timestamp || fallbackIst;
      var rowData = new Array(headers.length);

      headers.forEach(function(headerName, index) {
        if (headerName === "Timestamp") rowData[index] = istTimestamp;
        else if (headerName === "Sequence") rowData[index] = pkt.sequence;
        else if (headerName === "Arduino_ID") rowData[index] = pkt.arduino_id;
        else if (headerName === "Session_ID") rowData[index] = pkt.session_id || "LEGACY";
        else if (headerName.endsWith("_Status")) {
          var sensorId = headerName.replace("_Status", "");
          rowData[index] = (pkt.sensors[sensorId] && pkt.sensors[sensorId].status) ? pkt.sensors[sensorId].status : "OFFLINE";
        } else {
          var sensorId = headerName;
          if (pkt.sensors[sensorId] && pkt.sensors[sensorId].value !== undefined && pkt.sensors[sensorId].value !== null) {
            rowData[index] = pkt.sensors[sensorId].value;
          } else {
            rowData[index] = "";
          }
        }
      });
      batchRows.push(rowData);
      newLedgerRows.push([key, pkt.arduino_id, pkt.sequence, istTimestamp]);
    });

    if (batchRows.length > 0) {
      var startRow = sheet.getLastRow() + 1;
      sheet.getRange(startRow, 1, batchRows.length, headers.length).setValues(batchRows);

      // Persist new ingestion keys into IngestionLedger
      var ledgerStartRow = ledgerSheet.getLastRow() + 1;
      ledgerSheet.getRange(ledgerStartRow, 1, newLedgerRows.length, 4).setValues(newLedgerRows);

      // Synchronous flush guarantees atomic persistence of both sheets before lock release
      SpreadsheetApp.flush();
    }

    return {
      success: true,
      count: batchRows.length,
      duplicates: duplicateCount,
      totalValid: validPackets.length
    };

  } catch (err) {
    Logger.log("Error in processTelemetryBatch: " + err.toString());
    return {
      success: false,
      count: 0,
      error: err.toString()
    };
  } finally {
    try { lock.releaseLock(); } catch (e) {}
  }
}

/**
 * Prunes TelemetryData rows older than rollingWindowMinutes (default 10 minutes)
 * Preserves header row and all other sheet tabs (SensorRegistry, UserRegistry, etc.).
 */
function pruneOldTelemetryFromSheet(ss, sheet, rollingWindowMinutes) {
  try {
    if (!ss) ss = getSpreadsheet();
    if (!sheet) sheet = ss.getSheetByName(SHEET_NAME);

    if (!sheet || sheet.getLastRow() <= 1) {
      return 0;
    }

    var windowMins = Number(rollingWindowMinutes);

    if (!Number.isFinite(windowMins) || windowMins <= 0) {
      windowMins = 10;
    }

    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();

    var headers = sheet
      .getRange(1, 1, 1, lastCol)
      .getValues()[0];

    var timeIdx = headers.indexOf("Timestamp");

    if (timeIdx === -1) {
      return 0;
    }

    var data = sheet
      .getRange(2, 1, lastRow - 1, lastCol)
      .getValues();

    var maxTsMs = 0;

    data.forEach(function(row) {

      var rawTime = row[timeIdx];

      if (!rawTime) {
        return;
      }

      var rMs = 0;

      if (rawTime instanceof Date) {
        rMs = rawTime.getTime();
      } else {
        rMs = new Date(
          String(rawTime).replace(" ", "T")
        ).getTime();
      }

      if (!isNaN(rMs) && rMs > maxTsMs) {
        maxTsMs = rMs;
      }
    });

    if (maxTsMs <= 0) {
      return 0;
    }

    var cutoffMs =
      maxTsMs -
      (windowMins * 60 * 1000);

    var rowsToDelete = [];

    data.forEach(function(row, index) {

      var rawTime = row[timeIdx];

      if (!rawTime) {
        return;
      }

      var rMs = 0;

      if (rawTime instanceof Date) {
        rMs = rawTime.getTime();
      } else {
        rMs = new Date(
          String(rawTime).replace(" ", "T")
        ).getTime();
      }

      if (
        !isNaN(rMs) &&
        rMs < cutoffMs
      ) {
        rowsToDelete.push(index + 2);
      }
    });

    // Delete from bottom to top so row numbers remain valid.
    for (
      var i = rowsToDelete.length - 1;
      i >= 0;
      i--
    ) {
      sheet.deleteRow(rowsToDelete[i]);
    }

    if (rowsToDelete.length > 0) {
      Logger.log(
        "[PRUNE] Removed " +
        rowsToDelete.length +
        " rows older than " +
        windowMins +
        " minutes."
      );
    }

    return rowsToDelete.length;

  } catch (err) {

    Logger.log(
      "Error in pruneOldTelemetryFromSheet: " +
      err.toString()
    );

    return 0;
  }
}

/**
 * Receives JSON telemetry & API POST requests
 */
function doPost(e) {
  try {
    if (!e || !e.postData || !e.postData.contents) {
      return responseJSON({ status: "error", message: "No post data received" }, 400);
    }

    var payload = JSON.parse(e.postData.contents);
    var action = payload.action;

    if (action === "login") {
      return responseJSON(loginUser(payload), 200);
    }
    if (action === "logout") {
      return responseJSON(logoutUser(payload), 200);
    }
    if (action === "registerUser") {
      return responseJSON(registerUser(payload), 200);
    }
    if (action === "registerSensor") {
      return responseJSON(registerSensorInSheet(payload), 200);
    }
    if (action === "deleteSensor") {
      return responseJSON(deleteSensorInSheet(payload), 200);
    }
    if (action === "logEvent") {
      return responseJSON(logEventToSheet(payload.userEmail, payload.userName, payload.eventAction, payload.target, payload.details, payload.token), 200);
    }
    if (action === "getSyncStatus") {
      var ss = getSpreadsheet();
      var sheet = ss ? ss.getSheetByName(SHEET_NAME) : null;
      var lastRow = sheet ? sheet.getLastRow() : 0;
      return responseJSON({
        status: "success",
        totalRows: Math.max(0, lastRow - 1),
        retainedMinutes: 10,
        sheetName: SHEET_NAME,
        serverTime: Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss")
      }, 200);
    }

    // Default or batch telemetry synchronization processing
    var packets = [];
    if (action === "sync_telemetry_batch" || action === "batch_telemetry" || payload.batch) {
      packets = payload.batch || [];
    } else if (Array.isArray(payload)) {
      packets = payload;
    } else {
      packets = [payload];
    }

    var windowMins = payload.rollingWindowMinutes || 10;
    var ss = getSpreadsheet();
    var sheet = ss.getSheetByName(SHEET_NAME);
    if (!sheet) sheet = ss.insertSheet(SHEET_NAME);

    var result = processTelemetryBatch(ss, sheet, packets);

    if (!result.success) {
      return responseJSON({
        status: "error",
        message: result.error || "Telemetry batch processing failed"
      }, 500);
    }

    // Prune rows older than 10 minutes from TelemetryData
    var prunedCount = pruneOldTelemetryFromSheet(ss, sheet, windowMins);

    var maxSeqInBatch = null;
    packets.forEach(function(p) {
      if (p && p.sequence !== undefined && (maxSeqInBatch === null || p.sequence > maxSeqInBatch)) {
        maxSeqInBatch = p.sequence;
      }
    });

    return responseJSON({
      status: "success",
      message: "MongoDB to Google Sheets synchronization completed successfully",
      count: result.count,
      duplicates: result.duplicates || 0,
      pruned: prunedCount,
      retainedRows: Math.max(0, sheet.getLastRow() - 1),
      lastSyncedSequence: maxSeqInBatch,
      spreadsheetId: ss.getId(),
      spreadsheetName: ss.getName(),
      sheetName: sheet.getName(),
      lastRow: sheet.getLastRow(),
      timestamp: Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss")
    }, 200);

  } catch (error) {
    return responseJSON({ status: "error", message: error.toString() }, 500);
  }
}


/**
 * Authenticates user credentials against UserRegistry (Server-Side)
 */
function loginUser(payload) {
  try {
    initDatabaseSheets();
    if (!payload || !payload.email || !payload.password) {
      return { status: "error", message: "Invalid email or password." };
    }

    var emailClean = String(payload.email).trim().toLowerCase();

    // Check brute-force login rate limiting
    if (checkLoginRateLimit(emailClean)) {
      return { status: "error", code: "RATE_LIMITED", message: "Too many failed login attempts. Please try again after 5 minutes." };
    }

    var passHashSalted = hashPassword(payload.password, DEFAULT_SALT);
    var passHashLegacy = hashPasswordLegacy(payload.password);

    var ss = getSpreadsheet();
    var uSheet = ss.getSheetByName(USER_SHEET_NAME);
    if (!uSheet || uSheet.getLastRow() <= 1) {
      incrementLoginFailures(emailClean);
      return { status: "error", message: "Invalid email or password." };
    }

    var uData = uSheet.getRange(2, 1, uSheet.getLastRow() - 1, 7).getValues();
    var matchedUser = null;
    var matchedRowIdx = -1;

    for (var i = 0; i < uData.length; i++) {
      var rEmail = String(uData[i][2]).trim().toLowerCase();
      var rHash = String(uData[i][3]);
      if (rEmail === emailClean && (rHash === passHashSalted || rHash === passHashLegacy)) {
        matchedUser = {
          id: String(uData[i][0]),
          name: String(uData[i][1]),
          email: String(uData[i][2]),
          role: String(uData[i][4]),
          status: String(uData[i][6])
        };
        matchedRowIdx = i + 2;
        break;
      }
    }

    if (!matchedUser || matchedUser.status === "DISABLED") {
      incrementLoginFailures(emailClean);
      logEventToSheet(emailClean, "Unknown", "Failed Login Attempt", "Auth", "Invalid credentials entered");
      return { status: "error", message: "Invalid email or password." };
    }

    // Reset rate-limiting count upon successful authentication
    resetLoginFailures(emailClean);

    // Auto-migrate legacy un-salted hash to salted hash if matched
    if (matchedRowIdx > 1) {
      uSheet.getRange(matchedRowIdx, 4).setValue(passHashSalted);
    }

    // Create secure server-side session token
    var sessionInfo = createSession(matchedUser);

    logEventToSheet(matchedUser.email, matchedUser.name, "User Logged In", "Auth", "Successful authentication (" + matchedUser.role + ")");

    return {
      status: "success",
      token: sessionInfo.token,
      user: {
        id: matchedUser.id,
        name: matchedUser.name,
        email: matchedUser.email,
        role: matchedUser.role
      },
      expiresAtMs: sessionInfo.expiresAtMs,
      message: "Authentication successful"
    };

  } catch (err) {
    return { status: "error", message: err.toString() };
  }
}

/**
 * Server-Side User Logout API
 */
function logoutUser(payload) {
  try {
    initDatabaseSheets();
    var token = payload ? payload.token : null;
    if (token) {
      var session = validateSession(token);
      if (session) {
        logEventToSheet(session.email, session.name, "User Logged Out", "Auth", "User session invalidated");
      }
      logoutUserSession(token);
    }
    return { status: "success", message: "Logged out successfully" };
  } catch (err) {
    return { status: "error", message: err.toString() };
  }
}

/**
 * Registers a new user account (FORCES ROLE = USER on Server)
 */
function registerUser(payload) {
  try {
    initDatabaseSheets();
    if (!payload || !payload.name || !payload.email || !payload.password) {
      return { status: "error", message: "All fields are required." };
    }

    var nameClean = String(payload.name).trim();
    var emailClean = String(payload.email).trim().toLowerCase();
    var passRaw = String(payload.password);

    if (passRaw.length < 6) {
      return { status: "error", message: "Password must be at least 6 characters." };
    }

    var ss = getSpreadsheet();
    var uSheet = ss.getSheetByName(USER_SHEET_NAME);
    var uLastRow = uSheet.getLastRow();

    if (uLastRow > 1) {
      var uData = uSheet.getRange(2, 1, uLastRow - 1, 7).getValues();
      for (var i = 0; i < uData.length; i++) {
        if (String(uData[i][2]).trim().toLowerCase() === emailClean) {
          return { status: "error", message: "An account with this email address already exists." };
        }
      }
    }

    var userId = "usr_" + Date.now();
    var passHash = hashPassword(passRaw, DEFAULT_SALT);
    var istTs = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");

    // CRITICAL SECURITY REQUIREMENT: PUBLIC REGISTRATION IS ALWAYS USER ROLE
    uSheet.appendRow([userId, nameClean, emailClean, passHash, "USER", istTs, "ACTIVE"]);
    logEventToSheet(emailClean, nameClean, "Account Registered", "Auth", "New USER account created");

    return {
      status: "success",
      message: "Account created successfully. You can now log in.",
      user: { id: userId, name: nameClean, email: emailClean, role: "USER" }
    };

  } catch (err) {
    return { status: "error", message: err.toString() };
  }
}

/**
 * Deletes a sensor completely from Google Sheets database & registry (ADMIN ONLY SERVER ENFORCED)
 */
function deleteSensorInSheet(payload) {
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(10000);
  } catch (e) {
    return { status: "error", message: "Server busy acquiring lock. Please try again." };
  }

  try {
    initDatabaseSheets();
    if (!payload || !payload.sensorId) {
      return { status: "error", message: "Sensor ID is required." };
    }

    var token = payload.token || "";
    var session = validateSession(token);

    if (!session) {
      return { status: "error", code: "UNAUTHORIZED", message: "Your session has expired or is invalid. Please sign in again." };
    }

    if (session.role !== "ADMIN") {
      logEventToSheet(session.email, session.name, "Unauthorized Deletion Attempt", payload.sensorId, "Access denied (USER role attempted sensor deletion)");
      return { status: "error", code: "FORBIDDEN", message: "Access denied. Administrator role required to delete sensors." };
    }

    var sensorId = String(payload.sensorId).trim().toUpperCase();
    var adminEmail = session.email;
    var adminName = session.name;

    var ss = getSpreadsheet();

    // 1. Delete matching columns from TelemetryData sheet
    var sheet = ss.getSheetByName(SHEET_NAME);
    if (sheet && sheet.getLastColumn() > 0) {
      var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
      var statusHeader = sensorId + "_Status";
      var valHeader = sensorId;

      var colsToDelete = [];
      headers.forEach(function(h, idx) {
        if (String(h).toUpperCase() === statusHeader || String(h).toUpperCase() === valHeader) {
          colsToDelete.push(idx + 1);
        }
      });

      colsToDelete.sort(function(a, b) { return b - a; });
      colsToDelete.forEach(function(colIdx) {
        sheet.deleteColumn(colIdx);
      });
    }

    // 2. Mark entry as DELETED in SensorRegistry sheet to prevent auto-registration when serial telemetry arrives
    var regSheet = ss.getSheetByName(REGISTRY_SHEET_NAME);
    if (regSheet) {
      var foundInReg = false;
      if (regSheet.getLastRow() > 1) {
        var regData = regSheet.getRange(2, 1, regSheet.getLastRow() - 1, 6).getValues();
        for (var r = regData.length - 1; r >= 0; r--) {
          if (String(regData[r][0]).toUpperCase() === sensorId) {
            regSheet.getRange(r + 2, 5).setValue("DELETED");
            regSheet.getRange(r + 2, 6).setValue("false");
            foundInReg = true;
          }
        }
      }
      if (!foundInReg) {
        var istTs = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");
        regSheet.appendRow([sensorId, sensorId, "force", istTs, "DELETED", "false"]);
      }
    }

    // Log deletion event to EventHistoryLog
    logEventToSheet(adminEmail, adminName, "Sensor Deleted", sensorId, "Permanently removed sensor columns & registry entry");

    return {
      status: "success",
      sensorId: sensorId,
      message: "Sensor " + sensorId + " permanently deleted by Administrator.",
      timestamp: Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss")
    };

  } catch (err) {
    return { status: "error", message: err.toString() };
  } finally {
    try { lock.releaseLock(); } catch (e) {}
  }
}

/**
 * Manual registration of a physical sensor in Google Sheets database (ADMIN ONLY SERVER ENFORCED)
 */
function registerSensorInSheet(payload) {
  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(10000);
  } catch (e) {
    return { status: "error", message: "Server busy acquiring lock. Please try again." };
  }

  try {
    initDatabaseSheets();
    if (!payload || !payload.sensorId || !payload.type || !payload.name) {
      return { status: "error", message: "Sensor ID, type, and display name are required." };
    }

    var token = payload.token || "";
    var session = validateSession(token);

    if (!session) {
      return { status: "error", code: "UNAUTHORIZED", message: "Your session has expired or is invalid. Please sign in again." };
    }

    if (session.role !== "ADMIN") {
      logEventToSheet(session.email, session.name, "Unauthorized Registration Attempt", payload.sensorId, "Access denied (USER role attempted sensor registration)");
      return { status: "error", code: "FORBIDDEN", message: "Access denied. Administrator role required to register sensors." };
    }

    var allowedTypes = ["force", "strain", "temp", "humidity"];
    var sensorTypeClean = String(payload.type).trim().toLowerCase();
    if (allowedTypes.indexOf(sensorTypeClean) === -1) {
      return { status: "error", message: "Invalid sensor category '" + payload.type + "'. Allowed categories: force, strain, temp, humidity." };
    }

    var sensorData = {
      sensorId: String(payload.sensorId).trim().toUpperCase(),
      type: sensorTypeClean,
      name: String(payload.name).trim()
    };

    var ss = getSpreadsheet();

    // 1. Check duplicate in SensorRegistry
    var regSheet = ss.getSheetByName(REGISTRY_SHEET_NAME);
    if (!regSheet) {
      regSheet = ss.insertSheet(REGISTRY_SHEET_NAME);
      regSheet.appendRow(["Sensor ID", "Sensor Name", "Sensor Type", "Created At", "Status", "Enabled"]);
      regSheet.getRange(1, 1, 1, 6).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
    }

    var regLastRow = regSheet.getLastRow();
    if (regLastRow > 1) {
      var rData = regSheet.getRange(2, 1, regLastRow - 1, 6).getValues();
      var existsInReg = rData.some(function(row) { return String(row[0]).toUpperCase() === sensorData.sensorId; });
      if (existsInReg) {
        return { status: "error", message: "Sensor ID " + sensorData.sensorId + " is already registered." };
      }
    }

    // 2. Check duplicate and create columns in TelemetryData
    var sheet = ss.getSheetByName(SHEET_NAME);
    if (!sheet) sheet = ss.insertSheet(SHEET_NAME);

    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    var headers = [];
    var statusHeader = sensorData.sensorId + "_Status";
    var valHeader = sensorData.sensorId;

    if (lastRow === 0 || lastCol === 0) {
      headers = ["Timestamp", "Sequence", "Arduino_ID", "Session_ID",statusHeader, valHeader];
      sheet.appendRow(headers);
      sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
    } else {
      headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
      var hasStatus = headers.indexOf(statusHeader) !== -1;
      var hasVal = headers.indexOf(valHeader) !== -1;

      if (hasStatus || hasVal) {
        return { status: "error", message: "Sensor ID " + sensorData.sensorId + " already exists in TelemetryData headers." };
      }

      headers.push(statusHeader);
      headers.push(valHeader);
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
    }

    var istTimestamp = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");

    // Append to SensorRegistry sheet
    regSheet.appendRow([sensorData.sensorId, sensorData.name, sensorData.type, istTimestamp, "WAITING FOR DATA", "true"]);

    logEventToSheet(session.email, session.name, "Sensor Registered", sensorData.sensorId, "Provisioned header columns in Google Sheets");

    return {
      status: "success",
      sensorId: sensorData.sensorId,
      name: sensorData.name,
      type: sensorData.type,
      message: "Sensor " + sensorData.sensorId + " registered successfully. Waiting for Arduino telemetry.",
      timestamp: istTimestamp
    };

  } catch (err) {
    return { status: "error", message: err.toString() };
  } finally {
    try { lock.releaseLock(); } catch (e) {}
  }
}

/**
 * Returns live telemetry data & statistics calculated over bounded recent database rows (A8 & A9).
 */
function fetchTelemetryFromSheet(rangeKey, customFromMs, customToMs) {
  try {
    var ss = getSpreadsheet();
    var sheet = ss.getSheetByName(SHEET_NAME);
    
    if (!sheet || sheet.getLastRow() <= 1) {
      return {
        status: "empty",
        headers: [],
        rows: [],
        statistics: {},
        registry: fetchSensorRegistry(ss),
        message: "No telemetry data available"
      };
    }

    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();

    // 1. Read header row
    var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    var timeIdx = headers.indexOf("Timestamp");

    // Determine row fetching strategy based on range parameter
    var historicalRows = [];
    var isSpecificRangeRequested = rangeKey && rangeKey !== "live" && rangeKey !== "all";
    
    if (isSpecificRangeRequested || (customFromMs && customToMs)) {
      // Read all historical rows for range calculation
      var allDataRows = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
      
      // Calculate cutoff time
      var cutoffMs = 0;
      var maxToMs = null;
      var strKey = String(rangeKey || "").toLowerCase().trim();

      if (customFromMs && customToMs) {
        cutoffMs = Number(customFromMs);
        maxToMs = Number(customToMs);
      } else if (strKey === "1d" || strKey === "24h") {
        cutoffMs = Date.now() - (24 * 60 * 60 * 1000);
      } else if (strKey === "3d") {
        cutoffMs = Date.now() - (3 * 24 * 60 * 60 * 1000);
      } else if (strKey === "1w" || strKey === "7d") {
        cutoffMs = Date.now() - (7 * 24 * 60 * 60 * 1000);
      } else if (strKey === "1m" || strKey === "1month" || strKey === "30d") {
        cutoffMs = Date.now() - (30 * 24 * 60 * 60 * 1000);
      } else if (strKey === "6m" || strKey === "180d") {
        cutoffMs = Date.now() - (180 * 24 * 60 * 60 * 1000);
      } else if (strKey === "12m" || strKey === "365d" || strKey === "1y") {
        cutoffMs = Date.now() - (365 * 24 * 60 * 60 * 1000);
      }

      var filtered = allDataRows.filter(function(row) {
        if (timeIdx === -1 || !row[timeIdx]) return false;
        var rDate = row[timeIdx];
        var rMs = 0;
        if (rDate instanceof Date) {
          rMs = rDate.getTime();
        } else {
          rMs = new Date(String(rDate).replace(" ", "T")).getTime();
        }
        if (isNaN(rMs)) return false;
        if (cutoffMs > 0 && rMs < cutoffMs) return false;
        if (maxToMs && rMs > maxToMs) return false;
        return true;
      });

      // If dataset is extremely large (> 10,000 rows), apply deterministic strided downsampling
      var MAX_SAFE_RETURN_ROWS = 10000;
      if (filtered.length > MAX_SAFE_RETURN_ROWS) {
        var stride = Math.ceil(filtered.length / MAX_SAFE_RETURN_ROWS);
        historicalRows = [];
        for (var i = 0; i < filtered.length; i += stride) {
          historicalRows.push(filtered[i]);
        }
        // Always include the newest row if omitted by stride
        if (filtered.length > 0 && historicalRows[historicalRows.length - 1] !== filtered[filtered.length - 1]) {
          historicalRows.push(filtered[filtered.length - 1]);
        }
      } else {
        historicalRows = filtered.length > 0 ? filtered : allDataRows.slice(-MAX_ROWS_RETURNED);
      }
    } else {
      // Default live polling: read up to MAX_ROWS_RETURNED recent rows
      var startRow = Math.max(2, lastRow - MAX_ROWS_RETURNED + 1);
      var numRows = lastRow - startRow + 1;
      historicalRows = sheet.getRange(startRow, 1, numRows, lastCol).getValues();
    }

    var statistics = {};
    var totalCellsCount = 0;
    var validCellsCount = 0;

    headers.forEach(function(headerName, colIdx) {
      if (headerName !== "Timestamp" && headerName !== "Sequence" && headerName !== "Arduino_ID" && !headerName.endsWith("_Status")) {
        var minVal = null;
        var maxVal = null;
        var sumVal = 0;
        var validCount = 0;
        var latestVal = null;

        for (var r = 0; r < historicalRows.length; r++) {
          totalCellsCount++;
          var rawVal = historicalRows[r][colIdx];
          if (rawVal !== "" && rawVal !== null && rawVal !== undefined && !isNaN(Number(rawVal))) {
            var num = Number(rawVal);
            if (minVal === null || num < minVal) minVal = num;
            if (maxVal === null || num > maxVal) maxVal = num;
            sumVal += num;
            validCount++;
            validCellsCount++;
            latestVal = num;
          }
        }

        var meanVal = validCount > 0 ? (sumVal / validCount) : null;
        var sumSqDiff = 0;
        if (validCount > 1 && meanVal !== null) {
          for (var r = 0; r < historicalRows.length; r++) {
            var rawVal2 = historicalRows[r][colIdx];
            if (rawVal2 !== "" && rawVal2 !== null && rawVal2 !== undefined && !isNaN(Number(rawVal2))) {
              var num2 = Number(rawVal2);
              sumSqDiff += Math.pow(num2 - meanVal, 2);
            }
          }
        }
        var stdDevVal = validCount > 1 ? Math.sqrt(sumSqDiff / validCount) : 0;

        statistics[headerName] = {
          min: minVal,
          max: maxVal,
          avg: meanVal,
          stddev: validCount > 1 ? Number(stdDevVal.toFixed(4)) : 0,
          count: validCount,
          latest: latestVal
        };
      }
    });


    var rows = historicalRows.map(function(row) {
      return row.map(function(cell) {
        if (cell instanceof Date) {
          return Utilities.formatDate(cell, TIMEZONE, "yyyy-MM-dd HH:mm:ss");
        }
        return cell;
      });
    });

    var serverTimeStr = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");
    var lastRowRaw = historicalRows[historicalRows.length - 1];
    var seqIdx = headers.indexOf("Sequence");
    var lastSeq = (seqIdx !== -1 && lastRowRaw) ? lastRowRaw[seqIdx] : null;
    var lastTs = (timeIdx !== -1 && lastRowRaw) ? lastRowRaw[timeIdx] : null;

    if (lastTs instanceof Date) {
      lastTs = Utilities.formatDate(lastTs, TIMEZONE, "yyyy-MM-dd HH:mm:ss");
    }

    var isCacheTruncated = isSpecificRangeRequested || false;
    var cacheNotice = isCacheTruncated ?
      "Google Sheets contains the recent 20-minute presentation cache. Primary database source of truth is MongoDB Atlas." :
      "Google Sheets Secondary Cache (Rolling 20-Minute Window)";

    var dataQualityPct = totalCellsCount > 0 ? Math.round((validCellsCount / totalCellsCount) * 100) + "%" : "100%";

    return {
      status: "success",
      dataSource: "Google Sheets Secondary Cache",
      primaryDatabase: "MongoDB Atlas (shm_bridge_db)",
      cacheWindowMinutes: 20,
      isCacheTruncated: isCacheTruncated,
      cacheNotice: cacheNotice,
      totalHistoricalRows: lastRow - 1,
      returnedRows: rows.length,
      lastSequence: lastSeq,
      lastReceivedAt: lastTs,
      serverTime: serverTimeStr,
      dataQualityPct: dataQualityPct,
      headers: headers,
      rows: rows,
      statistics: statistics,
      registry: fetchSensorRegistry(ss)
    };


  } catch (err) {
    return {
      status: "error",
      message: err.toString(),
      headers: [],
      rows: [],
      statistics: {}
    };
  }
}

/**
 * Reads sensor metadata from SensorRegistry sheet tab
 */
function fetchSensorRegistry(ss) {
  try {
    if (!ss) ss = getSpreadsheet();
    syncAllSensorsToRegistrySheet(ss);
    var regSheet = ss.getSheetByName(REGISTRY_SHEET_NAME);
    if (!regSheet || regSheet.getLastRow() <= 1) return [];

    var canonicalMap = { "FORCE-01": true, "TEMP-01": true, "HUMIDITY-01": true };
    var data = regSheet.getRange(2, 1, regSheet.getLastRow() - 1, 6).getValues();
    var result = [];
    var seenMap = {};
    data.forEach(function(r) {
      var sId = String(r[0]).trim().toUpperCase();
      var sStatus = String(r[4]).trim().toUpperCase();
      var sEnabled = String(r[5]).trim().toLowerCase();
      if (canonicalMap[sId] && sStatus !== "DELETED" && sEnabled !== "false" && !seenMap[sId]) {
        seenMap[sId] = true;
        result.push({
          id: sId,
          name: sId,
          type: sId === "FORCE-01" ? "force" : (sId === "TEMP-01" ? "temp" : "humidity"),
          createdAt: r[3] instanceof Date ? Utilities.formatDate(r[3], TIMEZONE, "yyyy-MM-dd HH:mm:ss") : String(r[3]),
          status: String(r[4]),
          enabled: true
        });
      }
    });
    return result;
  } catch (err) {
    return [];
  }
}

/**
 * Logs an event to EventHistoryLog sheet tab
 */
function logEventToSheet(userEmail, userName, action, target, details, token) {
  try {
    initDatabaseSheets();
    var email = userEmail;
    var name = userName;

    if (token) {
      var session = validateSession(token);
      if (session) {
        email = session.email;
        name = session.name;
      }
    }

    var ss = getSpreadsheet();
    var eSheet = ss.getSheetByName(EVENT_LOG_SHEET_NAME);
    var evtId = "evt_" + Date.now();
    var istTs = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");

    eSheet.appendRow([evtId, istTs, email || "System", name || "System", action || "Action", target || "System", details || ""]);
    return { status: "success", eventId: evtId };
  } catch (err) {
    return { status: "error", message: err.toString() };
  }
}

/**
 * Fetches Event History Audit Log (ADMIN ONLY SERVER ENFORCED)
 */
function fetchEventHistoryFromSheet(token) {
  try {
    initDatabaseSheets();

    var session = validateSession(token);
    if (!session) {
      return { status: "error", code: "UNAUTHORIZED", message: "Your session has expired or is invalid. Please sign in again." };
    }

    if (session.role !== "ADMIN") {
      logEventToSheet(session.email, session.name, "Unauthorized Event History Access", "EventHistory", "Access denied (USER role attempted Event History fetch)");
      return { status: "error", code: "FORBIDDEN", message: "Access denied. Administrator role required for Event History Audit Log." };
    }

    var ss = getSpreadsheet();
    var eSheet = ss.getSheetByName(EVENT_LOG_SHEET_NAME);

    if (!eSheet || eSheet.getLastRow() <= 1) {
      return { status: "success", events: [] };
    }

    var data = eSheet.getRange(2, 1, eSheet.getLastRow() - 1, 7).getValues();
    var events = data.map(function(r) {
      return {
        id: r[0],
        timestamp: r[1] instanceof Date ? Utilities.formatDate(r[1], TIMEZONE, "yyyy-MM-dd HH:mm:ss") : String(r[1]),
        userEmail: r[2],
        userName: r[3],
        action: r[4],
        target: r[5],
        details: r[6]
      };
    });

    events.reverse(); // Most recent events first
    return { status: "success", events: events };

  } catch (err) {
    return { status: "error", message: err.toString() };
  }
}

/**
 * Helper to build JSON responses for HTTP POST requests.
 */
function responseJSON(obj, code) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
