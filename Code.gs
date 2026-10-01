/**
 * ============================================================================
 * STRUCTURAL HEALTH MONITORING — GOOGLE APPS SCRIPT BACKEND (Code.gs)
 * ============================================================================
 * PRIMARY DATABASE: MongoDB Atlas (Authoritative Permanent History)
 * SECONDARY CACHE: Google Sheets ("TelemetryData" tab — Rolling 10-Minute Window)
 * Batch Synchronization: Every 120 seconds (~120 readings per batch from MongoDB)
 * 
 * Rules:
 * 1. ONLY Code.gs and Index.html in Apps Script project.
 * 2. Database sheet tab MUST be named "TelemetryData".
 * 3. Timezone for timestamps is "Asia/Kolkata" (IST).
 * 4. XFrameOptionsMode ALLOWALL for Google Sites embedding.
 */

var SPREADSHEET_ID = "1zQkuCjkcBQP1UDKhM6w8F-gxD9xzPbDeMbHW8mKrDFU";
var SHEET_NAME = "TelemetryData";
var REGISTRY_SHEET_NAME = "SensorRegistry";
var TIMEZONE = "Asia/Kolkata";
var MAX_ROLLING_ROWS = 600; // Rolling 10 minutes (~600 rows @ 1Hz)

/**
 * Returns the target Google Spreadsheet.
 */
function getSpreadsheet() {
  if (SPREADSHEET_ID && SPREADSHEET_ID.length > 20) {
    try {
      return SpreadsheetApp.openById(SPREADSHEET_ID);
    } catch (e) {
      Logger.log("openById failed: " + e.toString());
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
 * SECURE MONGODB QUERY API PROXY (for Google Sites deployment)
 * ============================================================================
 */
function proxyMongoApi(request) {
  try {
    request = request || {};
    var path = String(request.path || "").trim();
    var method = String(request.method || "GET").toUpperCase();
    var query = request.query || {};
    var body = request.body || null;

    var allowedPaths = {
      "/api/v1/telemetry/latest": true,
      "/api/v1/telemetry/export": true,
      "/api/v1/telemetry/range": true,
      "/api/v1/telemetry/stats": true,
      "/api/v1/sync/status": true,
      "/health": true
    };

    if (!allowedPaths[path]) {
      return { status: "error", message: "Query API route not allowed." };
    }

    var properties = PropertiesService.getScriptProperties();
    var baseUrl = String(properties.getProperty("QUERY_API_BASE_URL") || "").trim();
    var apiKey = String(properties.getProperty("QUERY_API_KEY") || "").trim();

    if (!baseUrl || !apiKey) {
      return {
        status: "error",
        message: "Proxy not configured. Set QUERY_API_BASE_URL and QUERY_API_KEY in Script Properties."
      };
    }

    baseUrl = baseUrl.replace(/\/+$/, "");
    var queryParts = [];
    Object.keys(query).forEach(function(key) {
      var val = query[key];
      if (val !== null && val !== undefined && val !== "") {
        queryParts.push(encodeURIComponent(key) + "=" + encodeURIComponent(String(val)));
      }
    });

    var url = baseUrl + path + (queryParts.length > 0 ? "?" + queryParts.join("&") : "");
    var options = {
      method: method.toLowerCase(),
      headers: { "X-API-KEY": apiKey },
      muteHttpExceptions: true,
      followRedirects: true
    };

    if (method === "POST") {
      options.contentType = "application/json";
      options.payload = JSON.stringify(body || {});
    }

    var response = UrlFetchApp.fetch(url, options);
    var text = response.getContentText();
    return JSON.parse(text);
  } catch (err) {
    return { status: "error", message: "MongoDB Query API proxy error: " + err.toString() };
  }
}

/**
 * Serves HTML frontend or returns JSON API data for GET requests
 */
function doGet(e) {
  var action = e && e.parameter ? e.parameter.action : null;

  if (action === "getTelemetry") {
    var telemetryData = fetchTelemetryFromSheet();
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

  var htmlTemplate = HtmlService.createTemplateFromFile('index');
  return htmlTemplate.evaluate()
    .setTitle('BRIDGE SHM — Structural Health Monitoring')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

/**
 * Fetches rolling telemetry dataset from TelemetryData sheet tab
 */
function fetchTelemetryFromSheet() {
  try {
    var ss = getSpreadsheet();
    var sheet = ss ? ss.getSheetByName(SHEET_NAME) : null;

    if (!sheet || sheet.getLastRow() <= 1) {
      return { status: "empty", headers: [], rows: [], count: 0, message: "Waiting for Arduino telemetry data..." };
    }

    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    var dataRows = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

    var seqIdx = headers.indexOf("Sequence");
    var timeIdx = headers.indexOf("Timestamp");

    var lastSeq = null;
    var lastTs = null;

    if (dataRows.length > 0) {
      var newestRow = dataRows[dataRows.length - 1];
      if (seqIdx !== -1) lastSeq = newestRow[seqIdx];
      if (timeIdx !== -1) lastTs = newestRow[timeIdx];
    }

    return {
      status: "success",
      headers: headers,
      rows: dataRows,
      count: dataRows.length,
      lastSequence: lastSeq,
      lastReceivedAt: lastTs,
      serverTime: Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss")
    };
  } catch (err) {
    return { status: "error", message: "Failed to fetch telemetry from sheet: " + err.toString() };
  }
}

/**
 * High-Speed Bulk Telemetry Ingestion for 2-Minute Synchronizer Batch (~120 packets)
 */
function processTelemetryBatch(ss, sheet, packets) {
  if (!packets || !packets.length) {
    return { success: true, count: 0, duplicates: 0, message: "No packets to process" };
  }

  var lock = LockService.getScriptLock();
  try {
    lock.waitLock(10000);
  } catch (e) {
    return { success: false, count: 0, error: "Lock timeout" };
  }

  try {
    if (!ss) ss = getSpreadsheet();
    if (!sheet) sheet = ss.getSheetByName(SHEET_NAME);
    if (!sheet) sheet = ss.insertSheet(SHEET_NAME);

    // Filter valid packets
    var validPackets = [];
    packets.forEach(function(pkt) {
      if (!pkt || typeof pkt !== "object") return;
      if (!pkt.arduino_id || pkt.sequence === undefined || pkt.sequence === null) return;
      validPackets.push(pkt);
    });

    if (!validPackets.length) {
      return { success: true, count: 0, duplicates: 0, message: "No valid packets" };
    }

    validPackets.sort(function(a, b) { return a.sequence - b.sequence; });

    // Determine or build headers
    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    var headers = [];

    if (lastRow === 0 || lastCol === 0) {
      headers = ["Timestamp", "Sequence", "Arduino_ID", "Session_ID", "FORCE-01_Status", "FORCE-01", "TEMP-01_Status", "TEMP-01", "HUMIDITY-01_Status", "HUMIDITY-01"];
      sheet.appendRow(headers);
      sheet.getRange(1, 1, 1, headers.length).setFontWeight("bold").setBackground("#1E293B").setFontColor("#FFFFFF");
      lastRow = 1;
      lastCol = headers.length;
    } else {
      headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    }

    // Build batch rows
    var batchRows = [];
    var fallbackIst = Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd HH:mm:ss");

    validPackets.forEach(function(pkt) {
      var istTimestamp = pkt.timestamp || fallbackIst;
      var rowData = new Array(headers.length);

      headers.forEach(function(headerName, index) {
        if (headerName === "Timestamp") rowData[index] = istTimestamp;
        else if (headerName === "Sequence") rowData[index] = pkt.sequence;
        else if (headerName === "Arduino_ID") rowData[index] = pkt.arduino_id;
        else if (headerName === "Session_ID") rowData[index] = pkt.session_id || "LEGACY";
        else if (headerName.endsWith("_Status")) {
          var sensorId = headerName.replace("_Status", "");
          rowData[index] = (pkt.sensors && pkt.sensors[sensorId] && pkt.sensors[sensorId].status) ? pkt.sensors[sensorId].status : "OFFLINE";
        } else {
          var sensorId = headerName;
          if (pkt.sensors && pkt.sensors[sensorId] && pkt.sensors[sensorId].value !== undefined && pkt.sensors[sensorId].value !== null) {
            rowData[index] = pkt.sensors[sensorId].value;
          } else {
            rowData[index] = "";
          }
        }
      });
      batchRows.push(rowData);
    });

    if (batchRows.length > 0) {
      var startRow = sheet.getLastRow() + 1;
      sheet.getRange(startRow, 1, batchRows.length, headers.length).setValues(batchRows);
      SpreadsheetApp.flush();
    }

    return {
      success: true,
      count: batchRows.length,
      duplicates: 0,
      totalValid: validPackets.length
    };

  } catch (err) {
    Logger.log("Error in processTelemetryBatch: " + err.toString());
    return { success: false, count: 0, error: err.toString() };
  } finally {
    try { lock.releaseLock(); } catch (e) {}
  }
}

/**
 * Prunes TelemetryData rows to enforce rolling 10-minute (~600 readings) retention.
 * Deleting old rows from Google Sheets NEVER affects MongoDB Atlas permanent store.
 */
function pruneOldTelemetryFromSheet(ss, sheet, rollingWindowMinutes) {
  try {
    if (!ss) ss = getSpreadsheet();
    if (!sheet) sheet = ss.getSheetByName(SHEET_NAME);

    if (!sheet || sheet.getLastRow() <= 1) {
      return 0;
    }

    var windowMins = Number(rollingWindowMinutes) || 10;
    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();

    var headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    var timeIdx = headers.indexOf("Timestamp");

    if (timeIdx === -1) return 0;

    var data = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
    var totalDataRows = data.length;
    var rowsToDelete = [];

    // 1. Time-based cutoff pruning
    var maxTsMs = 0;
    data.forEach(function(row) {
      var rawTime = row[timeIdx];
      if (!rawTime) return;
      var rMs = (rawTime instanceof Date) ? rawTime.getTime() : new Date(String(rawTime).replace(" ", "T")).getTime();
      if (!isNaN(rMs) && rMs > maxTsMs) maxTsMs = rMs;
    });

    if (maxTsMs > 0) {
      var cutoffMs = maxTsMs - (windowMins * 60 * 1000);
      data.forEach(function(row, index) {
        var rawTime = row[timeIdx];
        if (!rawTime) return;
        var rMs = (rawTime instanceof Date) ? rawTime.getTime() : new Date(String(rawTime).replace(" ", "T")).getTime();
        if (!isNaN(rMs) && rMs < cutoffMs) {
          rowsToDelete.push(index + 2);
        }
      });
    }

    // 2. Count-based pruning (MAX_ROLLING_ROWS = 600)
    if (totalDataRows > MAX_ROLLING_ROWS) {
      var excessCount = totalDataRows - MAX_ROLLING_ROWS;
      for (var r = 0; r < excessCount; r++) {
        var rowNum = r + 2;
        if (rowsToDelete.indexOf(rowNum) === -1) {
          rowsToDelete.push(rowNum);
        }
      }
    }

    rowsToDelete.sort(function(a, b) { return a - b; });

    // Delete from bottom to top so row indices remain valid
    for (var i = rowsToDelete.length - 1; i >= 0; i--) {
      sheet.deleteRow(rowsToDelete[i]);
    }

    if (rowsToDelete.length > 0) {
      Logger.log("[PRUNE] Pruned " + rowsToDelete.length + " oldest rows from TelemetryData sheet.");
    }

    return rowsToDelete.length;

  } catch (err) {
    Logger.log("Error in pruneOldTelemetryFromSheet: " + err.toString());
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

    // Prune rows older than 10 minutes / >600 rows
    var prunedCount = pruneOldTelemetryFromSheet(ss, sheet, windowMins);

    var maxSeqInBatch = null;
    packets.forEach(function(p) {
      if (p && p.sequence !== undefined && (maxSeqInBatch === null || p.sequence > maxSeqInBatch)) {
        maxSeqInBatch = p.sequence;
      }
    });

    return responseJSON({
      status: "success",
      message: "MongoDB Atlas to Google Sheets synchronization completed successfully",
      count: result.count,
      pruned: prunedCount,
      retainedRows: Math.max(0, sheet.getLastRow() - 1),
      lastSyncedSequence: maxSeqInBatch,
      spreadsheetId: ss.getId(),
      sheetName: sheet.getName()
    }, 200);

  } catch (err) {
    return responseJSON({ status: "error", message: "Server error in doPost: " + err.toString() }, 500);
  }
}

function responseJSON(data, statusCode) {
  return ContentService.createTextOutput(JSON.stringify(data)).setMimeType(ContentService.MimeType.JSON);
}
