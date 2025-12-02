/** Code.gs **/
// Configuration helpers
function getBaseUrl_() {
  const props = PropertiesService.getScriptProperties();
  const baseUrl = props.getProperty('API_BASE_URL');
  if (!baseUrl) {
    throw new Error('API_BASE_URL not configured. Please set it in Script Properties.');
  }
  return baseUrl;
}

function getBearerToken_() {
  const props = PropertiesService.getScriptProperties();
  return props.getProperty('API_BEARER_TOKEN') || '';
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('User Audit')
    .addItem('Open User Audit Panel', 'openAuditSidebar')
    .addToUi();
  // Ensure the sidebar is always visible
  openAuditSidebar();
}

function openAuditSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('AuditSidebar')
    .setTitle('Drive Audit')
    .setWidth(320);
  SpreadsheetApp.getUi().showSidebar(html);
}

/**
 * Polls job status until completion
 */
function pollJobStatus_(jobId, maxAttempts = 120) {  // 120 * 3s = 6 minutes max
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const status = httpJsonGet_(`${getBaseUrl_()}/audit-status/${jobId}`);

      if (status.status === 'completed') {
        return status;
      } else if (status.status === 'failed') {
        throw new Error(status.error || 'Audit failed');
      }

      // Still processing, wait 3 seconds before next poll
      Utilities.sleep(3000);
    } catch (e) {
      if (attempt === maxAttempts - 1) {
        throw e;
      }
      Utilities.sleep(3000);
    }
  }
  throw new Error('Timeout: Audit took too long to complete');
}

/**
 * Runs audit for all users and returns an array of download links.
 */
function runAuditForAll() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Drive File Audit");
  const data = sheet.getDataRange().getValues();
  const downloadLinks = [];

  for (let i = 1; i < data.length; i++) {
    const email = data[i][0];
    if (!email) continue;
    try {
      // Start the audit job
      sheet.getRange(i+1, 2).setValue('⏳ Processing...');
      const jobResponse = httpJsonPost_(`${getBaseUrl_()}/audit-user`, { user_email: email });
      const jobId = jobResponse.job_id;

      // Poll for completion
      const result = pollJobStatus_(jobId);

      sheet.getRange(i+1, 2).setValue('✅ Ready');
      sheet.getRange(i+1, 3).setValue(new Date());
      sheet.getRange(i+1, 4).setFormula(`=HYPERLINK("${result.download_link}","Download")`);
      downloadLinks.push(result.download_link);
    } catch (e) {
      sheet.getRange(i+1, 2).setValue('❌ Failed');
      sheet.getRange(i+1, 3).setValue(e.toString());
    }
  }
  return downloadLinks;
}

/**
 * Transfers all folders; returns summary per row.
 */
function transferFoldersForAll() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Folder Transfer");
  const data = sheet.getDataRange().getValues();
  const summary = [];

  for (let i = 1; i < data.length; i++) {
    const [fromEmail, toEmail] = data[i];
    if (!fromEmail || !toEmail) continue;
    try {
      const result = httpJsonPost_(`${getBaseUrl_()}/transfer-owned-files`, {
        from_email: fromEmail,
        to_email: toEmail,
        transfer_type: 'direct'
      });
      sheet.getRange(i+1, 3).setValue('✅ Success');
      sheet.getRange(i+1, 4).setValue(result.folder_id || result.message);
      sheet.getRange(i+1, 5).setValue(new Date());
      summary.push({ fromEmail, toEmail, status: 'Success', info: result.folder_id || result.message });
    } catch (e) {
      sheet.getRange(i+1, 3).setValue('❌ Failed');
      sheet.getRange(i+1, 4).setValue(e.toString());
      sheet.getRange(i+1, 5).setValue(new Date());
      summary.push({ fromEmail, toEmail, status: 'Failed', error: e.toString() });
    }
  }
  return summary;
}

/**
 * Transfers single files; returns summary per row.
 */
function transferSingleFileForAll() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Single File Transfer");
  const data = sheet.getDataRange().getValues();
  const summary = [];

  for (let i = 1; i < data.length; i++) {
    const [fileId, fromEmail, toEmail] = data[i];
    if (!fileId || !fromEmail || !toEmail) continue;
    try {
      const result = httpJsonPost_(`${getBaseUrl_()}/transfer-single-file`, { file_id: fileId, from_email: fromEmail, to_email: toEmail });
      sheet.getRange(i+1, 4).setValue('✅ Success');
      sheet.getRange(i+1, 5).setValue(result.total_transferred);
      sheet.getRange(i+1, 6).setValue(JSON.stringify(result.errors));
      sheet.getRange(i+1, 7).setValue(new Date());
      summary.push({ fileId, fromEmail, toEmail, status: 'Success', transferred: result.total_transferred });
    } catch (e) {
      sheet.getRange(i+1, 4).setValue('❌ Failed');
      sheet.getRange(i+1, 5).setValue('');
      sheet.getRange(i+1, 6).setValue(e.toString());
      sheet.getRange(i+1, 7).setValue(new Date());
      summary.push({ fileId, fromEmail, toEmail, status: 'Failed', error: e.toString() });
    }
  }
  return summary;
}

/**
 * HTTP GET helper with retries and bearer auth
 */
function httpJsonGet_(url) {
  const maxAttempts = 3;
  let attempt = 0;
  let lastErr = null;
  while (attempt < maxAttempts) {
    try {
      const resp = UrlFetchApp.fetch(url, {
        method: 'get',
        headers: getBearerToken_() ? { Authorization: `Bearer ${getBearerToken_()}` } : {},
        muteHttpExceptions: true,
        timeout: 30000  // 30 seconds for status checks
      });
      const status = resp.getResponseCode();
      const text = resp.getContentText();
      if (status >= 200 && status < 300) {
        return JSON.parse(text || '{}');
      }
      // retry on 429/5xx
      if (status === 429 || status >= 500) {
        Utilities.sleep(Math.pow(2, attempt) * 500);
        attempt++;
        continue;
      }
      throw new Error(`HTTP ${status}: ${text}`);
    } catch (e) {
      lastErr = e;
      Utilities.sleep(Math.pow(2, attempt) * 500);
      attempt++;
    }
  }
  throw lastErr || new Error('Request failed');
}

/**
 * HTTP POST helper with retries and bearer auth
 */
function httpJsonPost_(url, body) {
  const maxAttempts = 3;
  let attempt = 0;
  let lastErr = null;
  while (attempt < maxAttempts) {
    try {
      const resp = UrlFetchApp.fetch(url, {
        method: 'post',
        contentType: 'application/json',
        headers: getBearerToken_() ? { Authorization: `Bearer ${getBearerToken_()}` } : {},
        payload: JSON.stringify(body),
        muteHttpExceptions: true,
        timeout: 360000  // 6 minutes (maximum allowed by Google Apps Script)
      });
      const status = resp.getResponseCode();
      const text = resp.getContentText();
      if (status >= 200 && status < 300) {
        return JSON.parse(text || '{}');
      }
      // retry on 429/5xx
      if (status === 429 || status >= 500) {
        Utilities.sleep(Math.pow(2, attempt) * 500);
        attempt++;
        continue;
      }
      throw new Error(`HTTP ${status}: ${text}`);
    } catch (e) {
      lastErr = e;
      Utilities.sleep(Math.pow(2, attempt) * 500);
      attempt++;
    }
  }
  throw lastErr || new Error('Request failed');
}