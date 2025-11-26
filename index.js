#!/usr/bin/env node
const fs = require('fs');
const http = require('http');
const path = require('path');
const AriClient = require('ari-client');
const { v4: uuidv4 } = require('uuid');
const dotenv = require('dotenv');
const mysql = require('mysql2/promise');

let currentConfig = null;
let recordingSearchDirs = new Set();

function logWithTimestamp(level, message, ...args) {
  const timestamp = new Date().toISOString();
  const logMethod = typeof console[level] === 'function' ? console[level] : console.log;

  if (typeof message === 'string') {
    logMethod.call(console, `${timestamp} ${message}`, ...args);
    return;
  }

  logMethod.call(console, timestamp, message, ...args);
}

function refreshConfig() {
  dotenv.config({ override: true });

  const {
    ARI_URL,
    ARI_USERNAME,
    ARI_PASSWORD,
    ARI_TRUNK,
    OUTBOUND_NUMBER,
    OUTBOUND_NUMBER_FILE,
    TARGET_ENDPOINT,
    TARGET_EXTENSION = '777',
    TARGET_CONTEXT = 'default2',
    STASIS_APP = 'outbound_dialer',
    CALL_TIMEOUT = '30',
    MAX_CC,
    RECORDINGS_DIR,
    RECORDING_FORMAT,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
    MYSQL_TABLE,
    CALLER_ID
  } = process.env;

  if (!ARI_URL || !ARI_USERNAME || !ARI_PASSWORD || !ARI_TRUNK) {
    const message =
      'Missing required configuration. Ensure ARI_URL, ARI_USERNAME, ARI_PASSWORD, and ARI_TRUNK are set.';
    logWithTimestamp('error', message);
    throw new Error(message);
  }

  if (!OUTBOUND_NUMBER && !OUTBOUND_NUMBER_FILE) {
    const message = 'Missing outbound configuration. Provide either OUTBOUND_NUMBER or OUTBOUND_NUMBER_FILE.';
    logWithTimestamp('error', message);
    throw new Error(message);
  }

  if (OUTBOUND_NUMBER && OUTBOUND_NUMBER_FILE) {
    logWithTimestamp(
      'warn',
      'Both OUTBOUND_NUMBER and OUTBOUND_NUMBER_FILE are set. Using numbers from OUTBOUND_NUMBER_FILE.'
    );
  }

  const parsedMaxCc = MAX_CC ? parseInt(MAX_CC, 10) : 1;
  if (!Number.isInteger(parsedMaxCc) || parsedMaxCc <= 0) {
    const message = 'MAX_CC must be a positive integer when specified.';
    logWithTimestamp('error', message);
    throw new Error(message);
  }

  const recordingFormat = (RECORDING_FORMAT || 'wav').trim() || 'wav';
  const resolvedRecordingsDir = path.resolve(
    (RECORDINGS_DIR && RECORDINGS_DIR.trim()) || path.join(process.cwd(), 'recordings')
  );

  const mysqlTableName = (MYSQL_TABLE && MYSQL_TABLE.trim()) || 'call_leg_timelines';
  const mysqlPortNumber = (() => {
    if (!MYSQL_PORT) {
      return null;
    }
    const parsed = parseInt(MYSQL_PORT, 10);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
  })();

  const mysqlConfigProvided = [MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_TABLE]
    .filter((value) => value != null && String(value).trim() !== '').length > 0;
  const mysqlConfigComplete = MYSQL_HOST && MYSQL_USER && MYSQL_DATABASE;

  const callTimeoutSeconds = (() => {
    const parsed = parseInt(CALL_TIMEOUT, 10);
    if (Number.isInteger(parsed) && parsed > 0) {
      return parsed;
    }
    return 30;
  })();

  return {
    ARI_URL,
    ARI_USERNAME,
    ARI_PASSWORD,
    ARI_TRUNK,
    OUTBOUND_NUMBER,
    OUTBOUND_NUMBER_FILE,
    TARGET_ENDPOINT,
    TARGET_EXTENSION,
    TARGET_CONTEXT,
    STASIS_APP,
    CALL_TIMEOUT,
    callTimeoutSeconds,
    MAX_CC,
    callConcurrencyLimit: parsedMaxCc,
    RECORDINGS_DIR,
    RECORDING_FORMAT,
    recordingFormat,
    resolvedRecordingsDir,
    recordingSearchDirList: [
      resolvedRecordingsDir,
      '/var/spool/asterisk/recording',
      '/var/spool/asterisk/monitor'
    ],
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
    MYSQL_TABLE,
    mysqlTableName,
    mysqlPortNumber,
    mysqlConfigProvided,
    mysqlConfigComplete,
    CALLER_ID
  };
}

let mysqlPool = null;
let mysqlLoggingEnabled = false;
let mysqlInitializationPromise = null;

function applyConfig(config) {
  recordingSearchDirs = new Set(config.recordingSearchDirList);

  try {
    if (!fs.existsSync(config.resolvedRecordingsDir)) {
      fs.mkdirSync(config.resolvedRecordingsDir, { recursive: true });
    }
  } catch (err) {
    const message = `Failed to ensure recordings directory at ${config.resolvedRecordingsDir}: ${err.message}`;
    logWithTimestamp('error', message);
    throw new Error(message);
  }

  mysqlInitializationPromise = null;
  mysqlLoggingEnabled = false;
  if (mysqlPool) {
    mysqlPool.end().catch(() => {});
    mysqlPool = null;
  }
}

currentConfig = refreshConfig();
applyConfig(currentConfig);

let dialerClient = null;
let dialerStarted = false;
let dialerStartingPromise = null;

function escapeIdentifier(identifier) {
  const name = String(identifier || '').trim();
  if (!name) {
    return '``';
  }
  return `\`${name.replace(/`/g, '``')}\``;
}

function toMysqlDateTime(value) {
  if (value == null) {
    return null;
  }

  const date = value instanceof Date ? value : new Date(value);
  const time = date.getTime();
  if (Number.isNaN(time)) {
    return null;
  }

  return new Date(time).toISOString().slice(0, 19).replace('T', ' ');
}

async function initializeMySql() {
  if (!currentConfig?.mysqlConfigProvided) {
    mysqlLoggingEnabled = false;
    return false;
  }

  if (!currentConfig.mysqlConfigComplete) {
    logWithTimestamp(
      'warn',
      '[MySQL] Partial configuration detected. Provide MYSQL_HOST, MYSQL_USER, and MYSQL_DATABASE to enable logging.'
    );
    mysqlLoggingEnabled = false;
    return false;
  }

  const connectionConfig = {
    host: currentConfig.MYSQL_HOST,
    user: currentConfig.MYSQL_USER,
    password: currentConfig.MYSQL_PASSWORD || undefined,
    database: currentConfig.MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0
  };

  if (currentConfig.mysqlPortNumber) {
    connectionConfig.port = currentConfig.mysqlPortNumber;
  }

  try {
    mysqlPool = mysql.createPool(connectionConfig);
    const tableIdentifier = escapeIdentifier(currentConfig.mysqlTableName);
    const createTableSql = `
      CREATE TABLE IF NOT EXISTS ${tableIdentifier} (
        call_id VARCHAR(64) NOT NULL PRIMARY KEY,
        recording_path TEXT,
        leg_a_status VARCHAR(255),
        leg_a_number VARCHAR(255),
        leg_a_channel VARCHAR(255),
        leg_a_paired_channel VARCHAR(255),
        leg_a_peer VARCHAR(255),
        leg_a_caller VARCHAR(255),
        leg_a_dial_string TEXT,
        leg_a_answered_by VARCHAR(255),
        leg_a_start DATETIME NULL,
        leg_a_answer DATETIME NULL,
        leg_a_end DATETIME NULL,
        leg_b_status VARCHAR(255),
        leg_b_number VARCHAR(255),
        leg_b_channel VARCHAR(255),
        leg_b_paired_channel VARCHAR(255),
        leg_b_peer VARCHAR(255),
        leg_b_caller VARCHAR(255),
        leg_b_dial_string TEXT,
        leg_b_answered_by VARCHAR(255),
        leg_b_start DATETIME NULL,
        leg_b_answer DATETIME NULL,
        leg_b_end DATETIME NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `;

    await mysqlPool.query(createTableSql);
    mysqlLoggingEnabled = true;
    logWithTimestamp('log', `[MySQL] Logging enabled using table ${tableIdentifier}.`);
    return true;
  } catch (err) {
    mysqlLoggingEnabled = false;
    if (mysqlPool) {
      mysqlPool.end().catch(() => {});
      mysqlPool = null;
    }
    throw err;
  }
}

function ensureMysqlInitialization() {
  if (!mysqlInitializationPromise) {
    mysqlInitializationPromise = initializeMySql().catch((err) => {
      logWithTimestamp('error', `[MySQL] Failed to initialize logging:`, err.message);
      return false;
    });
  }
  return mysqlInitializationPromise;
}

function normalizeTimelineString(value) {
  if (value == null) {
    return null;
  }
  const str = String(value).trim();
  return str.length > 0 ? str : null;
}

function extractTimelinePersistenceValues(timeline) {
  if (!timeline) {
    return {
      status: null,
      number: null,
      channel: null,
      pairedChannel: null,
      peer: null,
      caller: null,
      dialString: null,
      answeredBy: null,
      start: null,
      answer: null,
      end: null
    };
  }

  return {
    status: normalizeTimelineString(timeline.lastStatus),
    number: normalizeTimelineString(timeline.targetNumber),
    channel: normalizeTimelineString(timeline.channelId),
    pairedChannel: normalizeTimelineString(timeline.pairedChannelId),
    peer: normalizeTimelineString(timeline.peerName),
    caller: normalizeTimelineString(timeline.callerName),
    dialString: normalizeTimelineString(timeline.dialString),
    answeredBy: normalizeTimelineString(timeline.answeredBy),
    start: toMysqlDateTime(timeline.startedAt),
    answer: toMysqlDateTime(timeline.answeredAt),
    end: toMysqlDateTime(timeline.endedAt)
  };
}

async function persistLegTimelines(callId, callState, recordingPath) {
  if (!callId || !callState) {
    return;
  }

  const mysqlReady = await ensureMysqlInitialization();
  if (!mysqlReady || !mysqlLoggingEnabled || !mysqlPool) {
    return;
  }

  const legA = extractTimelinePersistenceValues(callState.legATimeline || null);
  const legB = extractTimelinePersistenceValues(callState.legBTimeline || null);

  const tableIdentifier = escapeIdentifier(currentConfig?.mysqlTableName || 'call_leg_timelines');
  const columns = [
    'call_id',
    'recording_path',
    'leg_a_status',
    'leg_a_number',
    'leg_a_channel',
    'leg_a_paired_channel',
    'leg_a_peer',
    'leg_a_caller',
    'leg_a_dial_string',
    'leg_a_answered_by',
    'leg_a_start',
    'leg_a_answer',
    'leg_a_end',
    'leg_b_status',
    'leg_b_number',
    'leg_b_channel',
    'leg_b_paired_channel',
    'leg_b_peer',
    'leg_b_caller',
    'leg_b_dial_string',
    'leg_b_answered_by',
    'leg_b_start',
    'leg_b_answer',
    'leg_b_end'
  ];

  const placeholders = columns.map(() => '?').join(', ');
  const insertSql = `
    INSERT INTO ${tableIdentifier} (${columns.join(', ')})
    VALUES (${placeholders})
    ON DUPLICATE KEY UPDATE
      recording_path = VALUES(recording_path),
      leg_a_status = VALUES(leg_a_status),
      leg_a_number = VALUES(leg_a_number),
      leg_a_channel = VALUES(leg_a_channel),
      leg_a_paired_channel = VALUES(leg_a_paired_channel),
      leg_a_peer = VALUES(leg_a_peer),
      leg_a_caller = VALUES(leg_a_caller),
      leg_a_dial_string = VALUES(leg_a_dial_string),
      leg_a_answered_by = VALUES(leg_a_answered_by),
      leg_a_start = VALUES(leg_a_start),
      leg_a_answer = VALUES(leg_a_answer),
      leg_a_end = VALUES(leg_a_end),
      leg_b_status = VALUES(leg_b_status),
      leg_b_number = VALUES(leg_b_number),
      leg_b_channel = VALUES(leg_b_channel),
      leg_b_paired_channel = VALUES(leg_b_paired_channel),
      leg_b_peer = VALUES(leg_b_peer),
      leg_b_caller = VALUES(leg_b_caller),
      leg_b_dial_string = VALUES(leg_b_dial_string),
      leg_b_answered_by = VALUES(leg_b_answered_by),
      leg_b_start = VALUES(leg_b_start),
      leg_b_answer = VALUES(leg_b_answer),
      leg_b_end = VALUES(leg_b_end)
  `;

  const values = [
    normalizeTimelineString(callId),
    normalizeTimelineString(recordingPath),
    legA.status,
    legA.number,
    legA.channel,
    legA.pairedChannel,
    legA.peer,
    legA.caller,
    legA.dialString,
    legA.answeredBy,
    legA.start,
    legA.answer,
    legA.end,
    legB.status,
    legB.number,
    legB.channel,
    legB.pairedChannel,
    legB.peer,
    legB.caller,
    legB.dialString,
    legB.answeredBy,
    legB.start,
    legB.answer,
    legB.end
  ];

  try {
    await mysqlPool.execute(insertSql, values);
    logWithTimestamp('log', `[${callId}] Persisted leg timelines to MySQL table ${tableIdentifier}.`);
  } catch (err) {
    logWithTimestamp('error', `[${callId}] Failed to persist leg timelines to MySQL:`, err.message);
  }
}

process.on('exit', () => {
  if (mysqlPool) {
    mysqlPool.end().catch(() => {});
  }
});

ensureMysqlInitialization();

const numberPattern = /^[0-9+*#]+$/;

function parseNumber(value, sourceDescription) {
  const trimmed = (value || '').trim();
  if (!trimmed) {
    return null;
  }
  if (!numberPattern.test(trimmed)) {
    logWithTimestamp('warn', `Skipping invalid number '${trimmed}' from ${sourceDescription}.`);
    return null;
  }
  return trimmed;
}

function loadNumbers() {
  if (!currentConfig) {
    return [];
  }

  if (currentConfig.OUTBOUND_NUMBER_FILE) {
    const resolvedPath = path.resolve(currentConfig.OUTBOUND_NUMBER_FILE);
    let fileContent;
    try {
      fileContent = fs.readFileSync(resolvedPath, 'utf8');
    } catch (err) {
      const message = `Failed to read OUTBOUND_NUMBER_FILE at ${resolvedPath}: ${err.message}`;
      logWithTimestamp('error', message);
      throw new Error(message);
    }

    const numbers = fileContent
      .split(/\r?\n/)
      .map((line) => parseNumber(line, `file '${resolvedPath}'`))
      .filter(Boolean);

    if (numbers.length === 0) {
      const message = `No valid outbound numbers found in ${resolvedPath}.`;
      logWithTimestamp('error', message);
      throw new Error(message);
    }
    return numbers;
  }

  const parsed = parseNumber(currentConfig.OUTBOUND_NUMBER, 'OUTBOUND_NUMBER');
  if (!parsed) {
    const message = 'OUTBOUND_NUMBER is empty or invalid.';
    logWithTimestamp('error', message);
    throw new Error(message);
  }
  return [parsed];
}

let outboundNumbers = [];
let numbersQueue = [];
let numbersDepletedLogged = false;
let lastStartRestarted = false;

function resetNumbersQueue() {
  outboundNumbers = loadNumbers();
  numbersQueue = [...outboundNumbers];
  numbersDepletedLogged = false;
}

resetNumbersQueue();

function getFallbackTargetEndpoint(config = currentConfig) {
  if (!config) {
    return 'Local/777@default';
  }
  return `Local/${config.TARGET_EXTENSION}@${config.TARGET_CONTEXT}`;
}

function getDestinationEndpoint(config = currentConfig) {
  const fallback = getFallbackTargetEndpoint(config);
  const candidate = config?.TARGET_ENDPOINT;
  if (candidate) {
    const trimmed = candidate.trim();
    if (trimmed) {
      return trimmed;
    }
  }
  return fallback;
}

const inFlightCalls = new Set();
const callNumberMap = new Map();
const activeCalls = new Map();
const channelToCallId = new Map();
const bridgeToCallId = new Map();
const linkedIdToCallId = new Map();
const recordingOwnership = new Map();

function safeStringify(value) {
  const seen = new Set();

  try {
    return JSON.stringify(
      value,
      (key, currentValue) => {
        if (typeof currentValue === 'bigint') {
          return currentValue.toString();
        }

        if (typeof currentValue === 'object' && currentValue !== null) {
          if (seen.has(currentValue)) {
            return '[Circular]';
          }
          seen.add(currentValue);
        }

        return currentValue;
      },
      2
    );
  } catch (err) {
    return `"[Unserializable: ${err.message}]"`;
  }
}

function stripLocalSuffix(channelName) {
  if (typeof channelName !== 'string') {
    return channelName;
  }
  return channelName.replace(/;[12]$/, '');
}

function isTargetLocalChannelName(channelName) {
  if (typeof channelName !== 'string' || channelName.length === 0) {
    return false;
  }

  const normalized = stripLocalSuffix(channelName);
  const config = currentConfig || { TARGET_EXTENSION: '777', TARGET_CONTEXT: 'default2' };
  const prefixWithContext = `Local/${config.TARGET_EXTENSION}@${config.TARGET_CONTEXT}`;
  if (config.TARGET_CONTEXT && normalized.startsWith(prefixWithContext)) {
    return true;
  }

  const genericPrefix = `Local/${config.TARGET_EXTENSION}@`;
  return normalized.startsWith(genericPrefix);
}

function toTimestampMs(value, fallback = Date.now()) {
  if (value == null) {
    return fallback;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const parsed = Date.parse(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }

  if (value instanceof Date) {
    const ms = value.getTime();
    if (!Number.isNaN(ms)) {
      return ms;
    }
  }

  return fallback;
}

function getEventTimestampMs(event, fallback) {
  if (!event) {
    return fallback ?? Date.now();
  }

  return toTimestampMs(event.timestamp, fallback ?? Date.now());
}

function getCallState(callId) {
  if (!activeCalls.has(callId)) {
    logWithTimestamp('log', `[${callId}] Initializing call state.`);
    activeCalls.set(callId, {
      bridge: null,
      bridges: new Set(),
      originatedPartner: false,
      channels: new Set(),
      channelRoles: new Map(),
      dialerChannelId: null,
      dialerUp: false,
      number: callNumberMap.get(callId) || null,
      recording: null,
      recordingPath: null,
      recordingId: null,
      recordingFormatUsed: null,
      createdAt: Date.now(),
      dialerConnectedAt: null,
      dialerHangupAt: null,
      dialedConnectedAt: null,
      dialedHangupAt: null,
      dialedChannelId: null,
      callConnectedAt: null,
      effectiveConnectedAt: null,
      answeredBy: null,
      answeredBySource: null,
      agentAnsweredAt: null,
      agentChannelId: null,
      agentChannels: new Set(),
      agentLegs: new Map(),
      linkedIds: new Set(),
      dialerHangupCause: null,
      dialedHangupCause: null,
      completedAtMs: null,
      summaryLogged: false,
      legATimeline: createLegTimeline('legA'),
      legBTimeline: createLegTimeline('legB'),
      cleanupWatchdog: null
    });
  }
  return activeCalls.get(callId);
}

function clearCallWatchdog(callState) {
  if (callState?.cleanupWatchdog) {
    clearTimeout(callState.cleanupWatchdog);
    callState.cleanupWatchdog = null;
  }
}

function scheduleCallWatchdog(client, callId, callState, callTimeoutSeconds = 30) {
  if (!callState) {
    return;
  }

  clearCallWatchdog(callState);

  const parsedTimeout = Number.isFinite(callTimeoutSeconds) ? callTimeoutSeconds : 30;
  const baseDelayMs = Math.max(parsedTimeout, 0) * 1000;
  const watchdogDelayMs = Math.max(baseDelayMs + 15000, 45000);

  callState.cleanupWatchdog = setTimeout(() => {
    const activeState = activeCalls.get(callId);
    if (!activeState) {
      return;
    }

    logWithTimestamp(
      'warn',
      `[${callId}] Call watchdog triggered after ${watchdogDelayMs} ms without cleanup. Forcing termination.`
    );

    clearCallWatchdog(activeState);

    cleanupCall(callId, client)
      .catch((err) => {
        logWithTimestamp('error', `[${callId}] Error during watchdog cleanup:`, err.message);
      })
      .finally(() => {
        markCallCompleted(client, callId);
      });
  }, watchdogDelayMs);
}

function createLegTimeline(role = null) {
  return {
    role,
    channelId: null,
    peerName: null,
    callerName: null,
    pairedChannelName: null,
    pairedChannelId: null,
    dialString: null,
    targetNumber: null,
    answeredBy: null,
    startedAt: null,
    answeredAt: null,
    endedAt: null,
    lastStatus: null
  };
}

function updateLegTimelineMetadata(timeline, channel) {
  if (!timeline || !channel) {
    return;
  }

  const { channelId: currentChannelId, peerName: currentPeerName } = timeline;
  const newChannelId = channel.id || null;
  const newPeerName = channel.name || null;
  const currentPeerIsLocal = typeof currentPeerName === 'string' && currentPeerName.startsWith('Local/');
  const channelChanged = Boolean(
    newChannelId && currentChannelId && currentChannelId !== newChannelId
  );

  if (
    newChannelId &&
    (currentChannelId == null || channelChanged || currentPeerIsLocal)
  ) {
    timeline.channelId = newChannelId;
  }

  if (
    newPeerName &&
    (
      currentPeerName == null ||
      currentPeerIsLocal ||
      (channelChanged && newPeerName !== currentPeerName)
    )
  ) {
    timeline.peerName = newPeerName;
  }

  if (channel.name && /;1$/.test(channel.name) && !timeline.pairedChannelName) {
    timeline.pairedChannelName = channel.name.replace(/;1$/, ';2');
  }
  if (channel.name && /;2$/.test(channel.name)) {
    timeline.callerName = timeline.callerName || channel.name;
  }
}

function normalizeLegATimestamp(timeline, field, timestamp) {
  if (
    !timeline ||
    timeline.role !== 'legA' ||
    timestamp == null ||
    (field !== 'startedAt' && field !== 'answeredAt' && field !== 'endedAt')
  ) {
    return timestamp;
  }

  const date = new Date(timestamp);
  const time = date.getTime();
  if (Number.isNaN(time)) {
    return timestamp;
  }

  date.setMilliseconds(0);
  return date.getTime();
}

function setLegTimelineTimestamp(timeline, field, timestamp) {
  if (!timeline || timestamp == null) {
    return;
  }

  const normalizedTimestamp = normalizeLegATimestamp(timeline, field, timestamp);

  if (timeline[field] == null) {
    timeline[field] = normalizedTimestamp;
  }
}

function assignLegDialString(timeline, dialString) {
  if (!timeline || !dialString) {
    return;
  }

  const trimmed = dialString.trim();
  if (!trimmed) {
    return;
  }

  if (!timeline.dialString) {
    timeline.dialString = trimmed;
  }

  if (timeline.role === 'legA' && !timeline.targetNumber) {
    const beforeAt = trimmed.split('@')[0] || trimmed;
    const afterLastSlashIndex = beforeAt.lastIndexOf('/');
    const candidateSource = afterLastSlashIndex >= 0 ? beforeAt.slice(afterLastSlashIndex + 1) : beforeAt;
    const numberMatch = candidateSource.match(/[0-9+*#]+/);
    const fallbackMatch = numberMatch || trimmed.match(/[0-9+*#]+/);

    if (fallbackMatch) {
      timeline.targetNumber = fallbackMatch[0];
    }
  }
}

const GENERIC_STATUS_VALUES = new Set([
  'RINGING',
  'DIALING',
  'TRYING',
  'PROGRESS',
  'UP',
  'DOWN',
  'HUNGUP',
  'UNKNOWN',
  'EARLY MEDIA'
]);

function normalizeStatus(status) {
  if (status == null) {
    return '';
  }

  const normalized = String(status).trim();
  if (!normalized) {
    return '';
  }

  if (/^NO\s?ANSWER$/i.test(normalized)) {
    return 'NO ANSWER';
  }
  if (/^ANSWER(ED)?$/i.test(normalized)) {
    return 'ANSWERED';
  }

  return normalized;
}

function pickStatusCandidate(...values) {
  let fallback = '';

  for (const value of values) {
    const normalized = normalizeStatus(value);
    if (!normalized) {
      continue;
    }

    const upper = normalized.toUpperCase();
    if (upper === 'ANSWERED') {
      return 'ANSWERED';
    }

    if (GENERIC_STATUS_VALUES.has(upper)) {
      if (!fallback) {
        fallback = normalized;
      }
      continue;
    }

    if (upper === 'NO ANSWER') {
      if (!fallback) {
        fallback = 'NO ANSWER';
      }
      continue;
    }

    return normalized;
  }

  return fallback;
}

function setLegTimelineStatus(timeline, status) {
  if (!timeline || !status) {
    return;
  }

  const normalized = normalizeStatus(status);
  if (!normalized) {
    return;
  }

  timeline.lastStatus = normalized;
}

function setLegAnsweredBy(timeline, value) {
  if (!timeline || !value) {
    return;
  }

  const trimmed = typeof value === 'string' ? value.trim() : '';
  if (!trimmed) {
    return;
  }

  if (!timeline.answeredBy) {
    timeline.answeredBy = trimmed;
  }
}

function toIsoTimestamp(value) {
  if (value == null) {
    return null;
  }

  const date = new Date(value);
  const time = date.getTime();
  if (Number.isNaN(time)) {
    return null;
  }

  return date.toISOString();
}

function formatLegTimeline(label, timeline) {
  if (!timeline) {
    return '';
  }

  const fields = [];
  if (timeline.lastStatus) {
    fields.push(`status=${timeline.lastStatus}`);
  }
  if (timeline.targetNumber) {
    fields.push(`number=${timeline.targetNumber}`);
  }
  if (timeline.channelId) {
    fields.push(`channel=${timeline.channelId}`);
  }
  if (timeline.pairedChannelId) {
    fields.push(`pairedChannel=${timeline.pairedChannelId}`);
  }
  if (timeline.peerName) {
    fields.push(`peer='${timeline.peerName}'`);
  }
  if (timeline.callerName) {
    fields.push(`caller='${timeline.callerName}'`);
  }
  if (timeline.dialString) {
    fields.push(`dialstring='${timeline.dialString}'`);
  }
  if (timeline.answeredBy) {
    fields.push(`answeredBy='${timeline.answeredBy}'`);
  }

  const startIso = toIsoTimestamp(timeline.startedAt);
  const answerIso = toIsoTimestamp(timeline.answeredAt);
  const endIso = toIsoTimestamp(timeline.endedAt);

  if (startIso) {
    fields.push(`start=${startIso}`);
  }
  if (answerIso) {
    fields.push(`answer=${answerIso}`);
  }
  if (endIso) {
    fields.push(`end=${endIso}`);
  }

  if (fields.length === 0) {
    return '';
  }

  return `${label}: ${fields.join(', ')}`;
}

function extractConnectedIdentity(channel) {
  if (!channel) {
    return null;
  }

  const connected = channel.connected || {};
  const caller = channel.caller || {};
  return (
    connected.name ||
    connected.number ||
    caller.name ||
    caller.number ||
    channel.name ||
    null
  );
}

const answeredIdentityPriority = {
  dialed: 1,
  agent: 2
};

function setAnsweredIdentity(callState, identity, source = 'dialed') {
  if (!callState || !identity) {
    return;
  }

  const currentPriority = answeredIdentityPriority[callState.answeredBySource] || 0;
  const incomingPriority = answeredIdentityPriority[source] || 0;

  if (!callState.answeredBy || incomingPriority >= currentPriority) {
    callState.answeredBy = identity;
    callState.answeredBySource = source;
  }
}

function updateAnsweredIdentity(callState, channel) {
  if (!callState) {
    return;
  }

  const identity = extractConnectedIdentity(channel);
  setAnsweredIdentity(callState, identity, 'dialed');
}

function updateCallConnectionState(callState, referenceTime) {
  if (!callState) {
    return;
  }

  const referenceMs =
    referenceTime != null ? referenceTime : Date.now();

  if (callState.dialerUp && callState.dialedConnectedAt && !callState.callConnectedAt) {
    callState.callConnectedAt = callState.callConnectedAt ?? referenceMs;
  }

  const talkStartCandidates = [];
  if (callState.agentAnsweredAt != null) {
    talkStartCandidates.push(callState.agentAnsweredAt);
  }
  if (callState.callConnectedAt != null) {
    talkStartCandidates.push(callState.callConnectedAt);
  }
  if (callState.dialedConnectedAt != null && callState.dialerConnectedAt != null) {
    talkStartCandidates.push(Math.max(callState.dialedConnectedAt, callState.dialerConnectedAt));
  }

  const talkStart =
    talkStartCandidates.length > 0 ? Math.min(...talkStartCandidates) : null;

  if (talkStart != null) {
    if (callState.callConnectedAt == null || callState.callConnectedAt > talkStart) {
      callState.callConnectedAt = talkStart;
    }
    if (callState.effectiveConnectedAt == null || callState.effectiveConnectedAt > talkStart) {
      callState.effectiveConnectedAt = talkStart;
    }
  } else if (callState.dialerConnectedAt != null) {
    if (callState.effectiveConnectedAt == null || callState.effectiveConnectedAt > callState.dialerConnectedAt) {
      callState.effectiveConnectedAt = callState.dialerConnectedAt;
    }
  } else {
    callState.effectiveConnectedAt = callState.effectiveConnectedAt ?? null;
  }
}

function resolveHangupCause(event, channel) {
  if (!event && !channel) {
    return null;
  }

  const cause = (
    event?.cause_txt ||
    event?.cause ||
    channel?.cause_txt ||
    channel?.cause ||
    channel?.hangupCause ||
    null
  );

  return cause != null ? String(cause) : null;
}

function calculateCallDurations(callState, completedAt = new Date()) {
  if (!callState) {
    return {
      primarySeconds: 0,
      primaryLabel: 'no connection',
      talkSeconds: 0,
      holdSeconds: 0,
      description: 'no connected legs'
    };
  }

  const completedAtDate = completedAt instanceof Date ? completedAt : new Date(completedAt);
  const completedAtMs = completedAtDate.getTime();

  const dialerConnectedAt = callState.dialerConnectedAt ?? null;
  const dialedConnectedAt = callState.dialedConnectedAt ?? null;
  const callConnectedAt = callState.callConnectedAt ?? null;
  const agentAnsweredAt = callState.agentAnsweredAt ?? null;

  const talkStart = agentAnsweredAt || callConnectedAt || null;
  const talkSeconds = talkStart
    ? Math.max(0, Math.round((completedAtMs - talkStart) / 1000))
    : 0;

  let holdSeconds = 0;
  if (dialerConnectedAt) {
    const holdEnd = talkStart || callConnectedAt || completedAtMs;
    holdSeconds = Math.max(0, Math.round((holdEnd - dialerConnectedAt) / 1000));
  }

  const effectiveConnectedAt =
    callState.effectiveConnectedAt ?? talkStart ?? dialerConnectedAt ?? null;
  const primarySeconds = effectiveConnectedAt
    ? Math.max(0, Math.round((completedAtMs - effectiveConnectedAt) / 1000))
    : 0;

  let primaryLabel = 'no connection';
  let description = 'no connected legs';
  if (talkStart && dialedConnectedAt) {
    primaryLabel = 'two-leg talk';
    description = `talkTime=${talkSeconds}s, holdBeforeAnswer=${holdSeconds}s`;
  } else if (dialerConnectedAt) {
    primaryLabel = 'single-leg hold';
    description = `holdTime=${holdSeconds}s (single-leg)`;
  }

  return {
    primarySeconds,
    primaryLabel,
    talkSeconds,
    holdSeconds,
    description
  };
}

function ensureAgentLeg(callState, channelId) {
  if (!callState || !channelId) {
    return null;
  }

  if (!(callState.agentLegs instanceof Map)) {
    callState.agentLegs = new Map();
  }

  if (!callState.agentLegs.has(channelId)) {
    callState.agentLegs.set(channelId, {
      channelId,
      identity: null,
      dialedAt: null,
      answeredAt: null,
      hangupAt: null,
      lastStatus: null
    });
  }

  return callState.agentLegs.get(channelId);
}

function calculateLegDurations(callState, completedAt = new Date()) {
  const completedAtDate = completedAt instanceof Date ? completedAt : new Date(completedAt);
  const completedAtMs = completedAtDate.getTime();

  const callStartMs = callState?.createdAt ?? completedAtMs;
  const legAAnswerAt = callState?.dialerConnectedAt ?? null;
  const legATalkStart = callState?.agentAnsweredAt ?? callState?.callConnectedAt ?? null;
  const legATalkEnd = callState?.dialerHangupAt ?? callState?.completedAtMs ?? completedAtMs;

  const legAWaitSeconds = legAAnswerAt
    ? Math.max(0, Math.round((legAAnswerAt - callStartMs) / 1000))
    : Math.max(0, Math.round((completedAtMs - callStartMs) / 1000));

  const legATalkSeconds = legATalkStart
    ? Math.max(0, Math.round((legATalkEnd - legATalkStart) / 1000))
    : 0;

  let agentLeg = null;
  if (callState?.agentLegs instanceof Map) {
    if (callState.agentChannelId && callState.agentLegs.has(callState.agentChannelId)) {
      agentLeg = callState.agentLegs.get(callState.agentChannelId);
    } else {
      for (const candidate of callState.agentLegs.values()) {
        if (candidate?.answeredAt) {
          agentLeg = candidate;
          break;
        }
      }
    }
  }

  const agentAnsweredAt = callState?.agentAnsweredAt ?? agentLeg?.answeredAt ?? null;
  const agentDialedAt = agentLeg?.dialedAt ?? callState?.dialedConnectedAt ?? null;
  const agentHangupAt = agentLeg?.hangupAt ?? callState?.dialedHangupAt ?? callState?.completedAtMs ?? completedAtMs;

  const legBWaitSeconds = agentAnsweredAt && agentDialedAt
    ? Math.max(0, Math.round((agentAnsweredAt - agentDialedAt) / 1000))
    : agentDialedAt
    ? Math.max(0, Math.round((completedAtMs - agentDialedAt) / 1000))
    : 0;

  const legBTalkSeconds = agentAnsweredAt
    ? Math.max(0, Math.round((agentHangupAt - agentAnsweredAt) / 1000))
    : 0;

  return {
    legA: { waitSeconds: legAWaitSeconds, talkSeconds: legATalkSeconds },
    legB: { waitSeconds: legBWaitSeconds, talkSeconds: legBTalkSeconds }
  };
}

function determineLegStatus(
  connectedAt,
  hangupCause,
  counterpartConnectedAt,
  fallbackCause,
  timelineStatus
) {
  if (connectedAt && counterpartConnectedAt) {
    return 'ANSWERED';
  }

  const candidate = pickStatusCandidate(hangupCause, fallbackCause, timelineStatus);

  if (connectedAt && !counterpartConnectedAt) {
    return candidate || 'NO ANSWER';
  }

  return candidate || 'NO ANSWER';
}

function resolveRecordingPath(callState) {
  if (!callState) {
    return '';
  }

  if (callState.recordingPath) {
    return callState.recordingPath;
  }

  const recordingId = callState.recordingId || callState.recording?.id;
  if (!recordingId) {
    return '';
  }

  const recordingFormatForLog =
    callState.recordingFormatUsed || callState.recording?.format || currentConfig.recordingFormat;
  const fileName = `${recordingId}.${recordingFormatForLog}`;

  const searchDirs = new Set(recordingSearchDirs);
  searchDirs.add(currentConfig.resolvedRecordingsDir);

  for (const dir of searchDirs) {
    const candidate = path.join(dir, fileName);
    try {
      if (fs.existsSync(candidate)) {
        callState.recordingPath = candidate;
        return candidate;
      }
    } catch (err) {
      // Ignore filesystem access issues for logging purposes.
    }
  }

  return fileName;
}

async function logCallSummary(callId, callState, completedAt = new Date()) {
  if (!callState || callState.summaryLogged) {
    return null;
  }

  const providedCompletedAt = completedAt instanceof Date ? completedAt : new Date(completedAt);
  const providedCompletedAtMs = providedCompletedAt.getTime();
  const fallbackCompletedAtMs = Number.isFinite(providedCompletedAtMs) ? providedCompletedAtMs : Date.now();
  const resolvedCompletedAtMs = callState.completedAtMs ?? fallbackCompletedAtMs;
  callState.completedAtMs = resolvedCompletedAtMs;
  const resolvedCompletedAtDate = Number.isFinite(providedCompletedAtMs) && providedCompletedAtMs === resolvedCompletedAtMs
    ? providedCompletedAt
    : new Date(resolvedCompletedAtMs);

  const durationInfo = calculateCallDurations(callState, resolvedCompletedAtDate);
  const legDurations = calculateLegDurations(callState, resolvedCompletedAtDate);

  const callStartMs = callState.createdAt != null ? callState.createdAt : resolvedCompletedAtMs;
  const callStartIso = new Date(callStartMs).toISOString();
  const number = callState.number || 'unknown';
  const legAStatus = determineLegStatus(
    callState.dialerConnectedAt,
    callState.dialerHangupCause,
    callState.dialedConnectedAt,
    callState.dialedHangupCause,
    callState.legATimeline?.lastStatus
  );
  const legBStatus = determineLegStatus(
    callState.dialedConnectedAt,
    callState.dialedHangupCause,
    callState.dialerConnectedAt,
    callState.dialerHangupCause,
    callState.legBTimeline?.lastStatus
  );

  setLegTimelineStatus(callState.legATimeline, legAStatus);
  setLegTimelineStatus(callState.legBTimeline, legBStatus);

  setLegTimelineTimestamp(callState.legATimeline, 'endedAt', resolvedCompletedAtMs);
  setLegTimelineTimestamp(callState.legBTimeline, 'endedAt', resolvedCompletedAtMs);

  let agentIdentity = callState.answeredBy || null;
  if (!agentIdentity && callState.agentChannelId && callState.agentLegs instanceof Map) {
    agentIdentity = callState.agentLegs.get(callState.agentChannelId)?.identity || null;
  }
  const recordingPath = resolveRecordingPath(callState);

  const summaryRecord = [
    callStartIso,
    number,
    legAStatus,
    String(legDurations.legA.waitSeconds),
    String(legDurations.legA.talkSeconds),
    legBStatus,
    agentIdentity || 'unknown',
    String(legDurations.legB.waitSeconds),
    String(legDurations.legB.talkSeconds),
    recordingPath
  ].join(';');

  logWithTimestamp('log', `[${callId}] Call summary: ${summaryRecord}`);
  logWithTimestamp(
    'log',
    `[${callId}] Call metrics: completedAt=${resolvedCompletedAtDate.toISOString()}, number=${number}, status=${legAStatus}/${legBStatus}, ` +
      `primaryDuration=${durationInfo.primarySeconds}s (${durationInfo.primaryLabel}), detail=${durationInfo.description}`
  );

  const legALog = formatLegTimeline('legA', callState.legATimeline);
  const legBLog = formatLegTimeline('legB', callState.legBTimeline);
  if (legALog || legBLog) {
    const parts = [];
    if (legALog) {
      parts.push(legALog);
    }
    if (legBLog) {
      parts.push(legBLog);
    }
    logWithTimestamp('log', `[${callId}] Leg timelines: ${parts.join(' | ')}`);
  }

  await persistLegTimelines(callId, callState, recordingPath);

  callState.summaryLogged = true;

  return summaryRecord;
}

async function moveRecordingToResolvedDir(recording, callId) {
  if (!recording?.id) {
    return;
  }

  const targetPath = path.join(currentConfig.resolvedRecordingsDir, `${recording.id}.${recording.format}`);

  try {
    await fs.promises.access(targetPath, fs.constants.F_OK);
    logWithTimestamp('log', `[${callId}] Recording already available at ${targetPath}.`);
    return;
  } catch (err) {
    // Target file does not exist yet – proceed with locating the source.
  }

  const sourceFileName = `${recording.id}.${recording.format}`;
  for (const dir of recordingSearchDirs) {
    const candidatePath = path.join(dir, sourceFileName);
    if (candidatePath === targetPath) {
      continue;
    }

    try {
      await fs.promises.access(candidatePath, fs.constants.F_OK);
    } catch (err) {
      continue;
    }

    try {
      await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
      try {
        await fs.promises.rename(candidatePath, targetPath);
        logWithTimestamp('log', `[${callId}] Moved recording from ${candidatePath} to ${targetPath}.`);
      } catch (renameErr) {
        if (renameErr?.code !== 'EXDEV') {
          throw renameErr;
        }

        await fs.promises.copyFile(candidatePath, targetPath);
        await fs.promises.unlink(candidatePath).catch(() => {});
        logWithTimestamp('log', `[${callId}] Copied recording from ${candidatePath} to ${targetPath}.`);
      }
      return;
    } catch (err) {
      logWithTimestamp('error', `[${callId}] Failed to move recording from ${candidatePath}:`, err.message);
    }
  }

  logWithTimestamp('warn', `[${callId}] Recording file ${sourceFileName} was not found for transfer.`);
}

async function recordingFileExists(recording) {
  if (!recording?.id) {
    return false;
  }

  const fileName = `${recording.id}.${recording.format}`;
  for (const dir of recordingSearchDirs) {
    const candidatePath = path.join(dir, fileName);
    try {
      await fs.promises.access(candidatePath, fs.constants.F_OK);
      return true;
    } catch (err) {
      // File not in this directory – keep checking others.
    }
  }

  return false;
}

function scheduleRecordingRetry(recordingId, delayMs, trigger) {
  const ownership = recordingOwnership.get(recordingId);
  if (!ownership) {
    return;
  }

  if (ownership.retryTimer) {
    clearTimeout(ownership.retryTimer);
  }

  ownership.retryTimer = setTimeout(() => {
    ownership.retryTimer = null;
    handleRecordingReady(recordingId, trigger).catch((err) => {
      logWithTimestamp('error', `[${ownership.callId}] Error processing recording '${recordingId}' (${trigger}):`, err.message);
    });
  }, delayMs);
}

async function handleRecordingReady(recordingId, trigger) {
  const ownership = recordingOwnership.get(recordingId);
  if (!ownership) {
    return;
  }

  const { callId, recording } = ownership;

  const exists = await recordingFileExists(recording);
  if (!exists) {
    const fromEvent = trigger.startsWith('event');
    const retryDelay = fromEvent ? 1000 : 5000;
    ownership.retryCounters = ownership.retryCounters || {};
    ownership.retryCounters[trigger] = (ownership.retryCounters[trigger] || 0) + 1;
    const attempt = ownership.retryCounters[trigger];
    logWithTimestamp('log', 
      `[${callId}] Recording '${recordingId}' not yet present after ${trigger} attempt #${attempt}. Retrying in ${retryDelay} ms.`
    );
    scheduleRecordingRetry(recordingId, retryDelay, trigger);
    return;
  }

  await moveRecordingToResolvedDir(recording, callId);

  if (ownership.retryTimer) {
    clearTimeout(ownership.retryTimer);
  }

  recordingOwnership.delete(recordingId);

  const callState = activeCalls.get(callId);
  if (callState?.recording?.id === recordingId) {
    callState.recording = null;
  }
}

async function cleanupCall(callId, client) {
  const callState = activeCalls.get(callId);
  if (!callState) {
    return;
  }

  clearCallWatchdog(callState);

  const completedAtMs = callState.completedAtMs ?? Date.now();
  callState.completedAtMs = completedAtMs;
  const completedAt = new Date(completedAtMs);
  await logCallSummary(callId, callState, completedAt);

  if (callState.bridges instanceof Set) {
    for (const trackedBridgeId of callState.bridges) {
      bridgeToCallId.delete(trackedBridgeId);
    }
    callState.bridges.clear();
  }

  if (callState.recording?.id) {
    const recordingState = callState.recording;
    const { id: name, bridgeId, mode } = recordingState;
    let stopped = false;

    if (mode === 'bridge' && bridgeId && client?.bridges?.stopMedia) {
      try {
        logWithTimestamp('log', `[${callId}] Stopping bridge recording '${name}' on bridge ${bridgeId}.`);
        await client.bridges.stopMedia({ bridgeId, media: 'recording' });
        stopped = true;
      } catch (err) {
        if (!/not found/i.test(err?.message || '')) {
          logWithTimestamp('error', `[${callId}] Failed to stop bridge recording ${name}:`, err.message);
        }
      }
    }

    if (!stopped && client?.recordings?.stop) {
      try {
        logWithTimestamp('log', `[${callId}] Stopping recording '${name}' via recordings API.`);
        await client.recordings.stop({ recordingName: name });
        stopped = true;
      } catch (err) {
        if (!/not found/i.test(err?.message || '')) {
          logWithTimestamp('error', `[${callId}] Failed to stop recording ${name}:`, err.message);
        }
      }
    }

    if (stopped) {
      logWithTimestamp('log', 
        `[${callId}] Recording '${name}' stop requested. Waiting for RecordingFinished event before moving file.`
      );

      if (recordingOwnership.has(recordingState.id)) {
        scheduleRecordingRetry(recordingState.id, 10000, 'fallback');
      } else {
        logWithTimestamp('warn', 
          `[${callId}] Recording '${name}' was not tracked for ownership. Scheduling immediate retry.`
        );
        recordingOwnership.set(recordingState.id, {
          callId,
          recording: recordingState,
          retryTimer: null,
          retryCounters: {}
        });
        scheduleRecordingRetry(recordingState.id, 5000, 'fallback');
      }
    }
  }

  if (callState.bridges && callState.bridges.size > 0) {
    for (const knownBridgeId of callState.bridges) {
      if (knownBridgeId) {
        bridgeToCallId.delete(knownBridgeId);
      }
    }
    callState.bridges.clear();
  }

  if (callState.bridge) {
    try {
      const bridgeId = callState.bridge.id;
      logWithTimestamp('log', `[${callId}] Destroying bridge${bridgeId ? ` ${bridgeId}` : ''}.`);
      if (bridgeId) {
        bridgeToCallId.delete(bridgeId);
      }
      await callState.bridge.destroy();
    } catch (err) {
      if (err?.message && !/not found/i.test(err.message)) {
        logWithTimestamp('error', 'Error destroying bridge:', err.message);
      }
    }
  }

  callState.channels.forEach((channelId) => channelToCallId.delete(channelId));
  callState.channelRoles.clear();
  if (callState.agentChannels) {
    callState.agentChannels.forEach((channelId) => channelToCallId.delete(channelId));
    callState.agentChannels.clear();
  }
  if (callState.linkedIds) {
    callState.linkedIds.forEach((linkedId) => linkedIdToCallId.delete(linkedId));
    callState.linkedIds.clear();
  }
  activeCalls.delete(callId);
  callNumberMap.delete(callId);
  logWithTimestamp('log', `[${callId}] Call state cleaned up.`);
}

function maybeOriginateNext(client) {
  const concurrencyLimit = currentConfig?.callConcurrencyLimit ?? 1;

  while (inFlightCalls.size < concurrencyLimit && numbersQueue.length > 0) {
    const nextNumber = numbersQueue.shift();
    originateCall(client, nextNumber).catch((err) => {
      logWithTimestamp('error', 'Unexpected error during call origination:', err.message);
    });
  }

  if (numbersQueue.length === 0 && inFlightCalls.size === 0 && !numbersDepletedLogged) {
    numbersDepletedLogged = true;
    logWithTimestamp('log', 'All outbound numbers have been processed.');
  }
}

function markCallCompleted(client, callId) {
  const removed = inFlightCalls.delete(callId);
  callNumberMap.delete(callId);
  if (removed) {
    maybeOriginateNext(client);
  }
}

async function originateCall(client, number) {
  const callId = uuidv4();
  const outboundEndpoint = `PJSIP/${number}@${currentConfig.ARI_TRUNK}`;
  const callTimeoutSeconds = currentConfig.callTimeoutSeconds;

  inFlightCalls.add(callId);
  callNumberMap.set(callId, number);

  const callState = getCallState(callId);
  if (callState && !callState.number) {
    callState.number = number;
  }
  if (callState) {
    if (!callState.legATimeline.targetNumber) {
      callState.legATimeline.targetNumber = number;
    }
    setLegTimelineTimestamp(callState.legATimeline, 'startedAt', callState.createdAt);
  }
  scheduleCallWatchdog(client, callId, callState, callTimeoutSeconds);

  logWithTimestamp('log', `[${callId}] Dialing outbound endpoint: ${outboundEndpoint}`);

  try {
    await client.channels.originate({
      endpoint: outboundEndpoint,
      app: currentConfig.STASIS_APP,
      appArgs: ['dialer', callId].join(','),
      callerId: currentConfig.CALLER_ID || undefined,
      timeout: callTimeoutSeconds
    });

    logWithTimestamp('log', `[${callId}] Origination request sent to outbound endpoint. Waiting for events...`);
  } catch (err) {
    logWithTimestamp('error', `[${callId}] Failed to originate outbound call:`, err.message);
    if (callState) {
      clearCallWatchdog(callState);
      activeCalls.delete(callId);
    }
    markCallCompleted(client, callId);
  }
}

function parseArgs(args) {
  if (!args) {
    return { role: 'unknown', callId: undefined };
  }

  let normalizedArgs;
  if (Array.isArray(args)) {
    normalizedArgs = args;
  } else if (typeof args === 'string') {
    normalizedArgs = args
      .split(',')
      .map((part) => part.trim())
      .filter((part) => part.length > 0);
  } else {
    normalizedArgs = [String(args)];
  }

  const [role, callId] = normalizedArgs;
  return { role: role || 'unknown', callId };
}

async function handleStasisStart(client, event) {
  const channel = event.channel;
  const { role, callId: providedCallId } = parseArgs(event.args);
  const callId = providedCallId || channel.id;
  const timestampMs = getEventTimestampMs(event);

  logWithTimestamp(
    'log',
    `[${callId}] handleStasisStart received for channel ${channel?.id || 'unknown'} (${channel?.name || 'unknown'}) role '${
      role || 'unknown'
    }'.`
  );

  const callState = getCallState(callId);
  channelToCallId.set(channel.id, callId);
  if (channel.linkedid) {
    linkedIdToCallId.set(channel.linkedid, callId);
    callState.linkedIds.add(channel.linkedid);
  }
  callState.channelRoles.set(channel.id, role);
  if (role === 'dialer') {
    callState.dialerChannelId = channel.id;
    callState.dialerUp = channel.state === 'Up';
    updateLegTimelineMetadata(callState.legATimeline, channel);
    setLegTimelineTimestamp(callState.legATimeline, 'startedAt', timestampMs);
    if (channel.state === 'Up') {
      setLegTimelineTimestamp(callState.legATimeline, 'answeredAt', timestampMs);
    }
  }
  if (!callState.number && callNumberMap.has(callId)) {
    callState.number = callNumberMap.get(callId);
  }

  logWithTimestamp('log', `[${callId}] Channel ${channel.id} entered Stasis with role '${role}'.`);

  if (!callState.bridge) {
    callState.bridge = client.Bridge();
    await callState.bridge.create({ type: 'mixing', name: `bridge-${callId}` });
    logWithTimestamp('log', `[${callId}] Bridge created: ${callState.bridge.id}.`);
    if (callState.bridge.id) {
      bridgeToCallId.set(callState.bridge.id, callId);
      if (callState.bridges) {
        callState.bridges.add(callState.bridge.id);
      }
    }
  }

  callState.channels.add(channel.id);

  try {
    await callState.bridge.addChannel({ channel: channel.id });
    logWithTimestamp('log', `[${callId}] Channel ${channel.id} added to bridge.`);
    if (role === 'dialer' && channel.state === 'Up') {
      logWithTimestamp(
        'log',
        `[${callId}] Dialer channel ${channel.id} is already Up; starting recording immediately.`
      );
      callState.dialerConnectedAt = callState.dialerConnectedAt || Date.now();
      callState.dialerUp = true;
      updateCallConnectionState(callState);
      await startDialerRecording(client, callId, callState);
    }
  } catch (err) {
    logWithTimestamp('error', 'Failed to add channel to bridge:', err.message);
  }

  if (role === 'dialed') {
    try {
      await client.channels.answer({ channelId: channel.id });
      logWithTimestamp('log', `[${callId}] Answered dialed channel ${channel.id}.`);
      updateAnsweredIdentity(callState, channel);
      callState.dialedChannelId = callState.dialedChannelId || channel.id;
      updateLegTimelineMetadata(callState.legBTimeline, channel);
      if (channel.state === 'Up') {
        callState.dialedConnectedAt = callState.dialedConnectedAt || Date.now();
        updateCallConnectionState(callState);
      }
      await startDialerRecording(client, callId, callState);
    } catch (err) {
      logWithTimestamp('error', 'Failed to answer dialed channel:', err.message);
    }
    return;
  }

  if (!callState.originatedPartner) {
    callState.originatedPartner = true;
    try {
      const destinationCallerId =
        (callState && callState.number) ||
        callNumberMap.get(callId) ||
        currentConfig.CALLER_ID ||
        undefined;
      const destinationEndpoint = getDestinationEndpoint();

      await client.channels.originate({
        endpoint: destinationEndpoint,
        app: currentConfig.STASIS_APP,
        appArgs: ['dialed', callId].join(','),
        timeout: currentConfig.callTimeoutSeconds,
        callerId: destinationCallerId
      });
      logWithTimestamp('log', `[${callId}] Originated call to destination endpoint: ${destinationEndpoint}.`);
    } catch (err) {
      logWithTimestamp('error', 'Failed to originate partner channel:', err.message);
      await cleanupCall(callId, client);
      markCallCompleted(client, callId);
    }
  }
}

async function handleStasisEnd(client, event) {
  const eventType = event?.type || 'unknown';
  const channel = event?.channel || null;
  const channelId = channel?.id;
  if (!channelId) {
    return;
  }

  const bridgeId = event?.bridge?.id || channel?.bridge?.id || null;
  let callId = channelToCallId.get(channelId) || null;

  if (!callId && bridgeId && bridgeToCallId.has(bridgeId)) {
    callId = bridgeToCallId.get(bridgeId);
  }

  let callState = callId ? activeCalls.get(callId) : null;

  if (!callId && channel?.linkedid) {
    if (linkedIdToCallId.has(channel.linkedid)) {
      callId = linkedIdToCallId.get(channel.linkedid);
      callState = callId ? activeCalls.get(callId) : null;
    }

    if (!callId) {
      for (const [candidateCallId, candidateState] of activeCalls.entries()) {
        if (candidateState?.linkedIds instanceof Set && candidateState.linkedIds.has(channel.linkedid)) {
          callId = candidateCallId;
          callState = candidateState;
          break;
        }
      }
    }
  }

  if (!callId || !callState) {
    if (eventType === 'ChannelDestroyed') {
      channelToCallId.delete(channelId);
    }
    return;
  }

  if (!channelToCallId.has(channelId)) {
    channelToCallId.set(channelId, callId);
  }
  if (callState.channels instanceof Set && !callState.channels.has(channelId)) {
    callState.channels.add(channelId);
  }
  if (bridgeId) {
    bridgeToCallId.set(bridgeId, callId);
    if (callState.bridges instanceof Set) {
      callState.bridges.add(bridgeId);
    }
  }
  if (channel?.linkedid) {
    linkedIdToCallId.set(channel.linkedid, callId);
    if (callState.linkedIds instanceof Set) {
      callState.linkedIds.add(channel.linkedid);
    }
  }

  let role = callState.channelRoles.get(channelId) || null;
  if (!role || role === 'unknown') {
    if (callState.dialerChannelId === channelId) {
      role = 'dialer';
    } else if (callState.dialedChannelId === channelId) {
      role = 'dialed';
    } else if (callState.legBTimeline?.channelId === channelId || callState.legBTimeline?.pairedChannelId === channelId) {
      role = 'dialed';
    } else if (callState.legATimeline?.channelId === channelId) {
      role = 'dialer';
    } else {
      const channelName = channel?.name || '';
      const baseName = stripLocalSuffix(channelName);
      const pairedName = callState.legBTimeline?.pairedChannelName || null;
      if (pairedName && stripLocalSuffix(pairedName) === baseName) {
        role = 'dialed';
      }
      if (callState.originatedPartner && isTargetLocalChannelName(baseName)) {
        role = 'dialed';
      }
    }

    if (!role) {
      if (!callState.dialerChannelId) {
        role = 'dialer';
      } else if (!callState.dialedChannelId) {
        role = 'dialed';
      }
    }

    if (role) {
      callState.channelRoles.set(channelId, role);
      if (role === 'dialer') {
        callState.dialerChannelId = callState.dialerChannelId || channelId;
        updateLegTimelineMetadata(callState.legATimeline, channel);
      } else if (role === 'dialed') {
        callState.dialedChannelId = callState.dialedChannelId || channelId;
        updateLegTimelineMetadata(callState.legBTimeline, channel);
      }
    }
  }

  role = role || 'unknown';
  const timestampMs = getEventTimestampMs(event);

  logWithTimestamp(
    'log',
    `[${callId}] handleStasisEnd received for channel ${channelId} (${channel?.name || 'unknown'}) role '${role}' (${eventType}).`
  );

  const hangupCause = resolveHangupCause(event, event.channel);
  if (role === 'dialer' && hangupCause) {
    callState.dialerHangupCause = hangupCause;
    const legTimeline = callState.legATimeline;
    const existingStatus = (legTimeline?.lastStatus || '').toUpperCase();
    if (legTimeline && existingStatus !== 'ANSWER' && existingStatus !== 'ANSWERED') {
      setLegTimelineStatus(legTimeline, hangupCause);
    }
  }
  if (role === 'dialed' && hangupCause) {
    callState.dialedHangupCause = hangupCause;
    const legTimeline = callState.legBTimeline;
    const existingStatus = (legTimeline?.lastStatus || '').toUpperCase();
    if (legTimeline && existingStatus !== 'ANSWER' && existingStatus !== 'ANSWERED') {
      setLegTimelineStatus(legTimeline, hangupCause);
    }
  }
  if (role === 'dialed') {
    updateAnsweredIdentity(callState, event.channel);
  }

  if (role === 'dialer') {
    callState.dialerHangupAt = callState.dialerHangupAt ?? timestampMs;
    setLegTimelineTimestamp(callState.legATimeline, 'endedAt', timestampMs);
  }
  if (role === 'dialed') {
    callState.dialedHangupAt = callState.dialedHangupAt ?? timestampMs;
    setLegTimelineTimestamp(callState.legBTimeline, 'endedAt', timestampMs);
  }
  if (role === 'agent' && callState.agentLegs instanceof Map) {
    const leg = callState.agentLegs.get(channelId);
    if (leg) {
      leg.hangupAt = leg.hangupAt ?? timestampMs;
    }
  }

  logWithTimestamp('log', `[${callId}] Channel ${channelId} with role '${role}' left Stasis (${eventType}).`);

  const isStasisEnd = eventType === 'StasisEnd';
  const isChannelDestroyed = eventType === 'ChannelDestroyed';

  if (isStasisEnd && (role === 'dialed' || role === 'dialer')) {
    const disconnectedSide = role === 'dialed' ? 'Destination' : 'Outbound';
    const remainingSide = role === 'dialed' ? 'outbound' : 'destination';
    logWithTimestamp('log',
      `[${callId}] ${disconnectedSide} channel disconnected. Hanging up remaining ${remainingSide} channels.`
    );
    for (const otherChannelId of callState.channels) {
      if (otherChannelId === channelId) {
        continue;
      }
      try {
        logWithTimestamp('log', `[${callId}] Hanging up channel ${otherChannelId}.`);
        await client.channels.hangup({ channelId: otherChannelId });
      } catch (err) {
        logWithTimestamp('error', `[${callId}] Failed to hang up channel ${otherChannelId}:`, err.message);
      }
    }
  }

  if (!isChannelDestroyed) {
    return;
  }

  callState.channels.delete(channelId);
  callState.channelRoles.delete(channelId);
  channelToCallId.delete(channelId);
  if (callState.agentChannels instanceof Set) {
    callState.agentChannels.delete(channelId);
  }
  if (callState.agentLegs instanceof Map) {
    callState.agentLegs.delete(channelId);
  }
  if (callState.dialerChannelId === channelId) {
    callState.dialerChannelId = null;
    callState.dialerUp = false;
  }
  if (callState.dialedChannelId === channelId) {
    callState.dialedChannelId = null;
  }
  if (callState.agentChannelId === channelId) {
    callState.agentChannelId = null;
  }

  if (callState.channels.size === 0) {
    logWithTimestamp('log', `[${callId}] No active channels remaining. Cleaning up call.`);
    if (timestampMs != null) {
      callState.completedAtMs = callState.completedAtMs ?? timestampMs;
    }
    await logCallSummary(callId, callState, new Date(timestampMs));
    await cleanupCall(callId, client);
    markCallCompleted(client, callId);
  }
}

async function startDialerRecording(client, callId, callState) {
  if (callState.recording?.id) {
    return;
  }

  const bridgeId = callState.bridge?.id;
  if (!bridgeId) {
    logWithTimestamp('warn', `[${callId}] Unable to start recording: bridge is not available.`);
    return;
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const recordingId = `${callId}-${timestamp}`;

  try {
    await client.bridges.record({
      bridgeId,
      name: recordingId,
      format: currentConfig.recordingFormat,
      ifExists: 'overwrite',
      maxDurationSeconds: 0,
      terminateOn: 'none'
    });

    callState.recording = {
      id: recordingId,
      format: currentConfig.recordingFormat,
      bridgeId,
      startedAt: new Date().toISOString(),
      mode: 'bridge'
    };
    callState.recordingId = recordingId;
    callState.recordingFormatUsed = currentConfig.recordingFormat;

    recordingOwnership.set(recordingId, {
      callId,
      recording: callState.recording,
      retryTimer: null,
      retryCounters: {}
    });

    const filePath = path.join(
      currentConfig.resolvedRecordingsDir,
      `${recordingId}.${currentConfig.recordingFormat}`
    );
    callState.recordingPath = filePath;

    logWithTimestamp('log',
      `[${callId}] Started bridge recording ${recordingId} on bridge ${bridgeId} to '${filePath}'.`
    );
  } catch (err) {
    logWithTimestamp('error', 
      `[${callId}] Failed to start bridge recording ${recordingId} on bridge ${bridgeId}:`,
      err.message
    );
  }
}

async function handleChannelStateChange(client, event) {
  const channel = event.channel;
  if (!channel) {
    return;
  }

  const channelId = channel.id;
  const callId = channelToCallId.get(channelId);
  if (!callId) {
    return;
  }

  const callState = activeCalls.get(callId);
  if (!callState) {
    return;
  }

  const timestampMs = getEventTimestampMs(event);
  const role = callState.channelRoles.get(channelId);

  logWithTimestamp(
    'log',
    `[${callId}] Channel state change for ${channelId} (${channel.name || 'unknown'}) -> ${channel.state || 'unknown'} role='${
      role || 'unknown'
    }'.`
  );

  if (role === 'dialer') {
    logWithTimestamp('log', `[${callId}] Processing dialer channel state change for ${channelId}.`);
    callState.dialerChannelId = channelId;
    callState.dialerUp = channel.state === 'Up';
    updateLegTimelineMetadata(callState.legATimeline, channel);
    setLegTimelineTimestamp(callState.legATimeline, 'startedAt', timestampMs);
    if (channel.state === 'Up') {
      setLegTimelineTimestamp(callState.legATimeline, 'answeredAt', timestampMs);
    }
    if (channel.state === 'Up') {
      callState.dialerConnectedAt = callState.dialerConnectedAt ?? timestampMs;
      updateCallConnectionState(callState, timestampMs);
      await startDialerRecording(client, callId, callState);
    }
    return;
  }

  if (role === 'dialed') {
    logWithTimestamp('log', `[${callId}] Processing dialed channel state change for ${channelId}.`);
    updateAnsweredIdentity(callState, channel);
    updateLegTimelineMetadata(callState.legBTimeline, channel);
    if (channel.state === 'Up') {
      setLegTimelineTimestamp(callState.legBTimeline, 'answeredAt', timestampMs);
      callState.dialedConnectedAt = callState.dialedConnectedAt ?? timestampMs;
      updateCallConnectionState(callState, timestampMs);
      await startDialerRecording(client, callId, callState);
    }
    return;
  }

  if (role === 'agent') {
    logWithTimestamp('log', `[${callId}] Processing agent channel state change for ${channelId}.`);
    const leg = ensureAgentLeg(callState, channelId);
    const identity = extractConnectedIdentity(channel) || leg?.identity || channel.name || null;
    if (identity) {
      if (leg) {
        leg.identity = identity;
      }
      setAnsweredIdentity(callState, identity, 'agent');
    }

    if (leg && !leg.dialedAt) {
      leg.dialedAt = timestampMs;
    }

    if (leg && channel.state === 'Up') {
      leg.answeredAt = leg.answeredAt ?? timestampMs;
      if (callState.agentAnsweredAt == null || callState.agentAnsweredAt > leg.answeredAt) {
        callState.agentAnsweredAt = leg.answeredAt;
      }
      callState.agentChannelId = callState.agentChannelId || channelId;
      updateCallConnectionState(callState, leg.answeredAt);
    }

    if (leg && (channel.state === 'Down' || channel.state === 'Hungup')) {
      leg.hangupAt = leg.hangupAt ?? timestampMs;
    }
    return;
  }

  logWithTimestamp('log', `[${callId}] Channel ${channelId} has unrecognized role during state change; skipping.`);
}

function handleDialEvent(event) {
  if (!event) {
    return;
  }

  const dialStatusRaw = event.dialstatus;
  const dialStatus = typeof dialStatusRaw === 'string' ? dialStatusRaw.toUpperCase() : '';

  const candidateChannels = [];
  if (event.caller) {
    candidateChannels.push({ channel: event.caller, relation: 'caller' });
  }
  if (event.peer) {
    candidateChannels.push({ channel: event.peer, relation: 'peer' });
  }

  const hasLocalCandidate = candidateChannels.some((candidate) => {
    const name = candidate.channel?.name || '';
    return name.startsWith('Local/');
  });
  let sawLegBTerminalChannel = !hasLocalCandidate;

  let callId = null;
  let fallbackCandidate = null;
  let fallbackMatchedLeg = null;
  let resolvedCallState = null;
  for (const candidate of candidateChannels) {
    const channel = candidate.channel;
    if (!channel) {
      continue;
    }
    if (channel.id && channelToCallId.has(channel.id)) {
      callId = channelToCallId.get(channel.id);
      break;
    }
    if (channel.linkedid && linkedIdToCallId.has(channel.linkedid)) {
      callId = linkedIdToCallId.get(channel.linkedid);
      break;
    }
  }

  if (!callId) {
    const dialString = typeof event.dialstring === 'string' ? event.dialstring : '';
    const dialNumber = dialString.split('@')[0] || '';

    if (dialNumber) {
      const matchingCallIds = [];
      for (const [activeCallId, number] of callNumberMap.entries()) {
        if (number === dialNumber && inFlightCalls.has(activeCallId)) {
          matchingCallIds.push(activeCallId);
        }
      }

      if (matchingCallIds.length === 1) {
        callId = matchingCallIds[0];
        resolvedCallState = getCallState(callId);
        for (const candidate of candidateChannels) {
          const channel = candidate.channel;
          if (!channel) {
            continue;
          }
          if (channel.id) {
            channelToCallId.set(channel.id, callId);
          }
          if (channel.linkedid) {
            linkedIdToCallId.set(channel.linkedid, callId);
            resolvedCallState?.linkedIds.add(channel.linkedid);
          }
        }

        logWithTimestamp(
          'log',
          `[${callId}] Dial event matched via dialstring '${dialNumber}'.`
        );
      }
    }
  }

  if (!callId) {
    for (const candidate of candidateChannels) {
      const channel = candidate.channel;
      const channelName = channel?.name;
      if (!channelName) {
        continue;
      }

      const baseName = stripLocalSuffix(channelName);
      if (!isTargetLocalChannelName(baseName)) {
        continue;
      }

      const matches = [];
      for (const [activeCallId, activeCallState] of activeCalls.entries()) {
        if (!activeCallState || !activeCallState.originatedPartner) {
          continue;
        }

        const legBTimeline = activeCallState.legBTimeline || null;
        if (!legBTimeline || legBTimeline.channelId || activeCallState.dialedChannelId) {
          continue;
        }

        let hasDialedRole = false;
        for (const role of activeCallState.channelRoles.values()) {
          if (role === 'dialed') {
            hasDialedRole = true;
            break;
          }
        }
        if (hasDialedRole) {
          continue;
        }

        matches.push({ activeCallId, activeCallState });
      }

      if (matches.length === 1) {
        const match = matches[0];
        callId = match.activeCallId;
        resolvedCallState = match.activeCallState || getCallState(callId);
        if (!fallbackCandidate) {
          fallbackCandidate = candidate;
        }
        if (!fallbackMatchedLeg) {
          fallbackMatchedLeg = 'legB';
        }

        const callState = resolvedCallState;
        const legBTimeline = callState?.legBTimeline || null;
        if (legBTimeline) {
          updateLegTimelineMetadata(legBTimeline, channel);
          assignLegDialString(legBTimeline, event.dialstring);
        }

        const matchingLocalChannels = candidateChannels.filter((entry) => {
          const entryName = entry.channel?.name;
          if (!entryName) {
            return false;
          }
          return stripLocalSuffix(entryName) === baseName;
        });

        for (const localCandidate of matchingLocalChannels) {
          const localChannel = localCandidate.channel;
          if (!localChannel) {
            continue;
          }

          if (localChannel.id) {
            channelToCallId.set(localChannel.id, callId);
            callState.channelRoles.set(localChannel.id, 'dialed');
            if (!callState.dialedChannelId) {
              callState.dialedChannelId = localChannel.id;
            }
            if (legBTimeline) {
              if (!legBTimeline.channelId) {
                legBTimeline.channelId = localChannel.id;
              } else if (!legBTimeline.pairedChannelId) {
                legBTimeline.pairedChannelId = localChannel.id;
              }
              if (!legBTimeline.pairedChannelName && localChannel.name) {
                if (/;1$/.test(localChannel.name)) {
                  legBTimeline.pairedChannelName = localChannel.name.replace(/;1$/, ';2');
                } else if (/;2$/.test(localChannel.name)) {
                  legBTimeline.pairedChannelName = localChannel.name;
                }
              }
            }
          }

          if (localChannel?.linkedid) {
            linkedIdToCallId.set(localChannel.linkedid, callId);
            if (callState?.linkedIds) {
              callState.linkedIds.add(localChannel.linkedid);
            }
          }
        }

        break;
      }
    }
  }

  if (!callId) {
    for (const candidate of candidateChannels) {
      const channel = candidate.channel;
      const channelName = channel?.name;
      if (!channelName) {
        continue;
      }

      const nameVariants = [channelName];
      if (/;1$/.test(channelName)) {
        nameVariants.push(channelName.replace(/;1$/, ';2'));
      } else if (/;2$/.test(channelName)) {
        nameVariants.push(channelName.replace(/;2$/, ';1'));
      }

      let matched = false;
      for (const [activeCallId, activeCallState] of activeCalls.entries()) {
        if (!activeCallState) {
          continue;
        }

        const legATimeline = activeCallState.legATimeline || null;
        const legBTimeline = activeCallState.legBTimeline || null;
        const legIdentifiers = [
          { leg: 'legA', value: legATimeline?.peerName },
          { leg: 'legA', value: legATimeline?.pairedChannelName },
          { leg: 'legB', value: legBTimeline?.peerName },
          { leg: 'legB', value: legBTimeline?.pairedChannelName }
        ];

        for (const variant of nameVariants) {
          if (!variant) {
            continue;
          }

          for (const identifier of legIdentifiers) {
            if (!identifier.value) {
              continue;
            }

            if (variant === identifier.value) {
              callId = activeCallId;
              fallbackCandidate = candidate;
              fallbackMatchedLeg = identifier.leg;
              matched = true;

              if (identifier.leg === 'legB') {
                updateLegTimelineMetadata(legBTimeline, channel);
                assignLegDialString(legBTimeline, event.dialstring);
                if (dialStatus === 'ANSWER') {
                  setLegAnsweredBy(
                    legBTimeline,
                    event.dialstring || channel?.name || event.peer?.name || event.caller?.name
                  );
                }
                if (channel?.id) {
                  channelToCallId.set(channel.id, activeCallId);
                  activeCallState.channelRoles.set(channel.id, 'dialed');
                  if (!activeCallState.dialedChannelId) {
                    activeCallState.dialedChannelId = channel.id;
                  }
                  if (!legBTimeline.pairedChannelId) {
                    legBTimeline.pairedChannelId = channel.id;
                  }
                }
                if (channel?.linkedid) {
                  linkedIdToCallId.set(channel.linkedid, activeCallId);
                  activeCallState.linkedIds.add(channel.linkedid);
                }
                if (!legBTimeline.pairedChannelName && channel?.name) {
                  legBTimeline.pairedChannelName = channel.name;
                }
              }

              break;
            }
          }

          if (matched) {
            break;
          }
        }

        if (matched) {
          break;
        }
      }

      if (matched) {
        break;
      }
    }

    if (!callId) {
      logWithTimestamp(
        'warn',
        `Dial event could not be associated with a callId (type=${event?.type || 'unknown'}, dialstring=${
          event?.dialstring || 'unknown'
        }, callerId=${event?.caller?.id || 'unknown'}, peerId=${event?.peer?.id || 'unknown'}). Event=${safeStringify(
          event
        )}`
      );
      return;
    }
  }

  const callState = resolvedCallState || getCallState(callId);
  const timestampMs = getEventTimestampMs(event);
  const agentChannel = event.peer || event.caller || null;
  const agentChannelId = agentChannel?.id || null;
  const identity = extractConnectedIdentity(agentChannel) || event.dialstring || agentChannel?.name || null;

  logWithTimestamp(
    'log',
    `[${callId}] Processing Dial event (status: ${dialStatus || 'UNKNOWN'}) with ${candidateChannels.length} candidate channel(s).`
  );

  const orderedCandidates =
    fallbackCandidate
      ? [
          fallbackCandidate,
          ...candidateChannels.filter((candidate) => candidate !== fallbackCandidate)
        ]
      : candidateChannels;

  for (const candidate of orderedCandidates) {
    const channel = candidate.channel;
    if (!channel?.id) {
      continue;
    }

    const channelId = channel.id;
    const relation = candidate.relation;
    const legATimeline = callState.legATimeline;
    const legBTimeline = callState.legBTimeline;
    const existingRole = callState.channelRoles.get(channelId) || null;

    logWithTimestamp(
      'log',
      `[${callId}] Evaluating Dial candidate ${channelId} (${relation}) name='${channel.name || 'unknown'}' role='${existingRole || 'unknown'}'.`
    );

    let isDialerChannel =
      (callState.dialerChannelId && callState.dialerChannelId === channelId) ||
      (legATimeline.channelId && legATimeline.channelId === channelId);
    let isDialedChannel =
      (callState.dialedChannelId && callState.dialedChannelId === channelId) ||
      (legBTimeline.channelId && legBTimeline.channelId === channelId);
    let isPairedLegBChannel =
      (!!legBTimeline.pairedChannelId && legBTimeline.pairedChannelId === channelId) ||
      (!!legBTimeline.pairedChannelName && channel.name === legBTimeline.pairedChannelName);

    if (existingRole === 'dialed') {
      isDialedChannel = true;
    }

    if (fallbackCandidate && candidate === fallbackCandidate) {
      channelToCallId.set(channelId, callId);
      if (channel.linkedid) {
        linkedIdToCallId.set(channel.linkedid, callId);
        callState.linkedIds.add(channel.linkedid);
      }
      if (fallbackMatchedLeg === 'legB') {
        callState.channelRoles.set(channelId, 'dialed');
        isDialedChannel = true;
        isPairedLegBChannel = true;
        if (!legBTimeline.pairedChannelId && channel.id) {
          legBTimeline.pairedChannelId = channel.id;
        }
        if (!legBTimeline.pairedChannelName && channel.name) {
          legBTimeline.pairedChannelName = channel.name;
        }
        logWithTimestamp(
          'log',
          `[${callId}] Fallback candidate ${channelId} matched leg B via timeline metadata.`
        );
      } else if (fallbackMatchedLeg === 'legA') {
        callState.channelRoles.set(channelId, 'dialer');
        isDialerChannel = true;
        logWithTimestamp('log', `[${callId}] Fallback candidate ${channelId} matched leg A.`);
      }
    } else if (fallbackMatchedLeg === 'legB' && fallbackCandidate && candidate !== fallbackCandidate) {
      if (!channelToCallId.has(channelId)) {
        channelToCallId.set(channelId, callId);
      }
      if (channel.linkedid && !linkedIdToCallId.has(channel.linkedid)) {
        linkedIdToCallId.set(channel.linkedid, callId);
        callState.linkedIds.add(channel.linkedid);
      }
      if (relation === 'peer') {
        callState.channelRoles.set(channelId, 'dialed');
        if (!callState.dialedChannelId) {
          callState.dialedChannelId = channelId;
        }
        if (legBTimeline.channelId !== channelId) {
          legBTimeline.channelId = channelId;
        }
        isDialedChannel = true;
        logWithTimestamp(
          'log',
          `[${callId}] Tagged candidate ${channelId} as leg B due to fallback peer relation.`
        );
      }
    }

    if (!isDialerChannel && !isDialedChannel && !isPairedLegBChannel) {
      const appearsToBeLegB =
        channelId !== callState.dialerChannelId &&
        (relation === 'peer' || /;1$/.test(channel.name || '')) &&
        callState.originatedPartner;
      if (appearsToBeLegB && !legBTimeline.channelId) {
        isDialedChannel = true;
        logWithTimestamp(
          'log',
          `[${callId}] Channel ${channelId} inferred as leg B due to heuristics (relation=${relation}).`
        );
      }
    }

    if (isDialerChannel) {
      logWithTimestamp(
        'log',
        `[${callId}] Updating leg A metadata using channel ${channelId} (${channel.name || 'unknown'}).`
      );
      updateLegTimelineMetadata(legATimeline, channel);
      if (!dialStatus) {
        setLegTimelineTimestamp(legATimeline, 'startedAt', timestampMs);
      }
      if (dialStatus === 'ANSWER') {
        setLegTimelineTimestamp(legATimeline, 'answeredAt', timestampMs);
      }
      if (dialStatus) {
        setLegTimelineStatus(legATimeline, dialStatus);
      }
      assignLegDialString(legATimeline, event.dialstring);
    }

    if (isDialedChannel || isPairedLegBChannel) {
      const channelName = channel?.name || '';
      if (!/;1$/.test(channelName)) {
        sawLegBTerminalChannel = true;
      }
      const previousDialedChannelId = callState.dialedChannelId;
      const legBPeerName = legBTimeline.peerName || '';
      const hasLocalPlaceholder = typeof legBPeerName === 'string' && legBPeerName.startsWith('Local/');

      if (
        existingRole === 'dialed' &&
        channelId &&
        (previousDialedChannelId !== channelId || hasLocalPlaceholder)
      ) {
        callState.dialedChannelId = channelId;
        if (legBTimeline.channelId !== channelId) {
          legBTimeline.channelId = channelId;
        }
        logWithTimestamp(
          'log',
          `[${callId}] Promoted dialed channel from ${previousDialedChannelId || 'none'} to ${channelId}.`
        );
      }

      logWithTimestamp(
        'log',
        `[${callId}] Updating leg B metadata using channel ${channelId} (${channel.name || 'unknown'}).`
      );
      updateLegTimelineMetadata(legBTimeline, channel);
      if (isDialedChannel && !callState.dialedChannelId && relation === 'peer' && channel.id) {
        callState.dialedChannelId = channel.id;
      }
      if (isPairedLegBChannel && !legBTimeline.pairedChannelId && channel.id) {
        legBTimeline.pairedChannelId = channel.id;
      }
      if (!dialStatus) {
        if (relation === 'peer' || !legBTimeline.startedAt) {
          setLegTimelineTimestamp(legBTimeline, 'startedAt', timestampMs);
        }
      }
      if (dialStatus === 'ANSWER') {
        setLegTimelineTimestamp(legBTimeline, 'answeredAt', timestampMs);
        setLegAnsweredBy(legBTimeline, event.dialstring || event.peer?.name || channel.name);
      }
      if (event.dialstring) {
        assignLegDialString(legBTimeline, event.dialstring);
      }
    } else {
      logWithTimestamp(
        'log',
        `[${callId}] Channel ${channelId} not treated as leg A/B during Dial event. isDialer=${isDialerChannel} isDialed=${isDialedChannel} isPaired=${isPairedLegBChannel}.`
      );
    }
  }

  let leg = null;
  if (
    agentChannelId &&
    (sawLegBTerminalChannel || !/Local\/.*;1$/.test(agentChannel?.name || ''))
  ) {
    leg = ensureAgentLeg(callState, agentChannelId);
    channelToCallId.set(agentChannelId, callId);
    callState.agentChannels.add(agentChannelId);
    callState.channelRoles.set(agentChannelId, 'agent');
    if (agentChannel?.linkedid) {
      linkedIdToCallId.set(agentChannel.linkedid, callId);
      callState.linkedIds.add(agentChannel.linkedid);
    }
  }

  if (leg && !leg.dialedAt) {
    leg.dialedAt = timestampMs;
  }

  if (leg && identity) {
    leg.identity = identity;
    setAnsweredIdentity(callState, identity, 'agent');
  }

  if (leg && dialStatus) {
    leg.lastStatus = dialStatus;
  }

  if (leg && dialStatus === 'ANSWER') {
    leg.answeredAt = leg.answeredAt ?? timestampMs;
    if (callState.agentAnsweredAt == null || callState.agentAnsweredAt > leg.answeredAt) {
      callState.agentAnsweredAt = leg.answeredAt;
    }
    if (agentChannelId) {
      callState.agentChannelId = agentChannelId;
    }
    updateCallConnectionState(callState, leg.answeredAt);
  } else if (leg && dialStatus && dialStatus !== 'RINGING') {
    leg.hangupAt = leg.hangupAt ?? timestampMs;
  }
}

function handleBridgeEnter(event) {
  const bridgeId = event?.bridge?.id;
  const channel = event?.channel;
  if (!bridgeId || !channel) {
    return;
  }

  let callId = channelToCallId.get(channel.id);
  if (!callId && bridgeToCallId.has(bridgeId)) {
    callId = bridgeToCallId.get(bridgeId);
  }
  if (!callId && channel.linkedid && linkedIdToCallId.has(channel.linkedid)) {
    callId = linkedIdToCallId.get(channel.linkedid);
  }

  if (!callId) {
    return;
  }

  const callState = activeCalls.get(callId);
  if (!callState) {
    return;
  }

  const existingRole = callState.channelRoles.get(channel.id) || null;
  let resolvedRole = existingRole;

  logWithTimestamp(
    'log',
    `[${callId}] handleBridgeEnter for channel ${channel.id} (${channel.name || 'unknown'}) bridge ${bridgeId} role '${
      existingRole || 'unknown'
    }'.`
  );

  if (callState.bridges) {
    callState.bridges.add(bridgeId);
  }
  bridgeToCallId.set(bridgeId, callId);

  if (channel.linkedid) {
    linkedIdToCallId.set(channel.linkedid, callId);
    callState.linkedIds.add(channel.linkedid);
  }

  if (!resolvedRole) {
    const channelName = channel.name || '';
    const baseName = stripLocalSuffix(channelName);
    const looksLikeLegB =
      isTargetLocalChannelName(baseName) &&
      callState.originatedPartner;

    if (looksLikeLegB) {
      resolvedRole = 'dialed';
      callState.channelRoles.set(channel.id, 'dialed');
      channelToCallId.set(channel.id, callId);
      callState.dialedChannelId = callState.dialedChannelId || channel.id;
      const legBTimeline = callState.legBTimeline;
      if (legBTimeline) {
        if (!legBTimeline.channelId) {
          legBTimeline.channelId = channel.id;
        }
        if (!legBTimeline.peerName) {
          legBTimeline.peerName = channelName || legBTimeline.peerName;
        }
        if (!legBTimeline.pairedChannelName && channelName) {
          if (/;1$/.test(channelName)) {
            legBTimeline.pairedChannelName = channelName.replace(/;1$/, ';2');
          } else if (/;2$/.test(channelName)) {
            legBTimeline.pairedChannelName = channelName;
          }
        }
        updateLegTimelineMetadata(legBTimeline, channel);
      }
      logWithTimestamp(
        'log',
        `[${callId}] Heuristically tagged bridge entrant ${channel.id} (${channelName || 'unknown'}) as leg B.`
      );
    }
  }

  const isKnownLeg = resolvedRole === 'dialer' || resolvedRole === 'dialed';
  if (isKnownLeg) {
    logWithTimestamp(
      'log',
      `[${callId}] Known channel ${channel.id} with role '${resolvedRole}' entered bridge ${bridgeId}.`
    );
    return;
  }

  callState.agentChannels.add(channel.id);
  callState.channelRoles.set(channel.id, 'agent');
  channelToCallId.set(channel.id, callId);

  const timestampMs = getEventTimestampMs(event);
  const leg = ensureAgentLeg(callState, channel.id);

  if (leg && !leg.dialedAt) {
    leg.dialedAt = timestampMs;
  }

  const identity = extractConnectedIdentity(channel);
  if (identity) {
    if (leg) {
      leg.identity = identity;
      leg.answeredAt = leg.answeredAt ?? timestampMs;
    }
    setAnsweredIdentity(callState, identity, 'agent');
    logWithTimestamp('log', `[${callId}] Agent channel ${channel.id} joined bridge ${bridgeId} as '${identity}'.`);
  } else {
    logWithTimestamp('log', `[${callId}] Agent channel ${channel.id} joined bridge ${bridgeId} but identity not yet available.`);
  }

  if (leg && leg.answeredAt != null) {
    if (callState.agentAnsweredAt == null || callState.agentAnsweredAt > leg.answeredAt) {
      callState.agentAnsweredAt = leg.answeredAt;
    }
    callState.agentChannelId = callState.agentChannelId || channel.id;
    updateCallConnectionState(callState, leg.answeredAt);
  }
}

async function start() {
  lastStartRestarted = false;

  if (dialerStarted && dialerClient) {
    if (numbersQueue.length === 0 && inFlightCalls.size === 0) {
      resetNumbersQueue();
      lastStartRestarted = true;
    }
    maybeOriginateNext(dialerClient);
    return dialerClient;
  }

  if (dialerStartingPromise) {
    return dialerStartingPromise;
  }

  const initializationPromise = (async () => {
    try {
      await ensureMysqlInitialization();
      const client = await AriClient.connect(
        currentConfig.ARI_URL,
        currentConfig.ARI_USERNAME,
        currentConfig.ARI_PASSWORD
      );

      client.on('*', (event) => {
        try {
          const eventType = event?.type || 'unknown';
          const eventPayload = safeStringify(event) || 'null';
          logWithTimestamp('log', `[ARI EVENT] ${eventType}: ${eventPayload}`);
        } catch (err) {
          const type = event?.type || 'unknown';
          logWithTimestamp('error', `Failed to log ARI event ${type}:`, err.message);
        }
      });

      client.on('StasisStart', (event) => {
        handleStasisStart(client, event).catch((err) => {
          logWithTimestamp('error', 'Error in StasisStart handler:', err.message);
        });
      });

      client.on('StasisEnd', (event) => {
        handleStasisEnd(client, event).catch((err) => {
          logWithTimestamp('error', 'Error in StasisEnd handler:', err.message);
        });
      });

      client.on('ChannelDestroyed', (event) => {
        handleStasisEnd(client, event).catch((err) => {
          logWithTimestamp('error', 'Error handling ChannelDestroyed event:', err.message);
        });
      });

      client.on('ChannelStateChange', (event) => {
        handleChannelStateChange(client, event).catch((err) => {
          logWithTimestamp('error', 'Error handling ChannelStateChange event:', err.message);
        });
      });

      client.on('Dial', (event) => {
        try {
          handleDialEvent(event);
        } catch (err) {
          logWithTimestamp('error', 'Error handling Dial event:', err.message);
        }
      });

      client.on('BridgeEnter', (event) => {
        try {
          handleBridgeEnter(event);
        } catch (err) {
          logWithTimestamp('error', 'Error handling BridgeEnter event:', err.message);
        }
      });

      client.on('RecordingFinished', (event) => {
        const recording = event.recording || {};
        const recordingName = recording.name || recording.id || event.recordingName;

        if (!recordingName) {
          logWithTimestamp('warn', 'Received RecordingFinished event without a recording name.');
          return;
        }

        let ownership = recordingOwnership.get(recordingName);
        if (!ownership) {
          for (const [candidateCallId, state] of activeCalls.entries()) {
            if (state.recording?.id === recordingName) {
              ownership = {
                callId: candidateCallId,
                recording: state.recording,
                retryTimer: null,
                retryCounters: {}
              };
              recordingOwnership.set(recordingName, ownership);
              break;
            }
          }
        }

        if (!ownership) {
          logWithTimestamp('warn', `RecordingFinished event received for '${recordingName}' but owning call was not found.`);
          return;
        }

        if (recording.format && ownership.recording) {
          ownership.recording.format = recording.format;
        }

        handleRecordingReady(recordingName, 'event').catch((err) => {
          logWithTimestamp('error', `Error handling RecordingFinished for '${recordingName}':`, err.message);
        });
      });

      await client.start(currentConfig.STASIS_APP);
      logWithTimestamp('log', `Subscribed to Stasis app: ${currentConfig.STASIS_APP}`);

      logWithTimestamp(
        'log',
        `Loaded ${outboundNumbers.length} outbound number(s). Dialing with max concurrency ${currentConfig.callConcurrencyLimit}.`
      );

      dialerClient = client;
      dialerStarted = true;
      maybeOriginateNext(client);
      lastStartRestarted = false;
      return client;
    } catch (err) {
      logWithTimestamp('error', 'Failed to start ARI dialer:', err.message);
      throw err;
    } finally {
      if (!dialerStarted) {
        dialerStartingPromise = null;
      }
    }
  })();

  dialerStartingPromise = initializationPromise;
  return initializationPromise;
}

function createControlServer({ host = '127.0.0.1', port = 3000, startPath = '/start' } = {}) {
  const server = http.createServer((req, res) => {
    (async () => {
      const method = req.method || 'GET';
      let pathname = '';

      try {
        const url = new URL(req.url || '/', `http://${host}:${port}`);
        pathname = url.pathname;
      } catch (err) {
        pathname = req.url || '';
      }

      if (method !== 'GET' || pathname !== startPath) {
        res.statusCode = 404;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ error: 'Not Found' }));
        return;
      }

      const wasStarted = dialerStarted;

      try {
        const refreshedConfig = refreshConfig();
        currentConfig = refreshedConfig;
        applyConfig(currentConfig);
        resetNumbersQueue();
        await ensureMysqlInitialization();

        await start();
        const restarted = wasStarted && lastStartRestarted;
        res.statusCode = wasStarted ? 200 : 201;
        res.setHeader('Content-Type', 'application/json');
        res.end(
          JSON.stringify({
            status: 'ok',
            message: restarted
              ? 'Dialer run restarted.'
              : wasStarted
              ? 'Dialer already running.'
              : 'Dialer started successfully.'
          })
        );
      } catch (err) {
        res.statusCode = 500;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ status: 'error', message: err.message || 'Failed to start dialer.' }));
      }
    })().catch((err) => {
      logWithTimestamp('error', 'Unexpected error handling control request:', err.message);
      if (!res.headersSent) {
        res.statusCode = 500;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ status: 'error', message: 'Internal Server Error' }));
      }
    });
  });

  server.on('error', (err) => {
    logWithTimestamp('error', 'Control server error:', err.message);
  });

  return server;
}

if (require.main === module) {
  const host = '127.0.0.1';
  const port = 3000;
  const server = createControlServer({ host, port, startPath: '/start' });

  server.listen(port, host, () => {
    logWithTimestamp('log', `Control server listening on http://${host}:${port}/start`);
  });
}

module.exports = {
  calculateCallDurations,
  calculateLegDurations,
  extractConnectedIdentity,
  getCallState,
  handleBridgeEnter,
  handleStasisEnd,
  handleDialEvent,
  logCallSummary,
  setAnsweredIdentity,
  updateCallConnectionState,
  updateAnsweredIdentity,
  start,
  createControlServer
};
