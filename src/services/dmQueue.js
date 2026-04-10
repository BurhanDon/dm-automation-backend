const FlowExecutionService = require('./FlowExecutionService');
const { getMemcacheClient } = require('../config/memcache');
const config = require('../config/env');

const MAX_ATTEMPTS = 3;
const BACKOFF_DELAY_MS = 2000;

const getAsync = (client, key) =>
  new Promise((resolve, reject) => {
    client.get(key, (err, value) => {
      if (err) {
        reject(err);
        return;
      }

      resolve(value);
    });
  });

const setAsync = (client, key, value, options = {}) =>
  new Promise((resolve, reject) => {
    client.set(key, value, options, (err, success) => {
      if (err) {
        reject(err);
        return;
      }

      resolve(success);
    });
  });

const deleteAsync = (client, key) =>
  new Promise((resolve, reject) => {
    client.delete(key, (err, success) => {
      if (err) {
        reject(err);
        return;
      }

      resolve(success);
    });
  });

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const executeWithRetry = async ({ conversationId, instagramAccountId, stepIndex = 0 }) => {
  let lastError;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    try {
      return await FlowExecutionService.executeFlow(conversationId, instagramAccountId, stepIndex);
    } catch (error) {
      lastError = error;

      if (attempt < MAX_ATTEMPTS) {
        const backoff = BACKOFF_DELAY_MS * Math.pow(2, attempt - 1);
        await delay(backoff);
      }
    }
  }

  throw lastError;
};

const add = async (_jobName, payload) => {
  const client = getMemcacheClient();
  const stepIndex = payload.stepIndex || 0;
  const lockKey = `dm-lock:${payload.conversationId}:${stepIndex}`;

  const existingLock = await getAsync(client, lockKey);

  if (existingLock) {
    return { queued: false, reason: 'duplicate_job_locked' };
  }

  await setAsync(client, lockKey, '1', {
    expires: Math.max(10, config.memcache.ttlSeconds),
  });

  setImmediate(async () => {
    try {
      await executeWithRetry({
        conversationId: payload.conversationId,
        instagramAccountId: payload.instagramAccountId,
        stepIndex,
      });
    } catch (error) {
      console.error('DM queue job failed:', error.message);
    } finally {
      try {
        await deleteAsync(client, lockKey);
      } catch (cleanupError) {
        console.error('Failed to release Memcached lock:', cleanupError.message);
      }
    }
  });

  return { queued: true };
};

const close = async () => Promise.resolve();

module.exports = {
  add,
  close,
};