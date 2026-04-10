const memjs = require('memjs');
const config = require('./env');

let memcacheClient;

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

const initMemcache = async () => {
  try {
    memcacheClient = memjs.Client.create(config.memcache.servers);
    await setAsync(memcacheClient, '__memcache_health__', 'ok', { expires: 5 });
    console.log('✓ Memcached connected');
  } catch (error) {
    console.error('✗ Memcached connection failed:', error.message);
    process.exit(1);
  }
};

const getMemcacheClient = () => {
  if (!memcacheClient) {
    throw new Error('Memcached client is not initialized');
  }

  return memcacheClient;
};

module.exports = {
  initMemcache,
  getMemcacheClient,
};