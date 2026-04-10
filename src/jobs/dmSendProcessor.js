const { connectDB, disconnectDB } = require('../config/database');
const { initMemcache } = require('../config/memcache');
const dmSendQueue = require('../services/dmQueue');

const startJobProcessor = async () => {
  try {
    await connectDB();
    console.log('✓ MongoDB connected for job processor');

    await initMemcache();
    console.log('✓ Memcached connected for job processor');

    console.log('✓ DM Send Queue is now in-process and Memcached-backed');
    console.log('✓ Keep this process running if you want a dedicated worker lifecycle');
  } catch (error) {
    console.error('Failed to start job processor:', error.message);
    process.exit(1);
  }
};

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down job processor gracefully');
  await dmSendQueue.close();
  await disconnectDB();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down job processor gracefully');
  await dmSendQueue.close();
  await disconnectDB();
  process.exit(0);
});

if (require.main === module) {
  startJobProcessor();
}

module.exports = dmSendQueue;
