const configData = require('../config/config.js');
const { db: { host, port, dbName, importCollection } } = configData;
const os = require('os');
const url = 'mongodb://' + host + ':' + port + '/' + dbName;

const MONGO_MONITOR_URI = 'mongodb://'+ host + ':' + port;
const { MongoClient } = require('mongodb');
const { execSync } = require('child_process');
var mongoose = require('mongoose');

var loggerMod = require('./loggerMod');
var dbLogFile = `importMongoose-${process.pid}.log`;

// MongoDB
// Resident Memory - Actual Physical memory that MongoDB is currently using. 
// Virtual Memory - Amount memory which it has access to. Bit more than the resident memory
// Wired Tiger Cache - 50% of Actual memory will be allocated. This is not just the available memory.
// 300GB - Available : 50 - Still Wired Tiger will allocate 150GB 

// Below options are set for mongoose based on the standard mongoose doc
// https://mongoosejs.com/docs/deprecations.html
//mongoose.set('useNewUrlParser',true);
mongoose.set('useCreateIndex',true);
mongoose.set('debug', true);
var dbLog = loggerMod.logger('import',dbLogFile);

// Track memory usage over time
let previousMemoryUsage = null;
let previousSwapUsage = null;

// Get MongoDB internal memory usage using a separate connection
async function getMongoMemoryUsage() {
  let client;
  try {
    client = new MongoClient(MONGO_MONITOR_URI);
    await client.connect();
    const adminDb = client.db().admin();
    const stats = await adminDb.serverStatus();

    // Mongodb resident memory 1024MB - Mongodb is holding ~1GB in RAM 
    // connections. - Number of active mongodb connections
    // virtual memory usage is high - indicates insufficient indexing or query execution

    return {
      mongoResident: stats.mem.resident + " MB",
      mongoVirtual: stats.mem.virtual + " MB",
      wiredTigerCache: stats.wiredTiger
        ? (stats.wiredTiger.cache["bytes currently in the cache"] / 1024 / 1024).toFixed(2) + " MB"
        : "N/A",
      wiredTigerDirtyCache: stats.wiredTiger
        ? (stats.wiredTiger.cache["tracked dirty bytes in the cache"] / 1024 / 1024).toFixed(2) + " MB"
        : "N/A",
      readTransactions: stats.wiredTiger.concurrentTransactions.read.out,
      writeTransactions: stats.wiredTiger.concurrentTransactions.write.out,
      activeConnections: stats.connections.current + " connections"
    };
  } catch (error) {
    console.error("Error fetching MongoDB memory stats:", error);
    return { mongoResident: "N/A", mongoVirtual: "N/A", wiredTigerCache: "N/A",wiredTigerDirtyCache: "N/A",readTransactions: "N/A",writeTransactions: "N/A",activeConnections: "N/A" };
  } finally {
    if (client) await client.close(); // Close only the monitoring connection
  }
}

function getSystemUsage() {
    const freeMemory = os.freemem() / 1024 / 1024; // Convert to MB
    const totalMemory = os.totalmem() / 1024 / 1024; // Convert to MB
    const memoryUsage = ((totalMemory - freeMemory) / totalMemory) * 100; // Percentage
    const cpus = os.cpus();
    const cpuLoad = cpus.map(cpu => cpu.times).reduce((acc, times) => {
      const total = times.user + times.nice + times.sys + times.idle + times.irq;
      return acc + (1 - times.idle / total);
    }, 0) / cpus.length * 100; // Average CPU load in percentage

    // Swap Usage (Only works on Linux/macOS)
    let swapUsage = "N/A";
    try {
        const swapFree = parseInt(execSync("free -m | awk '/Swap/ {print $4}'").toString().trim(), 10);
        const swapTotal = parseInt(execSync("free -m | awk '/Swap/ {print $2}'").toString().trim(), 10);
        swapUsage = swapTotal > 0 ? ((swapTotal - swapFree) / swapTotal * 100).toFixed(2) + "%" : "0%";
      } catch (err) {
        console.warn("Swap usage not available on this OS.");
    }

    return {
      memoryUsage: `${memoryUsage.toFixed(2)}%`,
      freeMemory: `${freeMemory.toFixed(2)} MB`,
      totalMemory: `${totalMemory.toFixed(2)} MB`,
      cpuLoad: `${cpuLoad.toFixed(2)}%`,
      swapUsage
    };
}

// Detect Memory Leaks
function detectMemoryLeak(currentMemoryUsage, currentSwapUsage) {
  if (previousMemoryUsage !== null) {
    const memoryIncrease = parseFloat(currentMemoryUsage) - parseFloat(previousMemoryUsage);
    if (memoryIncrease > 5) { // Alert if memory increases by more than 5% in one interval
      console.warn(`WARNING: Possible Memory Leak! Memory increased by ${memoryIncrease.toFixed(2)}%`);
    }
  }

  if (previousSwapUsage !== null) {
    const swapIncrease = parseFloat(currentSwapUsage) - parseFloat(previousSwapUsage);
    if (swapIncrease > 10) { // Alert if swap usage jumps by 10% or more
      console.warn(` WARNING: High Swap Usage! Swap increased by ${swapIncrease.toFixed(2)}%`);
    }
  }

  // Store values for the next comparison
  previousMemoryUsage = currentMemoryUsage;
  previousSwapUsage = currentSwapUsage;
}

// Enable Mongoose debug mode
mongoose.set('debug', async function (collectionName, method, query, doc, options) {
  const systemUsage = getSystemUsage();
  const mongoStats = await getMongoMemoryUsage();

  detectMemoryLeak(systemUsage.memoryUsage, systemUsage.swapUsage); // Check for memory leaks

  const logMessage = `[${new Date().toISOString()}] Collection: ${collectionName}, Method: ${method}, Doc: ${JSON.stringify(doc)}, Options: ${JSON.stringify(options)}\n`;
  const hwmsg = `System Usage: RAM ${systemUsage.memoryUsage}, Free Memory ${systemUsage.freeMemory}, CPU Load ${systemUsage.cpuLoad} Swap: ${systemUsage.swapUsage}`;
  const mongousage = `[${new Date().toISOString()}]  MongoDB Resident: ${mongoStats.mongoResident}, Virtual: ${mongoStats.mongoVirtual}, WiredTiger Cache: ${mongoStats.wiredTigerCache}, Dirty Cache: ${mongoStats.wiredTigerDirtyCache} ,Read Tx: ${mongoStats.readTransactions}, Write Tx: ${mongoStats.writeTransactions},Connections: ${mongoStats.activeConnections}\n`;

  // Write the log message to the file
  dbLog.debug(logMessage);
  dbLog.debug(hwmsg);
  dbLog.debug(mongousage);
});

//mongoose.set('useUnifiedTopology',true);

// family:4 // use IPV4, skip trying IPV6
// connection Object can be used to create or retrieve models

var conn = mongoose.createConnection(url, {useNewUrlParser:true,family:4,useUnifiedTopology:true});

module.exports = conn;

//module.exports.connect = async dsn => mongoose.connect(url,{useNewUrlParser:true, family:4});
