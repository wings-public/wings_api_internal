var path = require('path');
const spawn = require('child_process');
const schedule = require('node-schedule');
const {logger,loggerEnv} = require('../controllers/loggerMod');
const configData = require('../config/config.js');
const { app:{instance} } = configData;

schedule.scheduleJob('0 */6 * * *', function() {
    var pid = process.pid;
    var logFile = `scheduler-sample-sheet-${pid}.log`;
    //var logFile = loggerEnv(instance,logFile1);
    console.log(`logFile is ${logFile}`);
    var createLog = logger('scheduler',logFile);
    createLog.debug("Scheduler-Trigger parseSampleSheet");
    var parsePath = path.parse(__dirname).dir;
    var parseSheet = path.join(parsePath,'parser','parseSampleSheet.js');
    
    //var subprocess = spawn.fork(parseSheet);
    console.log("Callin subprocess ");
    var subprocess = spawn.fork(parseSheet);
    console.log(subprocess);
    //spawn('node', [parseSheet]);
});
