const spawn  = require('child_process');
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
var path = require('path');

const importQueueScraper = async (pid,sample,fileId,batchSize,assemblyType) => {
    try {
        // IMPORT
        var parsePath = path.parse(__dirname).dir;
        var importController = path.join(parsePath,'controllers','importController.js');
        var subprocess = spawn.fork(importController, ['-s',sample,'--vcf_file_id', fileId, '--batch', batchSize, '--pid', pid, '--assemblyType', assemblyType] );
        //var procPid = subprocess.pid;
        // import process completed
        await closeSignalHandler(subprocess);
        return "completed";
    } catch(err) {
        console.log("Does error gets logged here ");
        console.log(err);
        throw err;
    }
}

const trioQueueScrapper = async (pid,trioReq) => {
    try {
        // Trio request
        var parsePath = path.parse(__dirname).dir;
        var trioController = path.join(parsePath,'controllers','trioController.js');
        var jsonReq = JSON.stringify(trioReq);
        console.log("Logging JSON request in queue scrapper below");
        console.dir(jsonReq,{"depth":null});
        console.log("pid is "+pid);
        //var subprocess = spawn.fork(trioController, ['-i',trioReq['proband'],'--f',trioReq['father'],'--m',trioReq['mother'], '--pid', pid] );

        var subprocess = spawn.fork(trioController, ['--request_json',jsonReq, '--pid', pid] );
        //var procPid = subprocess.pid;
        // import process completed
        await closeSignalHandler(subprocess);
        return "completed";
    } catch(err) {
        console.log("Does error gets logged here ");
        console.log(err);
        console.dir(err,{"depth":null});
        throw err;
    }
}

module.exports = { importQueueScraper , trioQueueScrapper};