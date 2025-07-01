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

const importQueueScraperSV= async (pid,sample,fileId,batchSize,assemblyType,filetype) => {
    try {
        // IMPORT
        var parsePath = path.parse(__dirname).dir;
        var importController = path.join(parsePath,'controllers','importControllerSV.js');
        var subprocess = spawn.fork(importController, ['-s',sample,'--vcf_file_id', fileId, '--batch', batchSize, '--pid', pid, '--assemblyType', assemblyType, '--filetype',filetype] );
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


const importQueueScraperAnno = async (uDateId,fileId,assemblyType,annoType,fieldAnno) => {
    try {
        // IMPORT
        var parsePath = path.parse(__dirname).dir;
        var importController = path.join(parsePath,'controllers','importControllerAnno.js');
        console.log("Logging pid here "+pid);
        var pid = process.pid;
        console.log("importQueueScraperAnno function");
        console.log(`uDateId:${uDateId} fileId:${fileId}`);
        var subprocess = spawn.fork(importController, ['--vcf_file_id', fileId, '--pid', pid, '--import_id',uDateId,'--assemblyType', assemblyType, '--anno_type',annoType,'--field_anno',fieldAnno] );
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

const reannoQueueScraperAnno = async (uDateId,fileId,assemblyType,annoType,fieldAnno,updateState,archiveLog) => {
    try {
        // IMPORT
        var parsePath = path.parse(__dirname).dir;
        archiveLog.debug(`uDateId:${uDateId} fileId:${fileId} assemblyType:${assemblyType} annoType:${annoType} fieldAnno:${fieldAnno} updateState:${updateState}`);
        var reannoController = path.join(parsePath,'import','reannotateSampleQVer.js');
        archiveLog.debug("Logging pid here "+pid);
        var pid = process.pid;
        archiveLog.debug("Step 2 - reannoQueueScraperAnno function sending fork");
        archiveLog.debug(`uDateId:${uDateId} fileId:${fileId}`);
        var subprocess = spawn.fork(reannoController, ['--vcf_file_id', fileId,'--anno_type',annoType,'--field_anno',fieldAnno,'--update_state',updateState,'--assembly_type',assemblyType, '--import_id',uDateId] );
        //var procPid = subprocess.pid;
        archiveLog.debug(`subprocess is ${subprocess}`);
        // import process completed
        await closeSignalHandler(subprocess);
        archiveLog.debug(`Step 3 - ${fileId} ${uDateId} completed`);
        return "completed";
    } catch(err) {
        console.log("Does error gets logged here ");
        console.log(err);
        archiveLog.debug("reannoQueueScraperAnno function - error "+err);
        throw err;
    }
}


const printNumber = async (n) => {
    await new Promise(res => setTimeout(res, 10000)); // wait 4 sec
    console.log(n);
    return n;
}

const trioQueueScrapper = async (pid,trioReq) => {
    try {
        // Trio request
        var parsePath = path.parse(__dirname).dir;
        var trioController = path.join(parsePath,'controllers','trioController.js');
        var jsonReq = JSON.stringify(trioReq);
        //console.log("Logging JSON request in queue scrapper below");
        //console.dir(jsonReq,{"depth":null});
        //console.log("pid is "+pid);
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

const SVtrioQueueScrapper = async (pid,trioReq) => {
    try {
        // Trio request
        var parsePath = path.parse(__dirname).dir;
        var trioController = path.join(parsePath,'controllers','trioControlerSV.js');
        var jsonReq = JSON.stringify(trioReq);
        //console.log("Logging JSON request in queue scrapper below");
        //console.dir(jsonReq,{"depth":null});
        //console.log("pid is "+pid);
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

module.exports = { importQueueScraper , trioQueueScrapper,importQueueScraperAnno,printNumber,reannoQueueScraperAnno,SVtrioQueueScrapper,importQueueScraperSV};