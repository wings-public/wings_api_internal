#!/usr/bin/env node
'use strict';
const spawn  = require('child_process');
//const runningProcess = require('is-running');
var path = require('path');
const promisify = require('util').promisify;
const { createReadStream, createWriteStream, stat ,unlink,existsSync} = require('fs');
var stats = promisify(stat);
//var multer = require('multer');
const argParser = require('commander');
var loggerMod = require('../controllers/loggerMod');
const configData = require('../config/config.js');
const { app:{instance,logLoc}, db:{importStatsCollection,annotationDirSV} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const annotationDir = annotationDirSV; 
//var db = require('../controllers/db.js');
(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-s, --sample <vcf1>', 'sample_file or sample_url')
            .option('-vcf, --vcf_file_id <id1>', 'vcf file ID')
            .option('-b, --batch <batch>', 'batch size for bulkUpload')
            .option('-p, --pid <pid>', 'batch size for bulkUpload')
            .option('-a, --assemblyType <assemblyType>', 'assembly type or reference build used')
            .option('-ft, --filetype', 'file type - sv_vcf')
        argParser.parse(process.argv);

        var sample = argParser.sample;
        var fileId = argParser.vcf_file_id;
        var batchSize = argParser.batch;
        var pid = argParser.pid;
        var assemblyType = argParser.assemblyType;

        if ( assemblyType == "hg38" ) {
            assemblyType = "GRCh38";
        }
        
        pid = parseInt(pid);

        // Level 1 Processing
        var parsePath = path.parse(__dirname).dir;

        //var logFile = path.join(parsePath,'import','log',`import-controllers-logger-${fileId}-${pid}.log`);
        var logFile = `import-controllers-logger-${fileId}-${pid}.log`;
        var createLog = loggerMod.logger('import',logFile);

        // IMPORT

        var db = require('../controllers/db.js');
        //upate to importSampleSV.js
        var importScript = path.join(parsePath,'import','importSampleSV.js');
                
        createLog.debug("Import path is now "+importScript);
        createLog.debug("Import Sample Started for ID "+fileId);
       
        var status = {'status_description' : 'Import Sample - Started', 'log_location' : logFile,'status_id':2, 'finish_time': new Date() };
        var pushSt = {'status_log': {status:"Import Sample - Started",log_time: new Date()}};
        await updateStatus(db,pid,status,pushSt);

        var subprocess1 = spawn.fork(importScript, ['-s',sample,'--vcf_file_id', fileId, '--assembly', assemblyType, '--pid',pid, '--batch', batchSize] );
        //console.log("subprocess1 PID is "+subprocess1);
        var procPid = subprocess1.pid;
        //res.status(200).json({"id":procPid,"message":"Import Scheduled"});

        // import process completed
        await closeSignalHandler(subprocess1);
        
        var status = {'status_description' : 'Import Sample - Completed','status_id':3, 'finish_time': new Date()};
        var pushSt = {'status_log': {status:"Import Sample - Completed",log_time: new Date()}};
        await updateStatus(db,pid,status,pushSt).then ( () => {});

        createLog.debug("Import Sample Completed");
        console.log("Import Sample Completed");
        //commento per velocita'
        var annoScript = path.join(parsePath,'import','annotationSampleSV.js');
        createLog.debug("SV Annotation Script Path is now "+annoScript);
        var subprocess2 = spawn.fork(annoScript, ['-s',sample,'--vcf_file_id', fileId, '--assembly', assemblyType, '--pid',pid] );
        
        await closeSignalHandler(subprocess2);
        var status = {'status_description' : 'SV Annotation Process Completed','status_id':7,'finish_time': new Date()};
        var pushSt = {'status_log': {status:"SV Annotation Process Completed",log_time: new Date()}};
        updateStatus(db,pid,status,pushSt).then ( () => {});
        createLog.debug("SV Annotation Process Completed");
        //console.log("SV Annotation Process Completed"); 
        //check why
        process.exit(0)
    } catch(err) {
        console.log("*************Looks like we have received an error message");
        console.log(err);
        createLog.debug("Error caught "+err);
        //throw err;
        process.exit(1);
        //next(`${err}`);
    } finally {
        // nodejs request module and trigger https Annotation Request
        // handler specific to parent process
        process.on('beforeExit', (code) => {
            console.log(`--------- About to exit PARENT PROCESS with code: ${code}`);
            console.log("%%%%%%%%%%% BEFORE EXIT ********************** ");
        });

        console.log("************* Final Section of code block ************************ ");
        console.log("Do you pass by here in all scenarios ");
    }
}) ();

async function updateStatus(db,search,update,pushStat) {
    try {
        var id  = {'_id' : search};
        var set = {};
        if ( pushStat ) {
            set = {$set : update, $push : pushStat};
        } else {
            set = {$set : update };
        }
        
        /*console.log("##############################################");
        console.log("Request to update the status for the Import");
        console.log(id);
        console.log(set);
        console.log("##############################################");*/
        var statsColl = db.collection(importStatsCollection);
        var res = await statsColl.updateOne(id,set);
        return "success";
    } catch(err) {
        throw err;
    }
}

async function updateSampleSheetStat(db,id,stat) {
    try {

        console.log("Received request and updating status for sample sheet entry");
        console.log("FILEID is "+ id);
        console.log("STATUS is "+stat);
        var search = { fileID: id};
        var update = { $set : {status: stat}};
        var sampColl = db.collection(sampleSheetCollection);
        var res = await sampColl.updateOne(search,update);
        return "success";
    } catch(err) {
        throw err;
    }
}