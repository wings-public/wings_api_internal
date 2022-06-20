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
const { app:{instance,logLoc}, db:{importStatsCollection} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const getAnnoApiToken = require('../controllers/annotationController.js').getAnnoApiToken;
const triggerAnnotationStreamPipeline = require('../controllers/annotationController.js').triggerAnnotationStreamPipeline;
const getAnnotationStatus = require('../controllers/annotationController.js').getAnnotationStatus;
const downloadData = require('../controllers/annotationController.js').downloadData;
const verifyCS = require('../controllers/annotationController.js').verifyCS;
const updateAnnotations = require('../controllers/annotationController.js').updateAnnotations;
//var db = require('../controllers/db.js');

// node annotationProcessTest.js --sample "/home/nsattanathan/codeBaseRepo/wingsapi_dev_v10/src/logs/import/samples/tmp/novelVariants-8202202160003.vcf.gz" --vcf_file_id 8202202160003 --pid 3200 --assemblyType hg19
//node annotationProcessTest.js --sample "/logger/import/samples/tmp/novelVariants-672022060400112.vcf.gz" --vcf_file_id 672022060400112 --pid 3200 --assemblyType hg38

(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-s, --sample <vcf1>', 'sample_file or sample_url')
            .option('-vcf, --vcf_file_id <id1>', 'vcf file ID')
            .option('-p, --pid <pid>', 'Process id')
            .option('-a, --assemblyType <assemblyType>', 'assembly type or reference build used')
        argParser.parse(process.argv);

        var data = argParser.sample;
        var fileId = argParser.vcf_file_id;
        var pid = argParser.pid;
        var assemblyType = argParser.assemblyType;

        if ( assemblyType == "hg19" ) {
            assemblyType = "GRCh37";
        } else if ( assemblyType == "hg38" ) {
            assemblyType = "GRCh38";
        }
        
        pid = parseInt(pid);

	var db = require('../controllers/db.js');
        // Level 1 Processing
        var parsePath = path.parse(__dirname).dir;
        var logFile = `testAnno-controllers-${fileId}-${pid}.log`;
        console.log(`logFile:${logFile}`)
        var createLog = loggerMod.logger('import',logFile);

        var zipFile = data+'.gz';
        createLog.debug("Proceed to Annotation Swarm Process "); 

        // Connect and get token
        // Execute process
        var token = await getAnnoApiToken();
        console.log(token)
        createLog.debug("Response received from token creation request");
        //createLog.debug(token);
        // Execute Annotation based process.

        //var response = await triggerAnnotationStream(token,fileId,zipFile);
        
        
        createLog.debug("Received token from the API Call. Trigger Process to start the Annotations");
        var response = await triggerAnnotationStreamPipeline(token,data,zipFile,fileId,assemblyType,createLog);
        createLog.debug("What is the response sent by this function");
        createLog.debug(response);
	console.log(response);
        var status = {'status_description' : 'Annotation Process Started','status_id':5,'finish_time': new Date()};
        var pushSt = {'status_log': {status:"Annotation Process Started",log_time: new Date()}};
        await updateStatus(db,pid,status,pushSt);

        createLog.debug("Logging the response received from the triggerAnnotationStream function");
        createLog.debug(response); 

        process.exit(0);
    } catch(err) {
        console.log(err)
        process.exit(1);
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
		            
        var statsColl = db.collection(importStatsCollection);
        var res = await statsColl.updateOne(id,set);
        return "success";
    } catch(err) {
        throw err;
    }
}
