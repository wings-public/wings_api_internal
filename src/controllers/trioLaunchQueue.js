#!/usr/bin/env node
'use strict';
const spawn  = require('child_process');
//const runningProcess = require('is-running');
var path = require('path');
const promisify = require('util').promisify;
const { createReadStream, createWriteStream, stat ,unlink,existsSync} = require('fs');
var stats = promisify(stat);
const argParser = require('commander');
const {logger,loggerEnv} = require('../controllers/loggerMod');
const configData = require('../config/config.js');
const { app:{instance,logLoc}, db:{sampleAssemblyCollection,familyCollection,dbName,importCollection1,importCollection2,trioCollection} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const trioQueueScrapper = require('../routes/queueScrapper').trioQueueScrapper;
const SVtrioQueueScrapper = require('../routes/queueScrapper').SVtrioQueueScrapper;
const getConnection = require('../controllers/dbConn.js').getConnection;
const trioQueue = require('../controllers/entityController.js').trioQueue;
//var db = require('../controllers/db.js');
(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-f, --family_id <family_id>', 'family id')
            .option('-f, --trio_local_id <trio_local_id>', 'trio_local_id')
        argParser.parse(process.argv);

        var familyId = argParser.family_id;

        var pid = process.pid;

        var month = new Date().getMonth();
        var year = new Date().getFullYear();
        var date1 = new Date().getDate()+'-'+month+'-'+year;
        var logFile = `trio-launch-queue-${pid}-${date1}.log`;
        //var logFile = loggerEnv(instance,logFile1);
        var trioLog = logger('trio',logFile);

        trioLog.debug("family_id "+familyId);
        if ( familyId != null ) {
            var db = require('../controllers/db.js');
            const trioColl = db.collection(trioCollection);
            
            familyId = parseInt(familyId);
            var localID = argParser.trio_local_id;

            var doc = await trioColl.findOne({'TrioLocalID': localID,'TrioStatus':{$nin:['disabled','error']}});
            var trioReq = {};
            //console.log(doc);
            var trioArr = doc['trio'];
            var id = doc['TrioLocalID'];
            trioReq['assembly_type'] = doc['AssemblyType'];
            trioReq['localID']  = id;
            var tmpRel = {};
            for ( var idx in trioArr ) {
                var trHash = trioArr[idx];
                var relation = trHash['relation'];
                var fileID = trHash['fileID'];
                //console.log(`relation : ${relation}`);
                //console.log(`fileID: ${fileID}`);
                tmpRel[relation] = fileID;
            }
            trioReq['family'] = tmpRel;
            //console.log("Logging trioReq object below");
            console.dir(trioReq,{"depth":null});
            
            trioQueue.add( async () => { 
                try {
                    //console.log("Logging trioReq object inside queue ************");
                    console.dir(trioReq,{"depth":null});
                    // SV updates added to differentiate the queue based on file type
                    if (!id.includes('SV_VCF')) {
                        await trioColl.updateOne({'TrioLocalID':id,'TrioStatus':{$nin:['disabled','error']}},{$set : {'TrioStatus':'scheduled','TrioErrMsg':''}});
                        await trioQueueScrapper(pid,JSON.parse(JSON.stringify(trioReq)));
                        await trioColl.updateOne({'TrioLocalID':id,'TrioStatus':{$nin:['disabled','error']}},{$set : {'TrioStatus':'completed','TrioErrMsg':''}});
                    } else {
                        //console.log("Logging trioReq object inside queue ************");
                        await trioColl.updateOne({'TrioLocalID':id,'TrioStatus':{$nin:['disabled','error']}},{$set : {'TrioStatus':'scheduled','TrioErrMsg':''}});
                        await SVtrioQueueScrapper(pid,JSON.parse(JSON.stringify(trioReq)));
                        await trioColl.updateOne({'TrioLocalID':id,'TrioStatus':{$nin:['disabled','error']}},{$set : {'TrioStatus':'completed','TrioErrMsg':''}});
                    }
                } catch(err) {
                    console.log(err);
                    await trioColl.updateOne({'TrioLocalID':id,'TrioStatus':{$nin:['disabled','error']}},{$set : {'TrioStatus' : 'error','TrioErrMsg':`${err}`}});
                    trioLog.debug("Adding error for trioQueueScrapper");
                    trioLog.debug("Trio Request Error");
                    trioLog.debug(err);
                }
            });

        } else if ( type == "" ) {

        }

    } catch(err) {
        //throw err;
        console.log(err);
        process.exit(1);
    }
}) ();