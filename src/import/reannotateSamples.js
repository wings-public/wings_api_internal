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
const { app:{instance,logLoc}, db:{importStatsCollection,importCollection1,importCollection2,variantAnnoCollection1,variantAnnoCollection2} } = configData;
const importQueueScraperAnno = require('../routes/queueScrapper').importQueueScraperAnno;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const colors = require('colors');
//var db = require('../controllers/db.js');
var client;
var loggerMod = require('../controllers/loggerMod');

// Queue Process Modules
const {default: PQueue} = require('p-queue');
// create a new queue, and pass how many you want to scrape at once

// testing purpose : setting to 2 
var qSize = 2;
if ( process.env.REANNO_Q_SIZE ) {
    qSize = parseInt(process.env.REANNO_Q_SIZE);
}

const reannoqueue = new PQueue({ concurrency: qSize });

//node reannotateSamples.js --anno_type "VEP" --field_anno "RNACentral" --assembly_type "GRCh37"  --update_state 1

(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-m, --anno_type <VEP or CADD>', 'type of annotation')
            .option('-f, --field_anno <field>', 'annotation field type')
            .option('-a, --assembly_type <assemblyType>', 'assembly type or reference build used')
            .option('-v, --version_anno <version_anno>','old annotation version')
            .option('-u, --update_state <0 or 1>', '1 or 0')
        argParser.parse(process.argv);

        var assemblyType = argParser.assembly_type;
        var annoType = argParser.anno_type;
        var annoField = argParser.field_anno;
        var annoVer = argParser.version_anno|| "";

        //var annoTypeObj = {'VEP' : 1, 'CADD' : 1};

        // contains the allowed annotation types and annotation fields
        var annoTypeObj = {'CADD':1, 'VEP' : 1, 'Def' : 1};
        var annoFieldObj = {'MaxEntScan' : 1 , 'Encode' : 1,'RNACentral':1,'Def':1};

        if ((!argParser.assembly_type) || (!argParser.anno_type) || (!argParser.field_anno)) {
            argParser.outputHelp(applyFont);
            process.exit(1);
        }

        var updateAnno = argParser.update_state;

        var logFile = `reannotateSamplesReq-${process.pid}.log`;
        var createLog = loggerMod.logger('import',logFile);

        // check annoType
        var annoTArr = annoType.split(',');
        for ( var idx in annoTArr) {
            var aType = annoTArr[idx];
            if ( ! annoTypeObj[aType] ) {
                console.log("Unsupported annotation type");
                process.exit(1);
            }
            if ( aType == "Def" ) {
                annoType = 'Def';
            }
        }


        // check annoField
        var annoFArr = annoField.split('-');
        for ( var idx in annoFArr ) {
            var aField = annoFArr[idx];
            if ( ! annoFieldObj[aField] ) {
                console.log("Unsupported annotation field");
                process.exit(1);
            }
            if ( aField == "Def" ) {
                annoField = 'Def';
            }
        }

        // validate arguments
        if (['hg19','hg38','GRCh37','GRCh38'].indexOf(assemblyType) < 0 ) {
            throw "assemblyType: Supported options are hg19/hg38/GRCh37/GRCh38";
        }

        var db = require('../controllers/db.js');
        //createLog.debug("VariantAnnoCollection is "+variantAnnoCollection);
        var variantAnnoCollection;
        var importCollection;
        if ( assemblyType == "GRCh37") {
            variantAnnoCollection = variantAnnoCollection1;
            importCollection = importCollection1;
        } else if ( assemblyType == "GRCh38" ) {
            variantAnnoCollection = variantAnnoCollection2;
            importCollection = importCollection2;
        } 
        var annoCollection = db.collection(variantAnnoCollection);
        var importColl = db.collection(importCollection);

        createLog.debug(`updateAnno:${updateAnno}`);
        if ( updateAnno == 1 ) {
            createLog.debug(`launch updateAnnoState request`);
            // update the annotated status for the variants in the requested assembly
            var stUpdate = await updateAnnoState(db,annoCollection,createLog);
        }

        //console.log("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        // gets the list of fileIDs and executes annotation request for each fileID.
        var status = await execSampleAnno(db,importColl,annoType,annoField,assemblyType,createLog,annoVer);

        console.log("Logging status below");
        console.log(status);
        // commented process exist. this script should be UP for the queue to be alive?
        //process.exit(0);
        
    } catch(err) {
        console.log(err);
        process.exit(1);
    }
}) ();

// annotated state from 1 to 0 for all variants in the respective assembly collection
async function updateAnnoState(db,annoCollection,createLog) {
    try {
        createLog.debug("updateAnnoState requested")
        console.log("Launching request to update annotation collection");

        var reqUp = await annoCollection.updateMany({'annotated':1},{$set:{'annotated':0}});
        createLog.debug("updateAnnoState updated");
        var cnt = await annoCollection.find({'annotated':0}).count();
        console.log("Logging count below ----- ");
        console.log(cnt);

        return 'updated';
    } catch(err) {
        createLog.debug("Error in updateAnnoState");
        throw err;
    }
}

// 
async function execSampleAnno(db,importColl,annoType,annoField,assemblyType,createLog,annoVer) {
    try {

        var distinctFID = await importColl.distinct('fileID');
        createLog.debug(`List of distinct fileIDs to be reannotated`);
        createLog.debug(distinctFID,{"depth":null});

        var data = [];
        for (const checkFileID of distinctFID) {
            var statsColl = db.collection(importStatsCollection);
            // anno_ver will be "" for the existing samples.
            var query = {"fileID":checkFileID.toString(),"status_description" :"Import Request Completed","anno_ver":{$in:[annoVer,""]}};
            console.log("Logging the query statement below ----- ");
            console.log(query);
            var idExist = await statsColl.findOne(query);
            console.log(idExist);
            if ( idExist != null ) {
                data.push(checkFileID);
            }
        }

        //console.log(data);
        createLog.debug(`List of fileIDs to be reannotated in ${assemblyType}`);
        createLog.debug(data,{"depth":null});

        reannoqueue.on('active', () => {
            console.log(`Working on item #${++qcount}.  Size: ${reannoqueue.size}  Pending: ${reannoqueue.pending}`);
            createLog.debug(`Working on item #${++qcount}.  Size: ${reannoqueue.size}  Pending: ${reannoqueue.pending}`);
        });

        reannoqueue.on('add', () => {
            console.log(`Task is added.  Size: ${reannoqueue.size}  Pending: ${reannoqueue.pending}`);
        });

        reannoqueue.on('next', () => {
            console.log(`Task is completed.  Size: ${reannoqueue.size}  Pending: ${reannoqueue.pending}`);
        });

        for (const fileId of data) {
        //for ( let idx=0; idx < data.length; idx++ ) {
            //var fileId = data[idx];
            createLog.debug("*******************************************");
            createLog.debug("Starting Annotation for fileID "+fileId);
            console.log(`Starting Annotation for fileID ${fileId}`);
            //console.log("Logging fileId");
            //console.log(fileId);
            // execute reannotation request for every fileID

            console.log("------------------------------------");
            

            var qcount = 0;

            // Adding queue  -- Start
            reannoqueue.add( async () => { 
                try {
                    console.log(`Queued Request - Import Task- ${fileId}`);
                    console.log("Calling importQueueScraperAnno");
                    var fIDStr = fileId.toString();
                    console.log("START file ID **********************"+fIDStr);
                    var uDateId = "";
                    uDateId = await getuDateId(db,fIDStr);
                    console.log(`uDateId:${uDateId} fileId:${fileId} fIDStr:${fIDStr}`);
                    console.log("uDateId **********************"+uDateId);
            
                    createLog.debug("Logging uDateId for the above file "+uDateId);

                    await importQueueScraperAnno(uDateId,fileId,assemblyType,annoType,annoField);
                    createLog.debug(`fileId:${fileId} reannotation process completed`)
                    createLog.debug("await completed for importQueueScrapper");

                    // commented status update
                    /*await statsColl.updateOne({'_id':uDateId},
                    {$set : {'status_description' : 'Reannotation process Completed','status_id':8,'finish_time': new Date()},
                    $push: {'status_log': {status:"Reannotation process Completed",log_time: new Date()}}});*/
                    
                    createLog.debug(`Annotation Request Completed - ${fileId}`);
                    createLog.debug(`Done : Annotate Task - ${fileId}`); 
                    console.log(`Done : Annotate Task - ${fileId}`); 
                } catch(err) {
                     var id  = {'_id' : uDateId,'status_id':{$ne:9}};
                     var set = {$set : {'status_description' : 'Error', 'status_id':9 , 'error_info' : "Error occurred during import process",'finish_time': new Date()}, $push : {'status_log': {status:"Error",log_time: new Date()}} };
                     // commented status update
                    // await statsColl.updateOne(id,set); 
                     // update status for sample sheet entry
                     //await sampShColl.updateOne({"fileID":fileId }, {$set:{status: "import failed"}});
                     createLog.debug("Adding error for importQueueScrapper");
                     createLog.debug("Import Request Error");
                     createLog.debug(err);
                     console.log(err);
                }
            } );

            // End queue

            //var msg = await executeAnnoReq(db,fileId,annoType,annoField,assemblyType,createLog);
            //createLog.debug("Logging the message returned - "+msg);

            /*if ( msg == "error" ) {
                //console.log("Error in reannotation process for fileID "+fileId);
                createLog.debug("Error in reannotation process for fileID "+fileId);
            } else {
                //console.log("Done - reannotation process for fileID "+fileId);
                createLog.debug("Done - reannotation process for fileID "+fileId);
            }*/
            createLog.debug("--------------------------------------------------------");
        }

        return "success"; 

    } catch(err) {
        console.log("Error in Annotation - execSampleAnno function");
        console.log(err);
    }
}

async function getuDateId (db,fID) {
    try {
        console.log("******************************");
        console.log("getuDateId -------------------");
        console.log(`Arguments ${fID}`);
        var statsColl = db.collection(importStatsCollection);
        //console.log(fIDStr);
        var query = {'fileID':fID,"status_description" : "Import Request Completed"};
        console.log("Logging the query statement ");
        console.log(query);
        var uDateId = await statsColl.findOne(query,{'projection':{_id:1}});
        console.log(`**************** ${uDateId}`);
        if ( uDateId != null ) {
            //createLog.debug(`uDateId:${uDateId}`);
            console.log(`uDateId:${uDateId}`);
            uDateId = uDateId._id;
        }
        return uDateId;
    } catch(err) {
        throw err;
    }
}

function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


