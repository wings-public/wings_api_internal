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

//node reannotateSamples.js --anno_type "VEP" --field_anno "RNACentral" --assembly_type "GRCh37"  --update_state 1

(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-m, --anno_type <VEP or CADD>', 'type of annotation')
            .option('v,--vcf_file_id <fileID>','fileID to be reannotated')
            .option('-f, --field_anno <field>', 'annotation field type')
            .option('-u, --update_state <0 or 1>', '1 or 0')
            .option('-a,--assembly_type <GRCh37,GRCh38>','type of ref build')
            .option('-i,--import_id <82382>','import id of sample')
        argParser.parse(process.argv);

        var annoType = argParser.anno_type;
        var annoField = argParser.field_anno;
        var annoVer = argParser.version_anno|| "";
        var assemblyType = argParser.assembly_type;
        var fileId = argParser.vcf_file_id;
        var importId = argParser.import_id;

        //var annoTypeObj = {'VEP' : 1, 'CADD' : 1};

        // contains the allowed annotation types and annotation fields
        var annoTypeObj = {'CADD':1, 'VEP' : 1, 'Def' : 1};
        var annoFieldObj = {'MaxEntScan' : 1 , 'Encode' : 1,'RNACentral':1,'Def':1};

        if ((!argParser.assembly_type) || (!argParser.anno_type) || (!argParser.field_anno) || (!argParser.vcf_file_id) ) {
            argParser.outputHelp(applyFont);
            process.exit(1);
        }

        var updateAnno = argParser.update_state;

        var logFile = `reannotateSamplesReq-${process.pid}-${fileId}.log`;
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
        var status = await execSampleAnno(db,importColl,annoType,annoField,assemblyType,createLog,annoVer,fileId,importId);

        console.log("Logging status below");
        console.log(status);
        createLog.debug("execSampleAnno  finished ");
        // commented process exist. this script should be UP for the queue to be alive?
        process.exit(0);
        
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


async function execSampleAnno(db,importColl,annoType,annoField,assemblyType,createLog,annoVer,fileId,uDateId) {
    try {
        createLog.debug(`fileId:${fileId} reannotation process started`)
        await importQueueScraperAnno(uDateId,fileId,assemblyType,annoType,annoField);
        createLog.debug(`fileId:${fileId} reannotation process completed`)
        createLog.debug("await completed for importQueueScrapper");
        return "success"; 

    } catch(err) {
        console.log("Error in Annotation - execSampleAnno function");
        console.log(err);
    }
}


function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


