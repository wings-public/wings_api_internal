#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
const argParser = require('commander');
const colors = require('colors');

const { createLogger, format, transports } = require('winston');
const configData = require('../config/config.js');
const { db : {dbName,variantAnnoCollection1,variantAnnoCollection2,importCollection1,importCollection2}, app:{instance} } = configData;
var loggerMod = require('../controllers/loggerMod');

// needed only when the connection is trigerred by the script
//var createConnection = require('../controllers/dbConn.js').createConnection;
//const getConnection = require('../controllers/dbConn.js').getConnection;

/* Replace the database connection once the script is connected to the Router
Connection has to be created by Router
var db = require('../controllers/db.js');
*/

var bson = require("bson");
var BSON = new bson.BSON();
var client;
var annoFields = ['gene_id','fathmm-mkl_coding_pred','fathmm-mkl_coding_score','provean_score','polyphen2_hdiv_pred','metalr_score','impact','polyphen2_hvar_score','metasvm_pred','metasvm_score','cadd_phred','codons','mutationassessor_pred','loftool','consequence_terms','csn','spliceregion'];
// consequence terms & spliceregion will have array values

(async function () {
    argParser
        .version('0.1.0')
        .option('-i, --sid <sid>', 'sample to be annotated')
        .option('-c, --condition <condition>', 'annotate criteria')
        .option('--assembly, --assembly <GRCh37,GRCh38>', 'assembly type to decide the import collection to be used for import')
    argParser.parse(process.argv);


    if ((!argParser.condition) || (!argParser.assembly)) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }
    
    var annoCond = parseInt(argParser.condition);
    var assemblyType = argParser.assembly;
    var sid = argParser.sid;
    ///////////////////// Winston Logger //////////////////////////////

    var logFile = `updateTranscriptAnno-${sid}-${process.pid}.log`;
    //const filename = path.join(logDir, 'results.log');
    //var filename;

    // To be added to a separate library //////
    /*if ( instance == 'dev') {
        // Create the log directory if it does not exist
        const logDir = 'log';
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir);
        }
        filename = path.join(__dirname,logDir,logFile);
    } else {
        // env ( uat, prod ) will be using docker instance. Logs has to be created in the corresponding bind volumes defined in the docker-compose environment file
        // importLogPath will be defined in docker-compose environment variables section 
        filename = path.join(process.env.importLogPath,logFile);
    } */

    var createLog = loggerMod.logger('import',logFile);

    console.log("************** Updates are logged at "+logFile);

    /////////////////////// Winston Logger ///////////////////////////// 
    /*
    try {
        await createConnection();
    } catch(e) {
        console.log("Error is "+e);
    } */


    //client = getConnection();
    //const db = client.db(dbName);
    //console.log("VariantAnnoCollection is "+variantAnnoCollection);
    //var annoCollection = db.collection(variantAnnoCollection);

    //// Validate VCF File, ID and also ID for Multi-Sample VCF Files ///
    var db = require('../controllers/db.js');

    try {
        var val = await updateAnnotations(db,sid,createLog,assemblyType,annoCond);
        if ( val == "success" ) {
            process.exit(0);
        }
    } catch(err) {
        createLog.debug("Error is "+err);
        process.exit(0);
    }
})();

async function updateAnnotations(db,sid,createLog,assemblyType,annoCond) {
    //const getSuccess = new Promise( ( resolve ) => resolve("Success") );

    createLog.debug("db is "+db);
    sid = parseInt(sid);
    var variantAnnoCollection;
    var importCollection;
    if ( assemblyType == "GRCh37" ) {
        importCollection = importCollection1;
        variantAnnoCollection = variantAnnoCollection1;
    } else if ( assemblyType == "GRCh38" ) {
        importCollection = importCollection2;
        variantAnnoCollection = variantAnnoCollection2;
    }
    var annoColl = db.collection(variantAnnoCollection);
    createLog.debug("annoCollection is "+annoColl);
    
    var condition = {"annotated":annoCond};
    //console.log(condition);
    var data = await annoColl.find(condition);

    try {
        //console.log(data);
        var bulkOps = [];
        var count = 0;
        while ( await data.hasNext() ) {
            const doc = await data.next();
            //console.log(doc._id);
            var setFilter = {};
            var filter = {};
            var updateFilter = {};
            if ( doc['maxent'] && doc['reannotation'] ) {
                var variant = doc['_id'];
                createLog.debug(`Variant ${variant} ${count}`);
                ++count;
                // existing gene annotations
                var geneAnnotations = doc['annotation'] || [];
                //createLog.debug("Logging the existing annotations********************")
                //createLog.debug(geneAnnotations,{"depth":null});

                // reannotated gene object
                var reannotation = doc['reannotation'];

                var geneAnnoObj = {'geneAnno' : geneAnnotations };

                // Error to be handled
                /* TypeError: Cannot read property 'forEach' of undefined
    at updateAnnotations (/home/nsattanathan/codeBaseRepo/wingsapi_dev_v10/src/import/updateTranscriptAnno.js:143:38)
    at runMicrotasks (<anonymous>)
    at processTicksAndRejections (internal/process/task_queues.js:97:5)
    at async /home/nsattanathan/codeBaseRepo/wingsapi_dev_v10/src/import/updateTranscriptAnno.js:91:19 */

                // create a map of existing gene annotations
                var geneAnnoMap;
                try {
                    geneAnnoMap = new Map;
                geneAnnoObj.geneAnno.forEach(i => geneAnnoMap.set(i.transcript_id, i));
                } catch(err) {
                    createLog.debug("#################################");
                    createLog.debug("Logging the length of gene annotations");
                    createLog.debug(geneAnnotations.length);
                    createLog.debug("Logging the mongo doc which has issues----");
                    createLog.debug(doc);
                    createLog.debug(geneAnnotations,{"depth":null});
                }

                var coreGeneAnno = [];
                
                for ( var idx in reannotation ) {
                    // reannotated gene object
                    var geneObj = reannotation[idx];
                    //console.log(geneObj)
                    var transcript = geneObj['transcript_id'];
                    //console.log("checking for transcript "+transcript);
                    //console.log("Transcript is "+transcript);
                    // transcripts to be added to core Annotations
                    // check if reannotated transcript has 'maxent' annotation
                    // Not : If a transcript does not have maxent, we do not include that transcript
                    if ( 'maxentscan_ref' in geneObj ) {
                        //console.log("Logging gene object from reannotation");
                        //console.log(geneObj);
                        
                        if ( geneAnnoMap.has(transcript)) {
                            createLog.debug("Transcript present in existing annotation");
                            createLog.debug(transcript);
                            //console.log(geneObj)
                            //console.log("Transcript exists "+transcript);
                            var geneMap = geneAnnoMap.get(transcript);
                            //console.log("Logging geneMap below --------------------")
                            //console.log(geneAnnoMap.get(transcript));
                            geneAnnoMap.delete(transcript);
                            geneMap['maxentscan_ref'] = geneObj['maxentscan_ref'];
                            geneMap['maxentscan_alt'] = geneObj['maxentscan_alt'];
                            geneMap['maxentscan_diff'] = geneObj['maxentscan_diff'];
                            /*geneMap.set('maxentscan_ref',geneObj['maxentscan_ref']);
                            geneMap.set('maxentscan_alt',geneObj['maxentscan_alt']);
                            geneMap.set('maxentscan_diff',geneObj['maxentscan_diff']);*/
                            // convert geneMap to object
                            //const geneUpdObj = Object.fromEntries(geneMap);
                            //coreGeneAnno.push(geneUpdObj);
                            coreGeneAnno.push(geneMap);
                        } else {
                            // this transcript is not present in the existing annotation.
                            createLog.debug("Transcript not present in existing annotation");
                            createLog.debug(transcript);
                            coreGeneAnno.push(geneObj);
                        }
                    } else {
                        //createLog.debug("Transcript missed--------");
                        //createLog.debug(transcript);
                        // 1. Transcript does not have maxent annotation
                        // 2. Transcript is not present in existing annotation
                        if ( ! geneAnnoMap.has(transcript) ) {
                            coreGeneAnno.push(geneObj);
                        }
                    }
                }
                
                //console.log(coreGeneAnno)
                // check geneMap size
                //console.log(geneMap.size);
                // convert map to object

                if ( geneAnnoMap.size > 0 ) {
                    //console.log("check size of gene map-------"+geneAnnoMap.size)
                    //const existGeneObj = Object.fromEntries(geneAnnoMap);
                    //coreGeneAnno.push(existGeneObj);
                    //const existGeneObj = [];
                    for ( const[key,value] of geneAnnoMap ) {
                        //console.log("Logging key and value from map traversal")
                        //console.log(key);
                        //console.log(value);
                        //existGeneObj.push(value);
                        coreGeneAnno.push(value);
                    }
                    
                    //createLog.debug("Logging the entries that will be updated to annotations field");
                    //createLog.debug(coreGeneAnno,{"depth":null});

                    //console.log("Logging existing gene annotations")
                    //console.log(existGeneObj);
                }
                //console.log("Logging the gene annotations that will be updated----");
                //console.log(coreGeneAnno);
                //console.dir(coreGeneAnno,{"depth":null});
                // Array of transcript-gene Annotations
                setFilter['annotation'] = coreGeneAnno;
                // Changing 'annotated' from 2 to 1
                // Reason: Only variant annotations with 1 will be updated to import collection
                setFilter['annotated'] = 1;

                //console.dir(setFilter,{"depth":null});
                //console.dir(setFilter,{"depth":null});
                filter['filter'] = {'_id' : variant};
                filter['update'] = {$set : setFilter};

                updateFilter['updateOne'] = filter;
                
                bulkOps.push(updateFilter);
                if ( (bulkOps.length % 5000 ) === 0 ) {
                    //console.log("Execute the bulk update ");
                    // commenting below section for testing purpose
                    annoColl.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                        createLog.debug("Executed bulkWrite and the results are ");
                        createLog.debug(res.insertedCount, res.modifiedCount, res.deletedCount);
                        }).catch((err1) => {
                            createLog.debug("Error executing the bulk operations");
                            createLog.debug(err1);
                    });
                    bulkOps = [];
                }
            
            } // annotation if 
        } // while
        console.log("Variants having maxent annotation "+count);
    } catch(err) {
        console.log(err);
    }

    // load the remaining annotations present in bulkOps
    if ( bulkOps.length > 0 ) {
        createLog.debug("Update the remaining Annotations");
        try {
            // commented - testing
            var result = await annoColl.bulkWrite(bulkOps, { 'ordered': false });
            return "success";
        } catch(errLog) {
            createLog.debug("Error Log is "+errLog);
            throw errLog;
        }
    } else {
        // This condition is required to handle the case when the size of bulkOps data was loaded in the previous modulus 
        // When there is not enough data to be loaded to mongo db, we have to resolve the promise to ensure that it is resolved at the calling await
        return "success";
    }
}

function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


