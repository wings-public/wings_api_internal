#!/usr/bin/env node
'use strict';
/**
 * Module dependencies.
 */

const configData = require('../config/config.js');
const { db : {host,port,dbName,importCollection1,importCollection2,resultCollection,genePanelColl,variantQueryCounts,indSampCollection,trioCollection},app:{instance,liftMntSrc} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const spawn  = require('child_process');
const readline = require('readline');
var mapFilter = require('../config/queryFields.js');
//console.dir(mapFilter,{"depth":null});
const argParser = require('commander');
const colors = require('colors');
const fs = require('fs');
const Async = require('async');
const MongoClient = require('mongodb').MongoClient;

const assert = require('assert');
const path = require('path');
var loggerMod = require('../controllers/loggerMod');

var client;
var mongoHost = host;

//const createConnection = require('../controllers/dbFuncs.js').createConnection;
const createConnection = require('../controllers/dbConn.js').createConnection;
const getConnection = require('../controllers/dbConn.js').getConnection;
const createCollection = require('../controllers/dbFuncs.js').createCollection;
const checkCollectionExists = require('../controllers/dbFuncs.js').checkCollectionExists;
const createTTLIndex = require('../controllers/dbFuncs.js').createTTLIndex;
const getInheritanceData = require('../controllers/entityController.js').getInheritanceData;

( async function() {
    argParser
    .version('0.1.0')
    .option('-j, --json <json>', 'json file which has the specific input data for script')
    .option('-p, --post_json <post_json>', 'json data sent directly')
    
    argParser.parse(process.argv);

    //
    //var defaultProj = {'var_key':1,'sid':1,'start_pos':1,'stop_pos':1,'ref_all':1,'alt_all':1,'chr':1};
    var defaultProj = {'var_validate':0,'var_key':0,'pid':0};
    //var defaultProj = {'gene_annotations.consequence_terms' : 1,'gene_annotations.transcript_id':1};
    //var defaultProj = {'chr':1};
    
    // Start log initialize
    var createLog; var logFile;
    // end log initialise

    if ( ! argParser.json  && ! argParser.post_json ) {
        argParser.outputHelp();
    }

    if ( argParser.json || argParser.post_json ) {
        var db; 
        //var client; 
        var collection;
        var resultCol; var jsonFile; var rawData; var parsedJson;var queryCollection;
        try {
            //var client = getConnection();
            client = await createConnection();
            db = client.db(dbName);

            /*var ttlVal1 = 3000;
            if ( process.env.MONGO_RESULT_TTL ) {
                ttlVal1 = process.env.MONGO_RESULT_TTL;
            }*/
            resultCol = await initializeResultColl(client,db,3600);
            //resultCol = await initializeResultColl(client,db,ttlVal1);
            //console.dir(argParser.post_json);
            //console.dir(argParser.post_json,{"depth":null});
            if ( argParser.post_json ) {
                var parsedJsonTmp = argParser.post_json;
                parsedJson = JSON.parse(parsedJsonTmp);
            } else {
                jsonFile = argParser.json;
                rawData = fs.readFileSync(jsonFile);
                parsedJson = JSON.parse(rawData);
            }

            var assemblyCheck = "";
            if ( ! parsedJson['trio'] ) {
                assemblyCheck = parsedJson['assembly_type'];
            } else {
                assemblyCheck = await fetchTrioBuildType(db,parsedJson['trio']['trioLocalID']);
            }

            var assemblyInfo = {};
            if ( (assemblyCheck == 'GRCh37') || (assemblyCheck == 'hg19') ) {
                queryCollection = importCollection1;
                assemblyInfo['reqAssembly'] = 'hg19';
                assemblyInfo['reqColl'] = importCollection1;
            } else if ( (assemblyCheck == 'GRCh38')  || (assemblyCheck == 'hg38') ) {
                queryCollection = importCollection2;
                assemblyInfo['reqAssembly'] = 'hg38';
                assemblyInfo['reqColl'] = importCollection2;
            }

            if ( assemblyInfo['reqAssembly'] == "hg19" ) {
                assemblyInfo['altAssembly'] = "hg38";
                assemblyInfo['altColl'] = importCollection2;
            } else if ( assemblyInfo['reqAssembly']  == "hg38" ) {
                assemblyInfo['altAssembly'] = "hg19";
                assemblyInfo['altColl'] = importCollection1;
            }
            
            collection = db.collection(queryCollection); 
            
        } catch(error) {
            console.log("Error "+error);
            process.exit(0);
        }
        // logical filter based processing
        if ( parsedJson['invoke_type'] == "sample_variant" ) {
            logFile = 'querySample-'+process.pid+'.log';
            createLog = loggerMod.logger('query',logFile);
            createLog.debug("Received request for sample_variant");
            createLog.debug(parsedJson);
            if ( ! parsedJson['condition'] ) {
                argParser.outputHelp();
                process.exit(0);
            } 
            try {
                var projection = defaultProj;
                var batch = parsedJson['batch_range'] || 100;
                if ( parsedJson['trio']) {
                    parsedJson['output_columns'] = {'_id': 0,'fileID':0};
                } else if ( parsedJson['var_disc']) {
                    // including additional fields
                    parsedJson['output_columns'] = {"_id" : 0, "var_validate" : 0,"pid" : 0,"multi_allelic" : 0, "phred_polymorphism" : 0, "filter" : 0, "alt_cnt" : 0, "ref_depth" : 0, "alt_depth" : 0, "phred_genotype" : 0, "mapping_quality" : 0, "base_q_ranksum" : 0, "mapping_q_ranksum" : 0, "read_pos_ranksum" : 0, "strand_bias" : 0, "quality_by_depth" : 0, "fisher_strand" : 0, "vqslod" : 0, "gt_ratio" : 0, "ploidy" : 0, "somatic_state" : 0, "delta_pl" : 0, "stretch_lt_a" : 0, "stretch_lt_b" : 0, "trio_code" : 0};
                    // mask sensitive fields using mongo projections
                    //parsedJson['output_columns'] = {"_id" : 0, "var_validate" : 0,"pid" : 0, "v_type" : 0, "non_variant" : 0, "vcf_chr" : 0, "chr" : 0, "start_pos" : 0, "stop_pos" : 0, "ref_all" : 0, "alt_all" : 0, "multi_allelic" : 0, "phred_polymorphism" : 0, "filter" : 0, "alt_cnt" : 0, "ref_depth" : 0, "alt_depth" : 0, "phred_genotype" : 0, "mapping_quality" : 0, "base_q_ranksum" : 0, "mapping_q_ranksum" : 0, "read_pos_ranksum" : 0, "strand_bias" : 0, "quality_by_depth" : 0, "fisher_strand" : 0, "vqslod" : 0, "gt_ratio" : 0, "ploidy" : 0, "somatic_state" : 0, "delta_pl" : 0, "stretch_lt_a" : 0, "stretch_lt_b" : 0, "trio_code" : 0};
                }
                if ( parsedJson['output_columns']) {
                    projection = parsedJson['output_columns'];
                }
                var retFilter = await logicalFilterProcess(db,parsedJson,mapFilter,createLog);
                //console.log("Logging the initial filter ##########");
                //console.dir(retFilter,{"depth":null});
                //console.log("Printing the returned filter of the original request");
                //console.dir(retFilter,{"depth":null});
                //createLog.debug("Logging the mongodb filter created by logicalFilterProcess");
                //createLog.debug(retFilter);
                createLog.debug("Query to be launched for filters-------------------");
                createLog.debug(retFilter, {"depth":null});
                createLog.debug("-------------------------------------------------------");
                
                var res = "";
                if ( parsedJson['var_disc']) {
                    if ( parsedJson['var_disc']['geneID']) {
                        var coll1 = db.collection(assemblyInfo['reqColl']); 
                        var batchCnt = 1;
                        //console.log("First request launched with request collection");
                        var pid = process.pid;
                        pid = pid + (parsedJson['centerId'] * parsedJson['hostId']);
                        // Step 1 : fetch the variants for the requested assembly
                        // Write variants to results collection for requested assembly
                        var totalBatch = await processLogicalFilter(pid,retFilter,projection,batch,resultCol,db,coll1,createLog,parsedJson, assemblyInfo['reqAssembly'],batchCnt);

                        createLog.debug("Total Batch received from first request is "+totalBatch);
                        createLog.debug("Next request launched with alternate collection ");
                        var coll2 = db.collection(assemblyInfo['altColl']); 
                        var nextBatch = totalBatch + 1;

                        // Step3 : Pass the fetched filter object to retFilter
                        var varFileLoc = await writeVariantsFile(pid,retFilter,projection,coll2,resultCol,batch);

                        // call liftoverVariantFile Start liftover
                        // perform liftover
                        var parsePath = path.parse(__dirname).dir;
                        var liftoverScript = path.join(parsePath,'modules','liftoverVariantFile.js');
                        createLog.debug(`Launching request with the input ${varFileLoc}`);
                        createLog.debug("Assembly "+assemblyInfo['altAssembly']);
                        //console.log("Request id is "+pid);
                        // alternate assembly variants will be lifted over to the request assembly
                        var subprocess1 = spawn.fork(liftoverScript, ['--input',varFileLoc,'--assembly', assemblyInfo['altAssembly'], '--req_id', pid] );
                        
                        var variantFile = path.join(liftMntSrc,assemblyInfo['reqAssembly'],pid+'.vcf');
                        //console.log("Joined path file is "+variantFile);
                        var liftedVariant = "";
                        try {
                            await closeSignalHandler(subprocess1);
                            //console.log("Try Block");
                            createLog.debug("Liftover process completed now ");
                            // read the lifted variants and update the result collection using the unique id
                            await loadLiftedVariants(variantFile,resultCol,pid);

                            await invokeLiftoverResults(nextBatch,pid,resultCol,assemblyInfo['altAssembly'],createLog,parsedJson,batch);
                            //console.log(liftedVariant);
                        } catch(err) {
                            createLog.debug("Logging error from varDiscController-liftover error");
                            createLog.debug(err);
                            //console.log("Error in performing liftover ");
                        } 
                        // liftover completed

                        //var res1 = await processLogicalFilter(pid,retFilter,projection,batch,resultCol,db,coll2,createLog,parsedJson, assemblyInfo,nextBatch,"last");
                    } else if (parsedJson['var_disc']['region']) {
                        var coll1 = db.collection(assemblyInfo['reqColl']); 
                        var batchCnt = 1;
                    
                        var pid = process.pid;
                        pid = pid + (parsedJson['centerId'] * parsedJson['hostId']);

                        // Step 1 : fetch the variants for the requested assembly
                        // =======================================================
                        // Write variants to results collection for requested assembly
                        var totalBatch = await processLogicalFilter(pid,retFilter,projection,batch,resultCol,db,coll1,createLog,parsedJson, assemblyInfo['reqAssembly'],batchCnt);

                        createLog.debug("Total Batch received from first request is "+totalBatch);
                        createLog.debug("Next request launched with alternate collection ");
                        var coll2 = db.collection(assemblyInfo['altColl']); 
                        var nextBatch = totalBatch + 1;

                        // Step 2 : Convert the region coordinates to different assembly
                        // UCSC Liftover
                        // =============
                        // call liftoverVariantFile Start liftover
                        // perform liftover
                        var parsePath = path.parse(__dirname).dir;
                        var liftoverScript = path.join(parsePath,'modules','ucscLiftover.js');
                        createLog.debug(`Launching request with the input ${varFileLoc}`);
                        createLog.debug("Assembly "+assemblyInfo['altAssembly']);
                        //console.log("Request id is "+pid);
                        // alternate assembly variants will be lifted over to the request assembly
                        var region = parsedJson['var_disc']['region'];

                        //console.log(`Liftoverscript is ${liftoverScript}`);
                        //console.log(`Region ${region}`);
                        //console.log("Assembly Info "+assemblyInfo['reqAssembly']);
                        //console.log(`pid ${pid}`);
                        var subprocess1 = spawn.fork(liftoverScript, ['--variant',region,'--assembly', assemblyInfo['reqAssembly'], '--req_id', pid] );
                        
                        var variantFile = path.join(liftMntSrc,pid+'-variant.bed');

                        try {
                            // Check if liftover process has completed
                            await closeSignalHandler(subprocess1);

                            var liftedVariant = await readFile(variantFile,createLog);
                            //console.log("Lifted variant is "+liftedVariant);

                            //console.log("Liftover process completed try block");
                            //console.log("Try Block");
                            createLog.debug("Liftover process completed now ");

                            // Step3 : Update lifted region to the parsed json
                            // ================================================
                            // clone the parsed Json to another object. Update the lifted region
                            var clonedParsJson = { ... parsedJson};
                            //console.log("Logging the initial parsed json before update #####");
                            //console.dir(clonedParsJson,{"depth":null});

                            clonedParsJson['var_disc']['region'] = liftedVariant;

                            //console.log("Lifted variant is "+liftedVariant);
                            //console.log("Logging the initial parsed json after update #####");
                            //console.dir(clonedParsJson,{"depth":null});

                            // Step4 : Fetch the filter object 
                            // ====================================
                            var retFilter = await logicalFilterProcess(db,parsedJson,mapFilter,createLog);
                            //console.log("Logging the updated filter");
                            //console.dir(retFilter,{depth:null});

                            var coll2 = db.collection(assemblyInfo['altColl']); 

    
                            // Step 5 : Execute the filter and write the data to file
                            // =======================================================
                            // Write the variants to a file
                            var varFileLoc = await writeVariantsFile(pid,retFilter,projection,coll2,resultCol,batch);
                            //console.log("Step 5 - Data has been written to the file "+varFileLoc);

                            // Step 6 : Execute lift over for the above fetched variants
                            // ========================================================
                            var parsePath = path.parse(__dirname).dir;
                            var liftoverScript = path.join(parsePath,'modules','liftoverVariantFile.js');
                            createLog.debug(`Launching request with the input ${varFileLoc}`);
                            createLog.debug("Assembly "+assemblyInfo['altAssembly']);
                            //console.log("Request id is "+pid);

                            //console.log("Step 6 : Execute liftover");
                            // alternate assembly variants will be lifted over to the request assembly
                            var subprocess4 = spawn.fork(liftoverScript, ['--input',varFileLoc,'--assembly', assemblyInfo['altAssembly'], '--req_id', pid] );
                            
                            var variantFile = path.join(liftMntSrc,assemblyInfo['reqAssembly'],pid+'.vcf');
                            
                            try {
                                await closeSignalHandler(subprocess4);
                                //console.log("Lift over process has completed");
                                //console.log("Lifted Variant data file is "+variantFile);
                                //console.log("Try Block");
                                createLog.debug("Liftover process completed now ");
                                // read the lifted variants and update the result collection using the unique id
                                
                                await loadLiftedVariants(variantFile,resultCol,pid);
                                //console.log("LIfted variants has been loaded");

                                await invokeLiftoverResults(nextBatch,pid,resultCol,assemblyInfo['altAssembly'],createLog,parsedJson,batch);
                                //console.log("Lifted over variant data has been loaded to database.Results can be retrieved now");
                                //console.log(liftedVariant);
                            } catch(err) {
                                createLog.debug("Logging error from varDiscController-liftover error");
                                createLog.debug(err);
                            }
                            
                        } catch(err) {
                            console.log(err);
                            console.log("Catch block of liftover");
                        }

                    }   
                } else {
                    var batchCnt = 1;
                    var pid = process.pid;
                    res = await processLogicalFilter(pid,retFilter,projection,batch,resultCol,db,collection,createLog,parsedJson,assemblyInfo['reqAssembly'],batchCnt);                    
                }
                
                if ( res == "success" ) {
                    //client.close();
                }
            } catch(err) {
                process.send({"err":`${err}`});
                //process.exit(1);
            }
        } else if ( parsedJson['invoke_type'] == "chromosome_regions" ) {
            if ( ! parsedJson['filters'] ) { 
                argParser.outputHelp();
                process.exit(0);
            } 
            try {
                var projection = defaultProj;
                var batch = parsedJson['batch_range'] || 100;
                if ( parsedJson['output_columns'] ) {
                    projection = parsedJson['output_columns'];
                }
                var posFilter = await positionBasedFilter(parsedJson);
                console.dir(posFilter,{"depth":null});
                await processPosFilter(posFilter,projection,batch,resultCol,db,collection);
                client.close();
            } catch(err2) {
                console.log("Error in position based filter "+err2);
            }
        // to be added later to handle relation filter
        } else if ( parsedJson['invoke_type'] == "sample_family" ) {
            try {
                //console.log("sample_family structure defined");
                //console.log(client);
                if ( ! parsedJson['family_filter'] ) {
                    throw "family_filter is not defined in the JSON Structure";
                }
                if ( ! parsedJson['family_filter']['proband'] || ! parsedJson['family_filter']['family_id'] || ! parsedJson['family_filter']['inheritance'] || ! parsedJson['family_filter']['relatives'] ) {
                    //console.log(parsedJson);
                    throw "JSON Structure Error";
                }
                // obj has details on family relation, affected status.
                if ( parsedJson['family_filter']['inheritance'] == "denovo") {

                } else {
                    var obj = await getInheritanceData(parsedJson['family_filter']);
                    console.log(obj);
                    process.send({"debug":obj});
                }
            } catch(err) {
                process.send({"err":err});
                client.close();
            }
        } else if ( parsedJson['invoke_type'] == "get_count" ) {
            try {
                var reqID = process.pid;
                logFile = `getCount-${reqID}.log`;
                createLog = loggerMod.logger('query',logFile);
                //console.log("******** Log File name is "+logFile);
                createLog.debug("Received request for invoke_type get_count");
                createLog.debug(parsedJson);
                var result = await getCount(db,collection,parsedJson,mapFilter,reqID,createLog,assemblyInfo);
                //createLog.debug(result);
                //console.log(result);
                //process.send(result);
                client.close();
            } catch(err) {
                process.send({"err":`${err}`});
                client.close();
            }
        } else if ( parsedJson['invoke_type'] == "saved_filter_count" ) {
            try {
                var reqID = process.pid;
                logFile = `savedFilterCount-${reqID}.log`;
                createLog = loggerMod.logger('query',logFile);
                //console.log("******** Log File name is "+logFile);
                createLog.debug("Received request for invoke_type get_count");
                createLog.debug(parsedJson);
                var result = await getSavedFilterCnt(db,collection,parsedJson,mapFilter,reqID,createLog,assemblyInfo);
                //console.log(result);
                //process.send(result);
                client.close();
            } catch(err) {
                process.send({"err":`${err}`});
                client.close();
            }
        } else {
            createLog.debug("Looks like an issue with the invoke_type and sending the help message");
            argParser.outputHelp();
            process.exit(0);
        }
        //process.exit(0);
    }
} ) ();

async function positionBasedFilter(parsedJson) {
    try {
        var filterJson = parsedJson['filters'];
        var rootFilter = {};
        var orArr = [];
        //var failArr = [];
        for ( var fIdx in filterJson ) {
            var filterRule = {};
            var filterHash = filterJson[fIdx];
            var condHash = filterHash['c'];
            if ( condHash['add_rule_pos'] ) {
                var type = condHash['type'];
                var altRule = condHash['add_rule_pos'];
                var start_pos; var stop_pos;

                if ( altRule['start_pos']) {
                    start_pos = altRule['start_pos'];
                }
                if ( altRule['stop_pos']) {
                    stop_pos = altRule['stop_pos'];
                }
                filterRule['$and']  = [ {"chr" : condHash['rule']}, {'start_pos' : {$lte : start_pos}}, {'stop_pos' : {$gte : stop_pos}} ];
                //filterRule[dbField] = condHash['rule'];
                //var posFilter = { $and :  [ {'start_pos' : {$lte:position} }, {'stop_pos': {$gte : position} } ],'chr':positionChr};
                if ( type == "or" ) {
                    orArr.push(filterRule);
                } 
            } 
        }
        if ( orArr.length > 0 ) {
            rootFilter['$or'] = orArr;
        }
        return rootFilter;

    } catch(err) {
        throw err;
    }
}


async function processLogicalFilter(pid,filter,projection,batch,resultCol,db,collection,createLog,parsedJson,assemblyInfo,batchCnt,type = "done") {
    try {
        var defaultSort = {'chr':1,'start_pos':1};
        var resType = "requested";

        var logStream = await collection.find(filter).sort(defaultSort);
        logStream.project(projection); 
        var docs = 0;

        var timeField = new Date();
        //var batchCnt = 1;
        //var batchId = `batch${batchCnt}`
	    // Changing batchCnt format
        //console.log("Batch id received as input is "+batchCnt);
        var docId = 1;
        var loadHash = {};
        var bulkOps = [];

        createLog.debug(`Batch is ${batch}`);
        var hostInfo = {};
        if ( parsedJson['var_disc'] ) {
            hostInfo['centerId'] = parsedJson['centerId'];
            hostInfo['hostId'] = parsedJson['hostId'];
        }

        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            ++docs;
            bulkOps.push(doc);
            if ( bulkOps.length === batch ) {
                createLog.debug(`loadResultCollection ${batchCnt}`);
                console.log(`Data has been written to results collection with ${batchCnt} and ${pid}`);
                loadResultCollection(createLog,resultCol,type,pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
                //console.log("Batch count is "+batchCnt);
                batchCnt++;
                bulkOps = [];
                loadHash = {};
            } 
        }

        // check & process the last batch of data
        if ( bulkOps.length > 0 ) {
            createLog.debug(`invoke result collection for batch ${batchCnt} last`);
            //loadResultCollection(createLog,resultCol,"last",pid,batchId,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo);
            loadResultCollection(createLog,resultCol,"last",pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
        }
        if ( ! docs ) {
            if ( parsedJson['var_disc'] ) {
                loadResultCollection(createLog,resultCol,"last",pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
            } else {
                process.send({"count":"no_variants"});
            }
        }

        console.log("Batch count before returning result is "+batchCnt);
        //return "success";
        return batchCnt;
    } catch(err) {
        throw err;
    }
}

async function writeVariantsFile(pid,filter,projection,collection,resultCol,batch) {
    try {
        var defaultSort = {'chr':1,'start_pos':1};

        var basePath = path.parse(__dirname).dir;

        var vFile = 'inputVar-'+pid+'.csv';
        
        var varFile = path.join(basePath,'query',vFile); 
        //console.log("Variants file can be accessed at ------ "+varFile);
        // execute filter query on alternate collection 
        var logStream = await collection.find(filter).sort(defaultSort);
        logStream.project(projection); 
        var docs = 0;

        var timeField = new Date();
        var bulkOps = [];

        // write the variants to a file for liftover and also data with annotations.
        var wFd = fs.createWriteStream(varFile);
        var template ="##fileformat=VCFv4.1\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tb-11372";
        wFd.write(template+'\n');

        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            //console.log(doc);
            // clone the retrieved doc and add some fields
            var clonedDoc = { ... doc};
            var var_key = doc['var_key'];
            ++docs;
            clonedDoc['uid'] = docs;
            clonedDoc['pid'] = pid;
            clonedDoc['createdAt'] = timeField;
            bulkOps.push({"insertOne" : clonedDoc});
            // split variant
            var data = var_key.split('-');
            var chrReg = /^chr/i;
            var chrom = data[0];
            if ( ! chrom.match(chrReg)) {
                chrom = 'chr'+data[0];
            }
            var startPos = data[1];
            var ref = data[2];
            var alt = data[3];

            // docs is the count of doc. This is used to assign unique identifier for each row
            var line = `${chrom}\t${startPos}\t${docs}\t${ref}\t${alt}\t.\t.\t.\t.\t.\n`;
            //console.log("Logging the variant line below:");
            //console.log(line);
            wFd.write(line);
            
            if ( bulkOps.length === batch ) {
                //createLog.debug(`loadResultCollection ${batchId}`);
                const res1 = await resultCol.bulkWrite(bulkOps);
                //batchId = batchCnt;
                bulkOps = [];
            } 
        }

        // check & process the last batch of data
        if ( bulkOps.length > 0 ) {
            //createLog.debug(`invoke result collection for batch ${batchId} last`);
            const res1 = await resultCol.bulkWrite(bulkOps);
        }

        wFd.on('error',() => {
            console.log("Error emitted while writing data "+varFile);
        });

        wFd.on('finish',() => {
            console.log("Data written to file "+varFile);
        });
        wFd.end();
        console.log("Data written to file "+varFile);

        /*if ( ! docs ) {
            process.send({"count":"no_variants"});
        }*/

        return varFile;
    } catch(err) {
        throw err;
    }
}

async function loadLiftedVariants(variantFile,resultCol,pid) {
    var rd;
    try {
        //console.log("Reading file ----------------"+variantFile);
        if ( ! fs.existsSync(variantFile)) {
            return "success";
        } else {
            //createLog.debug(`Parsing filename ${sampleSh}`);
            var rd = readline.createInterface({
                input: fs.createReadStream(variantFile),
                console: false
            });
            
            var lineRe = /^#/g;
            var blankLine = /^\s+$/g;
            var lineArr = [];
            //varContLog.debug(`variantFile - ${variantFile}`);
            //console.log("Line Count is *************"+lineCnt);
            var bulkOps = [];
            //console.log("Reading file ----------------"+variantFile);
            rd.on('line', (line) => {
                if ( ! line.match(lineRe) && ! line.match(blankLine)) {
                    //console.log("Retrieved lifted over line is :");
                    //console.log(line);
                    var data = line.split('\t');
                    var liftedVarKey = data[0]+'-'+data[1]+'-'+data[3]+'-'+data[4];

                    var re = /chr/g;
                    if ( liftedVarKey.match(re)) {
                        liftedVarKey = liftedVarKey.replace(re, '');
                    }

                    var uniqId = data[2];
                    var filter = {}; var updateFilter = {};

                    filter['filter'] = {'uid':parseInt(uniqId),'pid':pid};
                    filter['update'] = {$set : {'lifted_var':liftedVarKey}};
                    updateFilter['updateOne'] = filter;
                    //console.log("Logging the update filter below :");
                    //console.dir(updateFilter,{"depth":null});
                    bulkOps.push(updateFilter);

                    if ( (bulkOps.length % 5000 ) === 0 ) {
                        //console.log("Execute the bulk update ");
                        resultCol.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                            //createLog.debug("Executed bulkWrite and the results are ");
                            //createLog.debug(res.insertedCount, res.modifiedCount, res.deletedCount);
                            }).catch((err1) => {
                                //createLog.debug("Error executing the bulk operations");
                                //createLog.debug(err1);
                        });
                        bulkOps = [];
                    }
                } 
            });

            return new Promise( resolve => {
                rd.on('close',  async () => {
                    //console.log("Reading Data Completed Start Traversing Array");
                    if ( bulkOps.length > 0 ) {
                        try {
                            var result = await resultCol.bulkWrite(bulkOps, { 'ordered': false });
                            resolve("success");
                        } catch(errLog) {
                            //createLog.debug("Error Log is "+errLog);
                            //throw errLog;
                        }
                        
                    }
                    resolve("success");
                }); 
            }, reject => {
                rd.on('error', async() => {
                    console.log("Error in reading file");
                    reject(err);
                })

            });
        } 
    } catch(err) {
        //varContLog.debug(err);
        throw err;
    }
}

async function invokeLiftoverResults(batchCnt,pid,resultCol,assemblyInfo,createLog,parsedJson,batch,type = "done") {
    try {

        var filter = { 'pid': pid, 'lifted_var': { $exists: true } };
        var logStream = await resultCol.find(filter);
        logStream.project({_id:0,'var_key':0,'uid':0}); 
        var docs = 0;
        var resType = "lifted";

        var timeField = new Date();
        
        //console.log("Batch id received as input is "+batchCnt);
        var loadHash = {};
        var bulkOps = [];

        createLog.debug(`Batch is ${batchCnt}`);
        var hostInfo = {};
        if ( parsedJson['var_disc'] ) {
            hostInfo['centerId'] = parsedJson['centerId'];
            hostInfo['hostId'] = parsedJson['hostId'];
        }

        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            var clonedDoc = { ... doc};
            ++docs;
            var storedKey = clonedDoc['lifted_var'];
            delete clonedDoc['lifted_var'];
            clonedDoc['var_key'] = storedKey;
            bulkOps.push(clonedDoc);
            if ( bulkOps.length === batch ) {
                createLog.debug(`loadResultCollection ${batchCnt}`);
                loadResultCollection(createLog,resultCol,type,pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
                //console.log("Batch count is "+batchCnt);
                batchCnt++;
                bulkOps = [];
                loadHash = {};
            } 
        }

        // check & process the last batch of data
        if ( bulkOps.length > 0 ) {
            createLog.debug(`invoke result collection for batch ${batchCnt} last`);
            //loadResultCollection(createLog,resultCol,"last",pid,batchId,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo);
            loadResultCollection(createLog,resultCol,"last",pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
        }

        if ( ! docs ) {
            if ( parsedJson['var_disc'] ) {
                loadResultCollection(createLog,resultCol,"last",pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
            }
            //process.send({"count":"no_variants"});
        }

        //console.log("Batch count before returning result is "+batchCnt);
        //return "success";
        return batchCnt;
    } catch(err) {
        throw err;
    }
}

function loadResultCollection(createLog,resultCol,reqVal,pid,batch,timeField,loadHash,hostInfo,parsedJson,assemblyInfo,resType) {
    var loadToDB = {};
    var idVal = pid+'_'+batch;

    var lastBatch = 0;
    if ( reqVal === "last" ) {
        lastBatch = 1;
    }

    loadToDB['_id'] = idVal;
    loadToDB['batch'] = batch;
    loadToDB['pid'] = pid;
    loadToDB['lastBatch'] = lastBatch;
    loadToDB['createdAt'] = timeField;

    if ( Object.keys(hostInfo).length !== 0 ) {
        loadToDB['hostId'] = hostInfo['hostId'];
        loadToDB['centerId'] = hostInfo['centerId'];
        loadToDB['assembly_type'] = assemblyInfo;
        loadToDB['result_type'] = resType;
    }
    
    loadToDB['documents'] = loadHash;
    /*if ( parsedJson['var_disc'] ) {
        if ( parsedJson['var_disc']['geneID']) {
            loadToDB[assemblyInfo] = loadHash;
        }
    } else {
        loadToDB['documents'] = loadHash;
    }*/
    
    
    //console.dir(loadToDB);
    var payload = { "batch" : batch,
                    "pid" : pid,
                    "req" : reqVal
                  };

    resultCol.insertOne(loadToDB).then(function(r) {
        createLog.debug("HEY THERE .. CHECK THE PAYLOAD ADDED BELOW ************************** ");
        createLog.debug(payload);
        //console.log(JSON.stringify(payload));
        process.send(JSON.stringify(payload)); // to be added once the child_process is attached
        //return "done";
    })
    .catch(function(err) {
        console.log(err);
        createLog.debug("************ INSIDE CATCH BLOCK of loadResultCollection **************** ");
        //throw err;
    });
}


async function processPosFilter(filter,projection,batch,resultCol,db,collection) {
    try {
         var posStream = await collection.find(filter);
         posStream.project(projection); 

        var hostInfo = {};
        var timeField = new Date();
        var pid = process.pid;
        var batchCnt = 1;
        var batchId = `batch${batchCnt}`
        var docId = 1;
        var loadHash = {};
        var bulkOps = [];

         while ( await posStream.hasNext() ) {
             const doc = await posStream.next();
             //console.log(doc);
             loadHash[docId] = doc;
             docId++;
         }
         loadResultCollection(resultCol,"last",pid,batchId,timeField,loadHash,hostInfo);
         return "success";
    } catch(err) {
        throw err;
    }
}


async function getSavedFilterCnt(db,collection,parsedJson, mapFilter,reqID,createLog,assemblyInfo) {
    try {
        var filters1 = parsedJson['condition'];
        var fileID = parsedJson['fileID'];
        var fid = parsedJson['fid'] || 0;

        //var altHash = { "homRef" : 0 , "het" : 1, "homAlt" : 2 ,'any_genotype' : [0,1,2],'any_alternative' : [1,2] };
        // filter1 : array
 
        createLog.debug("Inside getSavedFilterCnt function");
        var parentFilters = {};
        var levelParent = {};
        // hash to store filters
        var passNodeId = 0;
        var failNodeId = 0;
        var result = [];
        var savedFilterResult = {'fid' : fid};
        
        // Create an entry with request id for counts.
        var utcTime = new Date().toUTCString();
        var reqInfo = {};
        if ( parsedJson['var_disc']) {
            reqInfo = {_id:reqID,status:"inprogress",loadedTime : utcTime,processedTime : null,'req_type':'var_disc', 'hostId' : parsedJson['hostId'], 'centerId' : parsedJson['centerId']};
        } else {
            reqInfo = {_id:reqID,status:"inprogress",loadedTime : utcTime,processedTime : null};
        }
        await db.collection(variantQueryCounts).insertOne(reqInfo);
        process.send(reqID);

        for ( var fIdx in filters1 ) {
            var rootFilter = {};
            var altRootFilter = {};
            //var rootFilter = { "fileID" : fileID };
            //var altRootFilter = { "fileID" : fileID };
            var passArr = [];
            var altPassArr = [];
            var failArr = [];
            var altFailArr = [];

            var filterRule = {};
            var attrHash = {};
            var filters = filters1[fIdx];
            var condHash = filters['c'];
            var fname = condHash['field'];
            var type = condHash['type'];
            //var leaf = condHash['leaf'];
            var level = condHash['level'];
            var parent = condHash['parent'];
            var currentLevel = level;
            attrHash['type'] = type;
            attrHash['level'] = level;
            attrHash['parent'] = parent;

            if ( parsedJson['var_disc']) {
                if ( parsedJson['var_disc']['geneID']) {
                    // fetch file IDs for the sequence type
                    // top level query will be with fileID and geneID
                    var fileIDList = await fetchSeqTypeList(db,parsedJson['seq_type']);
                    // added sleep to introduce some delay and check inprogress message
                    //await new Promise(resolve => setTimeout(resolve, 10000));
                    var geneIDList = parsedJson['var_disc']['geneID'];
                    rootFilter = { "fileID" : {$in:fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList}};
                    altRootFilter = { "fileID" : {$in: fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList} };
                } else if ( parsedJson['var_disc']['region']) {
                    var fileIDList = await fetchSeqTypeList(db,parsedJson['seq_type']);
                    var region_req = parsedJson['var_disc']['region'];
                    var condHashTmp1 = {};
                    var condHashTmp2 = {};
                    var regData = region_req.split('-');
                    
                    condHashTmp2['start_pos'] = parseInt(regData[2]);
                    condHashTmp2['stop_pos'] = parseInt(regData[1]);
                    condHashTmp1['add_rule_pos'] = condHashTmp2;
                    condHashTmp1['rule'] = parseInt(regData[0]);
                    //console.log("Logging the condition hash here");
                    //console.log(condHashTmp1);
    
                    var filterR = await positionFilterRule(condHashTmp1);
                    filterR['fileID'] = {$in: fileIDList};

                    var tmpFil1 = Object.assign({}, filterR);
                    var tmpFil2 = Object.assign({}, filterR);
                    rootFilter = tmpFil1;
                    altRootFilter = tmpFil2;    
                }
            } else if ( parsedJson['trio']) {
                var trioLocalID = parsedJson['trio']['trioLocalID'];
                var trioCode = parsedJson['trio']['trio_code'];
                var trioFileList = await fetchTrioLocalList(db,trioLocalID);
                if ( parsedJson['trio']['IndividualID']) {
                    // restrict filter search based on IndividualID/fileID
                    var indId = parsedJson['trio']['IndividualID'];
                    var trFileID = await fetchTrioFileID(db,trioLocalID,indId);
                    rootFilter = { "fileID" : trFileID, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
                    altRootFilter = { "fileID" : trFileID, 'trio_code' : trioCode, 'alt_cnt': {$ne:0}};
                } else {
                    rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
                    altRootFilter = { "fileID" : {$in: trioFileList}, 'trio_code' : trioCode, 'alt_cnt': {$ne:0}};
                }
            } else {
                rootFilter = { "fileID" : fileID };
                altRootFilter = { "fileID" : fileID };
            }

            var dbField = mapFilter[fname] || fname; // retrieve from the stored mapFilter
            //var filterType = Object.keys(condHash['rule'])[0]; // $in or $eq or $ne

            //console.log("dbfield is "+dbField);
            //console.log("filterType is "+filterType);

            if ( dbField == "alt_cnt" ) {
                filterRule = await altCntFilterRule(condHash,dbField);
            } else if ( dbField == 'PanelID' ) {
                filterRule = await genePanelFilterRule(condHash,db,genePanelColl,createLog);
            } else {
                if ( condHash['add_rule_pos'] ) {
                    filterRule = await positionFilterRule(condHash);
                } else {
                    // check if formula is null or not. Based on this call another function that will process formula and create filter rule.
                    filterRule = await formulaFilterRule(condHash,dbField,createLog);
                    //filterRule[dbField] = condHash['rule'];
                    //console.log("Logging filter rule");
                    //console.dir(filterRule,{"depth":null});
                    createLog.debug("Filter rule is now :");
                    createLog.debug(filterRule);
                }
            }

            createLog.debug("Logging filters length");
            createLog.debug(filters1.length);
            if ( filters1.length == 1  ) {
                if ( type == "null" ) {
                    passArr.push(filterRule);
                }
                if ( currentLevel == 0 ) {
                    altFailArr.push(filterRule);
                }
            } else {
                // When more than 1 filter condition is present
                levelParent[level] = parent;
                var tmpAttr = { 'rule' : filterRule, 'attr' : attrHash };
                parentFilters[level] = tmpAttr;
                //parentFilters[level]['rule'] = filterRule;
                //parentFilters[level]['attr'] = attrHash;

                for ( let j = level; j > 0 && level > 0; j-- ) {
                    parent = levelParent[level];
                    var tmpFilterRule = parentFilters[parent]['rule'];
                    type = parentFilters[level]['attr']['type'];
                    createLog.debug("type is "+type);
                    if ( type == "pass" ) {
                        passArr.push(tmpFilterRule);
                        altPassArr.push(tmpFilterRule);
                    } else if ( type == "fail" ) {
                        failArr.push(tmpFilterRule);
                        altFailArr.push(tmpFilterRule);
                    } 
                    //console.dir(passArr,{"depth":null});
                    //console.dir(failArr,{"depth":null});
                    level = parent;
                }
        
                // add the current rule to the pass and fail array
                console.log("Logging the current level below");
                console.log(currentLevel);
                console.dir(filterRule);
                if ( currentLevel != 0 ) {
                    passArr.push(filterRule);
                    altFailArr.push(filterRule);
                } else {
                    passArr.push(filterRule);
                    altFailArr.push(filterRule);
                }

                //console.log("***********************");
                //console.dir(passArr,{"depth":null});
                //console.dir(failArr,{"depth":null});

            }

            // leaf processing inside for loop

            // This process will take place for every filter rule which is defined in the hash.

            passNodeId = condHash['pass_node_id'] || 0;
            failNodeId = condHash['fail_node_id'] || 0;

            if ( passArr.length > 0 ) {
                if ( rootFilter['$and']) {
                    // scan and push entries
                    // filters already present in the root Filter. Variant Discovery
                    var addedFilters1 = {};
                    // store copy of array
                    addedFilters1['$and'] = [ ...passArr];
                    var storedArr = rootFilter['$and'];

                    var tmpStoredArr = [ ...storedArr];

                    tmpStoredArr.push(addedFilters1);
                    //rootFilter['$and'].push(addedFilters1);
                    rootFilter['$and'] = tmpStoredArr;
                    //rootFilter['dummy'] = "DUMMY UPDATE";
                } else {
                    rootFilter['$and'] = passArr;
                }
            }

            if ( failArr.length > 0 ) {
                if ( rootFilter['$nor']) {
                    //console.log(passArr);
                    // scan and push entries
                    // filters already present in the root Filter. Variant Discovery
                    var addedFilters2 = {};
                    // store copy of array
                    addedFilters2['$nor'] = [ ...failArr];
                    var storedArr = rootFilter['$nor'];

                    var tmpStoredArr = [ ...storedArr];

                    tmpStoredArr.push(addedFilters2);
                    //rootFilter['$and'].push(addedFilters1);
                    rootFilter['$nor'] = tmpStoredArr;
                    //rootFilter['NORdummy'] = "DUMMY UPDATE";
                } else {
                    rootFilter['$nor'] = failArr;
                }
            }

            if ( altPassArr.length > 0 ) {
                if ( altRootFilter['$and']) {
                    var addedFilters3 = {};
                    addedFilters3['$and'] = [ ...altPassArr];
                    var storedArr = altRootFilter['$and'];
                    // store copy of array
                    var tmpStoredArr = [ ...storedArr];

                    tmpStoredArr.push(addedFilters3);
                    //rootFilter['$and'].push(addedFilters1);
                    altRootFilter['$and'] = tmpStoredArr;
                    //rootFilter['ALTANDDUMMY'] = "DUMMY UPDATE";
                } else {
                    altRootFilter['$and'] = altPassArr;
                }
            }

            if ( altFailArr.length > 0 ) {
                if ( altRootFilter['$nor']) {
                    var addedFilters4 = {};
                    addedFilters4['$nor'] = [ ...altFailArr];
                    var storedArr = altRootFilter['$nor'];
                    // store copy of array
                    var tmpStoredArr = [ ...storedArr];

                    tmpStoredArr.push(addedFilters4);
                    //rootFilter['$and'].push(addedFilters1);
                    altRootFilter['$nor'] = altFailArr;
                } else {
                    altRootFilter['$nor'] = altFailArr;
                } 
            }

            //console.log("Logging PARENT FILTERS hash ");
            //console.dir(parentFilters,{"depth":null});
            //console.log("Root Filter");
            //console.dir(rootFilter,{"depth":null});
            //console.log("Alternate root filter");
            //console.dir(altRootFilter,{"depth":null});

            createLog.debug("Root filter below ");
            createLog.debug("Logging the pass root mongodb filter");
            createLog.debug(rootFilter);
            //createLog.debug(rootFilter);
            createLog.debug("Alternate Root filter below ");
            //createLog.debug("Logging the fail alternate root filter mongodb");
            createLog.debug(altRootFilter);
            //createLog.debug(altRootFilter,{"depth":null});

            // Execute both queries in parallel
            var rootPromise = collection.find(rootFilter).count();
            var altRootPromise = collection.find(altRootFilter).count();


            const data = await Promise.all([rootPromise,altRootPromise]);

            var pCount = data[0];
            var fCount = data[1];

            // Variant Discovery
        // Fetch the variants for the request geneID in the other assembly
        // sum counts of request assembly and alternate assembly
        if ( parsedJson['var_disc'] ) {
            if ( parsedJson['var_disc']['geneID']) {
                var tmpColl = assemblyInfo['altColl'];
                var rootPromise1 = db.collection(tmpColl).find(rootFilter).count();
                var altRootPromise1 = db.collection(tmpColl).find(altRootFilter).count();

                const data1 = await Promise.all([rootPromise1,altRootPromise1]);
                var count1 = data1[0];
                var fCount1 = data1[1];
                pCount = pCount + count1;
                fCount = fCount + fCount1;
            }
        }

            var passHash = {'count' : pCount, 'childIds' : passNodeId , 'result_type': "pass"};
            var failHash = {'count' : fCount, 'childIds' : failNodeId , 'result_type': "fail"};
            result.push(passHash);
            result.push(failHash);
            createLog.debug("Count result can be returned now");
            createLog.debug(result);
            //return result;
            //console.dir(result,{"depth":null})
        }
        savedFilterResult['documents'] = result;
        //console.dir(savedFilterResult,{"depth":null});
        var procTime = new Date().toUTCString();

        if ( parsedJson['var_disc']) {
            await db.collection(variantQueryCounts).updateOne({_id:reqID},{$set:{'result':savedFilterResult,status:"completed",processedTime:procTime,'req_type':'var_disc', 'hostId' : parsedJson['hostId'], 'centerId' : parsedJson['centerId']}});
        } else if ( parsedJson['trio'] ) {
            await db.collection(variantQueryCounts).updateOne({_id:reqID},{$set:{'result':result,status:"completed",'req_type' : 'trio',processedTime:procTime}});
        } else {
            await db.collection(variantQueryCounts).updateOne({_id:reqID},{$set:{'result':savedFilterResult,status:"completed",processedTime:procTime}});
        }
        
        
        return "success";
        // commenting below code as we will handle the return of counts in async approach
        //return savedFilterResult;
        //return "success";
    } catch(err) {
        console.log("Error in getSavedFilterCnt");
        console.log(err);
        throw err;
    }  
}

async function getCount(db,collection,parsedJson, mapFilter,reqID,createLog,assemblyInfo) {
    try {

        var rootFilter = {};
        var altRootFilter = {};

        var filters1 = parsedJson['condition'];
        
        var passArr = [];
        var altPassArr = [];
        var failArr = [];
        var altFailArr = [];
        //var altHash = { "homRef" : 0 , "het" : 1, "homAlt" : 2 ,'any_genotype' : [0,1,2],'any_alternative' : [1,2] };
        // filter1 : array

        var fileID = parsedJson['fileID'];
        // Create an entry with request id for counts.
        var utcTime = new Date().toUTCString();
        var reqInfo = {};
        if ( parsedJson['var_disc']) {
            reqInfo = {_id:reqID,status:"inprogress",loadedTime : utcTime,processedTime : null,'req_type':'var_disc', 'hostId' : parsedJson['hostId'], 'centerId' : parsedJson['centerId']};
        } else {
            reqInfo = {_id:reqID,status:"inprogress",loadedTime : utcTime,processedTime : null};
        }
        await db.collection(variantQueryCounts).insertOne(reqInfo);
        process.send(reqID);

        if ( parsedJson['var_disc']) {
            //await new Promise(resolve => setTimeout(resolve, 10000));
            if ( parsedJson['var_disc']['geneID']) {
                // fetch file IDs for the sequence type
                // top level query will be with fileID and geneID
                var fileIDList = await fetchSeqTypeList(db,parsedJson['seq_type']);
                var geneIDList = parsedJson['var_disc']['geneID'];
                rootFilter = { "fileID" : {$in:fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList}};
                altRootFilter = { "fileID" : {$in: fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList} };
            } else if ( parsedJson['var_disc']['region']) {
                var fileIDList = await fetchSeqTypeList(db,parsedJson['seq_type']);

                //console.log("Variant Discovery Region type");
                //console.log(fileIDList);
                var region_req = parsedJson['var_disc']['region'];
                var condHashTmp1 = {};
                var condHashTmp2 = {};
                var regData = region_req.split('-');
                
                condHashTmp2['start_pos'] = parseInt(regData[2]);
                condHashTmp2['stop_pos'] = parseInt(regData[1]);
                condHashTmp1['add_rule_pos'] = condHashTmp2;
                condHashTmp1['rule'] = parseInt(regData[0]);
                var filterR = await positionFilterRule(condHashTmp1);
                filterR['fileID'] = {$in: fileIDList};

                //rootFilter = filterR;
                //altRootFilter = filterR;
                var tmpFil1 = Object.assign({}, filterR);
                var tmpFil2 = Object.assign({}, filterR);
                rootFilter = tmpFil1;
                altRootFilter = tmpFil2;
            }
        } else if ( parsedJson['trio']) {
            var trioLocalID = parsedJson['trio']['trioLocalID'];
            var trioCode = parsedJson['trio']['trio_code'];
            var trioFileList = await fetchTrioLocalList(db,trioLocalID);
            if ( parsedJson['trio']['IndividualID']) {
                var indId = parsedJson['trio']['IndividualID'];
                // restrict filter search based on IndividualID/fileID
                var trFileID = await fetchTrioFileID(db,trioLocalID,indId);
                rootFilter = { "fileID" : trFileID, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
                altRootFilter = { "fileID" : trFileID, 'trio_code' : trioCode, 'alt_cnt': {$ne:0}};
            } else {
                rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
                altRootFilter = { "fileID" : {$in: trioFileList}, 'trio_code' : trioCode, 'alt_cnt': {$ne:0}};
            }
            
        } else {
            rootFilter = { "fileID" : fileID };
            altRootFilter = { "fileID" : fileID };
        }
        
        createLog.debug("Inside getCount function");
        // hash to store filters
        var filterRuleHash = {};
        var passNodeId = 0;
        var failNodeId = 0;
        for ( var fIdx in filters1 ) {
            var filterRule = {};
            var filters = filters1[fIdx];
            var condHash = filters['c'];
            var fname = condHash['field'];
            var type = condHash['type'];
            var leaf = condHash['leaf'];
            var dbField = mapFilter[fname] || fname; // retrieve from the stored mapFilter
            //var filterType = Object.keys(condHash['rule'])[0]; // $in or $eq or $ne

            if ( dbField == "alt_cnt" ) {
                filterRule = await altCntFilterRule(condHash,dbField);
                //console.log("Logging filter rule");
                //console.dir(filterRule,{"depth":null});
            } else if ( dbField == 'PanelID' ) { 
                filterRule = await genePanelFilterRule(condHash,db,genePanelColl,createLog);
                //console.log("Logging filter rule");
                //console.dir(filterRule,{"depth":null});
            } else {
                if ( condHash['add_rule_pos'] ) {
                    filterRule = await positionFilterRule(condHash);
                } else {
                    // check if formula is null or not. Based on this call another function that will process formula and create filter rule.
                    filterRule = await formulaFilterRule(condHash,dbField,createLog);
                    //filterRule[dbField] = condHash['rule'];
                    //console.log("Logging filter rule");
                    //console.dir(filterRule,{"depth":null});
                    createLog.debug("formula filter rule is now :");
                    createLog.debug(filterRule);
                }
            }

            if ( filters1.length == 1 ) {
                if ( type == "null" ) {
                    passArr.push(filterRule);
                }
            } else {
                if ( filterRuleHash['filterRule'] ) {
                    // first store the current rule to a tmp var 
                    var tmpRuleStore = filterRule;
                    // retrieve previously stored rule
                    filterRule = filterRuleHash['filterRule'];
                    filterRuleHash['filterRule'] = tmpRuleStore;
                } else {
                    filterRuleHash['filterRule'] = filterRule;
                }
                
                if ( type == "pass" ) {
                    passArr.push(filterRule);
                    //if ( leaf != 1 ) {
                        altPassArr.push(filterRule);
                    //}
                } else if ( type == "fail" ) {
                    failArr.push(filterRule);
                    //if ( leaf != 1) {
                        altFailArr.push(filterRule);
                    //}
                }
            }
           

            // leaf processing inside for loop
            if ( leaf == 1 ) {
                passNodeId = condHash['pass_node_id'] || 0;
                failNodeId = condHash['fail_node_id'] || 0;
                if ( filterRuleHash['filterRule'] ) {
                    var storedRule = filterRuleHash['filterRule'];
                    passArr.push(storedRule);
                    altFailArr.push(storedRule);
                } else {
                    // when only one condition exists
                    altFailArr.push(filterRule);
                }
                /*if ( type == "pass" ) {
                    altFailArr.push(filterRule);
                } else if ( type == "fail" ) {
                    altPassArr.push(filterRule);
                } else if ( type == "null" ) {
                    altFailArr.push(filterRule);
                }*/
            }
        }


        if ( passArr.length > 0 ) {
            if ( rootFilter['$and']) {
                // scan and push entries
                // filters already present in the root Filter. Variant Discovery
                var addedFilters1 = {};
                addedFilters1['$and'] = [ ...passArr];
                var storedArr = rootFilter['$and'];

                var tmpStoredArr = [ ...storedArr];

                tmpStoredArr.push(addedFilters1);
                //rootFilter['$and'].push(addedFilters1);
                rootFilter['$and'] = tmpStoredArr;
                //rootFilter['dummy'] = "DUMMY UPDATE";
                
            } else {
                rootFilter['$and'] = passArr;
            }
        }


        if ( failArr.length > 0 ) {
            if ( rootFilter['$nor']) {

                //console.log(passArr);
                // scan and push entries
                // filters already present in the root Filter. Variant Discovery
                var addedFilters2 = {};
                addedFilters2['$nor'] = [ ...failArr];
                var storedArr = rootFilter['$nor'];

                var tmpStoredArr = [ ...storedArr];

                tmpStoredArr.push(addedFilters2);
                //rootFilter['$and'].push(addedFilters1);
                rootFilter['$nor'] = tmpStoredArr;
                //rootFilter['NORdummy'] = "DUMMY UPDATE";
            } else {
                rootFilter['$nor'] = failArr;
            } 
        }


        if ( altPassArr.length > 0 ) {
            if ( altRootFilter['$and']) {
                var addedFilters3 = {};
                addedFilters3['$and'] = [ ...altPassArr];
                var storedArr = altRootFilter['$and'];

                var tmpStoredArr = [ ...storedArr];

                tmpStoredArr.push(addedFilters3);
                //rootFilter['$and'].push(addedFilters1);
                altRootFilter['$and'] = tmpStoredArr;
                //rootFilter['ALTANDDUMMY'] = "DUMMY UPDATE";
            } else {
                altRootFilter['$and'] = altPassArr;
            }
            
        }

        if ( altFailArr.length > 0 ) {
            if ( altRootFilter['$nor']) {
                var addedFilters4 = {};
                addedFilters4['$nor'] = [ ...altFailArr];
                var storedArr = altRootFilter['$nor'];

                var tmpStoredArr = [ ...storedArr];

                tmpStoredArr.push(addedFilters4);
                //rootFilter['$and'].push(addedFilters1);
                altRootFilter['$nor'] = altFailArr;
            } else {
                altRootFilter['$nor'] = altFailArr;
            } 
            
        }

        createLog.debug("Root filter below ");
        createLog.debug("Logging the pass root mongodb filter");
        createLog.debug(rootFilter);
        //createLog.debug(rootFilter);
        createLog.debug("Alternate Root filter below ");
        //createLog.debug("Logging the fail alternate root filter mongodb");
        createLog.debug(altRootFilter);
        //createLog.debug(altRootFilter,{"depth":null});

        // Execute both queries in parallel
        var rootPromise = collection.find(rootFilter).count();
        var altRootPromise = collection.find(altRootFilter).count();


        const data = await Promise.all([rootPromise,altRootPromise]);
        var result = [];
        var count = data[0];
        var fCount = data[1];
        //console.log("Initial Count1 "+count);
        //console.log("Initial count2 "+fCount);

        // Variant Discovery
        // Fetch the variants for the request geneID in the other assembly
        // sum counts of request assembly and alternate assembly
        if ( parsedJson['var_disc'] ) {
            if ( parsedJson['var_disc']['geneID']) {
                var tmpColl = assemblyInfo['altColl'];
                //console.log("Printing tmpColl "+tmpColl);
                //console.log("Printing the filters below ");
                var rootPromise1 = db.collection(tmpColl).find(rootFilter).count();
                var altRootPromise1 = db.collection(tmpColl).find(altRootFilter).count();

                const data1 = await Promise.all([rootPromise1,altRootPromise1]);
                const count1 = data1[0];
                const fCount1 = data1[1];
                //console.log("Printing alternate counts1  "+count1);
                //console.log("Printing alternate counts2  "+fCount1);
                count = count + count1;
                fCount = fCount + fCount1;
                //console.log("PCounts after sum "+ count);
                //console.log("FCount after sum "+fCount);
            }
        }
        var passHash = { 'pass' : count, 'childIds' : passNodeId };
        var failHash = { 'fail' : fCount, 'childIds' : failNodeId };
        result.push(passHash);
        result.push(failHash);
        createLog.debug("Count result can be returned now");
        createLog.debug(result);
        var procTime = new Date().toUTCString();

        if ( parsedJson['var_disc']) {
            await db.collection(variantQueryCounts).updateOne({_id:reqID},{$set:{'result':result,status:"completed",processedTime:procTime, 'req_type':'var_disc', 'hostId' : parsedJson['hostId'], 'centerId' : parsedJson['centerId']}});
        } else if ( parsedJson['trio']) {
            await db.collection(variantQueryCounts).updateOne({_id:reqID},{$set:{'result':result,status:"completed",'req_type' : 'trio',processedTime:procTime}});
        } else {
            await db.collection(variantQueryCounts).updateOne({_id:reqID},{$set:{'result':result,status:"completed",processedTime:procTime}});
        }
        
        return "success";
        //return result;
        //return `PASS :${count} FAIL Items:${fCount}`;

        /*Promise.all([rootPromise,altPassArr]).then( (values) => {
            console.log(values);
        });*/
        /* Old Approach
        var count = await collection.find(rootFilter,{}).count();
        console.log("PASS COUNT " +count);
        var fCount = await collection.find(altRootFilter,{}).count();
        console.log("FAIL COUNT "+fCount);
        return `PASS Items:${count} FAIL Items:${fCount}`; */
        //return rootFilter;
    } catch(err) {
        throw err;
    }  
}

async function fetchSeqTypeList(db,seqType) {
    try {
        var fileIDList = [];
        var query = {'SeqTypeName':seqType,"state" :{ $ne: "unassigned"}};
        var res = await db.collection(indSampCollection).find(query,{projection:{fileID: 1,_id:0}});
        while ( await res.hasNext()) {
            const doc = await res.next();
            if ( Object.keys(doc).length > 0 ) {
                fileIDList.push(doc['fileID']);
            } 
        }
        return fileIDList;
    } catch(err) {
        throw err;
    }
}

async function fetchTrioLocalList(db,trioLocalID) {
    try {
        var fileIDList = [];
        var query = {"TrioLocalID" : trioLocalID,"TrioStatus" : "completed"};
        var res = await db.collection(trioCollection).find(query,{projection:{'trio.fileID': 1,_id:0}});
        while ( await res.hasNext()) {
            const doc = await res.next();
            if ( doc['trio'] ) {
                var trioList = doc['trio'];
                for ( var idx in trioList ) {
                    var elem = trioList[idx];
                    if ( elem.fileID ) {
                        fileIDList.push(elem.fileID);
                    }
                }
            }
        }
        return fileIDList;
    } catch(err) {
        throw err;
    }
}

async function fetchTrioFileID(db,trioLocalID,indId) {
    try {
        var query = {"TrioLocalID" : trioLocalID,"TrioStatus" : "completed","trio.individualID": indId};
        var trioFileID = "";
        var res = await db.collection(trioCollection).findOne(query,{projection:{'trio.fileID.$':1,_id:0}});
        if ( res ) {
            trioFileID = res['trio'][0]['fileID'];
        }
        return trioFileID;
    } catch(err) {
        throw err;
    }
}


async function fetchTrioBuildType(db,trioLocalID) {
    try {
        var query = {"TrioLocalID" : trioLocalID,"TrioStatus" : "completed"};
        var assembly = "";
        var res = await db.collection(trioCollection).findOne(query,{projection:{'AssemblyType': 1,_id:0}});
        //console.log(res);
        if ( res ) {
            assembly = res['AssemblyType']
        }
        return assembly;
    } catch(err) {
        throw err;
    }
}

async function fetchGeneID(filter,gpCollObj) {
    try {
        var geneIDs = [];
        var res = await gpCollObj.findOne({'_id':filter},{'GeneID':1});
        if ( (res != null) && (res['GeneID']) ) {
            geneIDs = res['GeneID'];
        }
        return geneIDs;
    } catch(err) {
        throw err;  
    }
}

function getFilterType (condHash) {
    var filterType = Object.keys(condHash['rule']);
    var reg = /$/;
    for ( var idx1 in filterType ) {
        var val = filterType[idx1];
        if ( val.match(reg)) {
            return val;
        }
    }
}

function createOprObj (v1,v2,opr1) {
    var exprHash = {
        '+': '$sum',
        '-': '$subtract',
        '/': '$divide',
        '*': '$multiply'
    };
    var operands = [];
    operands.push(v1,v2);
    var exprOpr = exprHash[opr1];
    var levelObj = {};
    levelObj[exprOpr] = operands;
    return levelObj;
}

async function formulaFilterRule(condHash,dbField,createLog) {
    try {
        var finalFilterRule = {};
        var filterRule = {};
        var divFilterRule = {};
        var divisor = 0;
        // {"rule" : {"$gt":3, "formula":null}}
        if (!condHash['rule']['formula'] ) {
            // delete formula key
            delete condHash['rule']['formula'];
            filterRule[dbField] = condHash['rule'];
            finalFilterRule = filterRule;
        } else {
            var rule = condHash['rule'];
            var filterType = getFilterType(condHash);
            //console.log("***************** filterType is "+filterType);
            var exprVal = condHash['rule'][filterType];
            var exprFormulaArr = [];
            var tmp = {};
            if (rule['formula']) {
                var formula = rule['formula'];
                var regexp1 = /\(/;
                if (formula.match(regexp1)) {
                    // (A+B)/C
                    var type1Check = /\(([\w-]+)([\+\-\*\/])([\w-]+)\)([\+\-\*\/])([\w-]+)/g;
                    var type1Res = type1Check.exec(formula);
                    //console.log(type1Res.length);
                    if ( (type1Res) && (type1Res.length == 6)) {
                        //console.dir(type1Res,{"depth":null});
                        var level1 = createOprObj('$'+type1Res[1],'$'+type1Res[3],type1Res[2]);
                        var level2 = createOprObj(level1,'$'+type1Res[5],type1Res[4]);
                        //console.dir(level2,{"depth":null});
                        //tmp[filterType].push(level2,exprVal);
                        createLog.debug("Formula type  (A+B)/C");
                        exprFormulaArr.push(level2,exprVal);

                        // Check if division is present.
                        if ( type1Res[4] == "\/") {
                            divisor = 1;
                            var operand1 = type1Res[5];
                            divFilterRule[operand1] = {'$ne' : 0};
                        }
                    }
                    // (A+B)/(C+D)
                    var type2Check = /\(([\w-]+)([\+\-\*\/])([\w-]+)\)([\+\-\*\/])\(([\w-]+)([\+\-\*\/])([\w-]+)\)/g;
                    var type2Res = type2Check.exec(formula);
                    //console.log(type2Res.length);
                    if ( (type2Res) && (type2Res.length == 8)) {
                        //console.dir(type2Res,{"depth":null});
                        var tmpExpr = [];
                        var level1 = createOprObj('$'+type2Res[1],'$'+type2Res[3],type2Res[2]);
                        var level2 = createOprObj('$'+type2Res[5],'$'+type2Res[7],type2Res[6]);
                        var level3 = createOprObj(level1,level2,type2Res[4]);

                        createLog.debug("Formula type  (A+B)/(C+D)");
                        // Check if Division operator is present
                        // If division is present, add rule to check the divisor is not 0
                        // Added to handle divide by 0 issue
                        if ( type2Res[4] == "\/") {
                            divisor = 1;
                            tmpExpr.push(level2,0);
                            var tmpR = {};
                            tmpR['$ne'] = tmpExpr;
                            divFilterRule['$expr'] = tmpR;
                        }

                        //console.dir(level3,{"depth":null});
                        //tmp[filterType].push(level3,exprVal);
                        exprFormulaArr.push(level3,exprVal);
                    }
                    // A/(B+C)
                    var type3Check = /([\w-]+)([\+\-\*\/])\(([\w-]+)([\+\-\*\/])([\w-]+)\)/g;
                    var type3Res = type3Check.exec(formula);
                    //console.log(type1Res.length);
                    if ( (type3Res) && (type3Res.length == 6)) {
                        var tmpExpr = [];
                        
                        //console.dir(type1Res,{"depth":null});
                        createLog.debug("Formula type  A/(B+C)");
                        var level1 = createOprObj('$'+type3Res[3],'$'+type3Res[5],type3Res[4]);    

                        var level2 = createOprObj('$'+type3Res[1],level1,type3Res[2]);
                        // Check if Division operator is present
                        // Added to handle divide by 0 issue
                        if ( type3Res[2] == "\/") {
                            divisor = 1;
                            tmpExpr.push(level1,0);
                            var tmpR = {};
                            tmpR['$ne'] = tmpExpr;
                            divFilterRule['$expr'] = tmpR;
                        } 

                        //console.dir(level2,{"depth":null});
                        //tmp[filterType].push(level2,exprVal);
                        exprFormulaArr.push(level2,exprVal);
                        //db.collection.find( { $and: [ { expr divisor rule }, { expr formula rule } ] } )

                    }

                } else {
                    // A+B
                    var inRegExp1 = /([\w-]+)([\+\-\*\/])([\w-]+)/g;
                    var result = inRegExp1.exec(formula);
                    if ((result) && (result.length == 4)) {
                        //console.dir(result, { "depth": null });
                        createLog.debug("Formula type A+B");
                        var level1 = createOprObj('$'+result[1],'$'+result[3],result[2]);
                        //console.dir(level1,{"depth":null});
                        //filterRule = {'$expr' : { filterType : [level1,exprVal]}}
                        exprFormulaArr.push(level1,exprVal);
                    }
                }
                //console.log("Tmp filter type is "+filterType);
                var tmp = {};
                tmp[filterType] = exprFormulaArr;
                filterRule['$expr'] = tmp;
                if ( divisor == 1 ) {
                    createLog.debug("Divisor rule set and logging divisor rule");
                    createLog.debug(divFilterRule,{"depth":null});
                    var appendRule = {};
                    var pushArr = [];
                    pushArr.push(divFilterRule,filterRule);
                    appendRule['$and'] = pushArr;
                    finalFilterRule = appendRule;
                } else {
                    finalFilterRule = filterRule;
                }
            }
        }
        //console.dir(finalFilterRule,{"depth":null});
        return finalFilterRule;
    } catch(err) {
        throw err;
    }
}

async function positionFilterRule(condHash) {
    try {
        var filterRule = {};
        var altRule = condHash['add_rule_pos'];
        //console.log("Logging altRule inside positionFilterRule");
        //console.log(altRule);
        var start_pos; var stop_pos;
        if ( altRule['start_pos']) {
            start_pos = altRule['start_pos'] || 0;
        }
        if ( altRule['stop_pos']) {
            stop_pos = altRule['stop_pos'] || 0;
        }
        filterRule['$and']  = [ {"chr" : condHash['rule']}, {'start_pos' : {$lte : start_pos}}, {'stop_pos' : {$gte : stop_pos}} ];
        return filterRule;
    } catch(err) {
        throw err;
    }
}

async function genePanelFilterRule(condHash,db,genePanelColl,createLog) {
    try {
        var gpCollObj = db.collection(genePanelColl);
        var filterRule = {};
        //var filterType = Object.keys(condHash['rule'])[0];
        if (!condHash['rule']['formula'] ) {
            // delete formula key
            delete condHash['rule']['formula'];
        }
        var filterType = getFilterType(condHash);
        // rule contains the PanelID : { "field" : "PanelID" , "rule" : { "$eq" : 123 },"type" : "null" , "leaf" : 0} }
        createLog.debug("Calling fetchGeneID ");
        
        var geneIDs = await fetchGeneID(condHash['rule'],gpCollObj);
        createLog.debug('GeneIDS are ');
        createLog.debug(geneIDs);
        if ( filterType == "$eq" ) {
            delete condHash['rule'][filterType];
            filterType = "$in";
        } else if ( filterType == "$ne" ) {
            delete condHash['rule'][filterType];
            filterType = "$nin";
        }

        var dbField = 'gene_annotations.gene_id';
        condHash['rule'][filterType] = geneIDs;
        filterRule[dbField] = condHash['rule'];
        return filterRule;

    } catch(err) {
        throw err;
    }
}

async function altCntFilterRule(condHash,dbField) {
    try {
        var altHash = { "homRef" : 0 , "het" : 1, "homAlt" : 2 ,'any_genotype' : [0,1,2],'any_alternative' : [1,2] };

        var numArr = [];
        var filterRule = {};
        if (!condHash['rule']['formula'] ) {
            // delete formula key
            delete condHash['rule']['formula'];
        }
        //var filterType = Object.keys(condHash['rule'])[0];
        var filterType = getFilterType(condHash);
        var lookUp = condHash['rule'][filterType];
        if ( filterType == "$in" ) {
            for ( var idx in lookUp ) {
                var val = lookUp[idx];
                numArr.push(altHash[val]);
            }
            condHash['rule'][filterType] = numArr;
        } else {
            if ( altHash[lookUp] ) {
                condHash['rule'][filterType] = altHash[lookUp];
            }
        }
        filterRule[dbField] = condHash['rule'];
        return filterRule;
    } catch(err) {
        throw err;
    }
}


async function logicalFilterProcess(db,parsedJson, mapFilter,createLog) {
    try {
        //console.dir(parsedJson);
        createLog.debug("Inside logicalFilterProcess function");
        var conditions = parsedJson['condition'];

        var fileID = parsedJson['fileID'];

        var rootFilter = {};
        if ( parsedJson['var_disc']) {
            if ( parsedJson['var_disc']['geneID']) {
                // fetch file IDs for the sequence type
                // top level query will be with fileID and geneID
                var fileIDList = await fetchSeqTypeList(db,parsedJson['seq_type']);
                var geneIDList = parsedJson['var_disc']['geneID'];
                rootFilter = { "fileID" : {$in:fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList}};
            } else if ( parsedJson['var_disc']['region']) {
                var fileIDList = await fetchSeqTypeList(db,parsedJson['seq_type']);
                var region_req = parsedJson['var_disc']['region'];
                var condHashTmp1 = {};
                var condHashTmp2 = {};
                var regData = region_req.split('-');
                
                condHashTmp2['start_pos'] = parseInt(regData[2]);
                condHashTmp2['stop_pos'] = parseInt(regData[1]);
                condHashTmp1['add_rule_pos'] = condHashTmp2;
                condHashTmp1['rule'] = parseInt(regData[0]);
                //console.log("Logging the condition hash here");
                //console.log(condHashTmp1);

                var filterR = await positionFilterRule(condHashTmp1);
                filterR['fileID'] = {$in: fileIDList};

                var tmpFil1 = Object.assign({}, filterR);
                rootFilter = tmpFil1; 
            }
        } else if ( parsedJson['trio']) {
            var trioLocalID = parsedJson['trio']['trioLocalID'];
            var trioCode = parsedJson['trio']['trio_code'];
            var trioFileList = await fetchTrioLocalList(db,trioLocalID);
            if ( parsedJson['trio']['IndividualID']) {
                var indId = parsedJson['trio']['IndividualID'];
                // restrict filter search based on IndividualID/fileID
                var trFileID = await fetchTrioFileID(db,trioLocalID,indId);
                rootFilter = { "fileID" : trFileID, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
            } else {
                rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
            }   
        } else {
            //console.log("Inside the regular filters");
            rootFilter = { "fileID" : fileID };
        }


        //var rootFilter = { "fileID" : fileID };
        //var altHash = { "homRef" : 0 , "het" : 1, "homAlt" : 2 ,'any_genotype' : [0,1,2],'any_alternative' : [1,2] };
        //var altHash = { "homRef" : 0 , "het" : 1, "homAlt" : 2 };
        var cumulatedBranches = [];

        for ( var cIdx in conditions ) {
            var filters1 = conditions[cIdx];
            var passArr = [];
            var failArr = [];
            //var altHash = { "het" : 1, "hom" : 0 };
            
            // filter1 : array
            // hash to store filters
            var filterRuleHash = {};
            createLog.debug("filter1 size is "+filters1.length);
            for ( var fIdx in filters1 ) {
                var filterRule = {};
                var filters = filters1[fIdx];
                var condHash = filters['c'];
                var fname = condHash['field'];
                var type = condHash['type'];
                var leaf = condHash['leaf'];
                var branchType = condHash['result_branch'];
                var dbField = mapFilter[fname] || fname; // retrieve from the stored mapFilter
                //var filterType = Object.keys(condHash['rule'])[0];

                //console.log("dbField is "+dbField);
                //console.log("Filter type is "+filterType);

                if ( dbField == "alt_cnt" ) {
                    filterRule = await altCntFilterRule(condHash,dbField);
                } else if ( dbField == 'PanelID' ) {
                    filterRule = await genePanelFilterRule(condHash,db,genePanelColl,createLog);
                } else {
                    if ( condHash['add_rule_pos'] ) {
                        filterRule = await positionFilterRule(condHash);
                    } else {
                        // check if formula is null or not. Based on this call another function that will process formula and create filter rule.
                        filterRule = await formulaFilterRule(condHash,dbField,createLog)
                        //filterRule[dbField] = condHash['rule'];
                        //console.log("Logging filter rule");
                        //console.dir(filterRule,{"depth":null});
                        createLog.debug("Filter rule is now :");
                        createLog.debug(filterRule);
                    }
                }

                if ( filters1.length == 1 ) {
                    if ( type == "null" ) {
                        if ( branchType == "pass" ) {
                            passArr.push(filterRule);
                        } else {
                            failArr.push(filterRule);
                        }
                        //passArr.push(filterRule);
                    }
                } else {
                    if ( filterRuleHash['filterRule'] ) {
                        // first store the current rule to a tmp var 
                        var tmpRuleStore = filterRule;
                        // retrieve previously stored rule
                        filterRule = filterRuleHash['filterRule'];
                        filterRuleHash['filterRule'] = tmpRuleStore;
                    } else {
                        filterRuleHash['filterRule'] = filterRule;
                    }
                    
                    if ( type == "pass" ) {
                        passArr.push(filterRule);   
                    } else if ( type == "fail" ) {
                        failArr.push(filterRule);
                    }
                }
            
                // leaf processing inside for loop
                // If leaf value is not available, then perform this step if the array index is the last element of the array
                if ( leaf == 1 ) {
                    if ( filterRuleHash['filterRule'] ) {
                        var storedRule = filterRuleHash['filterRule'];
                        if ( branchType == "pass" ) {
                            passArr.push(storedRule);
                        } else if ( branchType == "fail" ) {
                            failArr.push(storedRule);
                        }
                    } 
                }
                createLog.debug("Logging the data below in pass Array");
                createLog.debug(passArr,{"depth":null});
                createLog.debug("Logging the data below in fail array");
                createLog.debug(failArr),{"depth":null};
            } // filters array
            if ( conditions.length == 1 ) {
                if ( passArr.length > 0 ) {
                    if ( rootFilter['$and']) {
                        // scan and push entries
                        // filters already present in the root Filter. Variant Discovery
                        var addedFilters1 = {};
                        // store copy of array
                        addedFilters1['$and'] = [ ...passArr];
                        var storedArr = rootFilter['$and'];
    
                        var tmpStoredArr = [ ...storedArr];
    
                        tmpStoredArr.push(addedFilters1);
                        //rootFilter['$and'].push(addedFilters1);
                        rootFilter['$and'] = tmpStoredArr;
                        //rootFilter['dummy'] = "DUMMY UPDATE";
                    } else {
                        rootFilter['$and'] = passArr;
                    }
                }

                if ( failArr.length > 0 ) {
                    if ( rootFilter['$nor']) {
                        //console.log(passArr);
                        // scan and push entries
                        // filters already present in the root Filter. Variant Discovery
                        var addedFilters2 = {};
                        // store copy of array
                        addedFilters2['$nor'] = [ ...failArr];
                        var storedArr = rootFilter['$nor'];
    
                        var tmpStoredArr = [ ...storedArr];
    
                        tmpStoredArr.push(addedFilters2);
                        //rootFilter['$and'].push(addedFilters1);
                        rootFilter['$nor'] = tmpStoredArr;
                        //rootFilter['NORdummy'] = "DUMMY UPDATE";
                    } else {
                        rootFilter['$nor'] = failArr;
                    }
                }

            } else {
                //passArr , failArr
                var tmpFilter = {};
                var condArr = [];
                var tagCond = {};
                if ( passArr.length > 0 ) { 
                    //{'$and' : []}
                    tmpFilter['$and'] = passArr;
                    //condArr.push(tmpFilter); 
                }
                if ( failArr.length > 0 ) {
                    // {'$nor' : []}
                    tmpFilter['$nor'] = failArr;
                    //condArr.push(tmpFilter);
                }
                condArr.push(tmpFilter);
                // condArr : [ {'$and' : []}, {'$nor' : []}]
                tagCond['$and'] = condArr;
                // this is done to group all the conditions within the single branch
                // tagCond1 : { '$and' :   [ {'$and' : []}, {'$nor' : []}] }
                cumulatedBranches.push(tagCond);
                createLog.debug("Log the data in cumulated Branches and check ");
                createLog.debug(cumulatedBranches,{"depth":null});
                // [ tagCond1, tagCond2 ] ==> means one side of branch and another side of branch
            }
        } // conditions array

        if ( conditions.length > 1 ) {
            // {'$or' : }
            rootFilter['$or'] = cumulatedBranches;
        }
        createLog.debug("Logging the created filter");
        createLog.debug(rootFilter);
        return rootFilter;
    } catch(err) {
        createLog.debug("Logging an error in this function ");
        createLog.debug(`${err}`);
        throw err;
    }  
}

function applyFont(txt) {
  return colors.red(txt); //display the help text in red on the console
}

const initializeResultColl = async(client,db,ttlVal) => {
    var resCol;
    try {
        var obj = await checkCollectionExists(client,resultCollection);
        var obj1 = await createCollection(client,resultCollection);
        var res1 = await createTTLIndex(client,resultCollection,ttlVal);
        resCol = db.collection(resultCollection);
    } catch(err) {
        resCol = db.collection(resultCollection);
    }
    return resCol;
}

async function readFile(variantFile,varContLog) {
    var rd;
    try {
        var liftedVariant = null;
        console.log("File "+variantFile);
        if ( ! fs.existsSync(variantFile)) {
            console.log("-------- File does not exist------");
            return liftedVariant;
        } else {
            console.log("----------File exists----------------------");
            //createLog.debug(`Parsing filename ${sampleSh}`);
            var rd = readline.createInterface({
                input: fs.createReadStream(variantFile),
                //output: process.stdout,
                console: false
            });
            
            var lineRe = /^#/g;
            var blankLine = /^\s+$/g;
            var lineArr = [];
            varContLog.debug(`variantFile - ${variantFile}`);
            //console.log("Line Count is *************"+lineCnt);
            rd.on('line', (line) => {
                if ( ! line.match(lineRe) && ! line.match(blankLine)) {
                    //createLog.debug(line);
                    lineArr.push(line);
                    varContLog.debug(line);
                } 
            });

            return new Promise( resolve => {
                rd.on('close',  async () => {
                    //console.log("Reading Data Completed Start Traversing Array");
                    if ( lineArr.length > 0 ) {
                        liftedVariant = lineArr[0];
                    }
                    resolve(liftedVariant);
                }); 
            }, reject => {
                rd.on('error', async() => {
                    console.log("Error in reading file");
                    reject(err);
                })

            });
        } 
    } catch(err) {
        varContLog.debug(err);
        throw err;
    }
}

function getArray (filterType) {
    var retVal = [];
    if ( filterType === "Zygosity" ) {
        var gtHash = { 'any_genotype' : [0,1,2],
                       'heterozygous' : [1],
                       'any_alternative' : [1,2],
                       'hom_ref' : [0] };
        retVal = gtHash[option];
    }
    return retVal;
}



