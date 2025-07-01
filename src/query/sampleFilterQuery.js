#!/usr/bin/env node
'use strict';
/**
 * Module dependencies.
 */

const configData = require('../config/config.js');
const { db : {host,port,dbName,importCollection1,importCollection2,resultCollection,genePanelColl,variantQueryCounts,indSampCollection,trioCollection,reqTrackCollection,famAnalysisColl},app:{instance,liftMntSrc} } = configData;
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
const lodash = require("lodash")

var client;
var mongoHost = host;

//const createConnection = require('../controllers/dbFuncs.js').createConnection;
const createConnection = require('../controllers/dbConn.js').createConnection;
const getConnection = require('../controllers/dbConn.js').getConnection;
const createCollection = require('../controllers/dbFuncs.js').createCollection;
const checkCollectionExists = require('../controllers/dbFuncs.js').checkCollectionExists;
const createTTLIndex = require('../controllers/dbFuncs.js').createTTLIndex;
const createColIndex = require('../controllers/dbFuncs.js').createColIndex;
const getInheritanceData = require('../controllers/entityController.js').getInheritanceData;
const varPhenAnalysis = require('../controllers/varDisController.js').varPhenAnalysis;

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
            if ( ! parsedJson['trio'] && ! parsedJson['family'] ) {
                assemblyCheck = parsedJson['assembly_type'];
            } else if ( parsedJson['trio']) {
                assemblyCheck = await fetchTrioBuildType(db,parsedJson['trio']['trioLocalID']);
            } else if ( parsedJson['family']) {
                assemblyCheck = await fetchFamBuildType(db,parsedJson['family']['famLocalID']);
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
            console.log("Logging assembly details ")
            console.log(queryCollection)

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
            console.log("Sample Variant ######################");
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
                    //parsedJson['output_columns'] = {'_id': 0,'fileID':0};
                    if ( parsedJson['trio']['inheritance']) {
                        parsedJson['output_columns'] = {'_id': 0};
                    } 
                } else if ( parsedJson['var_disc']) {
                    // including additional fields
                    parsedJson['output_columns'] = {"_id" : 0, "var_validate" : 0,"pid" : 0,"multi_allelic" : 0, "phred_polymorphism" : 0, "filter" : 0, "alt_cnt" : 0, "ref_depth" : 0, "alt_depth" : 0, "phred_genotype" : 0, "mapping_quality" : 0, "base_q_ranksum" : 0, "mapping_q_ranksum" : 0, "read_pos_ranksum" : 0, "strand_bias" : 0, "quality_by_depth" : 0, "fisher_strand" : 0, "vqslod" : 0, "gt_ratio" : 0, "ploidy" : 0, "somatic_state" : 0, "delta_pl" : 0, "stretch_lt_a" : 0, "stretch_lt_b" : 0, "trio_code" : 0};
                    // mask sensitive fields using mongo projections
                    //parsedJson['output_columns'] = {"_id" : 0, "var_validate" : 0,"pid" : 0, "v_type" : 0, "non_variant" : 0, "vcf_chr" : 0, "chr" : 0, "start_pos" : 0, "stop_pos" : 0, "ref_all" : 0, "alt_all" : 0, "multi_allelic" : 0, "phred_polymorphism" : 0, "filter" : 0, "alt_cnt" : 0, "ref_depth" : 0, "alt_depth" : 0, "phred_genotype" : 0, "mapping_quality" : 0, "base_q_ranksum" : 0, "mapping_q_ranksum" : 0, "read_pos_ranksum" : 0, "strand_bias" : 0, "quality_by_depth" : 0, "fisher_strand" : 0, "vqslod" : 0, "gt_ratio" : 0, "ploidy" : 0, "somatic_state" : 0, "delta_pl" : 0, "stretch_lt_a" : 0, "stretch_lt_b" : 0, "trio_code" : 0};
                }
                if ( parsedJson['output_columns']) {
                    projection = parsedJson['output_columns'];
                }

                console.log("Call logicalFilter function")
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
                    var reqColl = db.collection(reqTrackCollection);
                    if ( parsedJson['var_disc']['geneID']) {
                        var coll1 = db.collection(assemblyInfo['reqColl']); 
                        var batchCnt = 1;
                        //console.log("First request launched with request collection");
                        var pid = await genReqID(reqTrackCollection,db,parsedJson);
                        if ( parsedJson['var_disc']['hpoList']) {
                            var payload = { "batch" : 1,
                                            "pid" : pid,
                                            "req" : "done"
                                        };
        
                            createLog.debug("Send the immediate response payload")
                            createLog.debug(payload);
                            console.log("Sending response payload")
                            process.send(JSON.stringify(payload));  
                        }


                        /*var pid = process.pid;
                        pid = pid + (parsedJson['centerId'] * parsedJson['hostId']);*/
                        await reqColl.updateOne({'_id':pid,},{$set:{'status':'inprogress'}});
                        // Step 1 : fetch the variants for the requested assembly
                        // Write variants to results collection for requested assembly
                        var totalBatch = await processLogicalFilter(pid,retFilter,projection,batch,resultCol,db,coll1,createLog,parsedJson, assemblyInfo['reqAssembly'],batchCnt);

                        createLog.debug("Total Batch received from first request is "+totalBatch);
                        createLog.debug("Next request launched with alternate collection ");
                        var coll2 = db.collection(assemblyInfo['altColl']); 
                        var nextBatch = totalBatch + 1;
                        
                        /////////////////// LIFTOVER FIX - ONLY if there is add_rule_pos///////////////////////////////////////////////////////////////////////     
                        var retRegion = await logicalFilterProcessRegion(db,parsedJson,mapFilter,createLog);
                        if (retRegion.length > 0) {    
                            // alternate assembly variants will be lifted over to the request assembly
                            //var region = parsedJson['var_disc']['region'];
                            // for now - support only liftover for 1 region
                            var region = retRegion[0];

                            //console.log(`Liftoverscript is ${liftoverScript}`);
                            //console.log(`Region ${region}`);
                            //console.log("Assembly Info "+assemblyInfo['reqAssembly']);
                            //console.log(`pid ${pid}`);
                            var parsePath = path.parse(__dirname).dir;
                            var liftoverScript = path.join(parsePath,'modules','ucscLiftover.js');
                            createLog.debug("Assembly "+assemblyInfo['altAssembly']);

                            var subprocess1 = spawn.fork(liftoverScript, ['--variant',region,'--assembly', assemblyInfo['reqAssembly'], '--req_id', pid] );
                                                    
                            var variantFile = path.join(liftMntSrc,pid+'-variant.bed');
                        
                            try {
                                // Check if liftover process has completed
                                await closeSignalHandler(subprocess1);
                            
                                var liftedVariant = await readFile(variantFile,createLog);

                                createLog.debug("Liftover process completed now ");
                            
                                // Step3 : Update lifted region to the parsed json
                                // ================================================
                                // clone the parsed Json to another object. Update the lifted region
                                //var clonedParsJson = { ... parsedJson};
                            
                                // find a solution to update the liftedVariant into the filter...
                                //clonedParsJson['var_disc']['region'] = liftedVariant;
                                // liftedVariant parameter is sent to update the filter with the lifted region
                                retFilter = await logicalFilterProcess(db,parsedJson,mapFilter,createLog,liftedVariant);

                                // update retFilter with this newly setup filter...
                            } catch(err) {

                            }
                        } 

                        /////////////////// LIFTOVER FIX ///////////////////////////////////////////////////////////////////////

                        // Step3 : Pass the fetched filter object to retFilter
                        // --- only this filter will be different if there is a chromosome-region condition
                        var varFileLoc = await writeVariantsFile(pid,retFilter,projection,coll2,resultCol,batch);
                        console.log(`varFileLoc:${varFileLoc}`)

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

                        // if hpoList is present, then include this criteria
                        if ( parsedJson['var_disc']['hpoList']) {
                            try {
                                var hpoList = parsedJson['var_disc']['hpoList'];
                                var varList = [];
                                var varPhen = await varPhenAnalysis(pid,hpoList,varList,assemblyInfo['reqAssembly'],parsedJson['seq_type']);
                                //console.log("Results from varPhenAnalysis ");
                                //console.dir(varPhen,{"depth":null});
                                if ( varPhen['status'] == 'no-variants') {
                                    await reqColl.updateOne({'_id':pid,},{$set:{'status':'no-variants'}});    
                                } else {
                                    await storeVarPhenRes(pid,varPhen,resultCol);
                                    await reqColl.updateOne({'_id':pid,},{$set:{'status':'completed'}});
                                    //console.log("Var Phen results stored in database")
                                }
                                // Send payload to indicate the process has completed
                                var payload = { "batch" : 0,
                                    "pid" : pid,
                                    "req" : 'last'
                                };
                                process.send(JSON.stringify(payload));

                            } catch(err) {
                                console.log(err);
                            }

                            // store the returned results in db
                        }

                    } else if (parsedJson['var_disc']['region']) {
                        var coll1 = db.collection(assemblyInfo['reqColl']); 
                        var batchCnt = 1;
                        var pid = await genReqID(reqTrackCollection,db,parsedJson);
                        // Generate request ID and send immediately
                        if ( parsedJson['var_disc']['hpoList']) {
                            var payload = { "batch" : 1,
                                            "pid" : pid,
                                            "req" : "done"
                                        };
        
                            createLog.debug("Send the immediate response payload")
                            createLog.debug(payload);
                            console.log("Sending response payload")
                            process.send(JSON.stringify(payload));  
                        }

                        await reqColl.updateOne({'_id':pid,},{$set:{'status':'inprogress'}});
                        //var pid = process.pid;
                        //pid = pid + (parsedJson['centerId'] * parsedJson['hostId']);

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
                        //createLog.debug(`Launching request with the input ${varFileLoc}`);
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

                                if ( parsedJson['var_disc']['hpoList']) {
                                    try {
                                        var hpoList = parsedJson['var_disc']['hpoList'];
                                        var varList = [];
                                        var varPhen = await varPhenAnalysis(pid,hpoList,varList,assemblyInfo['reqAssembly'],parsedJson['seq_type']);
                                        //console.log("Results from varPhenAnalysis ");
                                        //console.dir(varPhen,{"depth":null});
                                        if ( varPhen['status'] == 'no-variants') {
                                            await reqColl.updateOne({'_id':pid,},{$set:{'status':'no-variants'}});    
                                        } else {
                                            await storeVarPhenRes(pid,varPhen,resultCol);
                                            await reqColl.updateOne({'_id':pid,},{$set:{'status':'completed'}});
                                            //console.log("Var Phen results stored in database")
                                        }
                                        // Send payload to indicate the process has completed
                                        var payload = { "batch" : 0,
                                            "pid" : pid,
                                            "req" : 'last'
                                        };
                                        process.send(JSON.stringify(payload));
                                    } catch(err) {
                                        console.log(err);
                                    }
        
                                    // store the returned results in db
                                }
                                
                            } catch(err) {
                                createLog.debug("Logging error from varDiscController-liftover error");
                                createLog.debug(err);
                            }
                            
                        } catch(err) {
                            console.log(err);
                            console.log("Catch block of liftover");
                        }

                    }   
                } else if ( parsedJson['trio'] || parsedJson['family']) {
                    if ( parsedJson['trio'] && parsedJson['trio']['inheritance'] === "compound-heterozygous" ) {
                        createLog.debug("Inheritance Trio request")
                        //console.log("*******************************")
                        // suitable code section to create index for geneID in result collection
                        // Function to process filter and write results to resultCollection

                        var pid = await genReqID(reqTrackCollection,db,parsedJson);
                        //var pid = process.pid;
                        // For inheritance requests, this is just a dummy payload which is sent with the process id. The batch data is not yet loaded.
                        var payload = { "batch" : 1,
                                        "pid" : pid,
                                        "req" : "done"
                                    };
      
                        createLog.debug("Send the immediate response payload")
                        createLog.debug(payload);
                        console.log("Sending response payload")
                        process.send(JSON.stringify(payload));  

                        // 1. Before scanning, write the identified filtered variants to results collection
                        await trioVarResults(pid,projection,collection,resultCol,batch,retFilter,createLog);
                        
                        // ****************** ProbandFather (110) scanned against ProbandMother(101) *****
                        // 2. started with 110(proband-father) and scanned against 101(proband-mother)
                        createLog.debug("Invoke trioScanCH function with 101 Father as input. Compare with transcripts of Mom")
                        var reqFilter = {'pid':pid,'trio_code':"110"}
                        createLog.debug(reqFilter)
                        var logRes1 = await trioScanCH(pid,reqFilter,projection,resultCol,"101",createLog);
                        createLog.debug("Completed-trioScanCH function with 101 Father as input. Compare with transcripts of Mom")

                        // Clone filter and update trio code
                        var cloneFilter = {...reqFilter};
                        cloneFilter.trio_code = "101";
                        createLog.debug("Update the filter with trio_code 101");
                        
                        // ****************** ProbandMother (101) scanned against ProbandFather(110) *****
                        // 3. Repeat the scan. Filter Set : 101, scan against 110

                        //console.log("Logging the CLONE FILTER ****************************************");
                        //console.dir(cloneFilter,{"depth":null});
                        createLog.debug(cloneFilter);
                        createLog.debug("Invoke trioScanCH function with 110 Mother as input. Compare with transcripts of Dad")
                        var logRes2 = await trioScanCH(pid,cloneFilter,projection,resultCol,"110",createLog);

                        createLog.debug("Completed trioScanCH function with 110 Mother as input. Compare with transcripts of Dad")

                       
                        // Function to retrieve data from resultCollection,post process and write data (again) to result collection sorted by geneID
                        // set the batch to 1 and write results to result collection

                       // *********** Not required ************************************* 
                        /*var reqFileID = cloneFilter.fileID;
                        createLog.debug("Start- Write the (preprocess)results to results collection --1 ")
                        await trioVarResults(pid,reqFileID,projection,collection,resultCol,batch,cloneFilter,createLog);
                        createLog.debug("Finish- Write the (preprocess)results to results collection --1 ")*/

                        // Now clear the comp-het code - Add code
                        //await resetCompHetCode(reqFileID,collection,createLog);

                        // 4. Retrieve the variants based on pid,comp-het code and write the results to results collection
                        createLog.debug("Start- Write the (final)results to results collection ")
                       
                        await invokeTrioInheritResUpd(pid,resultCol,db,collection,assemblyInfo['reqAssembly'],createLog,parsedJson,batch);
                        createLog.debug("Finish- Write the (final)results to results collection ")

                    } else {
                        // else part - trio requests, trio inheritance(non comp het), family requests
                        //var pid = process.pid;
                        console.log("I am here but not allowed inside")
                        if ( parsedJson['trio'] && parsedJson['trio']['inheritance'])  {
                            var batchCnt = 1;
                            var pid = await genReqID(reqTrackCollection,db,parsedJson);
                            console.log("Generated pid "+pid);
                            console.log("here....")
                            // API based requests. maternal,paternal,recessive and denovo
                            if ( parsedJson['trio']['inheritance'] != "compound-heterozygous") {
                                var payload = { "batch" : 1,
                                            "pid" : pid,
                                            "req" : "done"
                                            };
        
                                createLog.debug("Send the immediate response payload")
                                createLog.debug(payload);
                                console.log("Sending response payload")
                                // Send the reponse payload immediately.
                                process.send(JSON.stringify(payload));
                                res = await processLogicalFilter(pid,retFilter,projection,batch,resultCol,db,collection,createLog,parsedJson,assemblyInfo['reqAssembly'],batchCnt);
                            }
                        } else {
                            //console.log("Now i am stepping in here.....");
                            var pid = await genReqID(reqTrackCollection,db,parsedJson);
                            console.log("generated pid is ...... "+pid)
                            
                            var batchCnt = 1;
                            if ( parsedJson['family']) {
                                //var pid = process.pid;
                                var payload = { "batch" : 1,
                                    "pid" : pid,
                                    "req" : "done"
                                };

                                // send process id immediately
                                process.send(JSON.stringify(payload));
                                await famVarResults(pid,projection,collection,resultCol,batch,retFilter,createLog,parsedJson,assemblyInfo['reqAssembly'],db,batchCnt);
                            } else {
                                // UI Trio Requests
                                res = await processLogicalFilter(pid,retFilter,projection,batch,resultCol,db,collection,createLog,parsedJson,assemblyInfo['reqAssembly'],batchCnt);
                            }
                        }
                                      
                    }
                } else {
                    // regular filters (Sample Discovery)
                    //console.log("This will be the last step......")
                    var batchCnt = 1;
                    var pid = await genReqID(reqTrackCollection,db,parsedJson);
                    //var pid = process.pid;
                    // query-fix
                    if ( parsedJson['api'] ) {
                        var payload = { "batch" : 1,
                                            "pid" : pid,
                                            "req" : "done"
                                            };
        
                                            
                        // query-fix
                        process.send(JSON.stringify(payload)); 
                    }
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
                //console.dir(posFilter,{"depth":null});
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
                    //console.log(obj);
                    process.send({"debug":obj});
                }
            } catch(err) {
                process.send({"err":err});
                client.close();
            }
        } else if ( parsedJson['invoke_type'] == "get_count" ) {
            try {
                //var reqID = process.pid;

                var reqID = await genReqID(reqTrackCollection,db,parsedJson);
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
                //var reqID = process.pid;
                var reqID = await genReqID(reqTrackCollection,db,parsedJson);
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

        var reqColl = db.collection(reqTrackCollection);
        // FIX28
        /*var dummyPayload = {"batch":1, "pid" : pid, "req": type};
        console.log("sending dummy payload")
        console.dir(dummyPayload,{"depth":null})
        process.send(JSON.stringify(dummyPayload));*/
        // FIX28

        //await new Promise(resolve => setTimeout(resolve,100000))
        console.log("Next step is here???")
        console.dir(filter,{"depth":null});
        var logStream = null;
        if ( parsedJson['var_disc']) {
            logStream = await collection.find(filter);
        } else {
            logStream = await collection.find(filter).sort(defaultSort);
        }
        
        logStream.project(projection); 
        var docs = 0;

        var timeField = new Date();
        //var batchCnt = 1;
        //var batchId = `batch${batchCnt}`
	    // Changing batchCnt for mat
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
            //console.log(doc);
            //console.log(docs);
            ++docs;
            bulkOps.push(doc);
            if ( bulkOps.length === batch ) {
                createLog.debug(`loadResultCollection ${batchCnt}`);
                //console.log(`Invoked results collection with batchcount ${batchCnt} and pid ${pid}`);
                loadResultCollection(createLog,resultCol,type,pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
                //console.log("Batch count is "+batchCnt);
                batchCnt++;
                bulkOps = [];
                loadHash = {};
            } 
        }

        // check & process the last batch of data
        if ( bulkOps.length > 0 ) {
            console.log("Now last set of data .... ")
            createLog.debug(`invoke result collection for batch ${batchCnt} last`);
            //loadResultCollection(createLog,resultCol,"last",pid,batchId,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo);
            if ( ! parsedJson['var_disc'] ) {
                type = "last"
            }
            //loadResultCollection(createLog,resultCol,"last",pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
            // Update-Variant Discovery : last batch has to be set to 1 only after the last assembly is processed
            console.log("hello... there")
            loadResultCollection(createLog,resultCol,type,pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
        }
        if ( ! docs ) {
            if ( parsedJson['var_disc'] ) {
                loadResultCollection(createLog,resultCol,type,pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
                //loadResultCollection(createLog,resultCol,"last",pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType)
            } else {
                // inheritance based requests - update the status in table.
                // Don't send the status
                if ( parsedJson['trio'] && parsedJson['trio']['inheritance'] ) {
                    reqColl.updateOne({'_id':pid},{$set:{'status':"no-variants"}})
                } else {
                    // sample-discovery and trio requests
                    // query-fix
                    // applicable for samp-disc api requests
                    if ( parsedJson['api'] || parsedJson['family']) {
                        reqColl.updateOne({'_id':pid},{$set:{'status':"no-variants"}});
                    } else {
                        // UI - samp disc, trio
                        console.log("Stepped here??")
                        process.send({"count":"no_variants"});
                    }
                    
                }
                
                // FIX28
                /*if ( parsedJson['trio']) {
                    loadResultCollection(createLog,resultCol,type,pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
                } else {
                    process.send({"count":"no_variants"});
                }*/
                // FIX28
            }
        }

        console.log("Batch count before returning result is "+batchCnt);
        //return "success";
        return batchCnt;
    } catch(err) {
        console.log(err)
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
        //console.log("loadLiftedVariants");
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
            var posData = storedKey.split('-');
            // update the below fields with the lifted position
            clonedDoc['chr'] = parseInt(posData[0]);
            clonedDoc['start_pos'] = parseInt(posData[1]);
            clonedDoc['ref_all'] = posData[2];
            clonedDoc['alt_all'] = posData[3];
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

    createLog.debug("Logging the idVal "+idVal);

    var lastBatch = 0;
    if ( reqVal === "last" ) {
        createLog.debug("LastBatch request with reqVal "+reqVal)
        lastBatch = 1;
    }

    loadToDB['_id'] = idVal;
    loadToDB['batch'] = batch;
    loadToDB['pid'] = pid;
    loadToDB['lastBatch'] = lastBatch;
    loadToDB['createdAt'] = timeField;

    //console.dir(loadToDB,{"depth":null})
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
        if ( parsedJson['trio'] && parsedJson['trio']['inheritance'] ) {
            createLog.debug("Trio Inheritance request")
            payload.batch = 0;
            if ( reqVal == "last") {
                process.send(JSON.stringify(payload));    
            } else {
                process.send(JSON.stringify({}));
            }
        } else {
            // FIX28
            /*if ( parsedJson['trio']) {
                process.send(JSON.stringify({}));
            } else {
                process.send(JSON.stringify(payload)); // to be added once the child_process is attached
            }*/
            // query-fix
            if ( parsedJson['api']  || parsedJson['family']) {
                payload.batch = 0;
                if ( reqVal == "last") {
                    process.send(JSON.stringify(payload));    
                } else {
                    process.send(JSON.stringify({}));
                }
            } else if (parsedJson['var_disc']['hpoList']) {
                process.send(JSON.stringify({}));
            } else {
                console.log("loadresults collection payload send")
                process.send(JSON.stringify(payload)); // to be added once the child_process is attached
            }

            createLog.debug("SampDisc, VarDisc or Trio request");
        }
        
    })
    .catch(function(err) {
        console.log("loadResultsCOllection catch block")
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
            // Remove the trailing and leading space of fname
            fname.trim();
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
                    rootFilter = { "fileID" : {$in:fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList},'alt_cnt': {$ne:0}};
                    altRootFilter = { "fileID" : {$in: fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList} ,'alt_cnt': {$ne:0}};
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
                } else if ( parsedJson['trio']['inheritance']) {
                    if (parsedJson['trio']['inheritance'] == "compound-heterozygous" ) {
                        rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': {$in:["110","101"]}, 'alt_cnt': {$eq:1}};
                        altRootFilter = { "fileID" : {$in: trioFileList}, 'trio_code' : {$in:["110","101"]}, 'alt_cnt': {$eq:1}};
                    }
                } else {
                    rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
                    altRootFilter = { "fileID" : {$in: trioFileList}, 'trio_code' : trioCode, 'alt_cnt': {$ne:0}};
                }
            } else if ( parsedJson['family']) {
                var famLocalID = parsedJson['family']['famLocalID'];
                // Code : 1- affected 2-unaffected
                var famCode = {$in:[1,2]};
                var famFileList = await fetchFamLocalList(db,famLocalID);
                if ( parsedJson['family']['IndividualID']) {
                    var indId = parsedJson['family']['IndividualID'];
                    // restrict filter search based on IndividualID/fileID
                    var famFileID = await fetchFamAnalysisFileID(db,famLocalID,indId);
                    rootFilter = { "fileID" : famFileID, famLocalID: famCode, 'alt_cnt': {$ne:0}};
                    altRootFilter = { "fileID" : famFileID, famLocalID : famCode, 'alt_cnt': {$ne:0}};
                } else {
                    rootFilter = { "fileID" : {$in:famFileList}, famLocalID: famCode, 'alt_cnt': {$ne:0}};
                    altRootFilter = { "fileID" : {$in: famFileList}, famLocalID : famCode, 'alt_cnt': {$ne:0}};
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
            } else if ( dbField == 'phen_gene' ) {
                filterRule = await getPhenFilterRule(condHash,db,createLog);
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
                //console.log("Logging the current level below");
                //console.log(currentLevel);
                //console.dir(filterRule);
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
        } else if (parsedJson['family']) {
            await db.collection(variantQueryCounts).updateOne({_id:reqID},{$set:{'result':result,status:"completed",'req_type' : 'family',processedTime:procTime}});
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
                rootFilter = { "fileID" : {$in:fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList},'alt_cnt': {$ne:0}};
                altRootFilter = { "fileID" : {$in: fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList} ,'alt_cnt': {$ne:0}};
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
            } else if ( parsedJson['trio']['inheritance']) {
                if (parsedJson['trio']['inheritance'] == "compound-heterozygous" ) {
                    rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': {$in:["110","101"]}, 'alt_cnt': {$eq:1}};
                    altRootFilter = { "fileID" : {$in: trioFileList}, 'trio_code' : {$in:["110","101"]}, 'alt_cnt': {$eq:1}};
                }
            } else {
                rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
                altRootFilter = { "fileID" : {$in: trioFileList}, 'trio_code' : trioCode, 'alt_cnt': {$ne:0}};
            }
            
        } else if ( parsedJson['family']) {
            var famLocalID = parsedJson['family']['famLocalID'];
            // Code : 1- affected 2-unaffected
            var famCode = {$in:[1,2]};
            var famFileList = await fetchFamLocalList(db,famLocalID);
            //console.log("Fetching the file ID list -- family")
            //console.log(famFileList)
            if ( parsedJson['family']['IndividualID']) {
                var indId = parsedJson['family']['IndividualID'];
                // restrict filter search based on IndividualID/fileID
                var famFileID = await fetchFamAnalysisFileID(db,famLocalID,indId);
                //console.log(famFileID)
                rootFilter = { "fileID" : famFileID,  'alt_cnt': {$ne:0}};
                rootFilter[famLocalID] = famCode;
                altRootFilter = { "fileID" : famFileID, 'alt_cnt': {$ne:0}};
                altRootFilter[famLocalID] = famCode;
            } else {
                rootFilter = { "fileID" : {$in:famFileList},  'alt_cnt': {$ne:0}};
                rootFilter[famLocalID] = famCode;
                altRootFilter = { "fileID" : {$in: famFileList}, 'alt_cnt': {$ne:0}};
                altRootFilter[famLocalID] = famCode;
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
            // Remove the trailing and leading space of fname
            fname.trim();
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
            } else if ( dbField == 'phen_gene' ) {
                filterRule = await getPhenFilterRule(condHash,db,createLog);
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
        //console.dir(rootFilter,{"depth":null})
        //console.dir(altRootFilter,{"depth":null})

        // Execute both queries in parallel
        var rootPromise = collection.find(rootFilter).count();
        var altRootPromise = collection.find(altRootFilter).count();


        const data = await Promise.all([rootPromise,altRootPromise]);
        var result = [];
        var count = data[0];
        var fCount = data[1];

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
        } else if (parsedJson['family']) {
            await db.collection(variantQueryCounts).updateOne({_id:reqID},{$set:{'result':result,status:"completed",'req_type' : 'family',processedTime:procTime}});
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


// Write the filtered results 110 + 101 to results collection
async function trioVarResults(pid,projection,collection,resultCol,batch,retFilter,createLog) {
    try {
        var defaultSort = {'chr':1,'start_pos':1};
        
        createLog.debug("trioVarResults- Logging the filter")
        createLog.debug(retFilter);
        createLog.debug("ProcessID "+pid);

        var logStream = await collection.find(retFilter).sort(defaultSort);
        logStream.project(projection); 
        var docs = 0;

        var timeField = new Date();
        var bulkOps = [];

        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            
            // clone the retrieved doc and add some fields
            var clonedDoc = { ... doc};
            var var_key = doc['var_key'];
            ++docs;
            createLog.debug("Writing document ..... "+docs);
            clonedDoc['pid'] = pid;
            clonedDoc['createdAt'] = timeField;
            bulkOps.push({"insertOne" : clonedDoc});

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
        createLog.debug("Documents written to results collection "+docs);

        /*if ( ! docs ) {
            process.send({"count":"no_variants"});
        }*/

        return "logged-result";
    } catch(err) {
        throw err;
    }
}

// Trio Inheritance function that write the results to resultCollection
// These results will once again be processed before it can retrieved.
async function trioVarResults_old(pid,reqFileID,projection,collection,resultCol,batch,cloneFilter,createLog) {
    try {
        var defaultSort = {'chr':1,'start_pos':1};
        // Filter for the specific fileID(proband fileID) and compound het variants
        // Here we can once again include the tree filters if required.
        // As the matching pair does not have all the filters applied
        var filter = {...cloneFilter};
        // remove trio_code from upFilter
        delete filter['trio_code'];
        // add comp-het:1 to the filter
        filter['comp-het'] = 1;

        createLog.debug("trioVarResults- Logging the updated filter")
        createLog.debug(filter);
        //var filter = { 'fileID': reqFileID, 'comp-het': 1 };

        var logStream = await collection.find(filter).sort(defaultSort);
        logStream.project({_id:0,'uid':0}); 
        //console.log(logStream)
        //logStream.project(projection); 
        var docs = 0;

        var timeField = new Date();
        var bulkOps = [];

        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            
            //console.log(doc)
            // clone the retrieved doc and add some fields
            var clonedDoc = { ... doc};
            var var_key = doc['var_key'];
            ++docs;
            createLog.debug("Updated Filter scanning document ..... "+docs);
            clonedDoc['pid'] = pid;
            clonedDoc['createdAt'] = timeField;
            bulkOps.push({"insertOne" : clonedDoc});

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

        /*if ( ! docs ) {
            process.send({"count":"no_variants"});
        }*/

        return "logged-result";
    } catch(err) {
        throw err;
    }
}

// Function to reset the compound het code
async function resetCompHetCode(idxFileID,collection,createLog) {
    try {
        var reqFilter = {'fileID' :idxFileID ,'comp-het':1};
        var setFilter = {'comp-het' : 0};
        createLog.debug("resetCompHetCode function")
        createLog.debug(reqFilter);
        createLog.debug(setFilter);
        await collection.updateMany(reqFilter, {$set:setFilter});
        createLog.debug("resetCompHetCode completed for reqFileID "+idxFileID);
    } catch(err) {
        console.log("Error in resetting comp-het code "+err);
    }
}


// Function to process the results specific to shared family variant analysis
async function famVarResults(pid,projection,collection,resultCol,batch,retFilter,createLog,parsedJson,assemblyInfo,db,batchCnt,type = "done") {
    try {
        var defaultSort = {'var_key' : 1};
        var resType = "requested";
        var reqColl = db.collection(reqTrackCollection);

        var hostInfo = {};
        var timeField = new Date();
        var specificAnnoObj = {"_id" : 0, "var_validate" : 0,"pid" : 0,"multi_allelic" : 0, "phred_polymorphism" : 0, "filter" : 0, "alt_cnt" : 0, "ref_depth" : 0, "alt_depth" : 0, "phred_genotype" : 0, "mapping_quality" : 0, "base_q_ranksum" : 0, "mapping_q_ranksum" : 0, "read_pos_ranksum" : 0, "strand_bias" : 0, "quality_by_depth" : 0, "fisher_strand" : 0, "vqslod" : 0, "gt_ratio" : 0, "ploidy" : 0, "somatic_state" : 0, "delta_pl" : 0, "stretch_lt_a" : 0, "stretch_lt_b" : 0, "fileID" : 0};

        var commonAnnoObj = { "gene_annotations" : 1, "RNACentral" : 1, "regulatory_feature_consequences" : 1, "motif_feature_consequences" : 1 , "gnomAD" : 1, "phred_score" : 1, "CLNSIG" : 1, "GENEINFO" : 1 , "chr":1, "start_pos" : 1};

        var specificAnno = Object.keys(specificAnnoObj);
        var commonAnno = Object.keys(commonAnnoObj);

        createLog.debug("famVarResults- Logging the filter")
        createLog.debug(retFilter);
        createLog.debug("ProcessID "+pid);

        var logStream = await collection.find(retFilter).sort(defaultSort);
        //console.log(retFilter);
        //console.dir(retFilter,{"depth":null});
        //logStream.project(projection); 
        var docs = 0;

        var timeField = new Date();
        var bulkOps = [];
        var loadHash = {};

        var varGroup = 0;
        var varCnt = 0;
        var dataSet = {};
        var fileAnnoArr = [];

        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            var clonedDoc = { ... doc};
            var finalDoc = {};
            var var_key = doc['var_key'];
            //console.log("Loop "+ doc['var_key']);
            ++docs;
            createLog.debug("Writing document ..... "+docs);
            //finalDoc['pid'] = pid;
            //finalDoc['createdAt'] = timeField;

            //console.log(varGroup);
            //console.log(doc['var_key']);
            if ( varGroup && doc['var_key'] != varGroup ) {
                //console.log("new variant key");
                //console.log(doc['var_key']);
                // store the variants 
                // prepare the filter
                // clone the retrieved doc and add some fields
                // document to be formatted.
                finalDoc['var_key'] = varGroup;
                // variant annotations
                finalDoc['annotations'] = dataSet['annotations'];
                // vcf annotations.
                finalDoc['vcfAnno'] = fileAnnoArr; 
                //console.log(finalDoc);
                bulkOps.push(finalDoc);

                varCnt =  0;
                dataSet = {};
                fileAnnoArr = [];
            }
            
            
            //console.log(`bulkops ${bulkOps.length} batch : ${batch}`);
            if ( bulkOps.length === batch ) {
                //createLog.debug(`loadResultCollection ${batchId}`);
                //const res1 = await resultCol.bulkWrite(bulkOps);
                //console.log("Calling result collection ------ famVarResults");
                loadResultCollection(createLog,resultCol,type,pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
                batchCnt++;
                loadHash = {};
                //batchId = batchCnt;
                bulkOps = [];
            } 

            // variant processing

            // storing the data related to the variant in the dataSet hash
            var id = doc['_id'];
            var fileID = doc['fileID'];
            //console.log("variant print "+doc['var_key']);
            dataSet['var_key'] = doc['var_key'];
            dataSet[fileID] = 1;
            var commonObj = lodash.omit(clonedDoc,specificAnno);
            var vcfAnno = lodash.omit(clonedDoc,commonAnno);
            fileAnnoArr.push(vcfAnno);
            //console.log("Logging common annotations ------ ");
            //console.log(commonObj);
            //console.log("Logging vcf annotations ----");
            //console.log(vcfAnno);
            dataSet['annotations'] = commonObj;
            varGroup = doc['var_key'];
            //console.log("old variant key");
            //console.log(varGroup);
        }

        // check & process the last batch of data
        // check and add this later . To Do 
        
        if ( bulkOps.length > 0 ) {
            //createLog.debug(`invoke result collection for batch ${batchId} last`);
            //const res1 = await resultCol.bulkWrite(bulkOps);
            type = "last";
            loadResultCollection(createLog,resultCol,type,pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
        }

        if ( ! docs ) {
            reqColl.updateOne({'_id':pid},{$set:{'status':"no-variants"}});
        }
        createLog.debug("Documents written to results collection "+docs); 

        /*if ( ! docs ) {
            process.send({"count":"no_variants"});
        }*/

        return "logged-result";
    } catch(err) {
        throw err;
    }
}


// Apply this in the result collection, based on pid
async function trioScanCH(pid,filter,projection,resultCol,tr_code,createLog) {
    try {

        // filter executed in result collection. indexed on pid
        var logStream = await resultCol.find(filter);
        //console.log(logStream)
        logStream.project(projection); 
        console.log("Received results from the logStream")
        var docs = 0;

        var timeField = new Date();
        var bulkOps = [];

        var count = 0;
        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            count++;
            createLog.debug("Scanning variant count .... "+count);
            
            var src_var = doc.var_key;
            createLog.debug("Retrieved source variant ")
            createLog.debug(src_var)
            var transcripts = [];
            if ( doc.gene_annotations.length > 0 ) {
                // returns an iterator object
                var val = doc.gene_annotations.values();
                //console.log(val.next().value);
                transcripts = await getTranscripts(val);
                //console.log(transcripts);
            }
            // Prepare filter to search the variants in index-mother having the transcripts
            //var tran_filter = {'pid' : pid, trio_code: tr_code, 'gene_annotations.transcript_id' : {$in : transcripts},'comp-het' : {$ne : 1}};
            var tran_filter = {'pid' : pid, trio_code: tr_code, 'gene_annotations.transcript_id' : {$in : transcripts}};
            
            createLog.debug("Logging the transcript filter ---- ")
            createLog.debug(tran_filter);
            var variants = await fetchCHMatch(tran_filter,resultCol);
            createLog.debug("Logging the identified comp-het variant")
            createLog.debug(variants);
            //console.log(variants)
            // Variants 
            if ( variants.length > 0 ) {
                // If there was a variant identified for the source variant transcript, also include the source variant to the variants array.
                variants.push(src_var);
                //console.log("Logging source variant");
                //console.log(src_var);
                var update_filter = {'pid':pid,'var_key' : {$in:variants}};
                //console.log("Executed update filter for the below combination");
                //console.dir(update_filter,{"depth":null});
                // Sets comp-het to 1 for the variant (src-var) and the corresponding variants which have the src-var transcript.
                var set_filter = {'comp-het':1}
                createLog.debug("Added src variant.Identified compound-het variant ......");
                createLog.debug(variants);
                // update comp-het:1 for these variants
                await resultCol.updateMany(update_filter, {$set:set_filter});
            }
        }
        return "logged-result";
    } catch(err) {
        throw err;
    }
}

// Function to fetch the transcripts from the transcripts object
// tran_obj is an iterator composed as array of hashes - array of transcript hashes.
async function getTranscripts(tran_obj) {
    var transcripts = [];
    try {
        for ( var val of tran_obj ) {
            if ( val.transcript_id ) {
                transcripts.push(val.transcript_id);
            }
        }
    } catch(err) {
        throw err;
    }
    return transcripts;
}

// Function to retrieve the matching variants based on the transcripts provided as filter
async function fetchCHMatch(tran_filter,collection) {
    var variants = [];
    try {
        //console.dir(tran_filter,{"depth":null});
        var logStream = await collection.find(tran_filter);
        logStream.project({'var_key':1,'trio_code':1});
        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            //console.log(doc);
            if ( doc.var_key ) {
                variants.push(doc.var_key)
            }
        }
    } catch(err) {
        throw err;
    }
    return variants;
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

// Get the file ID corresponding to the family local ID and individual ID
async function fetchFamAnalysisFileID(db,famLocalID,indId) {
    try {
        var query = {"_id" : famLocalID,"Status" : "Completed","fam_opts.individualID" : indId};
        console.log(query);
        var famFileID = "";
        var res = await db.collection(famAnalysisColl).findOne(query,{projection:{'fam_opts.fileID.$':1,_id:0}});
        if ( res ) {
            famFileID = res['fam_opts'][0]['fileID'];
        }
        return famFileID;
    } catch(err) {
        throw err;
    }
}

// Get the fileID list corresponding to the family Local ID
async function fetchFamLocalList(db,famLocalID) {
    try {
        var fileIDList = [];
        var query = {"_id" : famLocalID,"Status" : "Completed"};
        //console.log(query);
        var res = await db.collection(famAnalysisColl).find(query,{projection:{'fam_opts.fileID': 1}});
        //var res = await db.collection(famAnalysisColl).find(query);
        //console.log(res);
        while ( await res.hasNext()) {
            const doc = await res.next();
            //console.log(doc);
            if ( doc['fam_opts'] ) {
                var famList = doc['fam_opts'];
                for ( var idx in famList ) {
                    var elem = famList[idx];
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


// rel = "Proband" or "Father" or "Mother"
async function fetchTrioRelFileID(db,trioLocalID,rel) {
    try {
        var query = {"TrioLocalID" : trioLocalID,"TrioStatus" : "completed","trio.relation": rel};
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

async function fetchFamBuildType(db,famLocalID) {
    try {
        var query = {"_id" : famLocalID,"Status" : "Completed"};
        var assembly = "";
        //console.log("fetchFamBuildType")
        //console.log(famAnalysisColl)
        //console.log(query);
        var res = await db.collection(famAnalysisColl).findOne(query,{projection:{'details.assemblyType': 1,_id:0}});
        //console.log(res);
        if ( res ) {
            assembly = res['details']['assemblyType'];
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

// Workaround fix to include the lifted positions to the filter
async function positionFilterVarDiscLift(condHash,regionMap,createLog) {
    try {
        var filterRule = {};
        var altRule = condHash['add_rule_pos'];
        var chr_pos = regionMap.split('-');
        //console.log("Logging altRule inside positionFilterRule");
        //console.log(altRule);
        var start_pos; var stop_pos;
        if ( altRule['start_pos']) {
            //start_pos = altRule['start_pos'] || 0;
            start_pos = parseInt(chr_pos[2]);
        }
        if ( altRule['stop_pos']) {
            //stop_pos = altRule['stop_pos'] || 0;
            stop_pos = parseInt(chr_pos[1]);
        }
        //filterRule['$and']  = [ {"chr" : condHash['rule']}, {'start_pos' : {$lte : start_pos}}, {'stop_pos' : {$gte : stop_pos}} ];
        filterRule['$and']  = [ {"chr" : {"$eq":parseInt(chr_pos[0])}}, {'start_pos' : {$lte : start_pos}}, {'stop_pos' : {$gte : stop_pos}} ];
        createLog.debug("Updated lifted filter rule below ");
        createLog.debug(filterRule,{"depth":null});
        return filterRule;
    } catch(err) {
        throw err;
    }
}

async function fetchChrPosFilterRule(condHash,createLog) {
    try {
        var altRule = condHash['add_rule_pos'];
        var start_pos; var stop_pos;
        var region = "";
        if ( altRule['start_pos']) {
            start_pos = altRule['start_pos'] || 0;
        }
        if ( altRule['stop_pos']) {
            stop_pos = altRule['stop_pos'] || 0;
        }
        // Why are we swapping : Chr based filters with region - UI/DB already swaps start and stop_pos. 
        // We need to swap here as this result will be next sent to liftover.
        region = condHash.rule.$eq + "-" + stop_pos + "-" + start_pos;
        createLog.debug("Region is "+region);
        return region;
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

// Function to read the string of geneIDs and convert to array format
async function getPhenFilterRule(condHash,db,createLog) {
    try {
        var filterRule = {};

        if (!condHash['rule']['formula'] ) {
            // delete formula key
            delete condHash['rule']['formula'];
        }

        var filterType = getFilterType(condHash);
        var geneStr = condHash['rule'][filterType];
        
        var geneList = [];
        for ( var idx in geneStr ) {
            var gene = geneStr[idx];
            //console.log(gene);
            // check and remove leading space
            var re = /,\s+/g;
            var re1 = /^\s+/g;
            gene = gene.replace(re1,'');
            if (gene.match(re)) {
                gene = gene.replace(re, ',');
                //createLog.debug("New String is "+chr);   
            }
            //console.log("After removing space :")
            //console.log(gene)
            var tmpList = gene.split(",");
            geneList.push(...tmpList);
            //console.log(tmpList)
        }
        //console.log("Gene List after processing :");
        //console.log(geneList);
        var dbField = 'gene_annotations.gene_id';
        condHash['rule'][filterType] = geneList;
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

async function logicalFilterProcess(db,parsedJson, mapFilter,createLog,regionMap=false) {
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
                rootFilter = { "fileID" : {$in:fileIDList}, 'gene_annotations.gene_id' : {$in:geneIDList},'alt_cnt': {$ne:0}};
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
            //console.log("TRio *******************************")
            var trioLocalID = parsedJson['trio']['trioLocalID'];
            var trioCode = parsedJson['trio']['trio_code'];
            var trioFileList = await fetchTrioLocalList(db,trioLocalID);
            if ( parsedJson['trio']['IndividualID']) {
                var indId = parsedJson['trio']['IndividualID'];
                // restrict filter search based on IndividualID/fileID
                var trFileID = await fetchTrioFileID(db,trioLocalID,indId);
                rootFilter = { "fileID" : trFileID, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
            } else if ( parsedJson['trio']['inheritance']) {
                //console.log("logicalFilter Inheritance----------")
                if (parsedJson['trio']['inheritance'] == "compound-heterozygous" ) {
                    var fID = await fetchTrioRelFileID(db,trioLocalID,"Proband")
                    //rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': {$in:["110","101"]}, 'alt_cnt': {$eq:1}};
                    //rootFilter = { "fileID" : fID, 'trio_code': {$in:["110","101"]}, 'alt_cnt': {$eq:1}};
                    // Fetch only variants which are present in Index and Father.
                    // Begin analysis with variants present in Index and Father with a heterozygous variant filter
                    // To be reviewed : comp-het criteria should be removed from the root filter
                    //rootFilter = { "fileID" : fID, 'trio_code': {$in:["110"]}, 'alt_cnt': {$eq:1}, 'comp-het': {$ne:1},'gene_annotations' : {$ne : [ ]}};

                    // Retrieve the heterozygous variants present in proband-father(110) and proband-mother(101)
                    rootFilter = { "fileID" : fID, 'trio_code': {$in:["110","101"]}, 'alt_cnt': {$eq:1},'gene_annotations' : {$ne : [ ]}};
                } else {
                    // includes all the other scenarios
                    // maternal, paternal, denovo, recessive
                    
                    var inh_type = parsedJson['trio']['inheritance'];
                    var fID = await fetchTrioRelFileID(db,trioLocalID,"Proband")
                    var trio_code_hash = {"maternal" : "101", 
                                           "paternal" : "110", 
                                           "denovo" : "100",
                                           "recessive" : "111"
                                        };
                    var tr_code = trio_code_hash[inh_type];
                    createLog.debug("Logging the rootFilter in logicalFilterProcess");
                    // exluding hom-ref variants
                    rootFilter = { "fileID" : fID, 'trio_code': {$in:[tr_code]}, 'alt_cnt': {$ne:0}};
                    createLog.debug(rootFilter);
                }
            } else {
                rootFilter = { "fileID" : {$in:trioFileList}, 'trio_code': trioCode, 'alt_cnt': {$ne:0}};
            }   
        } else if ( parsedJson['family']) {
            var famLocalID = parsedJson['family']['famLocalID'];
            // Code : 1- affected 2-unaffected
            var famCode = {$in:[1,2]};
            var famFileList = await fetchFamLocalList(db,famLocalID);
            if ( parsedJson['family']['IndividualID']) {
                var indId = parsedJson['family']['IndividualID'];
                // restrict filter search based on IndividualID/fileID
                var famFileID = await fetchFamAnalysisFileID(db,famLocalID,indId);
                //rootFilter = { "fileID" : famFileID, 'alt_cnt': {$ne:0}};
                // don't exclude hom-ref variants
                rootFilter = { "fileID" : famFileID};
                rootFilter[famLocalID] = famCode;
            } else {
                //rootFilter = { "fileID" : {$in:famFileList}, 'alt_cnt': {$ne:0}};
                // don't exclude hom-ref variants
                rootFilter = { "fileID" : {$in:famFileList}};
                rootFilter[famLocalID] = famCode;
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
                        if ( regionMap!= false ) {
                            filterRule = await positionFilterVarDiscLift(condHash,regionMap,createLog);
                        } else  {
                            filterRule = await positionFilterRule(condHash);
                        }
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
        //console.log(err);
        createLog.debug("Logging an error in this function ");
        createLog.debug(`${err}`);
        throw err;
    }  
}

async function logicalFilterProcessRegion(db,parsedJson, mapFilter,createLog) {
    try {
        //console.dir(parsedJson);
        createLog.debug("Inside logicalFilterProcess function");
        var conditions = parsedJson['condition'];

        var regions_map = [];
        for ( var cIdx in conditions ) {
            var filters1 = conditions[cIdx];
            createLog.debug("filter1 size is "+filters1.length);
            for ( var fIdx in filters1 ) {
                var filterRule = {};
                var filters = filters1[fIdx];
                var condHash = filters['c'];
                if ( condHash['add_rule_pos'] ) {
                    // region contains chr-start_position-stop_position
                    var region = await fetchChrPosFilterRule(condHash,createLog);
                    createLog.debug("region is "+region);
                    regions_map.push(region);
                }

            } // filters array
        } // conditions array
        createLog.debug(regions_map);
        return regions_map;
    } catch(err) {
        //console.log(err);
        createLog.debug("Logging an error in this function ");
        createLog.debug(`${err}`);
        throw err;
    }
}


async function invokeTrioInheritRes(pid,resultCol,assemblyInfo,createLog,parsedJson,batch,type = "done") {
    try {

        var filter = { 'pid': pid, 'trio_uid': { $exists: true } };
        var trioSort = {'gene_annotations.gene_id':1,'var_key':1};
        var logStream = await resultCol.find(filter).sort(trioSort);
        logStream.project({_id:0,'var_key':0,'uid':0}); 
        var docs = 0;
        var batchCnt = 1;
        var resType = "requested";

        var timeField = new Date();
        
        //console.log("Batch id received as input is "+batchCnt);
        var loadHash = {};
        var bulkOps = [];

        createLog.debug(`Batch is ${batchCnt}`);
        var hostInfo = {};

        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            var clonedDoc = { ... doc};
            ++docs;
            var storedKey = clonedDoc['trio_uid'];
            delete clonedDoc['trio_uid'];
            //clonedDoc['var_key'] = storedKey;
            bulkOps.push(clonedDoc);
            if ( bulkOps.length === batch ) {
                createLog.debug(`loadResultCollection ${batchCnt}`);
                console.log(batchCnt)
                console.log("Calling loadResultCollection")
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


async function invokeTrioInheritResUpd(pid,resultCol,db,collection,assemblyInfo,createLog,parsedJson,batch,type = "done") {
    try {

        //var filter = { 'fileID': reqFileID, 'comp-het': 1 };
        var filter = {'pid':pid,'comp-het':1}
        var trioSort = {'gene_annotations.gene_id':1};
        var logStream = await resultCol.find(filter).sort(trioSort);
        //console.log(filter)
        //console.log(trioSort);
        //var logStream = await collection.find(filter).sort(trioSort);
        logStream.project({'comp-het':0, _id:0}); 
        var docs = 0;
        var batchCnt = 1;
        var resType = "requested";

        var timeField = new Date();
        var reqColl = db.collection(reqTrackCollection);
        //console.log("Batch id received as input is "+batchCnt);
        var loadHash = {};
        var bulkOps = [];

        createLog.debug(`Batch is ${batchCnt}`);
        var hostInfo = {};

        while ( await logStream.hasNext() ) {
            const doc = await logStream.next();
            //console.log(doc)
            //var clonedDoc = { ... doc};
            ++docs;
            //var storedKey = clonedDoc['trio_uid'];
            //delete clonedDoc['trio_uid'];
            //clonedDoc['var_key'] = storedKey;
            //bulkOps.push(clonedDoc);
            bulkOps.push(doc);
            if ( bulkOps.length === batch ) {
                createLog.debug(`loadResultCollection ${batchCnt}`);
                //console.log(batchCnt)
                //console.log("Calling loadResultCollection")
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
            if ( parsedJson['trio'] && parsedJson['trio']['inheritance'] ) {
                reqColl.updateOne({'_id':pid},{$set:{'status':"no-variants"}})
            }
            /*if ( parsedJson['var_disc'] ) {
                loadResultCollection(createLog,resultCol,"last",pid,batchCnt,timeField,bulkOps,hostInfo,parsedJson,assemblyInfo,resType);
            }*/
            //process.send({"count":"no_variants"});
        }
        createLog.debug("Documents added to result collection "+docs);

        //console.log("Batch count before returning result is "+batchCnt);
        //return "success";
        return batchCnt;
    } catch(err) {
        throw err;
    }
}

// Function to generate requestID for query requests(count and sample-query)
// This process is valid for Sample Discovery, Trio & Variant Discovery.
async function genReqID(reqCollName,db,parsedJson) {
    // Get the last inserted ID
    var reqColl = db.collection(reqCollName);
    var res = await reqColl.find({}).sort({_id:-1}).limit(1);
    const doc = await res.next();
    var timeField = new Date();
    var pid;
    if ( doc == null ) {
        console.log("Collection not created.insert with _id 1");
        pid = 1;
        if ( parsedJson['var_disc']) {
            pid = pid + (parsedJson['centerId'] * parsedJson['hostId']);
        }
        await reqColl.insertOne({'_id':pid,'createdAt':timeField,'status':'created'});
    } else {
        // latest pid 
        pid = doc._id;
        // increment pid
        pid = pid + 1; 
        // if variant disc, then include additional logic for pid
        if ( parsedJson['var_disc']) {
            pid = pid + (parsedJson['centerId'] * parsedJson['hostId']);
        }

        // confirm if the pid does not exist
        var res = await reqColl.findOne({_id:pid});
        if ( res ) {
            //console.log("increment and insert")
            // if pid exists, increment pid and insert
            pid = pid + 1;
            await reqColl.insertOne({'_id':pid,'createdAt':timeField,'status': 'created'});
        } else {
            //console.log("just insert")
            await reqColl.insertOne({'_id':pid,'createdAt':timeField,'status': 'created'});
        }
        //console.log(res);
        //console.log("insert a doc with a incremented ID");
    }
    //console.log(`Logging generated pid ${pid}`);
    return pid;
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
        var idx1 = {"gene_annotations.gene_id" : 1 };
        await createColIndex(db,resultCollection,idx1);
    }
    return resCol;
}

async function readFile(variantFile,varContLog) {
    var rd;
    try {
        var liftedVariant = null;
        if ( ! fs.existsSync(variantFile)) {
            return liftedVariant;
        } else {
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

// store the variant phenotype results which has the counts and annotations.
async function storeVarPhenRes(pid,varPhen,resultCol) {
    try {
        var loadToDB = {};
	    loadToDB['_id'] = pid;
        var timeField = new Date();
	    loadToDB['createdAt'] = timeField;
	    loadToDB['documents'] = varPhen;
        loadToDB['status'] = 'completed';
	 
	    await resultCol.insertOne(loadToDB);
        return "success";
    } catch(err) {
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



