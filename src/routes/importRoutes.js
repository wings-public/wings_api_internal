const spawn  = require('child_process');
const runningProcess = require('is-running');
var path = require('path');
var zlib = require('zlib');
const { createReadStream, createWriteStream, stat } = require('fs');
const promisify = require('util').promisify;
var stats = promisify(stat);
var multer = require('multer');
var loggerMod = require('../controllers/loggerMod');
//var upload1 = multer({dest : '/VariantDB_Mongo/entityApp/log/',filename: function(req,file,cb) { cb(null,file.originalname);}});
var storage = multer.diskStorage( {destination : (req,file,cb) => {cb(null,'/VariantDB_Mongo/entityApp/log/')}  , filename: (req,file,cb) => {cb(null,file.originalname)} });
var upload1= multer({storage: storage});
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const checkTrioMember = require('../controllers/entityController').checkTrioMember;
const triggerAssemblySampTrio = require('../controllers/entityController').triggerAssemblySampTrio;
const configData = require('../config/config.js');
const { app:{importQCnt},db : {dbName,importStatsCollection,sampleSheetCollection,sampleAssemblyCollection,fileMetaCollection,hostMetaCollection,indSampCollection,individualCollection,familyCollection} } = configData;
//const { db : {annotationEngine,annotationEngineUrl,annoApiUser}, app:{instance} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;

const getConnection = require('../controllers/dbConn.js').getConnection;
// Queue Process Modules
const {default: PQueue} = require('p-queue');
// create a new queue, and pass how many you want to scrape at once

const queue = new PQueue({ concurrency: parseInt(importQCnt) });
const importQueueScraper = require('../routes/queueScrapper').importQueueScraper;
const importQueueScraperAnno = require('../routes/queueScrapper').importQueueScraperAnno;

const importRoutes = (app) => {
    app.route('/importVCF')
    .post( loginRequired,async (req,res,next) => {
        // Request body contains the VCF Sample ID and VCF File Path/VCF URL
        var reqBody = req.body;
        console.log("Logging request body");
        console.log(reqBody);
        var sample = reqBody['Location'];
        var fileId = reqBody['fileID'];
        var batchSize = reqBody['batchSize'];
        var assemblyType = reqBody['assemblyType'];
        var seqType = reqBody['seqType'];
        var sampleLocalID = reqBody['sampleLocalID'];
        var indLocalID = reqBody['indLocalID'];
        var indID = reqBody['IndividualID'];
        var panelType = reqBody['panelType'];
        var fileType = reqBody['fileType'];

        var pid = process.pid;
        //console.log(res);
        console.log("Received Request for Sample *************** "+sample);
        console.log("Received Request for sample FILE ID ************** "+fileId);
        console.log("Process Related to this is "+pid);
        try {
             
            if ( ! sample  || ! fileId  || ! assemblyType || ! seqType || ! sampleLocalID || ! indLocalID || ! indID ) {
                throw "JSON Structure Error";
            }

            if ( seqType.toUpperCase() == "PANEL") {
                if ( !panelType ) {
                    throw "Panel Type mandatory for PANEL sequencing type";
                }
            }

            var urlRe = /https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)/i;
            //var serverRe = /^[\/a-zA-Z_0-9-\.]+\.(vcf|vcf.gz)$/i;
            var serverRe = /^[\/a-zA-Z_0-9-\.]+\.(vcf|vcf.gz|gvcf|gvcf.gz)$/i;
            if ( !sample.match(urlRe) && !sample.match(serverRe)) {
                throw "VCFLocation: Only https/http protocol supported for URL (OR) Linux Server file locations supported.Supported FileExtensions: .vcf or .vcf.gz";
            }

            if (['hg19','hg38','GRCh37','GRCh38'].indexOf(assemblyType) < 0 ) {
            //if ( assemblyType != 'hg19' || assemblyType != 'hg38' || assemblyType != 'GRCh37' || assemblyType != 'GRCh38' ) {
                throw "assemblyType: Supported options are hg19/hg38/GRCh37/GRCh38";
            }

            // Add logic to load the data into a table in mongo and respond as import scheduled
            // If process is triggered in same millisecond, then this ID will be same. Check if there is such a scenario
            // to prevent the risk of of two import sample requests in the same millisecond, appending sampleID to the created uDateId
            //var uDateId = new Date().valueOf();
            var uDateId = Date.now();
            //console.log('uDateId is '+uDateId);
            var tmpId = parseInt(fileId);
            console.log('tmpId is '+tmpId);
            uDateId = uDateId + tmpId;
            //console.log("Unique Date based ID is "+uDateId);

            var importMsg = {"id":uDateId,"status_description":"Import Request Scheduled",'status_id' : 1,'error_info' : null};
            res.status(200).json({"message":importMsg});
            // search if an entry exists in sample sheet collection
            var orCriteria = {};
            if ( (assemblyType == "hg19") || ( assemblyType == "GRCh37")) {
                orCriteria['cond'] =  [{AssemblyType: "hg19"},{AssemblyType: "GRCh37"}];
            } else if ((assemblyType == "hg38") || ( assemblyType == "GRCh38")) {
                orCriteria['cond'] = [{AssemblyType: "hg38"},{AssemblyType: "GRCh38"}];
            }
            

            //res.status(200).json({"id":uDateId,"message":"Import Request Scheduled"});

            var client = getConnection();
            const db = client.db(dbName);
            var statsColl = db.collection(importStatsCollection);
            var sidAssemblyColl = db.collection(sampleAssemblyCollection);

            const indColl = db.collection(individualCollection);
            const famColl = db.collection(familyCollection);

            // sample sheet entry for the below query will be updated with status 'inprogress'
            // search with upper case sequence type
            var search = { status: 'queued', FileLocation: sample, SeqTypeName: seqType.toUpperCase(),IndLocalID: indLocalID, $or : orCriteria['cond'] };
            var sampShColl = db.collection(sampleSheetCollection);
            // add fileID to the sample sheet collection
            await sampShColl.updateOne(search, {$set:{status: "inprogress",fileID: fileId}});
            // Also check and insert the IndividualID, fileID combination to the new collection. TODO

            indID = parseInt(indID);
            var uID = parseInt(fileId)+"-"+parseInt(indID);

            var exists = await db.collection(indSampCollection).findOne({_id:uID});

            var indFamily = await indColl.findOne({'_id':indID},{'projection':{'familyId':1}});

            var familyID = "";
            var trioStatus = 0;
            if ( ! exists ) {
                // check if Individual has family.
                // If yes, check if Individual is part of trio
                // If yes, include trio:1 in the insert request

                if ( indFamily ) {
                    familyID = indFamily['familyId'];
                    trioStatus = await checkTrioMember(indID,familyID);
                }
                // fileType was hardcoded as 'VCF' This will be replaced with the request parameter to distinguish VCF and gVCF
                await db.collection(indSampCollection).insertOne({_id: uID, SampleLocalID : sampleLocalID,IndLocalID : indLocalID,SeqTypeName : seqType.toUpperCase(),FileType: fileType,AssemblyType : assemblyType, individualID : indID, fileID: parseInt(fileId),trio:trioStatus,'panelType': panelType.toUpperCase()});
            }


            // update stats for sample sheet entry to inprogress
            //console.log(statsColl);

            var statsData = {'_id' : uDateId, 'fileID':fileId, 'sample_loc':sample, 'status_description':'Import Request Scheduled','status_id' : 1,'error_info' : null,'assembly_type': assemblyType, 'sched_time': new Date(),'status_log' : [{status:"Import Request Scheduled","log_time":new Date()}]};
            await statsColl.insertOne(statsData);

            // Traverse the Table and start the process. Make sure at a time, there are only 4 import process

            // Trigger Process Start
            var parsePath = path.parse(__dirname).dir;
            //var logFile = path.join(parsePath,'import','log',`import-route-logger-${fileId}-${uDateId}.log`);
            var logFile = `import-route-logger-${fileId}-${uDateId}.log`;
            var createLog = loggerMod.logger('import',logFile);
            createLog.debug("Import Request Scheduled");

            var qcount = 0;
            queue.on('active', () => {
                console.log(`Working on item #${++qcount}.  Size: ${queue.size}  Pending: ${queue.pending}`);
                createLog.debug(`Working on item #${++qcount}.  Size: ${queue.size}  Pending: ${queue.pending}`);
            });

            queue.add( async () => { 
                try {
                    await importQueueScraper(uDateId,sample,fileId,batchSize,assemblyType);
                    createLog.debug("await completed for importQueueScrapper");
                    // include annotation version if defined
                    var annoVer = "";
                    if ( process.env.CURR_ANNO_VER ) {
                        annoVer = process.env.CURR_ANNO_VER;
                    }

                    await statsColl.updateOne({'_id':uDateId},
                    {$set : {'status_description' : 'Import Request Completed','status_id':8,'finish_time': new Date(), "anno_ver": annoVer},
                    $push: {'status_log': {status:"Import Request Completed",log_time: new Date()}}});
                    // Commented - 28/06/2023 This collection is not used in trio.
                    //await sidAssemblyColl.insertOne({'_id':fileId,'assembly_type':assemblyType, "fileID": parseInt(fileId)});
                    // update status for sample sheet entry
                    await sampShColl.updateOne({"fileID":fileId }, {$set:{status: "import success"}});
                    // Here include trip related checks
                    createLog.debug("Import Request Completed");
                    console.log("Done : Import Task"); 
                    console.log("trioStatus is "+trioStatus);
                    if (trioStatus) {
                        await triggerAssemblySampTrio(familyID,seqType,assemblyType);
                    }
                    
                } catch(err) {
                     var id  = {'_id' : uDateId,'status_id':{$ne:9}};
                     var set = {$set : {'status_description' : 'Error', 'status_id':9 , 'error_info' : "Error occurred during import process",'finish_time': new Date()}, $push : {'status_log': {status:"Error",log_time: new Date()}} };
                     await statsColl.updateOne(id,set);
                     // update status for sample sheet entry
                     await sampShColl.updateOne({"fileID":fileId }, {$set:{status: "import failed"}});
                     createLog.debug("Adding error for importQueueScrapper");
                     createLog.debug("Import Request Error");
                     createLog.debug(err);
                     console.log(err);
                }
            } );

            createLog.debug("******************* QUEUE STATS *************************");
            createLog.debug('Added : importQueue to import router Queue');
            createLog.debug(`Queue Size : ${queue.size}`);
            createLog.debug(`Pending promises: ${queue.pending}`);
            
            // nodejs request module and trigger https Annotation Request
            // handler specific to parent process
            process.on('beforeExit', (code) => {
                createLog.debug(`--------- About to exit PARENT PROCESS with code: ${code}`);
                console.log(`--------- About to exit PARENT PROCESS with code: ${code}`);
            });
        } catch(err) {
            console.log("*************Looks like we have received an error message");
            console.log(err);
            next(`${err}`);
        }
    });

    app.route('/annotateVCF')
    .post( loginRequired,async (req,res,next) => {
        // Request body contains the VCF Sample ID and VCF File Path/VCF URL
        var reqBody = req.body;
        console.log("Logging request body");
        console.log(reqBody);
        var fileId = reqBody['fileID'];
        var assemblyType = reqBody['assemblyType'];
        // VEP or CADD 
        var annoType = reqBody['annoType'];

        // regulatory,maxentscan
        var annoField = reqBody['annoField'];

        var pid = process.pid;
        //console.log(res);
        console.log("Received Request for sample FILE ID ************** "+fileId);
        console.log("Process Related to this is "+pid);
        try {
             
            if (  ! fileId  || ! assemblyType  ) {
                throw "JSON Structure Error";
            }

            if (['hg19','hg38','GRCh37','GRCh38'].indexOf(assemblyType) < 0 ) {
            //if ( assemblyType != 'hg19' || assemblyType != 'hg38' || assemblyType != 'GRCh37' || assemblyType != 'GRCh38' ) {
                throw "assemblyType: Supported options are hg19/hg38/GRCh37/GRCh38";
            }

            
            var orCriteria = {};
            if ( (assemblyType == "hg19") || ( assemblyType == "GRCh37")) {
                orCriteria['cond'] =  [{AssemblyType: "hg19"},{AssemblyType: "GRCh37"}];
            } else if ((assemblyType == "hg38") || ( assemblyType == "GRCh38")) {
                orCriteria['cond'] = [{AssemblyType: "hg38"},{AssemblyType: "GRCh38"}];
            }

            //res.status(200).json({"id":uDateId,"message":"Import Request Scheduled"});

            var client = getConnection();
            const db = client.db(dbName);
            var statsColl = db.collection(importStatsCollection);

            var uDateId = await statsColl.findOne({'fileID':fileId},{'projection':{_id:1}});

            var annoMsg = {"id":uDateId,"status_description":"Reannotation Request Scheduled",'error_info' : null};
            res.status(200).json({"message":annoMsg});

            // update stats for sample sheet entry to inprogress
            //console.log(statsColl);

            // Traverse the Table and start the process. Make sure at a time, there are only 4 import process

            // Trigger Process Start
            var parsePath = path.parse(__dirname).dir;
            //var logFile = path.join(parsePath,'import','log',`import-route-logger-${fileId}-${uDateId}.log`);
            var logFile = `import-route-logger-${fileId}-${uDateId}.log`;
            var createLog = loggerMod.logger('import',logFile);
            createLog.debug("Logging request body below----------");
            createLog.debug(reqBody);
            createLog.debug("/annotateVCF request");
            createLog.debug(`fileID ${fileId}`);
            createLog.debug(`uDateId: ${uDateId}`);

            var qcount = 0;
            queue.on('active', () => {
                console.log(`Working on item #${++qcount}.  Size: ${queue.size}  Pending: ${queue.pending}`);
                createLog.debug(`Working on item #${++qcount}.  Size: ${queue.size}  Pending: ${queue.pending}`);
            });

            queue.add( async () => { 
                try {
                    console.log("Request - Import Task");
                    await importQueueScraperAnno(uDateId,fileId,assemblyType,annoType,annoField);
                    createLog.debug("await completed for importQueueScrapper");
                    await statsColl.updateOne({'_id':uDateId},
                    {$set : {'status_description' : 'Reannotation process Completed','status_id':8,'finish_time': new Date()},
                    $push: {'status_log': {status:"Reannotation process Completed",log_time: new Date()}}});
                    
                    // Here include trip related checks
                    createLog.debug("Annotation Request Completed");
                    console.log("Done : Annotate Task"); 
                    
                } catch(err) {
                     var id  = {'_id' : uDateId,'status_id':{$ne:9}};
                     var set = {$set : {'status_description' : 'Error', 'status_id':9 , 'error_info' : "Error occurred during import process",'finish_time': new Date()}, $push : {'status_log': {status:"Error",log_time: new Date()}} };
                     await statsColl.updateOne(id,set);
                     // update status for sample sheet entry
                     //await sampShColl.updateOne({"fileID":fileId }, {$set:{status: "import failed"}});
                     createLog.debug("Adding error for importQueueScrapper");
                     createLog.debug("Import Request Error");
                     createLog.debug(err);
                     console.log(err);
                }
            } );

            createLog.debug("******************* QUEUE STATS *************************");
            createLog.debug('Added : importQueue to import router Queue');
            createLog.debug(`Queue Size : ${queue.size}`);
            createLog.debug(`Pending promises: ${queue.pending}`);
            
            // nodejs request module and trigger https Annotation Request
            // handler specific to parent process
            process.on('beforeExit', (code) => {
                createLog.debug(`--------- About to exit PARENT PROCESS with code: ${code}`);
                console.log(`--------- About to exit PARENT PROCESS with code: ${code}`);
            });
        } catch(err) {
            console.log("*************Looks like we have received an error message");
            console.log(err);
            next(`${err}`);
        }
    });

    // This endpoint will be called to check the status of import process.
    app.route('/importStatus/:pid')
    .get(loginRequired,async(req,res,next) => {
        var pid = req.params.pid;
        try {
            if ( pid ) {
                pid = parseInt(pid);
                var client = getConnection();
                const db = client.db(dbName);
                var statsColl = db.collection(importStatsCollection);
                var projection = { 'status_description' : 1 ,'status_id' : 1 , 'error_info' : 1 };
                var statusMsg = await statsColl.findOne({'_id':pid}, {'projection':projection});
                //console.log("*** statusMsg **** "+statusMsg);
                if ( ! statusMsg ) {
                    next("invalid id");
                } else {
                    res.status(200).json({"message":statusMsg});
                }
            } 
        } catch(err1) {
            next(`${err1}`);
        }
    });

    // This endpoint will be called to check the status of import process.
    app.route('/scanSampleSheet/:batchSize')
    .get(loginRequired,async(req,res,next) => {
        try {
            var client = getConnection();
            const db = client.db(dbName);
            var sampleShColl = db.collection(sampleSheetCollection);
            var batchSize = parseInt(req.params.batchSize) || 100;
            //var testurl = req.protocol + '://' + req.get('host');
            const metaColl = db.collection(hostMetaCollection);
            var hostMeta = await metaColl.findOne({});
            //console.log("Logging the mongodb details of hostMeta below:");
            //console.log(hostMeta);
            var hostID = hostMeta['_id'] || null;
            
            //console.log("TEST URL is ");
            //console.log(testurl);
            //var projection = { 'status' : 1 };
            //var statusMsg = await statsColl.findOne({'_id':pid}, {'projection':projection});
            //var qFilter = {'processed' : 0};
            var dStream = await sampleShColl.aggregate(    [  
                {$match: {status: "queued"}},   
                {$sort: {"loadedTime" : 1}},
                {$limit : batchSize },	
                { $group:   {            
                    _id: {PIName: "$PIName",SampleLocalID: "$SampleLocalID", IndLocalID:"$IndLocalID", SeqTypeName: "$SeqTypeName"} ,            
                    Files: { 
                        $push:  { FileLocation: "$FileLocation", FileType: "$FileType", FileSourceType: "$FileSourceType", Description: "$Description", AssemblyType: "$AssemblyType" } 
                       },
                    seqMeta: {
                         $push : {SampleTypeName: "$SampleTypeName", SeqMachineName: "$SeqMachineName", SeqKitModelName:"$SeqKitModelName", SeqTargetCov: "$SeqTargetCov", SeqTargetReadLen: "$SeqTargetReadLen", SeqDate: "$SeqDate", loadedTime: "$loadedTime" , IndividualFName: "$IndividualFName", IndividualLName: "$IndividualLName", IndividualSex: "$IndividualSex",  PanelTypeName: "$PanelTypeName"} 
                        }
                    }      
                },
                {$sort: {"seqMeta.loadedTime" : 1}},
                {$limit : batchSize}   ] );

            //var dStream = await sampleShColl.find(qFilter, {'projection': {statMsg:0,processed: 0}});
            var obj = [];
            var bulkOps = [];
            while ( await dStream.hasNext()) {
                var doc = await dStream.next();
                //console.log(doc);
                var mainDoc = JSON.parse(JSON.stringify(doc['_id']));
                var seqData = JSON.parse(JSON.stringify(doc['seqMeta'][0]));
                var eachDoc = Object.assign(mainDoc,seqData);

                //var tmpDoc = JSON.parse(JSON.stringify(doc['docID'][0]));
                //var tmpDocId = tmpDoc['tempID'];
                //eachDoc['Host'] = testurl;
                eachDoc['Host'] = hostID;
                eachDoc['Files'] = doc['Files'];
                
                if ( eachDoc['IndividualSex'] && eachDoc['IndividualSex'].toUpperCase() == "MALE" ) {
                    eachDoc['IndividualSex'] = 1;
                } else if ( eachDoc['IndividualSex'] && eachDoc['IndividualSex'].toUpperCase() == "FEMALE" ) {
                    eachDoc['IndividualSex'] = 0;
                } else if ( eachDoc['IndividualSex'] && eachDoc['IndividualSex'].toUpperCase() == "UNKNOWN" ) {
                    eachDoc['IndividualSex'] = 2;
                }
                
                // Add any additional processing related to Individuals
                obj.push(eachDoc);
                //console.log(eachDoc);
                //console.log(doc);
                /*var filter = {};
                filter['filter'] = {'_id' : tmpDocId};
                filter['update'] = { $set: {reqSentTime : new Date(), 'statMsg' : 'sample sheet entries sent to wings'}};
		        // test setting added for ui team. will be changed to 0 after testing
                //filter['update'] = { $set: {'processed' : 0, 'statMsg' : 'sample sheet entries sent to wings'}};
                var updateFilter = {'updateOne' : filter};
                //console.log('update Filter');
                //console.log(updateFilter);
                bulkOps.push(updateFilter);*/
            }

            /*if ( bulkOps.length > 0 ) {
                var result = await sampleShColl.bulkWrite(bulkOps);
            }*/
            // test update setting for ui testing purpose
            //var result = await sampleShColl.updateMany({'processed':0},{$set: {'processed' : 0, 'statMsg' : 'sample sheet entries sent to wings'}});

            // actual setting
            // Below update request to be modified
            //var result = await sampleShColl.updateMany({'processed':0},{$set: {'reqSentTime" : new Date(), statMsg' : 'sample sheet entries sent to wings'}});
            
            res.status(200).json({"samplesheet":obj});
        } catch(err1) {
            next(`${err1}`);
        }
    });

    // endpoint created to provide status for a sample sheet entry
    // Unique ID required to identify the exact sample sheet entry

    app.route('/updateFileStatus')
    .post(loginRequired,async(req,res,next) => {
        var reqData = req.body;
        try {
            
            if ( ! reqData['UniqueID'] || ! reqData['Status']) {
                throw "UniqueID or Status not defined";
            }

            // allowed status : success or system-error or user-error
            if ( (reqData['Status'] != "success" ) && (reqData['Status'] != "system-error") && (reqData['Status'] != "user-error") ) {
                throw "Invalid State criteria";
            }

            var uniqKey = reqData['UniqueID'];
          
            // extract the field data from the Unique ID
            
            //console.log(reqData['state']);
            var search = {};
            var fileType = null;
            var client = getConnection();
            const db = client.db(dbName);
            var panelType = "";
            var seqType = "";
            if ( ('SampleLocalID' in uniqKey) && ('IndLocalID' in uniqKey ) && ('SequenceType' in uniqKey) && ('FileType' in uniqKey) && ('AssemblyType' in uniqKey )) {
                fileType = uniqKey['FileType'];
                panelType = uniqKey['PanelTypeName'] || '';
                seqType = uniqKey['SequenceType'];
                seqType = seqType.toUpperCase();
                search = {SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,FileType: uniqKey['FileType'],AssemblyType : uniqKey['AssemblyType'],status : 'queued'};

                if ( reqData['Status'] == "success") { 
                    if ( (fileType.toUpperCase()) == "FASTQ" ) {
                        // assembly type not needed for fastq filetype
                        search = {SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,FileType: uniqKey['FileType'],status : 'queued'};
                    }
                    var fileID = uniqKey['fileID'];
                    var indID = uniqKey['IndividualID'];
                    var uID = fileID+"-"+indID;
                    console.log("uID is "+uID);
                    // for fileTypes VCF and 
                    if ( (fileType.toUpperCase() == "VCF") || (fileType.toUpperCase() == "GVCF")) {
                        // check if id already exists
                        var exists = await db.collection(indSampCollection).findOne({_id:uID});
                        if ( ! exists ) {
                            if ( seqType == "PANEL" ) {
                                panelType = panelType.toUpperCase();
                            }
                            await db.collection(indSampCollection).insertOne({_id: uID, SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,FileType: uniqKey['FileType'],AssemblyType : uniqKey['AssemblyType'], individualID : indID, fileID: fileID,panelType:panelType});
                        }
                    }
                    
                }
            } else {
                throw "Some unique keys are not provided";
            }
            
            
            var sampShColl = db.collection(sampleSheetCollection);
            var msg = reqData['Msg'] || ''; 
            
            // fileType defined. fileID can be null
            // Mark only this file as error
            if ( fileType ) {
                //console.log("fileType is defined");
                if ( (fileType.toUpperCase()) == "FASTQ" ) {
                    search = {SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,FileType: uniqKey['FileType'],status : 'queued'};
                }
                if (seqType == "PANEL" ) {
                    if ( panelType == "") {
                        throw "panelType is mandatory for PANEL sequencing type";
                    }
                    search['PanelTypeName'] = panelType;
                }
            } else {
                // all entries for this match will be updated with the error status
                search = {SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,status : 'queued'};
            }
            
            if (reqData['Status'] == "system-error") {
                //console.log("************** Update entry with the status as system-error");
                var result = await sampShColl.updateMany(search,{$set : {status: reqData['Status'], msg_log: msg, blocked : 1}});
            } else {
                var result = await sampShColl.updateMany(search,{$set : {status: reqData['Status'], msg_log: msg}});
            }
            
            res.status(200).json({"message":"Success"});
        } catch(err) {
            next(`${err}`);
        }
    });
    

    app.route('/releaseSampShEntry')
    .post(loginRequired,async(req,res,next) => {
        var reqData = req.body;
        try {
            if ( ! reqData['UniqueID'] ) {
                throw "JSON Structure Error";
            }

            var uniqKey = reqData['UniqueID'];
          
            // extract the field data from the Unique ID
            
            //console.log(reqData['state']);
            var search = {};
            var fileType = null;
            var panelType = "";
            var seqType = "";
            if ( ('SampleLocalID' in uniqKey) && ('IndLocalID' in uniqKey ) && ('SequenceType' in uniqKey) && ('FileType' in uniqKey) && ('AssemblyType' in uniqKey )) {
                seqType = uniqKey['SequenceType'];
                seqType = seqType.toUpperCase();
                search = {SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,FileType: uniqKey['FileType'],AssemblyType : uniqKey['AssemblyType'], status : 'system-error', blocked : 1};
                fileType = uniqKey['FileType'];
                panelType = uniqKey['PanelTypeName'] || '';
                
                // assembly type will be null for fastq
                if ( (fileType.toUpperCase()) == "FASTQ" ) {
                    search = {SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,FileType: uniqKey['FileType'],status : 'system-error', blocked : 1};
                }
            } else {
                throw "Some unique keys are not provided";
            }
            
            var client = getConnection();
            const db = client.db(dbName);
            var sampShColl = db.collection(sampleSheetCollection);
            // do not update. find entry with status 'system-error' matching the unique ID
            // Then create a new entry  
            //var result = await sampShColl.updateOne(search,{$set : {status: reqData['Status']}});
            //var result = await sampShColl.findOne(search,{'projection':{'status':0,'_id':0}});

            //console.log("Search criteria logged");
            //console.log(search);

            // fileType defined. fileID can be null
            // Mark only this file as error
            if ( fileType ) {
                //console.log("fileType is defined");
                if ( (fileType.toUpperCase()) == "FASTQ" ) {
                    search = {SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,FileType: uniqKey['FileType'],status : 'system-error', blocked : 1};
                }
                if (seqType == "PANEL" ) {
                    search['PanelTypeName'] = panelType
                }
                //console.log(search);
            } else {
                // all entries for this match will be updated with the error status
                search = {SampleLocalID : uniqKey['SampleLocalID'],IndLocalID : uniqKey['IndLocalID'],SeqTypeName : seqType,status : 'system-error', blocked : 1};
            }

            //console.log("Logging the search query below ");
            //console.log(search);

            var result = await sampShColl.find(search,{'projection':{'_id':0,'blocked' : 0,'msg_log': 0}});
            var docs = 0;
            while ( await result.hasNext() ) {
                ++docs;
                const doc = await result.next();
                var mainDoc = doc;
                // add a new sample sheet entry with the status as 'queued'
                mainDoc['status'] = "queued";
                // update blocked field to indicate the sample was released
                await sampShColl.updateOne(search,{$set : {blocked : 0}});
                // generate id
                var randNum = Math.floor(Math.random() * 100) + 1;
                var id = new Date().valueOf() + randNum;
                mainDoc['_id'] = id;
                // create a new sample sheet entry with queued status
                await sampShColl.insertOne(mainDoc);
            }
            if ( docs ) {
                res.status(200).json({"message":"Success"});
            } else {
                throw "No matching entry in database";
            }
        } catch(err) {
            next(`${err}`);
        }
    });

    app.route('/updateFileIDState')
    .post(loginRequired,async(req,res,next) => {
        var reqData = req.body;
        try {
            if ( ! reqData['fileID'] || ! reqData['state']) {
                throw "JSON Structure Error";
            }
            //console.log(reqData['state']);
            if ( (reqData['state'] != "active") && (reqData['state'] != "inactive") ) {
                throw "Invalid State criteria";
            }
            var client = getConnection();
            const db = client.db(dbName);
            var fileMetaColl = db.collection(fileMetaCollection);
            var result = await fileMetaColl.updateOne({'fileID': reqData['fileID']},{$set:{'state': reqData['state']}},{upsert: true});
            res.status(200).json({"message":"Success"});
        } catch(err) {
            next(`${err}`);
        }
    });

    
    // This endpoint will be called to check the status of import process.
    app.route('/importStatusOld/:pid')
    .get(loginRequired,async(req,res,next) => {
        var pid = req.params.pid;
        try {
            if ( runningProcess(pid) ) {
                res.status(200).json({"message":"Import InProgress"});
            } else {
                res.status(200).json({"message":"Import Completed"});
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });

    app.route('/getNovelVariantsCompressed/:sid')
    .get(loginRequired,async(req,res,next) => {
        var sid = req.params.sid;
        req.setTimeout(200000000);
        console.log("Route Process SID is "+sid);
        try {
            var parsePath = path.parse(__dirname).dir;
            var nScript = path.join(parsePath,'import','getNovelVariants.js');
            subprocess = spawn.fork(nScript, ['--sid',sid] );
            subprocess.on('message', function(data) { // data has the absolute path of the novel variants vcf
                // Responding data/VCF variants as a tab file
                console.log("Message received from child_process "+data);
                var zipFile = data+'.gz'
                
                //Once a writable stream is closed, it cannot accept anymore data : this is why on the first execution data will be zipped properly, and on the second there will be "write after end error"
                var gzip = zlib.createGzip();
                createReadStream(data).pipe(gzip)
                    .on('error', (err) => {
                         console.log("Error is "+err);
                         console.dir(err,{"depth":null});
                    });
                res.status(400).json({"message":`Compressed Novel Variants can be accessed at ${zipFile}`});
            });
        } catch(err1) {
            //res.status(400).send("Failure-Error message "+err1);
            next(`${err1}`);
        }
    });

    // Retrieves the novel variants and the response is sent as a gz file
    // Variants will be stored as vcf zip file in the calling location
    app.route('/novelVariantsStreamZip/:sid')
    .get(loginRequired,async(req,res,next) => {
        var sid = req.params.sid;
        req.setTimeout(200000000);
        console.log("Route Process SID is "+sid);
        try {
            var parsePath = path.parse(__dirname).dir;
            var nScript = path.join(parsePath,'import','getNovelVariants.js');
            subprocess = spawn.fork(nScript, ['--sid',sid] );
            subprocess.on('message', function(data) { // data has the absolute path of the novel variants vcf
                // Responding data/VCF variants as a tab file
                console.log("Message received from child_process "+data);
                var zipFile = data+'.gz'
                res.writeHead(200, { "Content-Type" : "application/gzip", "Content-Disposition" : "attachment;filename="+zipFile });

                //createReadStream(data).pipe(gzip).pipe(res);
                // handle errors in gzip and then proceed to pipe the data to response
                // Application crashes if error is not handled properly
                //21

                //Once a writable stream is closed, it cannot accept anymore data : this is why on the first execution data will be zipped properly, and on the second there will be "write after end error"
                var gzip = zlib.createGzip();
                createReadStream(data).pipe(gzip)
                    .on('error', (err) => {
                         console.log("Error is "+err);
                         console.dir(err,{"depth":null});
                    })
                       .pipe(res)
                    .on('error', (err1) => {
                        // handle error
                        console.log("Error is "+err1);
                        console.dir(err1);
                     });
                
            });
        } catch(err1) {
            //res.status(400).send("Failure-Error message "+err1);
            next(`${err1}`);
        }
    });

    app.route('/uploadFile/')
    .post(loginRequired,async(req,res,next) => {
        //console.log(req);
        try {
            req.pipe(createWriteStream('./novelVariantsData.vcf'));
            res.send("Data uploaded");
        } catch(err) {
            //res.status(400).send("Failure-Error message "+err);
            next(`${err}`);
        }
    });

    // API Route endpoint to update the novel annotations generated by parser
    // upload Annotations --> validate checksum --> update annotation collection --> update import collection
    // multer npm module is used to upload data from multiple files
    // multer is used as req.files build in express has been deprecated from express 4.x
    app.route('/updateNovelAnnotations/')
    //.post(upload1.single('file1'),async(req,res) => { // working request with single file option
    //.post(upload1.single('file1'),async(req,res) => { // working request with single file option
      //.post(upload1.array('uploadData',10),async(req,res) => {
      .post(upload1.fields([{name: 'checksum', maxCount:1},{name: 'annotation', maxCount:1}]),loginRequired,async(req,res,next) => {
          console.dir(upload1,{"depth":null});
        //console.log(req);
        // req.body will contain text fields if any

        //node verifySha256.js <filename> <checksum_file></checksum_file>
        try {
            if ( req.files ) {
                console.log("Data available");
                console.dir(req.files);
                var cs = req.files.checksum[0]['path'];
                var anno = req.files.annotation[0]['path'];
                console.log("---- Checksum file available at "+cs);
                console.log("----- Annotation file available at "+anno);
                var parsePath = path.parse(__dirname).dir;
                var cScript = path.join(parsePath,'import','verifySha256.js');

                subprocess = spawn.fork(cScript, [anno,cs] );
                // check message of child process to get the checksum validation status
                subprocess.on('message', function(data) {
                    // response : valid or invalid
                    if ( data === "valid" ) {
                        //res.send("checksum validated");
                        res.status(200).json({"message":"checksum validated"});
                        var novelScript = path.join(parsePath,'parser','novelAnnotationUpload.js');
                        var annoScript = path.join(parsePath,'import','updateExistingAnnotations.js');
                        console.dir(parsePath);
                        console.log("Novel Annotation Script Path is  "+novelScript);
                        console.log("Annotation Script Path is now "+annoScript);

                        var sid = req.body.sid;
                        subprocess = spawn.fork(novelScript, ['--parser',"Novel",'--input_file', anno] );
                        var procPid = subprocess.pid;
            
                        console.log("Novel Annotation Upload process trigerred "+procPid);

                        // handler to listen when the child exits or has completed
                        subprocess.on('close',function(code) {
                            console.log("*********Child Process Exited. We will signal Express");
                            console.log("Start updating the Annotations to the novel variants in the import collection");
                            subprocessAnno = spawn.fork(annoScript, ['--sid',sid] );
                            var procPid = subprocessAnno.pid;
                            console.log("Import Collection Annotation Update Process now trigerred with process ID "+procPid);
                            // Sample data loaded to mongodb. We can start adding core Annotations.
                            // Adds Annotation to the non novel variants
                            if ( code != 0 ) {
                                console.log(`Child process exited with code ${code}`);
                            } 
                        });
                    } else if ( data === "invalid" ) {
                        //res.send("invalid checksum.Check source data");
                        res.status(200).json({"message":"invalid checksum.check source data"});
                    }
                }); 
            }
        } catch(err) {
            //res.status(400).send("Failure-Error message "+err1);
            //res.status(400).json({"message":`failure-Error ${err}`});
            next(`${err}`);
        }
        
    });
}


module.exports = { importRoutes };
