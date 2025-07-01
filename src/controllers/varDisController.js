const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');
const path = require('path');
const spawn  = require('child_process');
const configData = require('../config/config.js');
const { app:{instance} } = configData;
const {logger} = require('../controllers/loggerMod');
const { app: {tmpCenterID,tmpHostID,liftMntSrc,liftoverDocker},db : {dbName,variantDiscResults,importCollection1,importCollection2,indSampCollection,phenotypeColl,resultCollection} } = configData;
const readline = require('readline');
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const getConnection = require('../controllers/dbConn.js').getConnection;
const encryptJson = require('../misc/generateCrypto.js').encryptJson;

var pid = process.pid;
var uDateId = new Date().valueOf();
var logFile = `varDisc-control-logger-${pid}-${uDateId}.log`;
var varContLog = logger('var_disc',logFile);
const nodemailer = require("nodemailer");

const varPhenSearch = async(postReq,req_id) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const varResColl = db.collection(variantDiscResults);

        varContLog.debug("************ Logging started for the request ID "+req_id);
        varContLog.debug("-------------------------------------------------------------------------");
        varContLog.debug("Logging request details below:");
        var result = await varResColl.insertOne({_id:req_id,status:'inprogress'});

        varContLog.debug(postReq);
        var var_req = postReq['var_search']['variant'];
        var liftedAssembly;
        if ( (postReq['assembly'] == "hg19") || (postReq['assembly'] == "GRCh37")) {
            liftedAssembly = "hg38";
        } else if ((postReq['assembly'] == "hg38") || (postReq['assembly'] == "GRCh38")) {
            liftedAssembly = "hg19";
        }

        // perform liftover
        var parsePath = path.parse(__dirname).dir;
        var liftoverScript = path.join(parsePath,'modules','liftoverVariant.js');
        var subprocess1 = spawn.fork(liftoverScript, ['--variant',var_req,'--assembly', postReq['assembly'], '--req_id', req_id] );
        var procPid = subprocess1.pid;
        varContLog.debug("Liftover process now triggered with pid "+procPid);
        var variantFile = liftMntSrc+'/'+req_id+'-variant.txt';
        var liftedVariant = "";
        try {
            await closeSignalHandler(subprocess1);
            //console.log("Try Block");
            varContLog.debug("Liftover process completed now ");
            liftedVariant = await readFile(variantFile,varContLog);
            //console.log(liftedVariant);
        } catch(err) {
            console.log("Logging error from varDiscController-liftover error");
            console.log(err)
            varContLog.debug(err);
            varContLog.debug("Error in performing liftover ");
        }   

        varContLog.debug(`Value returned from the function is ${liftedVariant}`);
        var msg_log = "";
        if ( liftedVariant ) {
            varContLog.debug("Printing the lifted over variant data returned now");
            varContLog.debug(`LIFTED VARIANT ${liftedVariant}`);
            var re = /-/;
            if (liftedVariant.match(re) ) {
                varContLog.debug("Variant lifted over and hence the match regex passed");
            } else {
                msg_log = liftedVariant;
            }

        } else {
            varContLog.debug("No data in file");
        }
        // now only in one assembly. Later we can include search in both assemblies
        // Step2 - get fileID based on variant
        
        var phenotype = postReq['phen_term']['hpo_id'];
        var phenArr = [];
        phenArr.push(phenotype);

        var fileIdList = await fetchfileID(var_req,postReq['assembly'],varContLog);
        var newFileList = [];
        if ( liftedVariant ) {
            varContLog.debug(" We have some lifted variant.Lets check for fileIDs");
            newFileList = await fetchfileID(liftedVariant,liftedAssembly,varContLog);
            varContLog.debug("Logging to check for any fileIDs in the new assembly");
            varContLog.debug(newFileList);
        }
        if ( newFileList.length > 0 ) {
            fileIdList.push(...newFileList);
        }
        varContLog.debug("************ NEW FILE LIST");
        varContLog.debug(fileIdList);
        
        varContLog.debug("Logging post request below:");
        varContLog.debug(postReq);
        varContLog.debug("1.fileID of variant logged below :");
        varContLog.debug(fileIdList);

        var varIndList = await fetchVarInd(fileIdList,postReq['seq_type'],"var_exist",varContLog);
        varContLog.debug("2.Individuals having variant below:");
        varContLog.debug(varIndList);

        var noVarIndList = await fetchVarInd(fileIdList,postReq['seq_type'],"var_not_exist",varContLog);
        varContLog.debug("3.Individuals not having variant below :")

        varContLog.debug(noVarIndList);

        varContLog.debug("4.Function to retrieve phenotype for Individuals having variant");
        //var retObj1 = await checkPhenExists(varIndList,phenotype,varContLog);
        var retObj1 = await checkPhenExists(varIndList,phenArr,varContLog,req_id);
        varContLog.debug(retObj1);

        varContLog.debug("5.Function to retrieve phenotype for Individuals NOT having variant");
        //var retObj2 = await checkPhenExists(noVarIndList,phenotype,varContLog);
        var retObj2 = await checkPhenExists(noVarIndList,phenArr,varContLog);
        varContLog.debug(retObj2);

        // 9 seconds timeout
        //await new Promise(resolve => setTimeout(resolve, 15000));

        var result = "";

        var phen_or_nophen = {'variant-phenotype':retObj1['exist'],'variant-no-phenotype' : retObj1['not_exist'], 'no-variant-phenotype':retObj2['exist'],'no-variant-no-phenotype' : retObj2['not_exist']};       
        //var phen_var = {'phenotype':40,'no-phenotype':20};
        //var phen_no_var = {'phenotype':2,'no-phenotype':50};
        //var res_obj = {'overall' : phen_or_nophen};
        //result['overall'] = {'variant' : phen_var,'no-variant':phen_no_var};
    
        await varResColl.updateOne({_id:req_id},{$set:{'overall':phen_or_nophen,status:'completed','centerId': postReq['centerId'],'hostId' : postReq['hostId'] ,'msg': msg_log}});

        varContLog.debug("6:Overall result given below:");
        varContLog.debug(phen_or_nophen);
        varContLog.debug("Inserted results for the request ID");

        return "Success";
    } catch(err) {
        throw err;
    }
}


async function readFile(variantFile,varContLog) {
    var rd;
    try {
        var liftedVariant = null;
        if ( ! fs.existsSync(variantFile)) {
            //console.log(variantFile);
            resolve(liftedVariant);
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

const getVarPhenCount = async(reqID) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const varResColl = db.collection(variantDiscResults);
        var query = {_id:reqID};
        var varData = await varResColl.findOne(query,{projection:{_id:0}});

        if ( !varData ) {
            throw "invalid request id";
        }
        var retVal = {"centerId" : varData['centerId'],"hostId" : varData['hostId'],'overall': varData['overall'],status:varData['status'], msg: varData['msg'], result: varData['result'], annotation: varData['annotation']};

        return retVal;
    } catch(err) {
        throw err;
    }
}

const getGeneCount = async(postReq,req_id) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const varResColl = db.collection(variantDiscResults);

        varContLog.debug("************ Logging started for the request ID "+req_id);
        varContLog.debug("-------------------------------------------------------------------------");
        varContLog.debug("Logging request details below:");
        var result = await varResColl.insertOne({_id:req_id,status:'inprogress'});

        varContLog.debug(postReq);
        var gene_req = postReq['var_search']['geneID'];
        var passNodeId;
        var failNodeId;

        passNodeId =  0;
        failNodeId =  0;
        
        var queryCollection;
        if ( (postReq['assembly'] == "hg19") || (postReq['assembly'] == "GRCh37")) {
            queryCollection = importCollection1;
        } else if ((postReq['assembly'] == "hg38") || (postReq['assembly'] == "GRCh38")) {
            queryCollection = importCollection2; 
        }
        var qColl = db.collection(queryCollection); 
        var rootFilter = {'gene_annotations.gene_id' : {$in:gene_req} };
        var altRootFilter = {'gene_annotations.gene_id' : {$nin : gene_req }};


        // Execute both queries in parallel
        var count = await qColl.find(rootFilter).count();
        var result = [];
        var passHash = { 'pass' : count, 'childIds' : passNodeId };
        var failHash = { 'fail' : count, 'childIds' : failNodeId }
        result.push(passHash);
        result.push(failHash);
        //console.log(result);

        var procTime = new Date().toUTCString();
        await db.collection(variantDiscResults).updateOne({_id:req_id},{$set:{'result':result,status:"completed",processedTime:procTime,'centerId': postReq['centerId'],'hostId' : postReq['hostId']}});

        return "Success";

    } catch(err) {
        throw err;
    }
}


const getRegionCount = async(postReq,req_id) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const varResColl = db.collection(variantDiscResults);

        varContLog.debug("************ Logging started for the request ID "+req_id);
        varContLog.debug("-------------------------------------------------------------------------");
        varContLog.debug("Logging request details below:");
        var result = await varResColl.insertOne({_id:req_id,status:'inprogress'});

        varContLog.debug(postReq);
        var region_req = postReq['var_search']['region'];
        var condHash = {};
        var regData = region_req.split('-');
        condHash['rule'] = regData[0];
        condHash['start_pos'] = regData[2];
        condHash['stop_pos'] = regData[1];

        var filterRule = await positionFilterRule(condHash);
        var passNodeId;
        var failNodeId;

        passNodeId =  0;
        failNodeId =  0;
        
        var queryCollection;
        if ( (postReq['assembly'] == "hg19") || (postReq['assembly'] == "GRCh37")) {
            queryCollection = importCollection1;
        } else if ((postReq['assembly'] == "hg38") || (postReq['assembly'] == "GRCh38")) {
            queryCollection = importCollection2;
        }
        var qColl = db.collection(queryCollection);      

        // Execute both queries in parallel
        var count = await qColl.find(filterRule).count();
        //console.dir(filterRule,{"depth":null});
        var result = [];
        
        var passHash = { 'pass' : count, 'childIds' : passNodeId };
        var failHash = { 'fail' : count, 'childIds' : failNodeId };
        result.push(passHash);
        result.push(failHash);
        //console.log(result);

        var procTime = new Date().toUTCString();
        await db.collection(variantDiscResults).updateOne({_id:req_id},{$set:{'result':result,status:"completed",processedTime:procTime,'centerId': postReq['centerId'],'hostId' : postReq['hostId']}});

        return "Success";

    } catch(err) {
        throw err;
    }
}

async function positionFilterRule(condHash) {
    try {
        var filterRule = {};
        
        var start_pos; var stop_pos;
        if ( condHash['start_pos']) {
            start_pos = condHash['start_pos'] || 0;
        }
        if ( condHash['stop_pos']) {
            stop_pos = condHash['stop_pos'] || 0;
        }
        filterRule['$and']  = [ {"chr" : parseInt(condHash['rule'])}, {'start_pos' : {$lte : parseInt(start_pos)}}, {'stop_pos' : {$gte : parseInt(stop_pos)}} ];
        return filterRule;
    } catch(err) {
        throw err;
    }
}

const checkPhenExists = async(indIdList,phenTerm,varContLog,req_id="none") => {
    try {
        var client = getConnection();
        const db = client.db(dbName); 

        var matchStage = { $match : {"IndividualID" : { $in : indIdList } } };
        var groupStage = { $group: {"_id" : {IndividualID: "$IndividualID",hpo_id:"$hpo_id" }, "count" : {$sum : 1}} };
        //var matchStage2 = { "isMatch" : { $cond: [ {"$eq" : ["$_id.hpo_id",phenTerm] },"true","false"] } };

        // { $project: { _id: 1,  matchedField: { $cond: { if: { $in: ["$_id.hpo_id", ["HP:0001249", "HP:0000252"]] }, then: "true",  else: "false"  } } } }

        // need to decide - how phenTerm will be sent. should not affect existing implementation
        // phenTerm has to be array
        // this is not correct
        //var matchStage2 = { "isMatch" : { if: { $in: ["$_id.hpo_id", phenTerm ] }, then: "true",  else: "false"  } };
        var matchStage2 = {  "isMatch": { $cond: { if: { $in: ["$_id.hpo_id", phenTerm] }, then: "true",  else: "false"  } } };

        var projectStage = {$project : matchStage2};
        varContLog.debug("checkPhenExists matchStage : ");
        varContLog.debug(matchStage);
        varContLog.debug("checkPhenExists groupStage");
        varContLog.debug(groupStage);
        varContLog.debug("checkPhenExists matchStage2");
        varContLog.debug(matchStage2);

        var indPhenObj = await db.collection(phenotypeColl).aggregate([matchStage, groupStage, projectStage]);

        var indPhenExist = {};
        var indPhenNotExist = {};
        const indSet = new Set();

        while ( await indPhenObj.hasNext() ) {
            const doc = await indPhenObj.next();
            varContLog.debug(doc);
            var indId = doc['_id']['IndividualID'];
            if ( doc['isMatch'] == "true" ) {
                indPhenExist[indId] = 1;
            } else if ( doc['isMatch'] == "false" ) {
                indPhenNotExist[indId] = 1;
            }
            indSet.add(indId);
        }

        var existObj = Object.keys(indPhenExist);
        
        for ( var rIdx in existObj ) {
            var indKey = existObj[rIdx];
            if ( indPhenNotExist[indKey]) {
                delete indPhenNotExist[indKey];
            }
        }

        // Check if Individual does not exists in phenotype collection. No phenotype was registered.
        var not_exists_cnt = 0;
        //console.dir(indPhenExist);
        for ( var x in indIdList ) {
            var orgInd = indIdList[x];

            if ( ! indSet.has(orgInd)) {
                not_exists_cnt++;
                varContLog.debug(`checkPhenExists-Individual ${orgInd} does not exists in collection`);
            }
        }

        if ( req_id != "none" ) {
            const varResColl = db.collection(variantDiscResults);
            // existObj is an array of Individuals having the variant and the HPO-term
            await varResColl.updateOne({_id:req_id},{$set:{'ind_list': existObj}});
        }

        var notExistObj = Object.keys(indPhenNotExist);
        var notExistCnt = notExistObj.length + not_exists_cnt;
        
        var returnObj = { 'exist' : existObj.length, 'not_exist' : notExistCnt };
        return returnObj;

    } catch(err) {
        throw err;
    }
}

const fetchVarInd = async(fileIDList,seqType,type,varContLog) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var query = {};

        if (type == "var_exist") {
            query = {fileID : {$in:fileIDList},SeqTypeName: seqType};
        } else if ( type == "var_not_exist" ) {
            query = {fileID : {$nin:fileIDList},SeqTypeName: seqType};
        }
        varContLog.debug("fetchVarInd-Query is ");
        varContLog.debug(query);

        var res = await db.collection(indSampCollection).find(query,{projection:{individualID: 1,_id:0}});

        var indIdList = [];
        var newIndList = [];
        var indSet;
        // include another check if see if the Individuals are distinct
        while ( await res.hasNext()) {
            const doc = await res.next();
            if ( Object.keys(doc).length > 0 ) {
                indIdList.push(doc['individualID']);
            } 
        }
        if ( indIdList.length > 0 ) {
            indSet = new Set(indIdList);
            newIndList = Array.from(indSet);
        }

        return newIndList;
    } catch(err) {
        throw err;
    }
}



const fetchfileID = async(var_req,assemblyType,varContLog,queryType='single') => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var importCollection;
        
        if ( (assemblyType == "GRCh37") || (assemblyType == "hg19") ) {
            importCollection = db.collection(importCollection1);
        } else if ( (assemblyType == "GRCh38") || (assemblyType == "hg38") ) {
            importCollection = db.collection(importCollection2);
        }

        // exclude hom-ref
        // includes multiple variant criteria.
        // check existing implementation
        //var fileQuery = {'var_key':{$in:var_req},'alt_cnt':{$ne:0}};
        // single variant
        var fileQuery = {'var_key':var_req,'alt_cnt':{$ne:0}};
        // multiple variants
        if ( queryType != 'single' ) {
            fileQuery = {'var_key':{$in:var_req},'alt_cnt':{$ne:0}};
        }
        //console.log(assemblyType);
        //console.log(fileQuery);

        varContLog.debug(`Assembly is ${assemblyType}`);
        varContLog.debug("fetchfileID-fileQuery is");
        varContLog.debug(fileQuery);

        var results = await importCollection.find(fileQuery,{projection:{'fileID':1,'var_key':1}});
        var fileidList = [];
        while ( await results.hasNext()) {
            const doc = await results.next();
            //console.log(doc)
            fileidList.push(doc['fileID']);
        }
        // return only the unique items.
        // convert to set and back to list

        if ( fileidList.length > 0 ) {
            var indSet = new Set(fileidList);
            fileidList = Array.from(indSet);
        }
        //console.log(fileidList)
        return fileidList;

    } catch(err) {
        console.log(err)
        throw err;
    }
}

const fetchfileGTCnt = async(var_req,assemblyType,varContLog,queryType='single') => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var importCollection;
        
        if ( (assemblyType == "GRCh37") || (assemblyType == "hg19") ) {
            importCollection = db.collection(importCollection1);
        } else if ( (assemblyType == "GRCh38") || (assemblyType == "hg38") ) {
            importCollection = db.collection(importCollection2);
        }

        // exclude hom-ref
        // includes multiple variant criteria.
        // check existing implementation
        //var fileQuery = {'var_key':{$in:var_req},'alt_cnt':{$ne:0}};
        // single variant
        var fileQuery = {'var_key':var_req,'alt_cnt':{$ne:0}};
        // multiple variants
        if ( queryType != 'single' ) {
            fileQuery = {'var_key':{$in:var_req},'alt_cnt':{$ne:0}};
        }
        //console.log(assemblyType);
        //console.log(fileQuery);

        varContLog.debug(`Assembly is ${assemblyType}`);
        varContLog.debug("fetchfileID-fileQuery is");
        varContLog.debug(fileQuery);

        var results = await importCollection.find(fileQuery,{projection:{'fileID':1,'var_key':1,'alt_cnt':1}});
        var fileCntObj = {};
        var fileidList = [];
        var gtObj = {'het':{'cnt':0,'fileID':[]},'hom-alt':{'cnt':0,'fileID':[]}};
        while ( await results.hasNext()) {
            const doc = await results.next();
            //console.log(doc)
            fileidList.push(doc['fileID']);
            if ( doc['alt_cnt'] == 1 ) {
                gtObj['het']['cnt'] = gtObj['het']['cnt'] + 1;
                gtObj['het']['fileID'].push(doc['fileID']);
            } else if ( doc['alt_cnt'] == 2 ) {
                gtObj['hom-alt']['cnt'] = gtObj['hom-alt']['cnt'] + 1;
                gtObj['hom-alt']['fileID'].push(doc['fileID']);
            }
        }
        // return only the unique items.
        // convert to set and back to list
        var assignedFList = [];
        if ( fileidList.length > 0 ) {
            var indSet = new Set(fileidList);
            fileidList = Array.from(indSet);
            // check if the files are not in 'unassigned' status
            assignedFList = await fetchUnassignedFile(fileidList);

        }
        //console.log(fileidList)
        //fileCntObj['fileID'] = fileidList;
        fileCntObj['fileID'] = assignedFList;
        fileCntObj['gt'] = gtObj;
        //console.log("Logging object inside function ");
        console.dir(fileCntObj,{"depth":null});
        return fileCntObj;

    } catch(err) {
        console.log(err)
        throw err;
    }
}

async function fetchUnassignedFile(fileIDList) {
    try {
        var assignedFileList = [];
        var client = getConnection();
        const db = client.db(dbName);
        var query = {'fileID':{$in:fileIDList},"state" :{ $ne: "unassigned"}};
        var res = await db.collection(indSampCollection).find(query,{projection:{fileID: 1,_id:0}});
        while ( await res.hasNext()) {
            const doc = await res.next();
            if ( Object.keys(doc).length > 0 ) {
                assignedFileList.push(doc['fileID']);
            } 
        }
        return assignedFileList;
    } catch(err) {
        throw err;
    }
}
// function to get variant counts based on assembly type and sequence type
// variant frequency
const varCounts = async(postReq,req_id) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const varResColl = db.collection(variantDiscResults);

        var reqAssembly = postReq['assembly'];
        varContLog.debug("************ Logging started for the request ID "+req_id);
        varContLog.debug("-------------------------------------------------------------------------");
        varContLog.debug("Logging request details below:");
        var result = await varResColl.insertOne({_id:req_id,status:'inprogress'});

        varContLog.debug(postReq);
        var var_req = postReq['var_search']['var_key'];
        var liftedAssembly;
        if ( (postReq['assembly'] == "hg19") || (postReq['assembly'] == "GRCh37")) {
            liftedAssembly = "hg38";
        } else if ((postReq['assembly'] == "hg38") || (postReq['assembly'] == "GRCh38")) {
            liftedAssembly = "hg19";
        }

        // perform liftover
        var parsePath = path.parse(__dirname).dir;
        var liftoverScript = path.join(parsePath,'modules','liftoverVariant.js');
        var subprocess1 = spawn.fork(liftoverScript, ['--variant',var_req,'--assembly', postReq['assembly'], '--req_id', req_id] );
        var procPid = subprocess1.pid;
        varContLog.debug("Liftover process now triggered with pid "+procPid);
        var variantFile = liftMntSrc+'/'+req_id+'-variant.txt';
        var liftedVariant = "";
        try {
            await closeSignalHandler(subprocess1);
            //console.log("Try Block");
            varContLog.debug("Liftover process completed now ");
            liftedVariant = await readFile(variantFile,varContLog);
            //console.log(liftedVariant);
        } catch(err) {
            //console.log("Logging error from varDiscController-liftover error");
            //console.log(err);
            varContLog.debug(err);
            varContLog.debug("Error in performing liftover ");
        }   

        varContLog.debug(`Value returned from the function is ${liftedVariant}`);
        var msg_log = "";
        if ( liftedVariant ) {
            varContLog.debug("Printing the lifted over variant data returned now");
            varContLog.debug(`LIFTED VARIANT ${liftedVariant}`);
            var re = /-/;
            if (liftedVariant.match(re) ) {
                varContLog.debug("Variant lifted over and hence the match regex passed");
            } else {
                msg_log = liftedVariant;
            }

        } else {
            varContLog.debug("No data in file");
        }

        
        // now only in one assembly. Later we can include search in both assemblies
        // Step2 - get fileID based on variant

        //var gtObj = {'het':{'cnt':0,'fileID':[]},'hom-alt':{'cnt':0,'fileID':[]}};
        // request assembly - fileID list
        var fileCntObj = await fetchfileGTCnt(var_req,postReq['assembly'],varContLog);
        var fileIdList = fileCntObj['fileID'];

        // fetch the annotation of the variant. findOne to get only 1 annotation
        // function to fetch the annotation for the variant. 
        // provide fileID argument to filter only based on assigned file list

        var varAnno = null;
        if ( fileIdList.length > 0 ) {
            varAnno = await fetchVarAnno(var_req,reqAssembly,fileIdList[0]);
            //console.log("Logging variant annotation ");
            //console.log(varAnno);
        }


        // lifted assembly - fileID list
        var fileCntObjL = {};
        var newFileList = [];

        if ( liftedVariant ) {
            console.log("Request is not sent-------");
            varContLog.debug(" We have some lifted variant.Lets check for fileIDs");
            fileCntObjL = await fetchfileGTCnt(liftedVariant,liftedAssembly,varContLog);
            newFileList = fileCntObjL['fileID'];
            varContLog.debug("Logging to check for any fileIDs in the new assembly");
            varContLog.debug(newFileList);
        }
        //console.log()
        varContLog.debug("************ NEW FILE LIST");
        varContLog.debug(fileIdList);
        
        varContLog.debug("Logging post request below:");
        varContLog.debug(postReq);
        varContLog.debug("1.fileID of variant logged below :");
        varContLog.debug(fileIdList);

        // request assembly
        // het
        var varseqCntReq = await getVarSeqTypeCounts(fileIdList,reqAssembly,varContLog);
        // heterozygous
        // send request only if file length is > 0 
        var varseqCntReq1 = await getVarSeqTypeCounts(fileCntObj['gt']['het']['fileID'],reqAssembly,varContLog);
        //console.log("Request Assembly "+reqAssembly);
        // homozygous-alternate
        //console.dir(varseqCntReq1,{"depth":null});
        // { WES: 1, WGS: 1, PANEL: 0 }  => 1 hom-alt cnt for WES, 1 hom-alt cnt for WGS, 0 hom-alt cnt for PANEL
        var varseqCntReq2 = await getVarSeqTypeCounts(fileCntObj['gt']['hom-alt']['fileID'],reqAssembly,varContLog);
        //console.dir(varseqCntReq2,{"depth":null});
        // hom-alt
        varseqCntReq['WES']['het'] = varseqCntReq1['WES']['cnt'];
        varseqCntReq['WGS']['het'] = varseqCntReq1['WGS']['cnt'];
        varseqCntReq['PANEL']['het'] = varseqCntReq1['PANEL']['cnt'];
        varseqCntReq['WES']['hom-alt'] = varseqCntReq2['WES']['cnt'];
        varseqCntReq['WGS']['hom-alt'] = varseqCntReq2['WGS']['cnt'];
        varseqCntReq['PANEL']['hom-alt'] = varseqCntReq2['PANEL']['cnt'];
        
        // get the experiments 
        
        //console.log(varseqCntReq);
        // lifted assembly
        var varSeqCntLifted = await getVarSeqTypeCounts(newFileList,liftedAssembly,varContLog);
        // heterozygous
        var varSeqCntLifted1 = await getVarSeqTypeCounts(fileCntObjL['gt']['het']['fileID'],liftedAssembly,varContLog);
        // homozygous-alternate
        var varSeqCntLifted2 = await getVarSeqTypeCounts(fileCntObjL['gt']['hom-alt']['fileID'],liftedAssembly,varContLog);

        varSeqCntLifted['WES']['het'] = varSeqCntLifted1['WES']['cnt'];
        varSeqCntLifted['WGS']['het'] = varSeqCntLifted1['WGS']['cnt'];
        varSeqCntLifted['PANEL']['het'] = varSeqCntLifted1['PANEL']['cnt'];
        varSeqCntLifted['WES']['hom-alt'] = varSeqCntLifted2['WES']['cnt'];
        varSeqCntLifted['WGS']['hom-alt'] = varSeqCntLifted2['WGS']['cnt'];
        varSeqCntLifted['PANEL']['hom-alt'] = varSeqCntLifted2['PANEL']['cnt'];


        //console.log("Assembly - "+liftedAssembly);
        //console.dir(varSeqCntLifted1,{"depth":null});
        //console.dir(varSeqCntLifted2,{"depth":null});
        //console.log(varSeqCntLifted);

        var varSeqCntObj = {};
        varSeqCntObj[reqAssembly] = varseqCntReq;
        varSeqCntObj[liftedAssembly] = varSeqCntLifted;
        /*var vargtCntObj = {};
        vargtCntObj[reqAssembly] = fileCntObj['gt'];
        vargtCntObj[liftedAssembly] = fileCntObjL['gt'];
        console.log("Logging gt object ");
        console.dir(vargtCntObj,{"depth":null});*/

        // get the overall sample size 

        var sampleSizeSeq = await getSampSizeCnt(varSeqCntObj,varContLog);


        //console.log(varSeqCntObj);
        //var overallObj = {'present' : varSeqCntObj,'sample-size' : sampleSizeSeq}

        //await varResColl.updateOne({_id:req_id},{$set:{'overall':varSeqCntObj,status:'completed','centerId': postReq['centerId'],'hostId' : postReq['hostId'] ,'msg': msg_log}});
        await varResColl.updateOne({_id:req_id},{$set:{'overall':sampleSizeSeq,status:'completed','centerId': postReq['centerId'],'hostId' : postReq['hostId'] ,'msg': msg_log, 'annotation': varAnno}});


        return "Success";
    } catch(err) {
        throw err;
    }
}

const fetchVarAnno = async(var_key, assemblyType,fileID) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var importCollection;
        
        if ( (assemblyType == "GRCh37") || (assemblyType == "hg19") ) {
            importCollection = db.collection(importCollection1);
        } else if ( (assemblyType == "GRCh38") || (assemblyType == "hg38") ) {
            importCollection = db.collection(importCollection2);
        }

        var specificAnno = {"_id" : 0, "var_validate" : 0,"pid" : 0,"multi_allelic" : 0, "phred_polymorphism" : 0, "filter" : 0, "alt_cnt" : 0, "ref_depth" : 0, "alt_depth" : 0, "phred_genotype" : 0, "mapping_quality" : 0, "base_q_ranksum" : 0, "mapping_q_ranksum" : 0, "read_pos_ranksum" : 0, "strand_bias" : 0, "quality_by_depth" : 0, "fisher_strand" : 0, "vqslod" : 0, "gt_ratio" : 0, "ploidy" : 0, "somatic_state" : 0, "delta_pl" : 0, "stretch_lt_a" : 0, "stretch_lt_b" : 0, "trio_code" : 0,'fileID':0};

        var doc = await importCollection.findOne({'fileID':fileID,'var_key':var_key},{'projection':specificAnno});
        return doc;

    } catch(err) {
        throw err;
    }
}



// Function to aggregate and get the counts based on variant sequence type combinations
const getVarSeqTypeCounts = async(fileIDList,assembly,varContLog) => {
    try {
        var client = getConnection();
        const db = client.db(dbName); 

        var matchStage = { $match : {"fileID" : { $in : fileIDList } , 'state':{$ne:'unassigned'} } };

        var groupStage = { $group: {"_id" : {SeqTypeName: "$SeqTypeName"}, "count" : {$sum : 1}} };

        varContLog.debug("checkPhenExists matchStage : ");
        varContLog.debug(matchStage);
        varContLog.debug("checkPhenExists groupStage");
        varContLog.debug(groupStage);

        /*console.log("Logging the aggregate stages");

        console.log(matchStage);
        console.log(groupStage);*/

        //var initialCntObj = {'cnt':0,'het':0,'hom-alt':0}
        var fileSeqCntObj = await db.collection(indSampCollection).aggregate([matchStage, groupStage]);
        //var fileSeqCnt = {'WES' : 0, 'WGS' : 0, 'PANEL':0};
        var fileSeqCnt = {'WES' : {'cnt':0,'het':0,'hom-alt':0}, 'WGS' : {'cnt':0,'het':0,'hom-alt':0}, 'PANEL':{'cnt':0,'het':0,'hom-alt':0}};

        while ( await fileSeqCntObj.hasNext() ) {
            const doc = await fileSeqCntObj.next();
            //console.log(doc);
            //varContLog.debug(doc);
            var seqType = doc['_id']['SeqTypeName'];
            if ( doc['count'] ) {
                //fileSeqCnt[seqType] = doc['count'];                
                fileSeqCnt[seqType]['cnt'] = doc['count'];                
            } else {
                //fileSeqCnt[seqType] = 0;
                fileSeqCnt[seqType]['cnt'] = 0;                
            }
        }

        // { 'WES' : 2, 'WGS' : 5}
        //console.log(fileSeqCnt);
        return fileSeqCnt;

    } catch(err) {
        throw err;
    }
}

const getSampSizeCnt = async(varSeqCntObj,varContLog) => {
    try {
        var client = getConnection();
        const db = client.db(dbName); 
        //console.log("Logging the variant seq cnt object");
        //console.dir(varSeqCntObj,{"depth":null});
        //var matchStage = { $match:{'state':{$ne:'unassigned'} } };
        // updating match criteria to include only VCF samples. Not SV
        var matchStage = { $match:{'state':{$ne:'unassigned'},"FileType":{$in:["VCF","gVCF"] } } };

        var groupStage = { $group: {"_id" : {SeqTypeName: "$SeqTypeName",AssemblyType: "$AssemblyType"}, "count" : {$sum : 1}} };

        varContLog.debug("checkPhenExists matchStage : ");
        varContLog.debug(matchStage);
        varContLog.debug("checkPhenExists groupStage");
        varContLog.debug(groupStage);

        /*console.log("Logging the aggregate stages");

        console.log(matchStage);
        console.log(groupStage);*/

        var fileSeqCntObj = await db.collection(indSampCollection).aggregate([matchStage, groupStage]);
        var fileSeqCnt = {'WES' : 0, 'WGS' : 0, 'PANEL':0};
        var sampSize = []

        while ( await fileSeqCntObj.hasNext() ) {
            const doc = await fileSeqCntObj.next();
            var overallCnt = {};
            //console.log(doc);
            //varContLog.debug(doc);
            var seqType = doc['_id']['SeqTypeName'];
            var refBuild = doc['_id']['AssemblyType'];
            overallCnt['exp'] = seqType;
            overallCnt['ref_build'] = refBuild;
            //overallCnt['cnt'] = varSeqCntObj[refBuild][seqType];
            overallCnt['cnt'] = varSeqCntObj[refBuild][seqType]['cnt'];
            overallCnt['het'] = varSeqCntObj[refBuild][seqType]['het'];
            overallCnt['hom-alt'] = varSeqCntObj[refBuild][seqType]['hom-alt'];
            //overallCnt['gt'] = vargtCntObj[refBuild];
            overallCnt['sample-size'] = doc['count'] || 0;
            sampSize.push(overallCnt);
            /*var cnt = 0
            if ( doc['count'] ) {
                cnt = doc['count'];                
            } 
            fileSeqCnt[seqType] = cnt;
            console.dir(fileSeqCnt,{"depth":null})
            overallCnt[refBuild] = fileSeqCnt;*/
        }

        // { 'WES' : 2, 'WGS' : 5}
        //console.log(sampSize);
        return sampSize;

    } catch(err) {
        throw err;
    }
}

// multiple variants and multiple phenotype correlation
// testing purpose - this function will be trigerred from varDiscRoutes
const varPhenAnalysis = async(req_id,hpoList,varList, assembly,seqType) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const resCol = db.collection(resultCollection);
        // fetch from results collection when variants are not provided as input
        if ( varList.length == 0 ) {
            // Step1- fetch variants from results collections
             //console.log("Hello----");
            varList = await fetchResVariants(req_id,db,resCol);
            //console.log(varList);
        }
        varContLog.debug("Variant List length "+varList.length);
        //return varList;

        // Step 2 - Fetch the fileIDs based on the variant list
        // request assembly - fileID list
        //console.log(assembly);

        var varObj = {};
        var varNum = 0;
        for ( var idx in varList ) {
            ++varNum;
            varContLog.debug("Var number "+varNum);
            // to be done for each variant
            var varKey = varList[idx];
            var varCnt = {};
            var varCntAnno = {};
            
            var fileIdList = await fetchfileID(varKey,assembly,varContLog);
            //console.log("Logging fileID List ---- ");
            //console.log(fileIdList);

            // Step 3 - Get the Individuals having this variant
            var varIndList = await fetchVarInd(fileIdList,seqType,"var_exist",varContLog);
            //console.log("Ind with Variants");
            //console.log(varIndList);
            varContLog.debug("3.Individuals having variant below:");
            varContLog.debug(varIndList);
            //varCnt['varInd'] = varIndList.length;

            // Step 4 - Get the Individuals not having this variant
            var noVarIndList = await fetchVarInd(fileIdList,seqType,"var_not_exist",varContLog);
            //console.log("Ind without variant");
            //console.log(noVarIndList)
            varContLog.debug("4.Individuals not having variant below :");
            //varCnt['noVarInd'] = noVarIndList.length;

            varContLog.debug(noVarIndList);
            

            varContLog.debug("5.Function to retrieve phenotype for Individuals having variant");
            var retObj1 = await checkPhenExists(varIndList,hpoList,varContLog);
            //varContLog.debug(retObj1);
            //console.log("Individuals with Variant + phen -phen")
            //console.dir(retObj1,{"depth":null});
            varCnt['var+phen'] = retObj1['exist'];
            varCnt['var-phen'] = retObj1['not_exist'];

            varContLog.debug("6.Function to retrieve phenotype for Individuals NOT having variant");
            var retObj2 = await checkPhenExists(noVarIndList,hpoList,varContLog);
            //console.log("Individuals without Variant + phen -phen")
            //console.dir(retObj2,{"depth":null});
            //varContLog.debug(retObj2);
            varCnt['-var+phen'] = retObj2['exist'];
            varCnt['-var-phen'] = retObj2['not_exist'];

            var resObj = await resCol.findOne({"_id":varKey},{projection:{'annotations':1}});
            varCntAnno['annotations'] = resObj['annotations'];
            varCntAnno['counts'] = varCnt;
            //varCnt['annotations'] = resObj['annotations'];
            varObj[varKey] = varCntAnno;


            //console.log("Logging object details------")
            //console.dir(varCnt,{"depth":null});
            //console.dir(varObj,{"depth":null});
        }

        if ( varList.length >  0) {
            // Get the overall counts
            // temporary - repeat procedure, later create a function
            // search for multiple variants
            var fileIdList = await fetchfileID(varList,assembly,varContLog,'multiple');
            var overallVarCnt = {};
            // Step 1 - Get the Individuals having this variant
            var varIndListO = await fetchVarInd(fileIdList,seqType,"var_exist",varContLog);

            varContLog.debug("1.Overall- Individuals having variant below:");
            varContLog.debug(varIndListO);

            // Step 2 - Get the Individuals not having this variant
            var noVarIndListO = await fetchVarInd(fileIdList,seqType,"var_not_exist",varContLog);
            
            varContLog.debug("2.Overall-Individuals not having variant below :");

            varContLog.debug(noVarIndListO);
            
            varContLog.debug("3.Overall-Function to retrieve phenotype for Individuals having variant");
            //var overallPhen = await checkPhenExists(varIndList,hpoList,varContLog);
            // updating overall varind list
            var overallPhen = await checkPhenExists(varIndListO,hpoList,varContLog);
            
            overallVarCnt['var+phen'] = overallPhen['exist'];
            overallVarCnt['var-phen'] = overallPhen['not_exist'];

            varContLog.debug("4.Overall-Function to retrieve phenotype for Individuals NOT having variant");
            //var overallNoPhen = await checkPhenExists(noVarIndList,hpoList,varContLog);
            // updating overall novar ind list
            var overallNoPhen = await checkPhenExists(noVarIndListO,hpoList,varContLog);
            //console.log("Individuals without Variant + phen -phen")
            //console.dir(retObj2,{"depth":null});
            //varContLog.debug(retObj2);
            overallVarCnt['-var+phen'] = overallNoPhen['exist'];
            overallVarCnt['-var-phen'] = overallNoPhen['not_exist'];
            varObj['overall'] = overallVarCnt;
            varContLog.debug("Log overall counts and check");
            varContLog.debug(overallVarCnt,{"depth":null});
            varObj['status'] = 'completed'
        } else {
            varObj['status'] = 'no-variants'
        }


        /*if ( varList.length == 0 ) {
            varObj['status'] = 'no-variants'
        } else  {
            varObj['status'] = 'completed'
        }*/


        // return the status and object
        // store the data in database
        varContLog.debug("Variant Object will be returned");
        return varObj;


        //return fileIdList;

        //const varResColl = db.collection(variantDiscResults);
    } catch(err) {
        console.log(err);
        throw err;
    }
}

// Retrieve variants from results collection
const fetchResVariants = async(req_id,db,resCol) => {
    try {
        //const resCol = db.collection(resultCollection);
        var batchID = 1;
        var last = 0;
        var varList = [];
        // check if request id exists in result collection

        var reqExist = await resCol.findOne({"_id":`${req_id}_${batchID}`});
        varContLog.debug("fetchResVariants function");
        varContLog.debug(reqExist);

        if ( !reqExist) {
            throw "invalid or expired request id"
        }
        var bulkOps = [];
        // batch size. say, 2k variants. 20 batches
        while ( batchID < 20 ) {
            var idVal = `${req_id}_${batchID}`;
            varContLog.debug(idVal);

            var matchStage = {$match:{"_id":idVal}};
            var unwind = {$unwind : "$documents"};
            // $group based on variant key
            // include annotations , to retrieve them from projection
            var groupStage = {$group: {_id: "$documents.var_key", uniqueValue: { $first: "$documents.var_key" },gene_annotations: { $first: "$documents.gene_annotations" },gnomAD : {$first: "$documents.gnomAD"},phred_score : {$first: "$documents.phred_score"},motif_feature_consequences : {$first: "$documents.motif_feature_consequences"},regulatory_feature_consequences : {$first: "$documents.regulatory_feature_consequences"},RNACentral : {$first: "$documents.RNACentral"}}};
            var projectStage = {$project : {_id: 1, uniqueValue: 1, gene_annotations:1,gnomAD:1,phred_score:1,motif_feature_consequences:1, regulatory_feature_consequences:1,RNACentral:1}};
            
            var resultCursor = await resCol.aggregate([matchStage, unwind, groupStage, projectStage]);
            //resultCursor.project({'documents.var_key':1,'lastBatch' : 1}); 
            //{ "_id" : "3308_1", "documents" : [ { "var_key" : "19-17281821-G-GC" }, { "var_key" : "19-17281821-G-GC" } ] }
            while ( await resultCursor.hasNext() ) {
                const doc = await resultCursor.next();
                //console.log(doc);
                const varKey = doc['_id'];
                varList.push(varKey);
                
                varContLog.debug(batchID)
                // include createdAt for ttl based deletion
                var timeField = new Date();
                var insertDoc = {_id: varKey, 'annotations': {'gene_annotations':doc.gene_annotations,'gnomAD' : doc.gnomAD,'phred_score' : doc.phred_score, 'motif_feature_consequences' : doc.motif_feature_consequences,'regulatory_feature_consequences' : doc.regulatory_feature_consequences,'RNACentral' : doc.RNACentral},'createdAt':timeField};
                //console.log(insertDoc);
                bulkOps.push({'insertOne':insertDoc});
                // traverse documents.
                /*if ( doc.lastBatch == 1 ) {
                    break;
                }*/
                if ( (bulkOps.length % 100 ) === 0 ) {
                    //console.log("Execute the bulk update ");
                    resCol.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                            console.log("loaded")
                        }).catch((err1) => {
                            console.log(err1);
                    });
                    bulkOps = [];
                }

            }//resultcursor
            /*if ( last ) {
                break;
            }*/

            varContLog.debug("***************")
            batchID++;
        }// while loop batchId

        varContLog.debug("*****************8")
        if ( bulkOps.length > 0 ) {
            resCol.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
            }).catch((err1) => {
                console.log(err1);
            });
        }
        //console.log(varList);

        return varList;
    } catch(err) {
        console.log(err);
        throw err;
    }
}

// fetch the Individuals crs. to the variant and the request
const fetchVarSamples = async(queryID,variant) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const resCol = db.collection(resultCollection);

        varContLog.debug("fetchVarSamples function ");
        //console.log("fetchVarSamples function ");
        // check if the variant+phen combination exists
        //var res= await resCol.find({_id: parseInt(queryID)},{'projection': 'documents.1-17379-G-A.counts.var+phen':1});
        var projectionCond = {};
        // for testing - commented
        var projectionField = `documents.${variant}.counts.var+phen`;
        //var projectionFieldDoc = `${doc1}.documents.${variant}.counts.var+phen`;
        //console.log(`projectionFieldDoc:${projectionFieldDoc}`);
        //var projectionField = `documents.${variant}.counts.var-phen`;
        projectionCond[projectionField] = 1;
        varContLog.debug(projectionCond);
        varContLog.debug(`Checking for queryID ${queryID}`);
        //console.log(`Checking for queryID ${queryID}`);

        var curs1 = await resCol.find({_id: parseInt(queryID)},{'projection':projectionCond});
        var exists = 0;
        // check if request id exists in the results collection
        while ( await curs1.hasNext()) {
            const doc = await curs1.next();
            varContLog.debug("Document");
            varContLog.debug(doc,{"depth":null});
            //console.log(doc,{"depth":null});
            //console.log(variant);
            // check if the variant has var+phen 
            if ( doc != null && variant in doc['documents'] ) {
                if (doc['documents'][variant]['counts']['var+phen'] > 0 ) {
                    exists = 1;
                    varContLog.debug("Proceed to the next step----- ");
                    //console.log("Proceed to the next step----- ");
                } else {
                    console.log(doc);
                }
            } else {
                console.log("Else condition");
            }
        }
    
            // fetch the fileIDs
            var fileID = [];
            if ( exists ) {
                // fetch the fileIDs
                var matchObj = {_id:{$regex: queryID+'_*'}};
                //var matchObj = {_id: parseInt(queryID)};
    
                var curs2 = await resCol.aggregate([{$match:matchObj},{$unwind : "$documents"}]);
    
                while ( await curs2.hasNext()) {
                    const doc1 = await curs2.next();
                    varContLog.debug("Logging document from results collection ----- ");
                    varContLog.debug(doc1);
                    // Fetch the document crs. to the variant
                    if ( doc1.documents.var_key == variant ) {
                        varContLog.debug("Logging the document which has the matched variant ------");
                        varContLog.debug(doc1.documents);
                        varContLog.debug(doc1.documents.fileID);
                        varContLog.debug(`projection field is ${projectionField}`);
                        //console.log(projectionField);
                        if ( doc1.documents.fileID ) {
                            fileID.push(doc1.documents.fileID);
                        }
                    }
                }
            }
            varContLog.debug("fileID list given below ");
            varContLog.debug(fileID);
    
            // if fileID is defined, fetch the Ind ID
            const indSColl = db.collection(indSampCollection);
            var indID = [];
            var curs3 = await indSColl.find({'fileID': {$in:fileID}},{'projection': {'individualID':1}});
            while ( await curs3.hasNext()) {
                //console.log(doc2);
                if ( doc2.individualID ) {
                    indID.push(doc2.individualID);
                }
            }
            varContLog.debug("indID list given below ");
            varContLog.debug(indID);
    
            //console.dir(res,{"depth":null});
    
            return indID;
            // get the file-IndID 
            // wingsIndSampColl
    
    
        } catch(err) {
            console.log(err);
            throw err;
        }
}
    
    
// fetch the Individuals crs. to the variant and the request
const fetchInd = async(queryID,variant) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const varResColl = db.collection(variantDiscResults);

        varContLog.debug("fetchVarSamples function ");
        varContLog.debug(`Checking for queryID ${queryID}`);

        var curs1 = await varResColl.find({_id: parseInt(queryID),'overall.variant-phenotype':{$gt:0}},{'projection':{'ind_list':1}});
        var ind_list = "";
        // check if request id exists in the results collection
        while ( await curs1.hasNext()) {
            const doc = await curs1.next();
            varContLog.debug("Document");
            varContLog.debug(doc,{"depth":null});
            
            if ( doc != null && ind_list in doc ) {
                ind_list = doc['ind_list'];
            } else {
                console.log("Else condition");
            }
        }
    
        return ind_list;
    
    
        } catch(err) {
            console.log(err);
            throw err;
        }
}
    


// Send e-mail to the PIID
const contactPI = async(reqBody) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        varContLog.debug("contactPI function ");
        const resCol = db.collection(resultCollection);
        await sendMail(reqBody);
    } catch(err) {
        throw err;
    }
}

async function sendMail(reqBody) {
    try {
        var mailQueue = [];
        var bulkOpsQueue = [];
        varContLog.debug("sendMail function");
        varContLog.debug(reqBody);
        var request_id = reqBody.req_id;
        var variant = reqBody.variant;
        var pi_mail = reqBody.pi_mail;
        var center_id = reqBody.center_id;
        var piid = reqBody.piid;
        var hpo = reqBody.hpo;
        var req_user = reqBody.user_name;
        var req_user_affl = reqBody.user_affl;
        var pi_name = reqBody.pi_name;
        var central_req_id = reqBody.central_req_id;

        //console.log(hpo);
        const listHtml = generateListHtml(hpo);
        //console.log("list html page ");
        //console.log(listHtml);
        var mailJson = {"center_id" : center_id, "req_id" : request_id, "piid" : piid};

        var smtpHost = process.env.SMTP_SERVER || 'smtp.uantwerpen.be';
        var smtpPort = process.env.SMTP_PORT || 25;
        var smtpFrom = pi_mail || "nishkala.sattanathan@uantwerpen.be";
        var smtpTo = pi_mail || "nishkala.sattanathan@uantwerpen.be";

        let transporter = nodemailer.createTransport({
            host: smtpHost, // smtp server
            port: smtpPort,
            auth: false,
            tls: {
                  // if we are doing from the local host and not the actual domain of smtp server
                  rejectUnauthorized: false
                 }
          });
        
        const encryptedJson = encryptJson(mailJson);
        //console.log('Encrypted:', encryptedJson);
        
        var restApiURL = process.env.REST_API_DEV;
        if ( process.env.NODE_ENV == "prod" || process.env.NODE_ENV == "uat" ) {
            restApiURL = process.env.REST_API_PROD;
        }

        const apiUrlA = `${restApiURL}/variant/collaborate/approve?data=${encodeURIComponent(encryptedJson.data)}&iv=${encodeURIComponent(encryptedJson.iv)}`;
        //console.log('A API URL:', apiUrlA);

        const apiUrlR = `${restApiURL}/variant/collaborate/reject?data=${encodeURIComponent(encryptedJson.data)}&iv=${encodeURIComponent(encryptedJson.iv)}`;

        //console.log('R API URL ', apiUrlR);


        const htmlTemplate = `
        <html>
    <body>
      <h1>WiNGS - Sample Access Request</h1>
      <div>Dear ${pi_name},<br><br>
      Authorized WiNGS User, ${req_user} from the Center - ${req_user_affl} has executed  variant-phenotype correlation query for a specific gene/region and a list of phenotype terms.WiNGS Generated Request ID for the analysis : ${central_req_id}. Below you can see the phenotype terms used in the query: <br><br>
      ${listHtml} <br><br></div>
	  <div>Based on the results, there were samples within your center that has the variant ${variant} and atleast one of the phenotype mentioned above.
    To perform further analysis related to the specific phenotype, User ${req_user} would like to collaborate with your center.Please click on Approve if you agree to this request.By clicking on Approve, your contact details will be shared with the user.</div><br>
      <div>
        <table cellpadding="0" cellspacing="0" border="0" style="padding: 20px 0;">
          <tr>
            <td style="background-color: green; padding: 10px;">
              <a href="${apiUrlA}" style="color: white; text-decoration: none;">Approve</a>
            </td>
            <td style="width: 10px;">&nbsp;</td>
            <td style="background-color: red; padding: 10px;">
              <a href="${apiUrlR}" style="color: white; text-decoration: none;">Reject</a>
            </td>
          </tr>
        </table>
      </div>
      <br>
      <div>If you have any questions, feel free to <a href="mailto:nishkala.sattanathan@uantwerpen.be?subject=WiNGS%20Approval%20Request%20ID%20${central_req_id}&body=Please%20provide%20details%20about%20your%20question%20or%20issue%20here.">contact WiNGS team</a>.</div><br>
	  <div>Thank you,<br>WiNGS Admin Team</div>
    </body>
</html>
  `;
        

  //console.log("Logging the html template below ");
  //console.log(htmlTemplate);
          let info = await transporter.sendMail({
            from: smtpFrom, // sender address
            to: smtpTo, // list of receivers
            subject: "WiNGS Platform Collaboration Request - Pilot Phase", // Subject line
            //subject: "WiNGS Uantwerp Center Sample Sheet Validation Results ", // Subject line
            //text: mailMsg, // plain text body
            html: htmlTemplate // html body
          });

        //console.log(info);
        //console.log("Logging the info of the mail message transporter");
        //console.log(info);
        console.log("Message sent ID "+info.messageId);
        //console.log("Message sent: %s", info.messageId);
        return "success";
    } catch(err) {
        throw err;
    }
}

// Function to generate HTML for a list of items with boxed terms
function generateListHtml(items) {
    let listHtml = '<ul>';
    items.forEach(item => {
      listHtml += `<li style="padding: 5px;">${item}</li>`;
    });
    listHtml += '</ul>';
    return listHtml;
}

module.exports = {varPhenSearch,getVarPhenCount,getGeneCount,getRegionCount,varCounts,varPhenAnalysis, fetchVarSamples,contactPI,fetchInd};
