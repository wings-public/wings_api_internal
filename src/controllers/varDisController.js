const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');
const path = require('path');
const spawn  = require('child_process');
const configData = require('../config/config.js');
const { app:{instance} } = configData;
const {logger} = require('../controllers/loggerMod');
const { app: {tmpCenterID,tmpHostID,liftMntSrc,liftoverDocker},db : {dbName,variantDiscResults,importCollection1,importCollection2,indSampCollection,phenotypeColl} } = configData;
const readline = require('readline');
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const getConnection = require('../controllers/dbConn.js').getConnection;

var pid = process.pid;
var uDateId = new Date().valueOf();
var logFile = `varDisc-control-logger-${pid}-${uDateId}.log`;
var varContLog = logger('var_disc',logFile);

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
            console.log("Try Block");
            varContLog.debug("Liftover process completed now ");
            liftedVariant = await readFile(variantFile,varContLog);
            console.log(liftedVariant);
        } catch(err) {
            console.log("Logging error from varDiscController-liftover error");
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
        var retObj1 = await checkPhenExists(varIndList,phenotype,varContLog);
        varContLog.debug(retObj1);

        varContLog.debug("5.Function to retrieve phenotype for Individuals NOT having variant");
        var retObj2 = await checkPhenExists(noVarIndList,phenotype,varContLog);
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
        var retVal = {"centerId" : varData['centerId'],"hostId" : varData['hostId'],'overall': varData['overall'],status:varData['status'], msg: varData['msg'], result: varData['result']};

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
        console.log(result);

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
        console.dir(filterRule,{"depth":null});
        var result = [];
        
        var passHash = { 'pass' : count, 'childIds' : passNodeId };
        var failHash = { 'fail' : count, 'childIds' : failNodeId };
        result.push(passHash);
        result.push(failHash);
        console.log(result);

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

const checkPhenExists = async(indIdList,phenTerm,varContLog) => {
    try {
        var client = getConnection();
        const db = client.db(dbName); 

        var matchStage = { $match : {"IndividualID" : { $in : indIdList } } };
        var groupStage = { $group: {"_id" : {IndividualID: "$IndividualID",hpo_id:"$hpo_id" }, "count" : {$sum : 1}} };
        var matchStage2 = { "isMatch" : { $cond: [ {"$eq" : ["$_id.hpo_id",phenTerm] },"true","false"] } };

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


const fetchfileID = async(var_req,assemblyType,varContLog) => {
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
        var fileQuery = {'var_key':var_req,'alt_cnt':{$ne:0}};

        varContLog.debug(`Assembly is ${assemblyType}`);
        varContLog.debug("fetchfileID-fileQuery is");
        varContLog.debug(fileQuery);

        var results = await importCollection.find(fileQuery,{projection:{'fileID':1,'var_key':1}});
        var fileidList = [];
        while ( await results.hasNext()) {
            const doc = await results.next();
            fileidList.push(doc['fileID']);
        }
        return fileidList;

    } catch(err) {
        throw err;
    }
}

module.exports = {varPhenSearch,getVarPhenCount,getGeneCount,getRegionCount};
