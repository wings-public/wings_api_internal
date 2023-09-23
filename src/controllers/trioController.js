#!/usr/bin/env node
'use strict';
const spawn  = require('child_process');
//const runningProcess = require('is-running');
var path = require('path');
const promisify = require('util').promisify;
const { createReadStream, createWriteStream, stat ,unlink,existsSync} = require('fs');
var stats = promisify(stat);
const argParser = require('commander');
const {logger} = require('../controllers/loggerMod');
const configData = require('../config/config.js');
const { app:{instance,logLoc}, db:{sampleAssemblyCollection,familyCollection,importCollection1,importCollection2,trioCollection} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const trioQueueScraper = require('../routes/queueScrapper').trioQueueScrapper;
var familyFormat = require('../config/familyConf.json');
//var db = require('../controllers/db.js');
(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-r, --request_json <request_json>', 'json request with trio sample details')
            .option('-p, --pid <pid>', 'pid')
        argParser.parse(process.argv);

        var trioReq1 = argParser.request_json;
        var pid = argParser.pid;

        if ( argParser.request_json == undefined ) {
            argParser.outputHelp();
            process.exit(1);
        }

        var month = new Date().getMonth();
        var year = new Date().getFullYear();
        var date1 = new Date().getDate()+'-'+month+'-'+year;
        var logFile = `trio-controller-${pid}-${date1}.log`;
        //var logFile = loggerEnv(instance,logFile1);
        var trioCont = logger('trio',logFile);

        //console.log("************* TRIO CONTROLLER ");
        //console.dir(trioReq,{"depth":null});

        var db = require('../controllers/db.js');
        var queryCollection = "";
        //console.dir(trioReq,{"depth":null});

        var trioReq = JSON.parse(trioReq1);
        trioCont.debug(trioReq);

        // fetch details of trio using family ID

        var trioColl = db.collection(trioCollection);

        var familyId = parseInt(trioReq['family_id']);
        var localID = trioReq['localID'];
        await trioColl.updateOne({'TrioLocalID':localID,'TrioStatus':{$ne:'disabled'}},{$set : {'TrioStatus':'ongoing','TrioErrMsg':''}});

        if ( (trioReq['assembly_type'] == 'GRCh37') || (trioReq['assembly_type'] == 'hg19') ) {
            queryCollection = importCollection1;
        } else if ( (trioReq['assembly_type'] == 'GRCh38')  || (trioReq['assembly_type'] == 'hg38') ) {
            queryCollection = importCollection2;
        }
        trioCont.debug("queryCollection is "+queryCollection);
        var queryCollObj = db.collection(queryCollection); 

        trioCont.debug("Calling scanTrioSample");
        
        var familyColl = db.collection(familyCollection);
        //await familyColl.updateOne({'_id':localID,'TrioStatus':{$ne:'disabled'}},{$set : {'TrioStatus' : 'ongoing', 'TrioErrMsg' : ""}});
        var result = await scanTrioSample(trioReq,trioColl,queryCollObj,trioCont);
        var stop_time = new Date();
        console.log("Trio Controller stop time is "+stop_time);
        trioCont.debug("Result Done. Now Await timeout");
        // commenting debug timeout
        //await new Promise(resolve => setTimeout(resolve, 200000));
        //console.log("Timeout completed");
        trioCont.debug("Completed trio controller sample logic");
        process.exit(0);
    } catch(err) {
        console.log(err);
        //throw err;
        process.exit(1);
    }
}) ();

async function generateCodes(n){
    var states = [];
  
    // Convert to decimal
    var maxDecimal = parseInt("1".repeat(n),2);
    //console.log("maxDecimal is "+maxDecimal);
  
    // For every number between 1->decimal : we do not need code 000
    for(var i = 1; i <= maxDecimal; i++) {
      // Convert to binary, pad with 0, and add to final results
      states.push(i.toString(2).padStart(n,'0'));
    }
  
    return states;
  }

  
async function scanTrioSample(reqJson,trioColl,queryCollObj,trioCont) {
    try {
        
        var relations = Object.keys(reqJson['family']); // keys:Proband Father Mother
        var samples = Object.values(reqJson['family']); // values:samples
        var trioCodeHash = {};

        var localID = reqJson['localID'];
        trioCont.debug("Relations are "+relations);
        trioCont.debug("Samples are "+samples);
        
        // order in which the trio code is plotted. 111/011/101
        var order = familyFormat['trio']['order'];
        var maxCodes = await generateCodes(relations.length);
        for ( var idx in maxCodes ) {
            var c1 = maxCodes[idx];
            trioCodeHash[c1] = 0;
        }

        //console.dir(trioCodeHash);

        var sampleCodes = familyFormat['trio']['sample_codes'];
        var sampleCodeKeys = Object.keys(sampleCodes);
        // trioCodeHash is populated with the trio sample counts
        console.log("Time taken for Sample Count Start *********"+new Date());
        for (let i = 0; i < sampleCodeKeys.length ; i++ ) {
            var relation1 = sampleCodeKeys[i];
            trioCont.debug("Relation is "+relation1);
            var sampId = reqJson['family'][relation1];
            var trioCode = sampleCodes[relation1];
            trioCont.debug("Trio code is "+trioCode);
            trioCont.debug("Samp Id is "+sampId);
            var sampCount = await getSampleCount(queryCollObj,sampId);
            trioCont.debug(`Sample count for ${sampId} is ${sampCount}`);
            // unset trio_code to ensure there are no old traces of trio code for hom-ref variants
            trioCont.debug("Reset trio_code for hom-ref variants in sample  "+sampId);
            await queryCollObj.updateMany({'fileID':sampId,'alt_cnt':{$eq:0}},{$unset : {'trio_code':1}});
            trioCodeHash[trioCode] = sampCount;
        }

        console.log("Time taken for Sample Count Stop *********"+new Date());
        trioCont.debug(trioCodeHash);
     
        //var order = ['Proband','Father','Mother'];

        trioCont.debug("Trio code plot order is "+order);
        var sampleOrder = [];
        // traverse the order array. Example : Proband,Father,Mother
        for ( var idx1 in order ) {
            // fetch the familyType from order array
            var familyType = order[idx1];
            if ( reqJson['family'][familyType]) {
                // fetch the 'sid' corresponding to the family member type.
                // 1000001(Proband),1000002(Father),1000003(Mother)
                sampleOrder.push(reqJson['family'][familyType]);
            }
        }

        trioCont.debug(sampleOrder);

        var defaultSort = {'var_key':1};
        // hom-ref variants are excluded from trio analysis
        var filter = {'fileID':{$in:samples},'alt_cnt':{$ne:0}};
        var familyCursor = await queryCollObj.find(filter);
        familyCursor.sort(defaultSort);
        
        familyCursor.project({'var_key':1,'fileID':1}); 
        var varGroup = 0;
        var dataSet = {};
        var bulkOps = [];
        
        console.log("Time taken for actual trio processing Start #############"+new Date());
        while ( await familyCursor.hasNext() ) {
            const doc = await familyCursor.next();
            var docKey = doc['var_key'];
            //console.log(doc);
            //trioCont.debug("logging variant key");
            //trioCont.debug(docKey);
            // When the variant changed, we can start processing the data stored in the dataSet hash
            // assignCode is calculated based on the sampleOrder and the data stored in the dataSet hash
            if ( varGroup && doc['var_key'] != varGroup ) {
                //console.log("Variant type changed now ---");
                //console.dir(dataSet,{"depth":null});
                //console.log(varGroup);
                var updateFilter = await processVariants(sampleOrder,dataSet,trioCodeHash,samples);

                bulkOps.push(updateFilter); 

                dataSet = {};
            }
            if ( ( bulkOps.length > 0 ) && ((bulkOps.length % 5000 ) === 0 )) {
                trioCont.debug("Bulk ops length is "+bulkOps.length);
                //console.log("Execute the bulk update ");
                queryCollObj.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                    trioCont.debug("Executed bulkWrite and the results are ");
                    trioCont.debug(res.modifiedCount);
                    }).catch((err1) => {
                        trioCont.debug("Error executing the bulk operations");
                        trioCont.debug(err1);
                });
                bulkOps = [];
            }

            // storing the data related to the variant in the dataSet hash
            var id = doc['_id'];
            var sid = doc['fileID'];
            //dataSet['id'] = id;
            dataSet['var_key'] = doc['var_key'];
            dataSet[sid] = 1;
            varGroup = doc['var_key'];
            //trioCont.debug("Logging vargroup data");
            //trioCont.debug(dataSet);
            //console.log(doc);
        }

        // This is required to handle the last variant set of the trio stream of samples.
        var updateFilter1 = await processVariants(sampleOrder,dataSet,trioCodeHash,samples);
        bulkOps.push(updateFilter1);

        if ( bulkOps.length > 0 ) {
            trioCont.debug("Bulk ops length is ---"+bulkOps.length);
            //console.log("Execute the bulk update ");
            queryCollObj.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                trioCont.debug("Executed bulkWrite and the results are ----");
                trioCont.debug(res.modifiedCount);
                }).catch((err1) => {
                    trioCont.debug("Error executing the bulk operations");
                    trioCont.debug(err1);
            });
            bulkOps = [];
        }

        console.log("Time taken for actual trio processing Stop #############"+new Date());
        trioCont.debug(trioCodeHash);
        //console.dir(trioCodeHash,{"depth":null});

        //await familyColl.updateOne({'_id':familyId},{$set : {'TrioCode' : trioCodeHash}});
        // replace above with the trio collection
        await trioColl.updateOne({'TrioLocalID':localID,'TrioStatus':{$ne:'disabled'}},{$set : {'TrioCode' : trioCodeHash}});

        return "success";
    } catch(err) {
        console.log(err);
        throw err;
    }
}


async function processVariants(sampleOrder,dataSet,trioCodeHash,samples) {
    var assignCode = "";
    for ( var idx in sampleOrder ) {
        var orderSid = sampleOrder[idx];
                    
        if ( dataSet[orderSid] ) {
            var tmpCode = 1;
            assignCode = assignCode+tmpCode;
        } else {
            var tmpCode = 0;
            assignCode = assignCode+tmpCode;
        }
    }
    //trioCont.debug('Data is been process for the variant group '+varGroup);
    //trioCont.debug('Calculated assign code is '+assignCode);

    if (trioCodeHash[assignCode]) {
        var fetchCnt = trioCodeHash[assignCode];
        trioCodeHash[assignCode] = fetchCnt+1;
    } else {
        trioCodeHash[assignCode] = 1;
    }

    //trioCont.debug("Logging the trioCodeHash below:");
    //trioCont.debug(trioCodeHash);
    
    // prepare update statement
    var updateFilter = {};
    var filter = {'fileID' : {$in:samples}, 'var_key':dataSet['var_key']};
    var update = {$set : {'trio_code':assignCode}};
    updateFilter['updateMany'] = { filter,update};

    return updateFilter;
}

async function getSampleCount(queryCollObj,sid) {
    try {
        sid = parseInt(sid);
        //console.log("Sample ID "+sid);
        // ignore hom-ref variants. only heterozygous, hom-alt variants are considered for counts.
        var sampCnt = await queryCollObj.find({'fileID':sid,'alt_cnt':{$ne:0}}).count();
        //console.log("Sample Count is "+sampCnt);
        return sampCnt;
    } catch(err) {
        throw err;
    }
}