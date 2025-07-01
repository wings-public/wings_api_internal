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
const { app:{instance,logLoc}, db:{sampleAssemblyCollection,familyCollection,importCollection1,importCollection2,trioCollection,famAnalysisColl} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const trioQueueScraper = require('../routes/queueScrapper').trioQueueScrapper;
var familyFormat = require('../config/familyConf.json');
//var db = require('../controllers/db.js');
(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-r, --request_json <request_json>', 'json request details')
            .option('-p, --pid <pid>', 'pid')
        argParser.parse(process.argv);

        var famReq = argParser.request_json; 
        console.log(famReq);
        var pid = argParser.pid || process.pid;

        if ( argParser.request_json == undefined ) {
            argParser.outputHelp();
            process.exit(1);
        }

        var month = new Date().getMonth();
        var year = new Date().getFullYear();
        var date1 = new Date().getDate()+'-'+month+'-'+year;
        var logFile = `fam-controller-${pid}-${date1}.log`;
        //var logFile = loggerEnv(instance,logFile1);
        var famCont = logger('family',logFile);

        //console.log("************* TRIO CONTROLLER ");
        //console.dir(trioReq,{"depth":null});

        var db = require('../controllers/db.js');
        var queryCollection = "";
        //console.dir(trioReq,{"depth":null});

        var famReq = JSON.parse(famReq);
        console.log(famReq);
        famCont.debug(famReq);

        // fetch details of individuals using family local id

        var famAnalColl = db.collection(famAnalysisColl);

        console.log("Logging family details -----------");
        console.dir(famReq,{"depth":null});
        var famLocalID = famReq['family_local_id'];
        var affected_mem = famReq['affected_mem'];
        var unaffected_mem = famReq['unaffected_mem'] || [];
        
        console.log("Logging object details -----")
        console.log(famLocalID);
        console.log(affected_mem);
        console.log(unaffected_mem);


        var query = {};
        var status = 1;
        var msg = "";
        // check if the family local ID is valid
        query = { _id : famLocalID };
        msg = "Invalid family code";
        status = await checkValid(famAnalColl,famLocalID,query,msg);


        await famAnalColl.updateOne({'_id':famLocalID,'Status':{$ne:'disabled'}},{$set : {'Status':'started','ErrMsg':''}});

        // check if affected members in the list
        query = { _id : famLocalID,"details.family_analysis" : {$all : affected_mem } };
        msg = "Affected member not defined";
        status = await checkValid(famAnalColl,famLocalID,query,msg);

        // check if unaffected members in the list
        if ( unaffected_mem.length > 0 ) {
            query = { _id : famLocalID,"details.family_analysis" : {$all : unaffected_mem } };
            msg = "Unaffected member not defined";
            status = await checkValid(famAnalColl,famLocalID,query,msg);
        }

        console.log("Logging the status at famAnalysisCont");
        console.log("Logging status "+status);
        //console.log("Logging message "+msg);
        // if there are issues with the above validation then mark the status in the collection.

        if ( status ) {
            await famAnalColl.updateOne({'_id':famLocalID,'Status':{$ne:'disabled'}},{$set : {'Status':'ongoing','ErrMsg':''}});

            
            if ( (famReq['assembly_type'] == 'GRCh37') || (famReq['assembly_type'] == 'hg19') ) {
                queryCollection = importCollection1;
            } else if ( (famReq['assembly_type'] == 'GRCh38')  || (famReq['assembly_type'] == 'hg38') ) {
                queryCollection = importCollection2;
            }

            famCont.debug("queryCollection is "+queryCollection);
            var queryCollObj = db.collection(queryCollection); 

            famCont.debug("Calling family samples analysis");
            
            // To Do : 10/01/2024 
            // Analysis based on the defined code and add the codes to the query collection
            var result = await scanFamilyReq(famReq,famAnalColl,queryCollObj,famCont);
            var stop_time = new Date();
            console.log("Family  Controller stop time is "+stop_time);
            famCont.debug("Result Done. Now Await timeout");

            await famAnalColl.updateOne({'_id':famLocalID,'Status':{$ne:'disabled'}},{$set : {'Status':'Completed','ErrMsg':''}});

            // commenting debug timeout
            //await new Promise(resolve => setTimeout(resolve, 200000));
            //console.log("Timeout completed");
            famCont.debug("Completed family controller sample logic");
        } 
        process.exit(0);
    } catch(err) {
        console.log(err);
        //throw err;
        process.exit(1);
    }
}) ();

async function checkValid(famAnalColl,famLocalID,query,msg) {
    var status = 1;
    try {
        var res = await famAnalColl.findOne(query);
        console.log(query);
        if ( ! res ) {
            await famAnalColl.updateOne({'_id':famLocalID,'Status':{$ne:'disabled'}},{$set : {'Status':'Error','ErrMsg':msg}});
            status = 0;
        }
    } catch (err) {
        throw err;
    }
    return status;
}
 
async function scanFamilyReq(famReq,famAnalColl,queryCollObj,famCont) {
    try {
        
        var famLocalID = famReq['family_local_id'];
        var affected = famReq['affected_mem'];
        var unaffected = famReq['unaffected_mem'] || [];

        var genotype = famReq['genotype'] || '';
        var minAffected = famReq['min_affected'] || affected.length;
        console.log("Minimum affected based on array cnt "+minAffected);
        var assemblyType = famReq['assembly_type'];

        //var affectedObj = {};
        //var unaffectedObj = {};
        var fileIDArr = [];

        // affected member processing

        var affectedObj = await mapFileID(affected, famLocalID,famAnalColl);
        var affList = affectedObj['fileIDList'];

        // unaffected member processing
        var unaffectedObj = await mapFileID(unaffected, famLocalID,famAnalColl);
        var unaffList = unaffectedObj['fileIDList'];

        // concatenate affected + unaffected file list
        fileIDArr = affList.concat(unaffList);

        //console.log("Logging details in famAnalysisCont Script");
        //console.log(fileIDArr);
        //console.log(affectedObj);
        //await queryCollObj.updateMany({'fileID':sampId,'alt_cnt':{$eq:0}},{$unset : {'trio_code':1}});
          
        // Reset the family code
        var queryUnset = {'fileID':{$in:fileIDArr}};
        var famCodeUnset = {$unset : {[`${famLocalID}`]:1}};
        //console.log("Logging the unset queries ----------------------");
        //console.log(queryUnset);
        //console.log(famCodeUnset);
        await queryCollObj.updateMany(queryUnset,famCodeUnset);

        // Family Analysis Logic

        var defaultSort = {'var_key':1};
        // hom-ref variants are excluded from trio analysis
        var filter = {'fileID':{$in:fileIDArr},'alt_cnt':{$ne:0}};
        if ( genotype != '') {
            filter['alt_cnt'] = genotype;
        }

        //console.log("Logging filter after changing the user provide genotype filter")
        //console.dir(filter,{"depth":null});

        //console.log(filter);
        var familyCursor = await queryCollObj.find(filter);
        familyCursor.sort(defaultSort);
        
        familyCursor.project({'var_key':1,'fileID':1}); 
        var varGroup = 0;
        var varCnt = 0;
        var dataSet = {};
        var bulkOps = [];

        // minAffected criteria to pass a variant
        
        console.log("Time taken for actual family processing Start #############"+new Date());

        while ( await familyCursor.hasNext() ) {
            const doc = await familyCursor.next();
            var fileID = doc['fileID'];
            //console.log(doc);
            //famCont.debug("logging variant key");
            //famCont.debug(docKey);
            // When the variant changed, we can start processing the data stored in the dataSet hash
            // assignCode is calculated based on the sampleOrder and the data stored in the dataSet hash
            if ( varGroup && doc['var_key'] != varGroup ) {
                //console.log("Logging the variant count "+ varCnt);
                //console.log(`varCnt:${varCnt} minAffected:${minAffected}`);
                //console.log(`varGroup:${varGroup}`);
                //console.log(doc['var_key'] );
                if ( varCnt >= minAffected ) {
                    //console.log("Variant type changed now ---");
                    //console.dir(dataSet,{"depth":null});
                    //console.log(varGroup);
                    // To Do 
                    //console.log(famLocalID);
                    var updateFilterList = [];
                    var updateFilterList = await processVariants(famLocalID,dataSet,fileIDArr,affectedObj,unaffectedObj);
                    //console.log("Logging the update filter list value from process variants");
                    //console.log(updateFilterList);
                    //console.dir(updateFilter,{"depth":null});

                    // if affected and unaffected present, then there can be multiple update statements.
                    bulkOps.push(updateFilterList.pop());
                    
                    if (updateFilterList.length > 0 ) {
                        bulkOps.push(updateFilterList.pop());
                    }
                }
                varCnt =  0;
                dataSet = {};
            }

            if ( affectedObj[fileID] ) {
                varCnt++;
            }
            
            if ( ( bulkOps.length > 0 ) && ((bulkOps.length % 5000 ) === 0 )) {
                famCont.debug("Bulk ops length is "+bulkOps.length);
                //console.log("Execute the bulk update ");
                queryCollObj.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                    famCont.debug("Executed bulkWrite and the results are ");
                    famCont.debug(res.modifiedCount);
                    }).catch((err1) => {
                        famCont.debug("Error executing the bulk operations");
                        famCont.debug(err1);
                });
                bulkOps = [];
            }

            // storing the data related to the variant in the dataSet hash
            var id = doc['_id'];
            var sid = doc['fileID'];
            //dataSet['id'] = id;
            dataSet['var_key'] = doc['var_key'];
            if ( affectedObj[sid] ) {
                dataSet[sid] = 'affected';
            } else if ( unaffectedObj[sid] ) {
                dataSet[sid] = 'unaffected';
            }
            
            varGroup = doc['var_key'];
            //famCont.debug("Logging vargroup data");
            //famCont.debug(dataSet);
            //console.log(doc);
        }

        
        // This is required to handle the last variant set of the family stream of samples.
        
        if ( varCnt >= minAffected ) {
            // if affected and unaffected present, then there can be multiple update statements.
            var updateFilterList = [];
            var updateFilterList = await processVariants(famLocalID,dataSet,fileIDArr,affectedObj,unaffectedObj);

            bulkOps.push(updateFilterList.pop());
                    
            if (updateFilterList.length > 0 ) {
                bulkOps.push(updateFilterList.pop());
            }
        }

        // Bulk upload the variant update queries
        if ( bulkOps.length > 0 ) {
            famCont.debug("Bulk ops length is ---"+bulkOps.length);
            //console.log("Execute the bulk update ");
            queryCollObj.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                famCont.debug("Executed bulkWrite and the results are ----");
                famCont.debug(res.modifiedCount);
                }).catch((err1) => {
                    famCont.debug("Error executing the bulk operations");
                    famCont.debug(err1);
            });
            bulkOps = [];
        }

        console.log("Time taken for actual trio processing Stop #############"+new Date());
        //famCont.debug(trioCodeHash);
        

        //console.dir(trioCodeHash,{"depth":null});

        //await familyColl.updateOne({'_id':familyId},{$set : {'TrioCode' : trioCodeHash}});
        // replace above with the trio collection

        // status to Completed
        // await trioColl.updateOne({'TrioLocalID':localID,'TrioStatus':{$ne:'disabled'}},{$set : {'TrioCode' : trioCodeHash}});

        // family analysis logic

        return "success";
    } catch(err) {
        console.log(err);
        throw err;
    }
}

// Retrieves the fileID based on the member ID list passed as parameter
async function mapFileID(memList, famLocalID,famAnalColl) {
    try {
        var fileObj = {};
        var fileIDArr = [];
        for ( var idx in memList ) {
            var ind = memList[idx];
            var res = await famAnalColl.findOne({"_id" : famLocalID,"fam_opts.individualID":ind},{'projection':{"fam_opts.fileID.$":1}});

            if ( res.fam_opts ) {
                var fileID = res.fam_opts[0]['fileID'];
                fileIDArr.push(fileID);
                fileObj[fileID] = 1;
            }
            //console.log(res);
        }
        fileObj['fileIDList'] = fileIDArr;
        //console.log("Logging the file ID object in mapFileID function");
        //console.dir(fileObj,{"depth":null});

        return fileObj;
    } catch(err) {
        console.log(err);
        throw err;
    }
}

// Generates update filter based on the affected/unaffected list
async function processVariants(famLocalID,dataSet,samples,affectedObj,unaffectedObj) {
    
    var updateFilters = [];
    //fileIDList
    var affList = affectedObj['fileIDList'] || [];
    var unaffList = unaffectedObj['fileIDList'] || [];

    // ToDo : assignCode to be changed based on affected/unaffected.
    //console.log(famLocalID);
    // prepare update statement
    
    if ( affList.length > 0 ) {
        // assign code = 1 for affected family member
        var updateFilter = await prepareFilter(affList,dataSet['var_key'],famLocalID,1);
        updateFilters.push(updateFilter);
    } 
    if ( unaffList.length > 0 ) {
        // assign code = 2 for unaffected family member
        var updateFilter = await prepareFilter(unaffList,dataSet['var_key'],famLocalID,2);
        updateFilters.push(updateFilter);
    }

    return updateFilters;
}

// assignCode = 1 affected member
// assignCode = 2 unaffected member
async function prepareFilter(samples,var_key,famLocalID,assignCode) {
    try {
        var updateFilter = {};
        var filter = {'fileID' : {$in:samples}, 'var_key':var_key, 'alt_cnt':{$ne:0}};
        var update = {$set : {[`${famLocalID}`]:assignCode}};
        //console.log(update);
        updateFilter['updateMany'] = { filter,update};
        return updateFilter;
    } catch(err) {
        throw err;
    }
}
