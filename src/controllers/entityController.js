const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');
var bcrypt = require('bcrypt');
const Async = require('async');
const configData = require('../config/config.js');
var path = require('path');
const { app:{instance,logLoc} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
const { db : {host,port,dbName,apiUserColl,apiUser,familyCollection,individualCollection,variantAnnoCollection1,variantAnnoCollection2,resultCollection, revokedDataCollection,importStatsCollection,sampleSheetCollection,phenotypeColl,phenotypeHistColl,sampleAssemblyCollection, genePanelColl,fileMetaCollection,hostMetaCollection,indSampCollection,trioCollection,importCollection1,importCollection2} , app:{trioQCnt}} = configData;

const spawn  = require('child_process');
const getConnection = require('../controllers/dbConn.js').getConnection;
//const createApiUser = require('../controllers/userControllers.js').createApiUser;
const createTTLIndexExpire = require('../controllers/dbFuncs.js').createTTLIndexExpire;
const createColIndex = require('../controllers/dbFuncs.js').createColIndex;
const pedigreeConfig = require('../config/familyMemberType.json');
//const { create } = require('domain');
var pid = process.pid;
var uDateId = new Date().valueOf();
const getQueryCollName = require('../modules/genericReq.js').getQueryCollName;
var logFile = `entity-control-logger-${pid}-${uDateId}.log`;
//var logFile = loggerEnv(instance,logFile1);
var entityLog = logger('entity',logFile);

const {default: PQueue} = require('p-queue');

// create a new queue, and pass how many you want to scrape at once

const trioQueue = new PQueue({ concurrency: parseInt(trioQCnt) });

const initialize = async () => {
    //const getSuccess = new Promise( (resolve) => resolve("Success") );
    try {
        var resuser1 = await checkCollectionExists(apiUserColl);
        var resuser2 = await createCollection(apiUserColl);
    } catch(err) {
        console.log(`Collection ${apiUserColl} already exists`);
    }
    try {
        var retVal = await checkApiUser(apiUser);
        console.log("Logging the return value "+retVal);
        console.log(`apiUser is ${apiUser}`);
        if ( retVal == "not_exists" ) {
            await createApiUser(apiUser);
        } else {
            console.log(`${apiUser} already exists`);
        }
    } catch(err) {
        console.log(err);
    }


    try {
        var client = getConnection();
        var res1 = await checkCollectionExists(revokedDataCollection);
        var res2 = await createCollection(revokedDataCollection);
        // create ttl index on expireAt field and expire documents at a specific clock time.
        var ttlVal = 0;
        var res3 = await createTTLIndexExpire(client,revokedDataCollection,ttlVal);
    } catch(err) {
        console.log(`Collection ${revokedDataCollection} or index already exists`);
    }

    try {
        var res1 = await checkCollectionExists(sampleSheetCollection);
        var res2 = await createCollection(sampleSheetCollection);
    } catch(err) {
        console.log(`Collection ${sampleSheetCollection} already exists`);
    }

    try {
        var res1 = await checkCollectionExists(importCollection1);
        var res2 = await createCollection(importCollection1);
    } catch(err) {
        console.log(`Collection ${importCollection1} already exists`);
        var idx1 = {"gene_annotations.gene_id" : 1 };
        try {
            var client = getConnection();
            
            await createColIndex(client.db(dbName),importCollection1,idx1);
        } catch(err) {
            console.log("Index already exists in importCollection1");
        }
    }

    try {
        var res1 = await checkCollectionExists(importCollection2);
        var res2 = await createCollection(importCollection2);
    } catch(err) {
        console.log(`Collection ${importCollection2} already exists`);
        var idx1 = {"gene_annotations.gene_id" : 1};
        
        try {
            var client = getConnection();
            await createColIndex(client.db(dbName),importCollection2,idx1);
        } catch(err) {
            console.log("Index already exists in importCollection2");
        }
        
        
    }


    try {
        var res1 = await checkCollectionExists(importStatsCollection);
        var res2 = await createCollection(importStatsCollection);
    } catch(err) {
        console.log(`Collection ${importStatsCollection} already exists`);
    }

    try {
        console.log(phenotypeColl);
        var res1 = await checkCollectionExists(phenotypeColl);
        var res2 = await createCollection(phenotypeColl);
    } catch(err) {
        console.log(`Collection ${phenotypeColl} already exists`);
    }

    try {
        console.log(phenotypeHistColl);
        var res1 = await checkCollectionExists(phenotypeHistColl);
        var res2 = await createCollection(phenotypeHistColl);
    } catch(err) {
        console.log(`Collection ${phenotypeHistColl} already exists`);
    }

    try {
        var res5 = await checkCollectionExists(variantAnnoCollection1);
        console.log("RES5 "+res5);
        var res6 = await createCollection(variantAnnoCollection1);
        console.log("RES6 "+res6);
        var res7 = await checkCollectionExists(variantAnnoCollection2);
        console.log("RES7 "+res7);
        var res8 = await createCollection(variantAnnoCollection2);
        console.log("RES8 "+res8);
    } catch(err1) {
        console.log(`Collection ${err1} already exists`);
        //throw err1;
    }

    try {
        console.log("-----Trying to create sampleAssemblyCollection "+sampleAssemblyCollection);
        var res12 = await checkCollectionExists(sampleAssemblyCollection);
        var crres12 = await createCollection(sampleAssemblyCollection);
    } catch(errRef) {
        console.log("Could not create due to error "+errRef);
        //throw errRef;
    }

    try {
        var res13 = await checkCollectionExists(genePanelColl);
        var crres13 = await createCollection(genePanelColl);
    } catch(gpErr) {
        console.log(`Collection ${genePanelColl} already exists`);
    }

    try {
        var res14 = await checkCollectionExists(fileMetaCollection);
        var crres14 = await createCollection(fileMetaCollection);
    } catch(fmErr) {
        console.log(`Collection ${fileMetaCollection} already exists`);
    }

    try {
        var res15 = await checkCollectionExists(hostMetaCollection);
        var crres15 = await createCollection(hostMetaCollection);
    } catch(hmErr) {
        console.log(`Collection ${hostMetaCollection} already exists`);
    }

    try {
        var res1 = await checkCollectionExists(individualCollection);
        var res2 = await createCollection(individualCollection);
        var res3 = await checkCollectionExists(familyCollection);
        var res4 = await createCollection(familyCollection);
        return "Success";
        //return await getSuccess;
    } catch(err) {
        throw err;
    }
};

const initializeLogLoc = async() => {
    var logDir = path.join(logLoc,'import','samples','tmp');
    fs.mkdirSync(logDir,{recursive:true});
    return "created";
}

const checkApiUser = async (apiUser) => {
    var client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(apiUserColl);
    try {
        //var res1 = await checkCollectionExists(apiUserColl);
        //var res2 = await createCollection(apiUserColl);
        var doc = await collection.findOne({'user':apiUser});
        if ( doc ) {
            return "exists";
        } else {
            return "not_exists";
        }
        //return "Success";
    } catch(err1) {
        throw err1;
    }
};

const createApiUser = async (apiUser) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const collection = db.collection(apiUserColl);
        // Auto generate a salt and hash
        var hashPassword = bcrypt.hashSync(`${apiUser}@C#01`, 10);
        var data = {'_id':apiUser,'user':apiUser, 'hashPassword' : hashPassword,'createDate': Date.now()};
        
        var result = await collection.insertOne(data);
        test.equal(1,result.insertedCount);
        return "Success";
    } catch(err) {
        throw err;
    }
};

const storeMultiple = async  (jsonData1) => {
        var client = getConnection();
        const db = client.db(dbName);
        const collection = db.collection(individualCollection);
        var bulkOps = [];
        if ( ! jsonData1['Individuals']) {
            throw 'JSON Structure Error';
        }
        var jsonData = jsonData1['Individuals'];
        for ( var hashIdx in jsonData ) {
            var indHash = jsonData[hashIdx];
            if ( ! indHash['IndividualID'] || ! indHash['Meta'] ) {
                throw 'JSON Structure Error';
            }
            var doc = createDoc(indHash);
            bulkOps.push(doc);
        }
        var lgt = bulkOps.length;

        //const getSuccess = new Promise( ( resolve ) => resolve("Success") );

        try {
            var r = await collection.insertMany(bulkOps);
            test.equal(lgt,r.insertedCount);
            return "Success";
            //var retValue = getSuccess;
            //return await getSuccess;
        } catch(e) {
            // Throw the error to catch and handle the error in the calling function
            throw e;
        }
};


const updateFamily = async (jsonData1) => {
    var client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(familyCollection);

    if ( ! jsonData1['Family'] ) {
        throw "JSON Structure Error";
    }

    var jsonData = jsonData1['Family'];
    var bulkOps = [];
    for ( var hashIdx in jsonData ) {
        var indHash = jsonData[hashIdx]; // array index holding hash data
        if ( ! indHash['FamilyID'] || ! indHash['Meta'] ) {
            throw "JSON Structure Error";
        }

        var indId = indHash['FamilyID'];
        var meta = indHash['Meta'];
        var metaKeys = Object.keys(meta);
        var updateFilter = {};
        var filter = {};
        var setFilter = {};
        for ( var kIdx in metaKeys ) {
            var keyName = metaKeys[kIdx];
            var val = meta[keyName];
            setFilter[keyName] = val;
        }
        filter['filter'] = { '_id' : indId };
        filter['update'] = { $set : setFilter };
        updateFilter['updateOne'] = filter;
        bulkOps.push(updateFilter);
    }
    console.dir(bulkOps,{"depth":null});
    try {
        var res = await collection.bulkWrite(bulkOps);
        return "Success";
    } catch(e) {
        throw e;
    };
};

const updateData = async (jsonData1) => {
    //var client = await createConnection();
    var client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(individualCollection);
    var jsonData = jsonData1['Individuals'];

    var bulkOps = [];
    for ( var hashIdx in jsonData ) {
        var indHash = jsonData[hashIdx]; // array index holding hash data
        var indId = indHash['IndividualID'];
        var meta = indHash['Meta'];
        var metaKeys = Object.keys(meta);
        var updateFilter = {};
        var filter = {};
        var setFilter = {};
        for ( var kIdx in metaKeys ) {
            var keyName = metaKeys[kIdx];
            var val = meta[keyName];
            setFilter[keyName] = val;
        }
        filter['filter'] = { '_id' : indId };
        filter['update'] = { $set : setFilter };
        updateFilter['updateOne'] = filter;
        bulkOps.push(updateFilter);
    }

    try {
        var res = await collection.bulkWrite(bulkOps);
        return "Success";
    } catch(e) {
        throw e;
    };
};

const readData = async (indId) =>  {
    var client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(individualCollection);
    var filter = {'_id':indId};
    //console.log("Filter that was set for Mongo is "+filter);
    //console.dir(filter,{"depth":null});
    try {
        var doc = await collection.findOne(filter);
        //console.log(doc);
        const getDoc = new Promise( ( resolve ) => resolve(doc) );
        //console.log("Check for the Promise return value. Success/Failure");
        //console.log(getDoc);
        return await getDoc;
    } catch (e) {
        throw "Error";
    }
};

const getAttrData = async (filter) => {
    var client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(individualCollection);
    var obj = [];
    var proj = {'_id':1}; // Projection to retrieve only the Individual IDs
    try {
        var dataStream = collection.find(filter);
        dataStream.project(proj);
        while ( await dataStream.hasNext() ) {
            const doc = await dataStream.next();
            obj.push(doc);
        }
        const getObj = new Promise( ( resolve ) => resolve(obj) );
        return await getObj;
    } catch (e) {
        throw e;
    }
};

// Return document corresponding to the family ID
const getFamily = async(id) => {
    var client = getConnection();
    const db = client.db(dbName);
    const indColl = db.collection(individualCollection);
    const famColl = db.collection(familyCollection);
    try {
        id = parseInt(id);
        var lookUpFilter = { $lookup : {'from' : familyCollection, 'localField' : 'familyId', 'foreignField' : '_id', 'as': "family_data" } };
        var project1 = { $project : {'family_data.pedigree' : 0 }};
        var unwind1 = { $unwind: { path :"$family_data" } };
        var aggData = await indColl.aggregate([ { $match: {'familyId' : id ,'proband' : 1}},lookUpFilter, unwind1, project1]);

        //var aggData = await familyColl.aggregate([ { $match: {'_id' : famId }},lookUpFilter, unwind1,unwind2, resMatch, project1,projMatch ]);

        var obj = [];
        var familyDoc = {};
        //relativeData['proband'] = proband;

        while ( await aggData.hasNext() ) {
            const doc = await aggData.next();
            console.log(doc);
            //var { relatives : { ID,memberType, FamilySide }, relative_data: {Affected } } = doc;
            var newDoc = { '_id' : doc.family_data._id,
                           'Desc' : doc.family_data.Desc,
                           'CenterID' : doc.family_data.CenterID,
                           'UserID' : doc.family_data.UserID,
                           'PIID' : doc.family_data.PIID,
                           'DateAdd' : doc.family_data.DateAdd,
                           'ProbandID' : doc._id,
                           'IndividualFName' : doc.IndividualFName,
                           'IndividualLName' : doc.IndividualLName,
                           'LocalID' : doc.LocalID
                        };
                        console.log("logging new doc here");
                        console.log(newDoc);
            obj.push(newDoc);
        }
        if ( ! obj.length ) {
            var filter = { '_id' : id };    
            var projection = { 'pedigree' : 0 , 'TrioCnt' : 0};    
            var doc = await famColl.findOne(filter,{'projection':projection});
            if ( doc == null ) {
                throw "Could not find any family with the provided ID";
            }
            doc['ProbandID'] = null;
            doc['IndividualFName'] = null;
            doc['IndividualLName'] = null;
            doc['LocalID'] = null;
            obj.push(doc);
        }

        return obj;
    } catch (e) {
        throw e;
    }
}

// Get Individual data corresponding to PIID
const getPIData = async(piid,entityType) => {
    var client = getConnection();
    const db = client.db(dbName);
    var colType = individualCollection;
    if ( entityType == "family" ) {
        colType = familyCollection;
    }
    const collection = db.collection(colType);
    try {
        var obj = [];
        var filter = {};
        piid = parseInt(piid);
        console.log("Logging the piid provided as argument "+piid);
        if ( piid != -1 ) {
            // Get all the families belonging to the center
            filter = {"PIID" : piid};
        }
        var dStream = await collection.find(filter);
        while ( await dStream.hasNext()) {
            const doc = dStream.next();
            //console.log("Document");
            //console.log(doc);
            obj.push(await doc);
        }
        //console.log("Log the response here ");
        //console.dir(obj,{"depth":null});
        return obj;
    } catch(err) {
        throw e;
    }
}

// creating a specific function to return family data based on PI
const getFamilyPIData = async(piid) => {
    var client = getConnection();
    const db = client.db(dbName);
    const indCollObj = db.collection(individualCollection);
    const famCollObj = db.collection(familyCollection);
    // individualCollection familyCollection
    try {
        var obj = [];
        var filter = {};
        var indFilter = {'proband' : 1 };
        piid = parseInt(piid);
        //console.log("Logging the piid provided as argument "+piid);
        if ( piid != -1 ) {
            // Get all the families belonging to the center
            filter = {"PIID" : piid};
            indFilter['PIID'] = piid;
        }
        var indFilter = Object.assign(indFilter);
        console.dir(indFilter,{"depth":null});
        var indDStream = await indCollObj.find(indFilter);
        var indObj = {};
        while ( await indDStream.hasNext()) {
            const doc = await indDStream.next();
            //console.log("Individual doc");
            //console.log(doc);
            if ( doc['familyId'] ) {
                var famId = doc['familyId'];
                indObj[famId] = doc;
            }
        }

        var famArr = [];
        //console.log("family filter below");
        //console.log(filter);
        var famDStream = await famCollObj.find(filter,{ 'projection': { 'pedigree' : 0 , 'TrioCnt' : 0} });
        while ( await famDStream.hasNext() ) {
            const doc = await famDStream.next();
            //console.log("Logging family document");
            //console.log(doc);
            var famId = doc['_id'];
            doc['ProbandID'] = null;
            doc['IndividualFName'] = null;
            doc['IndividualLName'] = null;
            doc['LocalID'] = null;

            if ( indObj[famId] ) {
                var indDoc = indObj[famId];
                doc['ProbandID'] = indDoc['_id'];
                doc['IndividualFName'] = indDoc['IndividualFName'];
                doc['IndividualLName'] = indDoc['IndividualLName'];
                doc['LocalID'] = indDoc['LocalID'];
            }
            famArr.push(doc);
        }
        //console.log("Log the response here ");
        //console.dir(obj,{"depth":null});
        return famArr;
    } catch(err) {
        throw err;
    }
}


const createFamily = async (jsonData) => {
    var client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(familyCollection);

    if ( ! jsonData['_id'] || ! jsonData['Desc'] || ! jsonData['PIID'] ) {
        throw "JSON Structure Error";
    }

    /*jsonData['TrioCnt'] = 0;
    jsonData['TrioStatus'] = "inactive";
    jsonData['TrioID'] = null;*/
    //const getSuccess = new Promise( (resolve) => resolve("Success") );
    try {
        var result = await collection.insertOne(jsonData);
        test.equal(1,result.insertedCount);
        return "Success";
        //return await getSuccess;
    } catch(e) {
        throw e;
    }
}

const addPedigree = async (reqBody,reqType) => {
    const client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(familyCollection);
    try {
        //var pedigreeReq = {};
        reqType = reqType || 'relativePedigree';
        entityLog.debug("addPedigree function");
        entityLog.debug(`reqType is ${reqType}`);
        if ( reqType == "probandPedigree" ) {
            var jsonData = {'pedigree':[]};
            var familyID = reqBody['familyID'];
            //var storereq = pedigreeConfig['pedigreeConfig']['familyMemberType']['Proband']['store'];            
            //var pedigreeReq = Object.assign(storereq,pedigreeConfig['pedigreeConfig']['defaultPhenoSetup']);
            //var pedigreeReq = pedigreeConfig['pedigreeConf']['defaultPhenoSetup'];

            // Note : This is done to make sure a copy of pedigreeConfig json is stored in pedigreeReq. Otherwise, the address of pedigreeConfig json will be stored and upcoming code sections will alter the json directly which is not expected.
            var pedigreeReq = Object.assign({},pedigreeConfig['pedigreeConf']['defaultPhenoSetup']);

            // Retrieve the below mentioned values from the request body
            // For Defined Individuals : We are not storing RelativeName,RelativeBirthdate,RelativeGender,FamilyID(to be fetched from Individuals). Reason : Difficult to manage Individual meta data updates in multiple locations
            // We are not storing FamilyMemberTypeID,Family_side -> we will fetch from config json when sending results. We do not expect any updates to be done for these fields.
            entityLog.debug("addPedigree function. logging the request body");
            entityLog.debug(reqBody);
            pedigreeReq['FamilyMemberTypeName'] = reqBody['pedigree'][0]['FamilyMemberTypeName'];
            pedigreeReq['Node_Key'] = reqBody['pedigree'][0]['Node_Key'];
            pedigreeReq['IndividualID'] = reqBody['IndividualID'];
            pedigreeReq['DateAdd'] = reqBody['pedigree'][0]['DateAdd'];
            pedigreeReq['RelativeGender'] = reqBody['pedigree'][0]['RelativeGender'];
            pedigreeReq['UserID'] = reqBody['pedigree'][0]['UserID'];

            // pre-computed trio analysis procedure - disable(Request : https://dev.azure.com/wingsorg/wings_api_dev/_workitems/edit/197)
            //pedigreeReq['sid'] = reqBody['pedigree'][0]['sid'] || null;
            /*jsonData['TrioCnt'] = 0;
            
            if ( reqBody['pedigree'][0]['sid'] != null ) {
                jsonData['TrioCnt'] = 1; 
            }*/
            jsonData['pedigree'].push(pedigreeReq);
            
            entityLog.debug("Logging the jsonData");
            entityLog.debug(jsonData);
            var id = { '_id' : familyID };
            set = { $set : jsonData};
            //console.dir(jsonData,{"depth":null});
            var result = await collection.updateOne(id,set);
            //console.dir(pedigreeConfig,{"depth":null});
            return "Success";
        } else { // not a proband.
            var familyID = reqBody['_id'];
            var id = { '_id' : familyID };
            if ( reqBody['update']['pedigree']) {
                var jsonData = {'pedigree' : []};
                var pedArr = reqBody['update']['pedigree'];
                for ( var idx in pedArr ) {
                    var relHash = pedArr[idx];
                    entityLog.debug("Traversing the request and logging each relative hash inside them");
                    entityLog.debug(relHash);

                    // for now we are not storing Family_side in mongodb. we will show these fields when sending response to ESAT
                    // if required for any other use case related to Inheritance, we will support adding these fields to mongodb
                    if ( 'Family_side' in relHash ) {
                        entityLog.debug("Key detected Family_side-delete");
                        delete relHash['Family_side'];
                    }

                    if ( 'FamilyID' in relHash ) {
                        entityLog.debug("Key detected FamilyID-delete");
                        delete relHash['FamilyID'];
                        //console.dir(relHash,{"depth":null});
                    }
                    if ( 'FamilyMemberTypeID' in relHash ) {
                        entityLog.debug("key detected FamilyMemberTypeID-delete")
                        delete relHash['FamilyMemberTypeID'];
                    }
                    if ( 'RelativeName' in relHash ) {
                        entityLog.debug("key detected RelativeName-delete");
                        delete relHash['RelativeName'];
                    }
                    if ( 'RelativeBirthdate' in relHash ) {
                        entityLog.debug("key detected RelativeBirthdate-delete");
                        delete relHash['RelativeBirthdate'];
                        //console.log("Logging after deleting birth date");
                        //console.dir(relHash,{"depth":null});
                    }
                    /*if ( relHash['RelativeGender']) {
                        delete relHash['RelativeGender'];
                    }*/
                    if ( 'RelativeStatus' in relHash ) {
                        entityLog.debug("key detected RelativeStatus-delete");
                        delete relHash['RelativeStatus'];
                    }
                    //console.dir(relHash);
                    // This is done to get a copy of the json and not the address.
                    var pedConf = JSON.parse(JSON.stringify(pedigreeConfig['pedigreeConf']['defaultPhenoSetup']));
                    entityLog.debug("Checking for defaultPhenoSetup");
                    entityLog.debug(pedConf);
                    //console.dir(pedConf);
                    //var pedHash = Object.assign(relHash,pedigreeConfig['pedigreeConfig']['defaultPhenoSetup']);
                    var pedHash = Object.assign(relHash,pedConf);
                    entityLog.debug("Logging pedHash after merging Objects");
                    entityLog.debug(pedHash,{"depth":null});
                    //console.dir(pedHash);
                    // Considering 'sid' to be present in jsonData['pedigree'] hash
                    jsonData['pedigree'].push(pedHash);
                }
                entityLog.debug("Logging JSON Data that will be added to database for the pedigree structure");
                entityLog.debug(jsonData,{"depth":null});
                set = { $push : { 'pedigree' : { $each : jsonData['pedigree'] } } };
                var result = await collection.updateOne(id,set);
                return "Success";
            }
        }
    } catch(err) {
        //entityLog.debug("Logging errr and checking "+err);
        console.log(err);
        throw err;
    }
}

/*const addPedigree = async (jsonData) => {
    var client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(familyCollection);
    try {
        var id = { '_id' : jsonData['_id'] };
        var set = {};
        if ( jsonData['add'] ) {
            // first time when the pedigree has to be added
            set = { $set : jsonData['add'] };
        } else if ( jsonData['update'] ) {
            // Add Positional Array update for update pedigree requests.
            //set = { $set : jsonData['update'] };
            // push the additional nodekeys that has to be added to the pedigree
            set = { $push : { 'pedigree' : { $each : jsonData['update']['pedigree'] } } };
        }
        var result = await collection.updateOne(id,set);
        return "Success";
    } catch(e) {
        throw e;
    }
}*/

/*
const showPedigree = async (familyId) => {
    var client = getConnection();
    const db = client.db(dbName);
    const collection = db.collection(familyCollection);
    try {
        var filter = { '_id' : familyId };    
        var projection = { 'pedigree' : 1 };    
        var doc = await collection.findOne(filter,{'projection':projection});
        const getDoc = new Promise( ( resolve ) => resolve(doc) );
        return await getDoc;
    } catch(e) {
        throw e;
    }
} */

const checkIndFilter = async(filter) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const indColl = db.collection(individualCollection);
        //console.log("Checking for filter "+filter);
        var result = await indColl.findOne(filter);
        //console.log("Result is "+result);
        return result;
    } catch(err) {
        throw err;
    }
}
const getUnassignedInd = async(filter) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const indColl = db.collection(individualCollection);
        // set filter to check for Individuals that do not have a familyID assigned
        filter['familyId'] = null;
        var obj = [];
        //console.log("Logging filter to get unassigned Individuals");
        //console.log(filter);
        var ds = await indColl.find(filter,{'projection':{'Relevant_Clinical_Info':0,'CenterID':0} });
        
        while ( await ds.hasNext()) {
            const doc = ds.next();
            //console.log(doc);
            obj.push(await doc);
        }
        return obj;
    } catch(err) {
        throw err;
    }

}

const getDefinedRelatives = async(reqBody) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const indColl = db.collection(individualCollection);
        var proband = reqBody['proband'];
        var queryFilter = { '_id' : reqBody['proband'], 'Affected' : 1, 'proband' : 1 };
       
        var familyData = await indColl.findOne(queryFilter, {'projection': {'familyId' : 1 } });
        //console.log(`familyData is :${familyData}`);

        // Validation : Check if proband is affected
        if ( ! familyData ) {
            //console.log(`Individual ${proband} is not Proband or not Affected. Inheritance filter cannot be applied`);
            throw `Individual ${proband} is not Proband or not Affected. Inheritance filter cannot be applied`;
        }

        // Validation : Check if proband is assigned to a family
        if ( familyData['familyId'] ) {
            var famId = familyData['familyId'];
            // Apply Aggregation and get the Individuals added to the proband-family structure
            var familyColl = db.collection(familyCollection);
            var familyData = await familyColl.findOne({'_id':famId, 'relatives.Individual': {$exists : true}}, {'projection' : {'relatives.Individual': 1 } });
            if ( ! familyData ) {
                throw `Individual ${proband} does not have any relatives defined as Individuals.Inheritance filter cannot be applied`;
            }

            var obj = {};
            obj['proband'] = proband;
            obj['familyId'] = famId;
            var relatives = [];
            for ( var rIdx in familyData['relatives'] ) {
                //console.log(`Logging data for the index ${rIdx}`);
                var rel = familyData['relatives'][rIdx];
                //console.log(rel);
                if ( Object.keys(rel).length > 0 ) { 
                    relatives.push(rel);
                }
            }
            obj['relatives'] = relatives;
            //console.dir(familyData,{"depth":null});
            return obj;
        } else {
            throw `Individual ${proband} is not assigned to any family. Inheritance filter cannot be applied`;
        }
        
    } catch(err) {
        throw err;
    }

}


const getInheritanceData = async(reqBody) => {
    try {
        //console.log("Function createFamily");
        var client = getConnection();
        const db = client.db(dbName);
        const indColl = db.collection(individualCollection); 
        var proband = reqBody['proband'];
        var famId = reqBody['family_id'];
        var queryFilter = { '_id' : reqBody['proband'], 'Affected' : 1, 'proband' : 1 };
       
        //var familyData = await indColl.findOne(queryFilter, {'projection': {'familyId' : 1 } });
        //console.log(`familyData is :${familyData}`);
        
        // Apply Aggregation and get the Individuals added to the proband-family structure
        var familyColl = db.collection(familyCollection);
        // lookup and retrieve only the relatives that are defined in the system.
        var lookUpFilter = { $lookup : {'from' : individualCollection, 'localField' : 'relatives.Individual', 'foreignField' : '_id', 'as': "relative_data" } };
        // unwind has to be applied to traverse the array structure and apply the match criteria 
        var unwind1 = { $unwind: { path :"$relative_data" } };
        var unwind2 = { $unwind : "$relatives" };
        var resMatch = { $match : { "relative_data._id" : { $ne : proband } } };
        // projection on array of relatives : relatives defined as Individuals and also include Affected Status from Individual Collection
        var project1 = { $project : { "_id": 0, 'relatives.memberType' : 1, 'relatives.Individual' : 1, 'relatives.FamilySide': 1, 'relative_data.Affected' : 1, "isMatch" : {$cond: [ { "$eq" : ["$relatives.Individual", "$relative_data._id"]} , 1,0 ] }}};
        var projMatch = { $match: { "isMatch" : 1 } };

        var aggData = await familyColl.aggregate([ { $match: {'_id' : famId }},lookUpFilter, unwind1,unwind2, resMatch, project1,projMatch ]);

        var obj = [];
        var relativeData = {};
        //relativeData['proband'] = proband;

        while ( await aggData.hasNext() ) {
            const doc = await aggData.next();
            console.log(doc);
            var { relatives : { ID,memberType, FamilySide }, relative_data: {Affected } } = doc;
            //relativeData[memberType] = 
            obj.push(doc);
        }
        return obj;
    } catch(err) {
        throw err;
    }

}

const updateFamilySid = async(reqBody) => {
    var client = getConnection();
    const db = client.db(dbName);
    try {
        entityLog.debug("updateFamilySid function");
        var indId = parseInt(reqBody['IndividualID']);
        var sLocalID = reqBody['SampleLocalID'];
        var seqType = reqBody['SequenceType'];
        var action = reqBody['action'];
        var panelType = reqBody['PanelType'];

        if ( seqType.toUpperCase() == "PANEL") {
            panelType = panelType.toUpperCase();
        }
        entityLog.debug("Individual ID is "+indId);
        const indColl = db.collection(individualCollection);
        const famColl = db.collection(familyCollection);
        const indSColl = db.collection(indSampCollection);
        const trioColl = db.collection(trioCollection);
        var indFamily = await indColl.findOne({'_id':indId},{'projection':{'familyId':1}});
        entityLog.debug(indFamily);
        //console.dir(indFamily,{"depth":null});
        if ( indFamily == null ) {
            throw `Individual ${indId} not present`;
        }
        
        var triggerCompute = 0;
        entityLog.debug("familyId is defined");
        var familyId = indFamily['familyId'];

        // Retrieve the trio count and pedigree details
        var familyData = await famColl.findOne({'_id':familyId,'pedigree.IndividualID':indId},{'projection':{'pedigree.$':1}});
        entityLog.debug(`Logging the existing family data below for Individual ${indId}`);
        entityLog.debug(familyData);

        // few updates to be performed even if Individual is not part of a family

        var filter = {'individualID':indId,'SeqTypeName':seqType,'SampleLocalID':sLocalID};
        if ( action == "assign" ) {
            // Check if there is a change
            filter['state'] = {$ne:'unassigned'};
            if ( seqType.toUpperCase() == "PANEL") {
                filter['panelType'] = panelType;
            }
            var cnt = await indSColl.find(filter).count();
            //console.log("Logging filter below :");
            //console.log(filter);
            //console.log("Count is "+cnt);
            if ( cnt == 0 ) {
                // check if there are any entries for this sampleLocalID and experiment. If so, then assign them to this Individual.
                if ( seqType.toUpperCase() == "PANEL") {
                    indSColl.updateMany({'SeqTypeName':seqType,'SampleLocalID':sLocalID,'panelType':panelType},{$set:{'individualID':indId,'state':'assigned','panelType':panelType}});
                } else {
                    indSColl.updateMany({'SeqTypeName':seqType,'SampleLocalID':sLocalID},{$set:{'individualID':indId,'state':'assigned'}});
                }
                
                triggerCompute = 1;
            }
        } else if ( action == "unassign" ) {
            // Check if there is change
            filter['state'] = {$eq:'unassigned'};
            if ( seqType.toUpperCase() == "PANEL") {
                filter['panelType'] = panelType;
            }
            var cnt = await indSColl.find(filter).count();
            if ( cnt == 0 ) {
                // updateMany to handle multiple assemblies hg19,hg38
                // applicable only if Individual is part of family and was part of trio.
                // reset trio 
                if ( seqType.toUpperCase() == "PANEL") {
                    // reset trio
                    indSColl.updateMany({'individualID':indId,'SeqTypeName':seqType,'SampleLocalID':sLocalID,'panelType':panelType,'trio':1},{$set:{'trio':0}});
                    // unassigning Individual and SampleLocalID.
                    // All files related to this experiment will be unassigned from this Individual. Ex: Variant Discovery
                    indSColl.updateMany({'individualID':indId,'SeqTypeName':seqType,'SampleLocalID':sLocalID,'panelType':panelType},{$set:{'state':'unassigned'}});
                } else {
                    // reset trio
                    indSColl.updateMany({'individualID':indId,'SeqTypeName':seqType,'SampleLocalID':sLocalID,'trio':1},{$set:{'trio':0}});
                    // unassigning Individual and SampleLocalID.
                    // All files related to this experiment will be unassigned from this Individual. Ex: Variant Discovery
                    indSColl.updateMany({'individualID':indId,'SeqTypeName':seqType,'SampleLocalID':sLocalID},{$set:{'state':'unassigned'}});
                }
                
                
                triggerCompute = 1;
            }
            
        }

        // If Individual is part of family
        if ( indFamily['familyId']) {
            var familyId = indFamily['familyId'];
            //console.log(`Family ID is ${familyId}`);
            if ( familyData['pedigree']) {
                var nodeKey = familyData['pedigree'][0]['Node_Key'];

                //console.log(`Node Key is ${nodeKey}`);
                entityLog.debug("nodekey is "+nodeKey);

                // Launch the compute process if the sid was updated for a defined individual who is a proband or father or mother
                entityLog.debug("Logging family pedigree");
                entityLog.debug(familyData['pedigree']);

                // Is the individual present as proband or father or mother in the family ?
                var testKey = [0,1,2].indexOf(nodeKey);
                entityLog.debug('Test nodekey result is '+testKey);
                if ( (nodeKey == 0) || (nodeKey == 1) || (nodeKey == 2) ) {
                    if ( action == "assign" ) {
                        if ( triggerCompute ) {
                            if ( seqType.toUpperCase() == "PANEL") {
                                indSColl.updateMany({'SeqTypeName':seqType,'SampleLocalID':sLocalID,'individualID':indId,'panelType':panelType},{$set:{'trio':1,'state':'assigned'}});
                            } else {
                                indSColl.updateMany({'SeqTypeName':seqType,'SampleLocalID':sLocalID,'individualID':indId},{$set:{'trio':1,'state':'assigned'}});
                            }                            

                            // fetch current trio counter
                            var currentTrioCounter = await fetchTrioCounter(familyId);
                            //console.log(`Retrieving the trio counter after this assignment ${currentTrioCounter}`);
                            if ( currentTrioCounter == 3 ) {
                                var famIndArr = await getFamTrio(familyId);

                                //console.log(famIndArr);
                                var concatStr = familyId.toString();
                                // search only for specific sequence type
                                var trioGroupCursor = await indSColl.aggregate([{$match:{individualID:{$in:famIndArr},trio:1,'SeqTypeName':seqType,'panelType':panelType,'state':{$ne:'unassigned'}}},{$group:{"_id":{"SeqTypeName":"$SeqTypeName","AssemblyType":"$AssemblyType","panelType":"$panelType",trioLocalID : {$concat:["$SeqTypeName","-","$AssemblyType","-",concatStr,"$panelType"]} },total:{"$sum":1},trio:{$push:{fileID:"$fileID",individualID:"$individualID"}}}},{$project:{"_id":1,"trio":1,"trioLocalID":1}}]);

                                var insertArr = await scanTrioCursor(trioGroupCursor,familyId);         

                                var trColl = db.collection(trioCollection);
                                // check if there are any entries for family id and then insert.
                                await trColl.insertMany(insertArr);

                                //console.log("Client is this before launching !!");
                                //console.log(client);
                                //console.dir(trioQueue,{"depth":null});
                                // To be verified and enabled later

                                // Get the list of trio local IDs
                                
                                var basePath = path.parse(__dirname).dir;
                                var trioLaunchScript = path.join(basePath,'controllers','trioLaunchQueue.js');  
                                entityLog.debug("TRIO ************ Launch script is "+trioLaunchScript);

                                var localCursor = await trColl.find({'familyID':familyId,'SeqTypeName':seqType},{'projection':{'TrioLocalID':1}});
                                // loop : if there are multiple assemblies
                                while ( await localCursor.hasNext()) {
                                    var doc = await localCursor.next();
                                    var localID = doc['TrioLocalID'];
                                    const trioChildProc = spawn.fork(trioLaunchScript,['--family_id',familyId, '--trio_local_id', localID],{'env':process.env});
                                }

                            }
                        }
                    } else if ( action == "unassign" ) {
                        // Unassign the sample id for the Individual
                        //console.log("Action was unassign");
                        //console.log(`Value of triggerCompute is ${triggerCompute}`);
                        if ( triggerCompute ) {
                            // familyId indicates the family to which this Individual is tagged
                            if ( seqType.toUpperCase() == "PANEL") {
                                await trioColl.updateMany({'familyID':familyId,SeqTypeName:seqType,'trio.individualID':indId,'panelType':panelType},{$set:{'TrioStatus':'disabled','TrioCode':null}});
                            } else {
                                await trioColl.updateMany({'familyID':familyId,SeqTypeName:seqType,'trio.individualID':indId},{$set:{'TrioStatus':'disabled','TrioCode':null}});
                            }
                            
                        }
                    }
                } else {
                    entityLog.debug(`Individual ${indId} is not a trio member as proband,father or mother`)
                }
            }
        } else {
            //console.log(`Individual ${indId} is not assigned to any family`);
            entityLog.debug(`Individual ${indId} is not assigned to any family`);
        }
        return "Success";
    } catch(err) {
        throw err;
    }
}

const triggerAssemblySampTrio = async (familyId,seqType,assemblyType) => {
    var client = getConnection();
    const db = client.db(dbName);
    const indSColl = db.collection(indSampCollection);
    const trColl = db.collection(trioCollection);

    try {
        var famIndArr = await getFamTrio(familyId);
        var concatStr = familyId.toString();
        
        var trioGroupCursor = await indSColl.aggregate([{$match:{individualID:{$in:famIndArr},trio:1,'SeqTypeName':seqType,'state':{$ne:'unassigned'},'AssemblyType':assemblyType}},{$group:{"_id":{"SeqTypeName":"$SeqTypeName","AssemblyType":"$AssemblyType",trioLocalID : {$concat:["$SeqTypeName","-","$AssemblyType","-",concatStr]} },total:{"$sum":1},trio:{$push:{fileID:"$fileID",individualID:"$individualID"}}}},{$project:{"_id":1,"trio":1,"trioLocalID":1}}]);

        var insertArr = await scanTrioCursor(trioGroupCursor,familyId); 
        //console.log("Scanned trio array . Details below:");
        //console.log(insertArr);
        await trColl.insertMany(insertArr);

        var basePath = path.parse(__dirname).dir;
        var trioLaunchScript = path.join(basePath,'controllers','trioLaunchQueue.js');  
        entityLog.debug("TRIO ************ Launch script is "+trioLaunchScript);

        var localCursor = await trColl.findOne({'familyID':familyId,'SeqTypeName':seqType,'AssemblyType':assemblyType},{'projection':{'TrioLocalID':1}});
        //console.log("localCursor is "+localCursor);
        if ( localCursor ) {
            var doc = localCursor;
            var localID = doc['TrioLocalID'];
            //console.log("Launching trio request for LocalID "+localID);
            const trioChildProc = spawn.fork(trioLaunchScript,['--family_id',familyId, '--trio_local_id', localID],{'env':process.env});
            return "success";
        }

    } catch(err) {
        return err;
    }
}

const checkProband = async (probandId) => {
    var client = getConnection();
    const db = client.db(dbName);
    const indColl = db.collection(individualCollection);
    entityLog.debug("checkProband function");
    var famId = "";
    try {
        var query = {'_id':probandId, 'proband':1, 'familyId' : {$exists:true} };
        entityLog.debug("Executing query "+query);
        var queryResult = await indColl.findOne(query);
        entityLog.debug("queryResult "+queryResult);
        if ( queryResult ) {
            famId = queryResult['familyId'];
            return famId;
        } else {
            //throw `${probandId} is not proband`;
            return null;
        }
    } catch(err) {
        throw err;
    }
}

const getFamilyPIID = async(PIID) => {
    var client = getConnection();
    const db = client.db(dbName);
    const famColl = db.collection(familyCollection);
    
    try {
        var query = {};
        if ( PIID != -1 ) {
            query['PIID'] = PIID;
        }
        var familyID = [];
        //console.log(query);
        var famCur = await famColl.find(query,{'projection':{'_id':1}});
        while ( await famCur.hasNext()) {
            var doc = await famCur.next();
            //console.log(doc);
            if ( doc['_id']) {
                familyID.push(doc['_id']);
            }
        }
        //console.log("Logging inside function");
        //console.log(familyID);
        return familyID;
    } catch(err) {
        throw err;
    }
}
const assignInd = async (familyId,indId,proband) => {
    var client = getConnection();
    const db = client.db(dbName);
    const indColl = db.collection(individualCollection);
    //const getSuccess = new Promise( (resolve) => resolve("Success") );
    // check if the family ID is present in family collection before assigning the family ID to Individual Collection
    try {
        var id = { '_id' : indId };
        // check if individual exists first
        var indExists = await indColl.findOne(id);
        if ( !indExists ) {
            throw `${indId} does not exists in database.Create Individual and proceed`;
        }
        var set = { $set : {'familyId':familyId, 'proband' : proband} };
        var queryFilter = { '_id' : indId, 'familyId' : {$exists:false} };
        var notExists = await indColl.findOne(queryFilter);
        entityLog.debug("Function-assignInd");
        entityLog.debug("notExists object is logged below ");
        entityLog.debug(notExists);
        //exists = null if id is present 
        if ( notExists ) {
            entityLog.debug("Individual does not have any family. We can proceed and assign to a family");
            var result = await indColl.updateOne(id,set);
            return "Success";
        } else {
            entityLog.debug(`Logging details of Individual ${indId}`);
            entityLog.debug(notExists);
            throw `Individual ${indId} is already assigned to a family`;
        }
    } catch(err) {
        throw err;
    }
}

const showPedigree = async(familyId) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var familyColl = db.collection(familyCollection);
        
        entityLog.debug("showPedigree function");
        // lookup and retrieve only the relatives that are defined in the system.
        var lookUpFilter = { $lookup : { from : individualCollection, localField : 'pedigree.IndividualID', foreignField : '_id', as: "relative_data" } };
        
        var unwind2 = { $unwind: "$pedigree" };
        var aggData = await familyColl.aggregate( [ unwind2 , { $match: { '_id' : familyId } },lookUpFilter ], { cursor: {} } );
    
        var pedigreeHash = { '_id' : familyId, 'pedigree' : [] };
        while ( await aggData.hasNext() ) {
            const doc = await aggData.next();
            entityLog.debug("Logging the document for the data retrieved for the Individual");
            entityLog.debug(doc);
            
            var relatives = doc.pedigree;
            relatives.FamilyID = familyId;
            relatives.RelativeName = relatives.RelativeName || null;
            relatives.RelativeBirthdate = relatives.RelativeBirthdate || null;
            if ( 'RelativeStatus' in relatives ) {
                relatives.RelativeStatus = relatives.RelativeStatus;
            } else {
                relatives.RelativeStatus = null;
            }
            
            //relatives.RelativeStatus = relatives.RelativeStatus || null;
	
	    // Temporary Fix added on 18th June 2020
            /*if ( 'Node_key' in relatives ) {
                    delete relatives['Node_key'];
            }*/

            var memberType = relatives.FamilyMemberTypeName;
            // json data retrieved using copy mechanism to copy the data and not the address of json
            var showConf = JSON.parse(JSON.stringify(pedigreeConfig['pedigreeConf']['familyMemberType'][memberType]['show']));

            if ( doc.relative_data.length > 0 ) {
                var relData = doc.relative_data[0];
                // let us merge the data
                relatives.RelativeName = relData.IndividualFName+' '+relData.IndividualLName;
                relatives.RelativeBirthdate = relData.IndividualBirthDate;
                relatives.RelativeStatus = relData.IndividualStatus;
                entityLog.debug("Individual defined!");
                entityLog.debug(relatives);
            } else {
                entityLog.debug("Individual not defined");
                entityLog.debug(relatives);
            }
            var showRel = Object.assign(relatives,showConf);
            pedigreeHash['pedigree'].push(showRel);
        }
        return pedigreeHash;
    } catch(err) {
        console.log(err);
        throw err;
    }
}


const getRelativeData = async(familyId,key) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var familyColl = db.collection(familyCollection);
        
        // lookup and retrieve only the relatives that are defined in the system.
        var lookUpFilter = { $lookup : { from : individualCollection, localField : 'pedigree.Individual', foreignField : '_id', as: "relative_data" } };
        
        //var unwind1 = { $unwind : "$relatives" };
        var unwind2 = { $unwind: "$pedigree" };
        var unwind3 = { $unwind: { path : "$relative_data" } };
        
        //var project = { $project : { 'relatives' : 1 , 'relative_data' : 1 } };
        //var project = { $project : { 'relative_data' : 1 } }; // remove comment later

        // unwind on an embedded array field, splits the array into multiple documents.
        // unwind relatives, unwind pedigree , perform match and then lookup in Individual collection for corresponding Individual id
        //var aggData = await familyColl.aggregate( [ unwind1, unwind2 , { $match: { '_id' : familyId , 'pedigree.nodekey': key, 'relatives.nodekey' : key } },lookUpFilter, project ], { cursor: {} } );

        //var aggData = await familyColl.aggregate( [ unwind2 , { $match: { '_id' : familyId , 'pedigree.nodekey': key} },lookUpFilter, project ], { cursor: {} } );

        var aggData = await familyColl.aggregate( [ unwind2 , { $match: { '_id' : familyId , 'pedigree.nodekey': key} },lookUpFilter ], { cursor: {} } );

        while ( await aggData.hasNext() ) {
            const doc = await aggData.next();
            //console.log("Logging the document for the data retrieved for the Individual");
            //console.log(doc);
            var relatives = doc.pedigree;
            if ( doc.relative_data.length > 0 ) {
                var relData = doc.relative_data[0];
                // let us merge the data
                relatives.relativeName = relData.IndividualFName+' '+relData.IndividualLName;
                relatives.relativeBirthDate = relData.IndividualBirthDate;
                //console.log("Individual defined!");
                //console.log(relatives);
            } else {
                //console.log("Individual not defined");
                //console.log(relatives);
            }
        }
        return relatives;
        //return "Success";
    } catch(err) {
        console.log(err);
        throw err;
    }
}

const updateRelative = async (type,jsonData) => {
    var client = getConnection();
    const db = client.db(dbName);
    const familyColl = db.collection(familyCollection);

    entityLog.debug("Received request - updateRelative");
    // only check if key exists. to make sure it still validates when Node_Key value is 0
    if ( 'Node_Key' in jsonData['update']  && jsonData['update']['relatives'] ) {
        try {
            // id indicates family ID
            var id = jsonData['_id'];
            var nodekey = jsonData['update']['Node_Key'];
            entityLog.debug(`id is ${id}`);
            entityLog.debug(`nodekey is ${nodekey}`);
            var relData = jsonData['update']['relatives'];

            var storedTrioCounter = await fetchTrioCounter(id);
            // disable trio analysis pre-compute procedure
            /*var trioRes = await familyColl.findOne({'_id':id,'TrioCnt':{$gte:0}},{'projection': {'TrioCnt':1,'_id':0}});
            var storedTrioCounter = 0;
            var currentTrioCounter = 0;
            if ( 'TrioCnt' in trioRes ) {
                currentTrioCounter = trioRes['TrioCnt'];
            } 
            storedTrioCounter = currentTrioCounter; */
            // Fetch father data present in family collection which has trio field set and > 0

            var trioDoc = {};
            if ( nodekey == 0 || nodekey == 1 ) {
                trioDoc = await familyColl.findOne({'_id':id,'pedigree.Node_Key':nodekey},{'projection':{'pedigree.Node_Key.$':1 }});
            }
            entityLog.debug("Logging doc of nodekey "+nodekey);
            entityLog.debug(trioDoc);
            if ( type == "definedInd" ) {
                //var reqSid = jsonData['update']['relatives']['sid'];
                var reqIndId = jsonData['update']['relatives']['IndividualID'];
                //entityLog.debug("Request SID is "+reqSid);
                if ( nodekey == 0 || nodekey == 1) {
                    // check1 : relative to individual or first time /updateRelative request
                    // check2 : indId present in database is not same as indId present in request. This is to make sure the trio is not incremented every time an updateRelative request is received.
                    if ( (trioDoc['pedigree'][0]['IndividualID'] == null) && (reqIndId != null) && (trioDoc['pedigree'][0]['IndividualID'] != reqIndId)) {
                        // increment trio
                        /*entityLog.debug("currentTrioCounter is "+currentTrioCounter);
                        currentTrioCounter++;
                        entityLog.debug("currentTrioCounter is "+currentTrioCounter);*/
                        // set trio code = 1 for all files related to this Individual(father/mother)
                        var res2 = await updateTrio(reqIndId,1,"set");
                    }
                    // rare scenario. This will not happen in a regular case.
                    // This will handle "defined sid" to "null sid" for a defined Individual
                    // This may not be required in the new approach. Check and remove.
                    if ( (trioDoc['pedigree'][0]['IndividualID'] != null) && (reqIndId == null) && (trioDoc['pedigree'][0]['IndividualID'] != reqIndId)) {
                        // decrement trio
                        //currentTrioCounter--;
                        // set trio code = 0 for all files related to this Individual(father/mother)
                        var res2 = await updateTrio(reqIndId,0,"unset");
                    }

                }
                

                entityLog.debug("updateRelative function-Defined Individual");
                entityLog.debug(`Type is ${type}`);

                // remove the keys from relData which we do not want to update in database
                // RelativeName,RelativeBirthdate,RelativeGender,RelativeStatus

                // checking if the key exists in hash. does not check value of hash key
                if ( 'RelativeName' in relData ) {
                    delete relData['RelativeName'];
                }
                if ( 'RelativeBirthdate' in relData ) {
                    delete relData['RelativeBirthdate'];
                }
                /*if ( 'RelativeGender' in relData ) {
                    delete relData['RelativeGender'];
                }*/
                if ( 'RelativeStatus' in relData ) {
                    delete relData['RelativeStatus'];
                }

                // Reason : If we add a relative who is not defined as an Individual, RelativeName, RelativeBirthDate and RelativeStatus will be stored in database
                // If we want to update the same relative as an Individual who is defined in System, then it is important to remove the existing entries in database.
                var searchR = { '_id' : id, 'pedigree.Node_Key': nodekey };
                var setVal = { $unset : { 'pedigree.$.RelativeName' : 1 , 'pedigree.$.RelativeBirthdate' : 1, 'pedigree.$.RelativeStatus' : 1 } };
                // search for the specific Node ID to be removed and set it to Null
                var relUpdRes1 = await familyColl.updateOne(searchR,setVal);

                entityLog.debug("Check the status after the keys are removed");
                entityLog.debug(relData,{"depth":null});
            } else { // relative , not defined individual.
                var reqIndId = jsonData['update']['relatives']['IndividualID'];
                if ( nodekey == 0  || nodekey == 1 ) {
                    // check1 : Individual to relative
                    // check2 : indId present in database is not same as indId present in request. This is to make sure the trio is not incremented every time an updateRelative request is received.
                    if ( (trioDoc['pedigree'][0]['IndividualID'] != null) && (reqIndId == null) && (trioDoc['pedigree'][0]['IndividualID'] != reqIndId)) {
                        // decrement trio
                        //currentTrioCounter--;
                        // set trio code = 0 for all files related to this Individual(father/mother)
                        var res2 = await updateTrio(reqIndId,0,"unset");
                    }
                } 
            }
            var search = { "_id" : id, 'pedigree.Node_Key': nodekey };
            var setFields = {};
            for ( var key in relData ) {
                var val = relData[key];
                var keyVal = 'pedigree.$.'+key;
                setFields[keyVal] = val;
            }
            // incremented trio counter or decremented trio counter has to be saved in database
            /*if (currentTrioCounter) {
                setFields['TrioCnt'] = currentTrioCounter;
            }
            if ( currentTrioCounter < 3 ) {
                setFields['TrioStatus'] = "inactive";
            }*/
            
            var setObj = { $set: setFields };
            entityLog.debug("search criteria added below");
            entityLog.debug(search);
            entityLog.debug("Set criteria added below");
            entityLog.debug(setObj);
            
            var res = await familyColl.updateOne(search,setObj);

            // fetch trio individuals from family.

            // Check the trio counter value
            
            //var res = await indSColl.aggregate({$match:{IndLocalID:{$in:["uisampletestInd1","uisampletestInd2","uisampletestInd3"]}}},{$group:{_id:"$IndLocalID"}},{$count:"trio_counter"});

            var currentTrioCounter = await fetchTrioCounter(id);
            // compare current and stored trio counter
            // 
            
            // Check res for the trio counter value.

            // 
            // Disable trio analysis pre-compute procedure
            // add checks to make sure pre-compute is fired only when there is a change.
            
            if ( (storedTrioCounter != currentTrioCounter) && (currentTrioCounter == 3 ) ) {
            //if ( currentTrioCounter == 3 )  {
                // Trigger the trio pre-compute process
                // family_id
                entityLog.debug("Trigger the trio pre-compute process");
                var basePath = path.parse(__dirname).dir;
                var trioLaunchScript = path.join(basePath,'controllers','trioLaunchQueue.js');  
                entityLog.debug("TRIO ************ Launch script is "+trioLaunchScript);

                // fetch trio data from file collection

                var famIndArr = await getFamTrio(id);

                var indSColl = db.collection(indSampCollection);
                var concatStr = id.toString();

                var trioGroupCursor = await indSColl.aggregate([{$match:{individualID:{$in:famIndArr},trio:1,'state':{$ne:'unassigned'}}},{$group:{"_id":{"SeqTypeName":"$SeqTypeName","AssemblyType":"$AssemblyType","panelType":"$panelType",trioLocalID : {$concat:["$SeqTypeName","-","$AssemblyType","-",concatStr,"$panelType"]} },total:{"$sum":1},trio:{$push:{fileID:"$fileID",individualID:"$individualID"}}}},{$project:{"_id":1,"trio":1,"trioLocalID":1}}]);

                var insertArr = await scanTrioCursor(trioGroupCursor,id);

                var trColl = db.collection(trioCollection);
                // check if there are any entries for family id and then insert.
                await trColl.insertMany(insertArr);

                //console.log("Client is this before launching !!");
                //console.log(client);
                //console.dir(trioQueue,{"depth":null});
                // To be verified and enabled later

                // Get the list of trio local IDs
            

                var localCursor = await trColl.find({'familyID':id},{'projection':{'TrioLocalID':1}});
                while ( await localCursor.hasNext()) {
                    var doc = await localCursor.next();
                    entityLog.debug("Trio process in updateRelative");
                    entityLog.debug(doc);
                    var localID = doc['TrioLocalID'];
                    entityLog.debug(localID);
                    const trioChildProc = spawn.fork(trioLaunchScript,['--family_id',id, '--trio_local_id', localID],{'env':process.env});
                }
                
            } 
            
            return "Success";
        } catch(err) {
            throw err;
        }
    } else {
        throw "JSON Structure Error ";
    }  
}

const getIndMeta = async(id) => {
    try {
        var meta = {};
        var client = getConnection();
        const db = client.db(dbName);
        const indColl = db.collection(individualCollection);
        var filter = { '_id' : id };
        var indMeta = await indColl.findOne(filter);
        meta['IndividualID'] = id;
        meta['IndividualFullname'] = indMeta['IndividualFName']+' '+indMeta['IndividualLName'];
        meta['IndividualGender'] = indMeta['IndividualSex'];
        meta['IndividualBirthDate'] = indMeta['IndividualBirthDate'];
        meta['FamilyID'] = indMeta['familyId'];
        return meta;
    } catch(err) {
        throw err;
    }
}

const getProbandName = async(probandId) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const indColl = db.collection(individualCollection);
        var probandName = "";
        if ( probandId ) {
            probandId = parseInt(probandId);
        }
        var indMeta = await indColl.findOne({'_id':probandId,'proband':1},{'projection':{'IndividualFName':1,'IndividualLName':1}});
        entityLog.debug("Individual Meta details are logged below");
        entityLog.debug(indMeta);
        if ( indMeta ) {
           probandName = indMeta['IndividualFName']+indMeta['IndividualLName'];
        }
        return probandName;
    } catch(err) {
        throw err;
    }
}

const getTrioFamily = async(type,reqId) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const trioColl = db.collection(trioCollection);
        const familyColl = db.collection(familyCollection);
        
        var familyId = "";
        reqId = parseInt(reqId);
        var filter = {};
        var familyIdArr = [];
        if ( type == "proband") {
            familyId = await checkProband(reqId);
            filter['familyID'] = familyId;
            entityLog.debug("Request ID is "+reqId);
            familyIdArr.push(familyId);
        } else if ( type == "family" ) {
            //console.log("Currently present here ");
            filter['familyID'] = reqId;
            familyIdArr.push(reqId);
        } else if  (type == "piid")  {
            //console.log("*****************************");
            familyIdArr = await getFamilyPIID(reqId);
            //filter['familyID'] = {$in:familyIdArr};
        }

        var resultArr = [];
        //console.log("Logging family id array");
        //console.log(familyIdArr);
        for ( var fidx in familyIdArr ) {
            var famID = familyIdArr[fidx];
            var filter = {'familyID' : famID};
            
            //console.log(filter);
            //console.log(famID);
            entityLog.debug("Filter we tried in getTrioFamily is ");
            entityLog.debug(filter);
            var trioCursor = await trioColl.find(filter);
            var trioInfo = [];
            var mainTrioHash = {};
            while ( await trioCursor.hasNext() ) {
                const doc = await trioCursor.next();
                var docId = doc['TrioLocalID'];
                entityLog.debug("Logging the document for the data retrieved for the Individual");
                entityLog.debug(doc);
                var familyData = {};
                var familyInfo = doc.trio;
                mainTrioHash['FamilyID'] = doc['familyID'];

                familyData['TrioLocalID'] = docId;
                familyData['TrioStatus'] = doc['TrioStatus'];

                for ( var idx in familyInfo ) {
                    var famHash = familyInfo[idx];
                    //console.dir(famHash,{"depth":null});
                    if ( famHash['relation'] == 'Proband' ) {
                        entityLog.debug("Family hash node key is 2");
                        mainTrioHash['ProbandID'] = famHash['individualID'];
                        familyData['ProbandFileID'] = famHash['fileID'];  
                        var probandN = await getProbandName(famHash['individualID']);  
                        mainTrioHash['ProbandFullName'] = probandN; 
                    } else if ( famHash['relation'] == 'Father' ) {
                        mainTrioHash['FatherID'] = famHash['individualID'];
                        familyData['FatherFileID'] = famHash['fileID'];
                    } else if ( famHash['relation'] == 'Mother' ) {
                        mainTrioHash['MotherID'] = famHash['individualID'];
                        familyData['MotherFileID'] = famHash['fileID'];
                    }
                }
                trioInfo.push(familyData);
            }
            if ( mainTrioHash['FamilyID']) {
                mainTrioHash['Trios'] = trioInfo;
                resultArr.push(mainTrioHash);
            }
            
        }
        return resultArr;
    } catch(err) {
        throw err;
    }
}

const getTrioMeta = async(trioId) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const trioColl = db.collection(trioCollection);
        var filter = {'TrioLocalID': trioId,'TrioStatus': 'completed'};

        var trioCursor = await trioColl.find(filter);
        var trioInfo = [];
        while ( await trioCursor.hasNext() ) {
            const doc = await trioCursor.next();
            var docId = doc['TrioLocalID'];
            entityLog.debug("Logging the document for the data retrieved for the Individual");
            entityLog.debug(doc);
            
            var familyInfo = doc.trio;
            for ( var idx in familyInfo ) {
                var famHash = familyInfo[idx];
                // proband
                if ( famHash['relation'] == 'Proband' ) {
                    entityLog.debug("Family hash node key is 2");
                    var meta = await getIndMeta(famHash['individualID']);  
                    trioInfo[0] = meta;
                // father
                } else if ( famHash['relation'] == 'Father' ) {
                    var meta = await getIndMeta(famHash['individualID']);
                    //trioInfo.push(familyData);
                    trioInfo[1] = meta;
                // mother
                } else if ( famHash['relation'] == 'Mother' ) {
                    var meta = await getIndMeta(famHash['individualID']);
                    //trioInfo.push(familyData);
                    trioInfo[2] = meta;
                }
            } 
        }
        return trioInfo;

    } catch(err) {
        throw err;
    }
}

// Function to fetch the mongodb data for the specific trio local ID 
// Collection : wingsTrioColl
const fetchTrioEntry = async(trioId) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const trioColl = db.collection(trioCollection);
        var filter = {'TrioLocalID': trioId,'TrioStatus': 'completed'};

        var trioCursor = await trioColl.find(filter,{'projection':{_id:0}});
        var trioInfo = [];
        while ( await trioCursor.hasNext() ) {
            const doc = await trioCursor.next();
            trioInfo.push(doc);
        }
        return trioInfo;

    } catch(err) {
        throw err;
    }
}

// Trio : Fetch annotations for a specific variant in a specific file
const fetchVarAnno = async(var_key,fileID,trio_code,assembly) => {
    try {

        var collName = await getQueryCollName(assembly);
        //console.log("Assembly is "+assembly);
        //console.log("Coll is "+collName);
        var client = getConnection();
        const db = client.db(dbName);
        const queryColl = db.collection(collName);
        var filter = {'fileID':fileID,'var_key':var_key,'trio_code':trio_code}
        //console.log(filter);
        var doc = await queryColl.findOne(filter,{'projection':{'ref_all':1,'alt_all':1,'ref_depth':1,'alt_depth':1,'alt_cnt':1,'phred_genotype':1,'filter':1,'_id':0}});
        return doc;        
    } catch(err) {
        throw err;
    }
}

// Fucntion that will be invoked from the route endpoint /fetchTrioVariantAnno
const trioVarAnno = async(reqBody) => {
    try {
        var trioLocalID = reqBody['trioLocalID'];
        var docs = await fetchTrioEntry(trioLocalID);
        //console.log("fetching docs "+docs);
        // fetch the first document
        var doc = docs[0];
        var assembly = doc['AssemblyType'];
        var trioFam = doc['trio'];
        
        // Response is sent in the order of Proband,Father and Mother
        var annoResp = [];
        for (idx in trioFam ) {
            var relation = trioFam[idx]['relation'];
            var fileID = trioFam[idx]['fileID'];
            var annoDoc = await fetchVarAnno(reqBody['variant'],fileID,reqBody['trio_code'],assembly);
            //console.log("Relation"+relation);
            //console.log("fileID "+fileID);
            //console.log(annoDoc);
            var cloneDoc = {'relation':relation,'result_type':"No variant call"};
            if ( annoDoc ) {
                cloneDoc = { ... annoDoc};
                cloneDoc['relation'] = relation;
                cloneDoc['result_type'] = "Variant call";
            }
            annoResp.push(cloneDoc);
        }
        return annoResp;
        
    } catch(err) {
        throw err;
    }
}

const getTrioFamilyOld = async(type,reqId) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const familyColl = db.collection(familyCollection);

        
        var familyId = "";
        var filter = {'TrioCnt':3};
        if ( type == "proband") {
            reqId = parseInt(reqId);
            familyId = await checkProband(reqId);
            filter['_id'] = familyId;
            entityLog.debug("Request ID is "+reqId);
        } 

        
        if ( type == "family" && reqId['FamilyID'] != -1 ) {
            //console.log("Currently present here ");
            filter['_id'] = reqId['FamilyID'];
        } else if ( reqId['FamilyID'] == -1 ) {
            filter['PIID'] = reqId['PIID'];
        }

        entityLog.debug("Filter we tried in getTrioFamily is ");
        entityLog.debug(filter);
        var famCursor = await familyColl.find(filter);
        var trioInfo = [];
        while ( await famCursor.hasNext() ) {
            const doc = await famCursor.next();
            var docId = doc['_id'];
            entityLog.debug("Logging the document for the data retrieved for the Individual");
            entityLog.debug(doc);
            var familyData = {};
            var familyInfo = doc.pedigree;
            familyData['FamilyID'] = docId;
            familyData['TrioStatus'] = doc['TrioStatus'];
            familyData['TrioID'] = doc['TrioID'];

            for ( var idx in familyInfo ) {
                var famHash = familyInfo[idx];
                if ( famHash['Node_Key'] == 2 ) {
                    entityLog.debug("Family hash node key is 2");
                    familyData['ProbandID'] = famHash['IndividualID'];  
                    var probandN = await getProbandName(famHash['IndividualID']);  
                    familyData['ProbandFullName'] = probandN; 
                } else if ( famHash['Node_Key'] == 0 ) {
                    familyData['FatherID'] = famHash['IndividualID'];
                } else if ( famHash['Node_Key'] == 1 ) {
                    familyData['MotherID'] = famHash['IndividualID'];
                }
            }
            trioInfo.push(familyData);
        }
        return trioInfo;
    } catch(err) {
        throw err;
    }
}

const unassignRelative = async(familyID,filter) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const familyColl = db.collection(familyCollection);
        const indColl = db.collection(individualCollection);
        const trioColl = db.collection(trioCollection);

        entityLog.debug("unassignRelative function");
        if ( 'Node_Key' in filter  && 'IndividualID' in filter ) {
            entityLog.debug("Logging request filter below")
            entityLog.debug(filter);
            var nodekey = filter['Node_Key'];
            var indID = filter['IndividualID'];
            var userID = filter['UserID'];

            // Check IndividualID is not null. Then check if Individual id belongs to father or mother
            // Disable changes related to automated pre-compute process
            // start
            if ( indID != null && (nodekey == 0 || nodekey == 1) ) {
                var familyData = await familyColl.findOne({'_id':familyID,'pedigree.IndividualID':indID},{'projection':{'pedigree.$':1}});
                if ( familyData['pedigree']) {
                    // reset trio value and also decrement trio counter
                    entityLog.debug("Logging request to reset trio to 0");
                    var res3 = await updateTrio(indID,0,"unset");

                    entityLog.debug("Request to reset Trio Status and also Trio Code");
                    await trioColl.updateMany({'familyID':familyID,'trio.individualID':indID},{$set:{'TrioStatus':'disabled','TrioCode':null}});
                }
            }
            // finish
            
            // Step 1 : Unassign Individual from family. unset familyId and proband
            entityLog.debug("Step 1 : Unassign Individual from family. unset familyId and proband");
            var searchQ = {'_id' : indID};
            var setFam = { $unset : {'familyId':1,'proband':1} };
            entityLog.debug(setFam);
            entityLog.debug(searchQ);
            var relUpdRes = await indColl.updateOne(searchQ,setFam);
            entityLog.debug("FamilyID removed from Individual Collection");

            //console.log(relUpdRes);
            // Step 2 : Remove Individual ID and sid reference from pedigree structure

            entityLog.debug("Step 2 : Remove Individual ID reference from pedigree structure");
            var searchR = { '_id' : familyID, 'pedigree.Node_Key': nodekey };
            //var setVal = { $pull : { 'relatives' : { 'relatives.$.ID' : relID } } };
            //var setVal = { $unset : { 'pedigree.$.IndividualID' : 1 } };
            var setVal = { $set : { 'pedigree.$.IndividualID' : null ,'pedigree.$.UserID' : userID } };
            //var setVal = { $set : { 'pedigree.$.IndividualID' : null , 'pedigree.$.sid' : null, 'pedigree.$.UserID' : userID } };
            
            entityLog.debug(searchR);
            entityLog.debug(setVal);
            // search for the specific Node ID to be removed and set it to Null
            var relUpdRes1 = await familyColl.updateOne(searchR,setVal);
            // remove the null relative that was set in previous step.
            // two step process required as array based positional deletions cannot be done in a single step.
            //var removeNull = await familyColl.updateOne({ '_id' : filter['familyID'] },{$pull : {'pedigree' : null}});
            //var relUpdRes1 = await familyColl.remove(searchR);
            entityLog.debug("Relative removed from Relative structure");
            //console.log(relUpdRes1.result);
            return "Success";
        } else {
            throw "JSON Structure Error";
        }
    } catch(err) {
        entityLog.debug("present in catch block");
        //console.log(err);
        throw err;
    }
}

const removeRelative = async(filter) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const familyColl = db.collection(familyCollection);
        const indColl = db.collection(individualCollection);

        const trioColl = db.collection(trioCollection);
        entityLog.debug("removeRelative function");
        if ( filter['_id'] && filter['remove']['Node_Key'] != null ) {
            var tmpnodekey = filter['remove']['Node_Key'];
            var familyId = filter['_id'];
            entityLog.debug(filter);
            if ( filter['remove']['IndividualID'] ) {
                // start
                var indId = filter['remove']['IndividualID'];
                // check if the nodekey belongs to proband or father or mother
                if ( (tmpnodekey == 0 ) || ( tmpnodekey == 1 ) || ( tmpnodekey == 2 ) ) {
                    var familyData = await familyColl.findOne({'_id':familyId,'pedigree.IndividualID':indId},{'projection':{'pedigree.$':1}});
                    if ( familyData['pedigree']) {
                        // reset trio values and update trio counter
                        var res2 = await updateTrio(indId,0,"unset");

                        await trioColl.updateMany({'familyID':familyId,'trio.individualID':indId},{$set:{'TrioStatus':'disabled','TrioCode':null}});
                    }
                } 
                // finish

                // if the relative exists as an individual in wings system
                // unassign the familyID crs. to the individual ID
                //console.log("**** Present here");
                var id = filter['remove']['IndividualID'];
                var searchQ = {'_id' : id};
                var setFam = { $unset : {'familyId':1, 'proband' : 1} };
                var relUpdRes = await indColl.updateOne(searchQ,setFam);
                entityLog.debug("FamilyID removed from Individual Collection");
                entityLog.debug(relUpdRes);
            }
            

            var nodekey = filter['remove']['Node_Key'];

            var searchR = { '_id' : filter['_id'], 'pedigree.Node_Key': nodekey };
            //var setVal = { $pull : { 'relatives' : { 'relatives.$.ID' : relID } } };
            var setVal = { $unset : { 'pedigree.$' : 1 } };
            entityLog.debug("unset pedigree for nodekey "+nodekey);
            // search for the specific Node ID to be removed and set it to Null
            var relUpdRes1 = await familyColl.updateOne(searchR,setVal);
            // remove the null relative that was set in previous step.
            // two step process required as array based positional deletions cannot be done in a single step.
            var removeNull = await familyColl.updateOne({ '_id' : filter['_id'] },{$pull : {'pedigree' : null}});
            //var relUpdRes1 = await familyColl.remove(searchR);
            entityLog.debug("Relative removed from Relative structure");
            entityLog.debug(relUpdRes1.result);
            return "Success";
        } else {
            throw "JSON Structure Error";
        }
    } catch(err) {
        console.log("present in catch block");
        console.log(err);
        throw err;
    }
}

const checkTrioQueue = () => {
    trioQueue.on('active', async() => {
        console.log(`Working on item #${++qcount}.  Size: ${trioQueue.size}  Pending: ${trioQueue.pending}`);
        return "Success";
    });
}

const getTrioCodes = async(type,param2) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const trioColl = db.collection(trioCollection);
        var localID = "";
        var trioRespId = "";
        if ( type == "trio_local_id" ) {
            localID = param2;
            trioRespId = localID;
        } else if ( type == "post_req" ) {
            localID = param2['TrioLocalID'];
            //var updateTrId = await familyColl.updateOne({'_id':famId},{$set:{'TrioID':trioId}});
            trioRespId = localID;
        }

        var filter = {'TrioLocalID': localID,'TrioStatus': 'completed'};
        var trioCursor = await trioColl.findOne(filter,{'projection':{'TrioCode':1}});
        //console.log("Printing the familyCursor");
        //console.log(familyCursor);
        var trioResp = [];
        if ( trioCursor == null ) {
            throw "No trio data available";
        }
        if ( trioCursor != null && 'TrioCode' in trioCursor ) {
            var trioCodes = trioCursor['TrioCode'];
            var trioKeys = Object.keys(trioCodes);
            for ( var idx in trioKeys){
                var trKey = trioKeys[idx];
                var cnt = trioCodes[trKey];
                var tmpRsp = { 'TrioID': trioRespId,'Code': trKey, "Count":cnt};
                trioResp.push(tmpRsp);
            }
        } 
        return trioResp;
    } catch(err) {
        throw err;
    }
}

const createDoc = (jsonInd) => {
    var doc = {};
    doc['_id'] = jsonInd['IndividualID'];
    var meta = jsonInd['Meta'];

    // Retrieve the meta keys defined in the Individual JSON and create the document
    var metaKeys = Object.keys(meta);
    //console.log(metaKeys);
    for ( var kIdx in metaKeys ) {
        var keyName = metaKeys[kIdx];
        doc[keyName] = meta[keyName];
    } 
    return doc;
};

// Connect to MongoDB and check if the collection exists. Returns Promise
const checkCollectionExists = async (colName) => {
    var client = getConnection();
    const db = client.db(dbName);
    const getSuccess = new Promise( ( resolve ) => resolve("Success") );
    try {
        var items = await db.listCollections({name:colName}).toArray();
        test.equal(0,items.length);
        return await getSuccess;
    } catch(err) {
        throw err;
    }
};

// Create the Collection passed as argument. Returns Promise;
const createCollection = async (colName) => {
    var client = getConnection();
    const db = client.db(dbName);
    const getSuccess = new Promise ( (resolve) => resolve("Success") );
    try {
        var result = await db.createCollection(colName,{'w':1});
        return await getSuccess;
    } catch(err) {
        throw err;
    }
};

const getResultCollObj = async () => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var resColl = db.collection(resultCollection);
        return resColl;
    } catch(err) {
        throw err;
    }
};

const scanTrioCursor = async (trioGroupCursor,familyId) => {
    try {
        var insertArr = [];
        while( await trioGroupCursor.hasNext()) {
            var doc = await trioGroupCursor.next();
            var trioArr = doc['trio'];
            if ( trioArr.length > 2 ) {
                var newDoc = {};
                newDoc['TrioLocalID'] = doc['_id']['trioLocalID'];
                newDoc['familyID'] = familyId;
                newDoc['SeqTypeName'] = doc['_id']['SeqTypeName'];
                newDoc['AssemblyType'] = doc['_id']['AssemblyType'];
                newDoc['panelType'] = doc['_id']['panelType'];
                newDoc['TrioStatus'] = 'pending';
                var updTrioArr = [];
                for ( var idx in trioArr ) {
                    var trioMap = trioArr[idx];
                    var indId = trioMap['individualID'];
                    // id : family ID
                    var relation = await getFamRelation(indId,familyId);
                    trioMap['relation'] = relation;
                    updTrioArr.push(trioMap);
                }
                newDoc['trio'] = updTrioArr;
                //console.log("Logging trio doc after family updates");
                //console.log(newDoc);
                insertArr.push(newDoc);
            }
        }
        return insertArr;
    } catch(err) {
        throw err;
    }
}

const updateTrio = async(indId,trioCode,type) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var indSColl = db.collection(indSampCollection);
        indId = parseInt(indId);
        var search = {'individualID':indId,$or:[{trio:1},{trio:{$exists:false}}]};
        if ( type == "set" ) {
            search = {'individualID':indId,$or:[{trio:0},{trio:{$exists:false}}]};
        }
        await indSColl.updateMany(search,{$set:{trio:trioCode}});
        return "Success";
    } catch(err) {
        throw err;
    }
}


const  getFamTrio = async(famId) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var famColl = db.collection(familyCollection);
        famId = parseInt(famId);

        var doc = await famColl.findOne({_id:famId,'pedigree.Node_Key':{$in:[0,1,2]}},{'projection':{'pedigree.Node_Key':1,'pedigree.IndividualID':1}});

        var indId = [];
        if ( doc ) {
            var pedigreeArr = doc.pedigree;

            // 0 1 2 of pedigree
            for ( var idx in pedigreeArr ) {
                var hVal = pedigreeArr[idx];
                var indVal = hVal['IndividualID'];
                indId.push(indVal);
            }
        }
        return indId;
    } catch(err) {
        throw err;
    }
}

const fetchTrioCounter = async(famId) => {
    try {
            var client = getConnection();
            const db = client.db(dbName);
            var indSColl = db.collection(indSampCollection);
            var famIndArr = await getFamTrio(famId);
            
            var trioCursor = await indSColl.aggregate([{$match:{'individualID':{$in:famIndArr},'trio':1,'state':{$ne:'unassigned'}}},{$group:{_id:"$individualID"}},{$count:'trio_counter'}]);
            
            var trioCounter = null;
            while ( await trioCursor.hasNext() ) {
                const doc = await trioCursor.next();
                if ( doc['trio_counter']) {
                    trioCounter = doc['trio_counter'];
                }
            }
            return trioCounter;
    } catch(err) {
        throw err;
    }
}

const getFamRelation = async(indId,id) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var famColl = db.collection(familyCollection);
        var doc = await famColl.findOne({'_id':id,'pedigree.IndividualID':indId},{'projection':{'pedigree.FamilyMemberTypeName.$':1}});
        //console.log("Logging doc in getFamRelation");
        //console.log(doc);

        var relation = "";
        if ( doc && doc['pedigree']) {
            var doc1 = doc['pedigree'][0];
            relation = doc1['FamilyMemberTypeName'] || '';
        }
        return relation;
    } catch(err) {
        throw err;
    }
}

// Checks if the given indId is a member of Trio in the family id.
const checkTrioMember = async(indId,id) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        var famColl = db.collection(familyCollection);
        var doc = await famColl.findOne({'_id':id,'pedigree.IndividualID':indId},{'projection':{'pedigree.FamilyMemberTypeName.$':1}});

        var status = 0;    
        if ( doc && doc['pedigree']) {
            var relation = "";
            var doc1 = doc['pedigree'][0];
            relation = doc1['FamilyMemberTypeName'] || '';

            if ( (relation == "Proband") || (relation == "Father") || (relation == "Mother") ) {
                status = 1;
            }
        }
        return status;
    
    } catch(err) {
        throw err;
    }
}
/*
async function createConnection() {
    const url = `mongodb://${host}:${port}`;
    var client = await MongoClient.connect(url,{ useNewUrlParser : true });
    return client;
}
*/

module.exports = { initialize,initializeLogLoc,checkApiUser, storeMultiple, checkProband, updateData, readData, getAttrData, createDoc, createFamily, assignInd, addPedigree, showPedigree, updateRelative, updateFamily, getPIData, getFamily ,getResultCollObj, getUnassignedInd, checkIndFilter, removeRelative, getDefinedRelatives, getInheritanceData, getRelativeData , getFamilyPIData, unassignRelative ,checkTrioQueue, getTrioCodes,getTrioFamily,getTrioMeta,getTrioFamilyOld,updateFamilySid,updateTrio,getFamTrio,checkTrioMember,triggerAssemblySampTrio,trioVarAnno,trioQueue };
