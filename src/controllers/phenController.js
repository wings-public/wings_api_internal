const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');

const configData = require('../config/config.js');
const { app:{instance} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
const { db : {dbName,phenotypeColl,phenotypeHistColl} } = configData;

const getConnection = require('../controllers/dbConn.js').getConnection;

var pid = process.pid;
var uDateId = new Date().valueOf();
var logFile = `phen-control-logger-${pid}-${uDateId}.log`;
//var logFile1 = `phen-control-logger-${pid}-${uDateId}.log`;
//var logFile = loggerEnv(instance,logFile1);
var phenContLog = logger('phenotype',logFile);

const addPhenTerms = async(postReq) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const phenCollObj = db.collection(phenotypeColl);
        var indId = postReq['IndividualID'];
        var hpoAttr = postReq['HPOs'][0];
        var omim = postReq['OMIMs'];
        var genes = postReq['Genes'];
        
        var hpoID = hpoAttr['HPOID'];

        //hpoAttr['status'] = 'active';
        //var collId = indId+hpoID;
        var checkPhen = {'IndividualID':indId,'hpo_id':hpoID,'status':'active'};
        var data = await phenCollObj.findOne(checkPhen);
        if ( data ) {
            throw `${hpoID} already exists for Individual ${indId}`;
        }

        var addPhen = { 'IndividualID':indId, 'hpo_id' : hpoID, 'status' : 'active','hpo' : hpoAttr, 'omim' : omim , 'gene' : genes };

        var result = await phenCollObj.insertOne(addPhen);
        test.equal(1,result.insertedCount);

        phenContLog.debug("Phenotype terms added to Individual");

        var histObj = JSON.parse(JSON.stringify(hpoAttr));
        histObj['IndividualID'] = indId;
        histObj['Operation'] = "Inserted";
        histObj['UserID_Update'] = null;
        histObj['DateUpdate'] = null;

        phenContLog.debug(histObj);
        const phenHistColl = db.collection(phenotypeHistColl);
        var histRes = await phenHistColl.insertOne(histObj);
        test.equal(1,histRes.insertedCount);
        phenContLog.debug("Phenotype history updated");
        return "Success";
    } catch(err) {
        throw err;
    }
}

const updatePhenTerms = async(postReq) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const phenCollObj = db.collection(phenotypeColl);
        const phenHistColl = db.collection(phenotypeHistColl);
        var indId = postReq['IndividualID'];
        var hpoID = postReq['HPOID'];
        phenContLog.debug("updatePhenTerms : Logging received request body");
        phenContLog.debug(postReq);

        phenContLog.debug("updatePhenTerms : Logging received request body");
        phenContLog.debug(postReq);
        var search = {'IndividualID':indId, 'hpo_id' : hpoID,'status':'active'};
        var storedData = await phenCollObj.findOne(search,{'projection':{'hpo.UserID':1,'_id':0,'omim':1,'gene':1}});
        phenContLog.debug("logging stored data");
        phenContLog.debug(storedData);
        phenContLog.debug("Logging the HPO_Status value");
        phenContLog.debug(postReq['HPO_Status']);
       
        var upd = {};
        var addSet = {};
        var comment = '';
        // Step 1 : Search and check the type of update request
        if ( postReq['Onset_Year']) {
            upd = { $set: {'hpo.Onset_Year' : postReq['Onset_Year'] , 'hpo.UserID' : postReq['UserID']} };
            phenContLog.debug("Step1:Onset_Year update requested");
            comment = 'Onset_Year update request';
        } else if ( 'Onset_Month' in postReq ) {
            upd = { $set: {'hpo.Onset_Month' : postReq['Onset_Month'] , 'hpo.UserID' : postReq['UserID']} };
            phenContLog.debug("Step1:Onset_Month update requested");
            comment = 'Onset_Month update request';
        } else if ( 'HPO_Severity' in postReq ) {
            upd = { $set: {'hpo.HPO_Severity' : postReq['HPO_Severity'], 'hpo.UserID' : postReq['UserID']} };
            phenContLog.debug("Step1:HPO_Severity update requested");
            comment = 'HPO_Severity update request';
        } else if ( 'HPO_Status' in postReq ) {
            if ( postReq['HPO_Status'] == 1 ) {
                // add the OMIMs and Genes
                //var upd = {'HPO_Status':postReq['HPO_Status'], 'omim' : postReq['']}
                phenContLog.debug("Step1:HPO_Status-add omims genes update requested");
                upd = { $set : { 'hpo.HPO_Status':postReq['HPO_Status'] ,'hpo.UserID' : postReq['UserID'] } };
                //addSet = { $addToSet : {'omim' : postReq['OMIMs'], 'gene' : postReq['Genes']}};
                //addSet = { $addToSet : { 'omim': {$each : postReq['OMIMs'] } } };
                addSet = { $addToSet : { 'omim': { $each : postReq['OMIMs'] } , 'gene' : { $each : postReq['Genes']}} };
                comment = 'HPO_Status update and add omims genes';
            } else if ( (! postReq['HPO_Status'] ) || (postReq['HPO_Status'] == 2 ) ) {
                // delete the OMIMs and Genes
                phenContLog.debug("Step1:HPO_Status-delete omims genes update requested");
                upd = { $set : {'hpo.HPO_Status':postReq['HPO_Status'], 'omim' : [], 'gene' : [] , 'hpo.UserID' : postReq['UserID']} };
                comment = 'HPO_Status update and delete omims genes';
            }
        }

        // Step 2 : Update hpo attributes
        var result = await phenCollObj.updateOne(search,upd);
        phenContLog.debug("Step 2:Update hpo attributes done");

        // Step 3 : Get the existing hpo attribute hash stored in db & add history
        var hpoStored = await phenCollObj.findOne(search,{'projection':{'hpo.UserID':0}});
        var hpoHist = hpoStored['hpo'];
        hpoHist['IndividualID'] = hpoStored['IndividualID'];
        hpoHist['UserID'] = storedData.hpo.UserID;
        hpoHist['UserID_Update'] = postReq['UserID'];
        hpoHist['DateUpdate'] = postReq['DateUpdate'];
        hpoHist['Operation'] = "Updated";
        hpoHist['comment'] = comment;
        phenContLog.debug(hpoHist);
        var histRes = await phenHistColl.insertOne(hpoHist);
        test.equal(1,histRes.insertedCount);
        phenContLog.debug("Step3:Phenotype history updated");

        // Step 4 : Add omims/genes if requested
        if ( postReq['HPO_Status'] && postReq['HPO_Status'] == 1 ) {
            var result1 = await phenCollObj.updateOne(search,addSet);
            phenContLog.debug("Step 4:Add omims genes done");
        }
        phenContLog.debug("updatePhenTerms-Update request done");
        return "Success";
    } catch(err) {
        throw err;
    }
}


const delPhenTerms = async(postReq) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const phenCollObj = db.collection(phenotypeColl);
        var indId = postReq['IndividualID'];
        var hpoAttr = postReq['HPOs'][0];
        
        var hpoID = hpoAttr['HPOID'];

        //hpoAttr['status'] = 'active';
        var search = {'IndividualID':indId, 'hpo_id' : hpoID,'status':'active'};
        //var collId = indId+hpoID;
        
        // Get HPO stored data. This will be used to log history
        var hpoStData = await phenCollObj.findOne(search,{'projection':{'omim':0,'gene':0}});
        var hpoStored = JSON.parse(JSON.stringify(hpoStData));
        var hpoHist = hpoStored['hpo'];
        hpoHist['IndividualID'] = hpoStored['IndividualID'];
        hpoHist['UserID'] = hpoStored.hpo.UserID;
        hpoHist['UserID_Update'] = hpoAttr['UserID'];
        hpoHist['DateUpdate'] = hpoAttr['DateUpdate'];
        hpoHist['Operation'] = "Deleted";
        hpoHist['comment'] = `${hpoID} Deleted`;
        
        // update the HPOID document status to inactive
        var upd = {$set:{'status' : 'inactive'}};
        var res1 = await phenCollObj.updateOne(search,upd);

        // log the action to history collection
        phenContLog.debug(hpoHist);
        const phenHistColl = db.collection(phenotypeHistColl);
        var histRes = await phenHistColl.insertOne(hpoHist);
        test.equal(1,histRes.insertedCount);
        phenContLog.debug("Phenotype history updated");
        return "Success";
    } catch(err) {
        throw err;
    }
}

const getHpoHist = async(individualID) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const phenHistColl = db.collection(phenotypeHistColl);

        var histTmp = await phenHistColl.find({'IndividualID':individualID},{'projection': {'_id':0,'comment':0,'IndividualID':0}});
        var hist = histTmp.sort({'HPOID':1});
        var obj = {'IndividualID':individualID,'history':[]};
        while ( await hist.hasNext()) {
            const doc = hist.next();
            //console.log(doc);
            obj['history'].push(await doc);
        }
        return obj;
    } catch(err) {
        throw err;
    }
}

const getIndHPO = async(individualID) => {
    // db query snapshot
    //db.indPhenotype.aggregate([ {$match: {"IndividualID":10032893298,'status':"active"}},{ $group: { _id: "$IndividualID", omim: {$push: "$omim"} ,gene: {$push: "$gene"} } }, {$unwind:{path:"$omim",preserveNullAndEmptyArrays:true}},{$unwind:{path:"$omim",preserveNullAndEmptyArrays:true}},{$unwind:{path:"$gene",preserveNullAndEmptyArrays:true}},{$unwind:{path:"$gene",preserveNullAndEmptyArrays:true} }, { $group: { _id: "$_id", omim: {$addToSet: "$omim"} , gene:{$addToSet: "$gene"}} } ] );
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const phenCollObj = db.collection(phenotypeColl);

        // Step1 : Get the HPO terms assigned to the Individual.Filter using 'status':"active"
        var hpoTerms = await phenCollObj.find({'IndividualID':individualID,'status':"active"},{'projection':{'_id':0,'omim':0,'gene':0}});
        var hpoArr = [];
        var omimArr = [];
        var geneArr = [];
        var selectObj = {"IndividualID":individualID,"HPOs":hpoArr,"OMIMs":omimArr,"Genes":geneArr};

        while ( await hpoTerms.hasNext()) {
            const hpoDoc = await hpoTerms.next();
            if ( hpoDoc['hpo']) {
                hpoArr.push(hpoDoc['hpo']);
            }
        }
        if ( hpoArr.length > 0 ) {
            // aggregation 
            var match = {$match: {"IndividualID":individualID,'status':"active"} };
            var group1 = { $group: { _id: "$IndividualID", omim: {$push: "$omim"} ,gene: {$push: "$gene"} } };
            var unwind1 = { $unwind:{path:"$omim",preserveNullAndEmptyArrays:true} };
            var unwind2 = {$unwind:{path:"$gene",preserveNullAndEmptyArrays:true}};
            var setGroup = { $group: { _id: "$_id", omim: {$addToSet: "$omim"} , gene:{$addToSet: "$gene"}} };

            // Aggregation sequence : match the query request, group based on Individual ID , omim and gene. 
            // After applying group stage, documents are aggregated as [[omim1,omim2,omim3....]], [[gene1,gene2..]]
            // For the above reason, it is important to unwind them twice. 
            // Then , apply group criteria and addToSet will make sure there are no duplicate omims or genes
            var aggInfo = await phenCollObj.aggregate([match,group1,unwind1,unwind1,unwind2,unwind2,setGroup]);
            phenContLog.debug(match);
            phenContLog.debug(group1);
            phenContLog.debug(unwind1);
            phenContLog.debug(unwind2);
            phenContLog.debug(setGroup);
            while (await aggInfo.hasNext()) {
                var aggDoc = await aggInfo.next();
                phenContLog.debug(aggDoc);
                omimArr = aggDoc['omim'];
                geneArr = aggDoc['gene'];
            }
            selectObj['OMIMs'] = omimArr;
            selectObj['Genes'] = geneArr;
            return selectObj;
        } else {
            return selectObj
        }
    } catch(err) {
        throw err;
    }
}

module.exports = { addPhenTerms , updatePhenTerms , delPhenTerms, getHpoHist, getIndHPO};
