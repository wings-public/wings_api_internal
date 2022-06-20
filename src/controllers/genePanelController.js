const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');

const configData = require('../config/config.js');
const { app:{instance} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
const { db : {dbName,genePanelColl} } = configData;

const getConnection = require('../controllers/dbConn.js').getConnection;

var pid = process.pid;
var uDateId = new Date().valueOf();
var logFile = `gene-panel-control-logger-${pid}-${uDateId}.log`;
//var logFile1 = `phen-control-logger-${pid}-${uDateId}.log`;
//var logFile = loggerEnv(instance,logFile1);
var gpLog = logger('genepanel',logFile);

//Create genepanel based on the request body provided as input
const createGenePanel = async(reqBody) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const gpColl = db.collection(genePanelColl);
        var reqObj = reqBody['Genepanel'];
        var insertManyArr = [];
        for ( var idx1 in reqObj ) {
            var reqArr = reqObj[idx1];
            if ( reqArr['PanelID'] && reqArr['UserID'] && reqArr['GeneID'] ) {
                var doc = {'_id':reqArr['PanelID'],'GeneID':reqArr['GeneID'], 'UserID' : reqArr['UserID']};
                insertManyArr.push(doc);
            } else {
                throw "Invalid JSON structure for addGenePanel";
            }
        }
        var result = await gpColl.insertMany(insertManyArr);
        return "Success";
    } catch(err) {
        throw err;
    }
}

const selectGenepanel = async(genepanelID) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const dbColl = db.collection(genePanelColl);
        var doc = await dbColl.findOne({'_id':parseInt(genepanelID)});
        return doc;
    } catch(err) {
        throw err;
    }
}

const modifyGenepanel = async(reqObj) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const dbColl = db.collection(genePanelColl);
        var filter = {'_id': reqObj['PanelID']};
        var updateSt = {'UserID': reqObj['UserID']};
        if ( reqObj['GeneID'] ) {
            updateSt['GeneID'] = reqObj['GeneID'];
        } 
        var updateSet = {$set: updateSt};
        var exUpd = await dbColl.updateOne(filter,updateSet);
        return "Success";
    } catch(err) {
        throw err;
    } 
}

const delGenepanel = async(reqObj) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const dbColl = db.collection(genePanelColl);
        var panelId = reqObj['PanelID'];
        var delEx = await dbColl.deleteOne({'_id':panelId});
        return "Success";
    } catch(err) {
        throw err;
    } 
}

module.exports = { createGenePanel, selectGenepanel, modifyGenepanel,delGenepanel };
