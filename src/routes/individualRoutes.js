
const readData = require('../controllers/entityController.js').readData;
const getAttrData = require('../controllers/entityController.js').getAttrData;
const storeMultiple = require('../controllers/entityController.js').storeMultiple;
const updateData = require('../controllers/entityController.js').updateData;
const getPIData = require('../controllers/entityController.js').getPIData;
const getUnassignedInd = require('../controllers/entityController.js').getUnassignedInd;
//const logger = require('../controllers/logger.js').logger;
const configData = require('../config/config.js');
const { app:{instance} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const path = require('path');
// logger specific settings
var pid = process.pid;
var uDateId = new Date().valueOf();
var logFile = `individual-routes-logger-${pid}-${uDateId}.log`;
//var logFile = loggerEnv(instance,logFile1);
var createLog = logger('individual',logFile);

/*
import { 
    readData, 
    getAttrData, 
    storeMultiple
} from '../controllers/entityController';
*/

// app is created in entity.js and will be passed to the routes.
const indRoutes = (app) => {
    // Retrive Individual data based on the Individual ID
    // Adding endpoint calls with route that can be chained to specific endpoints later.
    app.route('/ind/hello')
    .get( loginRequired,function (req,res,next) { // async not needed
        try {
           res.status(200).json({'message': 'Success'});
        } catch(err1) {
            //res.status(400).json({'message' : "failure"});
            next(`${err1}`);
        }
    } ); // remove semicolon if a chain request needs to be added.

    app.route('/logdata')
    .get( function (req,res,next) { // async not needed
        try {
            //var d = new Date();
            var d = new Date().toISOString().slice(0,10);
            var filename = `wingsApiQuery-${d}.log`;
            var path1 = path.parse(__dirname).dir;
            var logFile = path.join(path1,"controllers","log",filename);
            res.sendFile(logFile);
        } catch(err1) {
            //res.status(400).json({'message' : "failure"});
            next(`${err1}`);
        }
    } );

    app.route('/getReqLog/:logfile')
    .get( function (req,res,next) { // async not needed
        try {
            var fileName = req.params.logfile;
            var path1 = path.parse(__dirname).dir;
            var logFile = path.join(path1,"log","track-request.log");
            res.sendFile(logFile);
            //var logFile = "";
           //res.status(200).json({'message': 'Success'});
        } catch(err1) {
            //res.status(400).json({'message' : "failure"});
            next(`${err1}`);
        }
    } );


    // Get Family data based on FamilyID or PIID
    // If PIID = -1, get all the families in the center
    // Else, get the families belonging to the specific PIID
    app.route('/getIndividual/:type/:ID')
    .get(loginRequired,async (req,res,next) => {
        try {
            if ( req.params.type ) {
                var type = req.params.type;
                if ( type == "PIID" ) {
                    if ( req.params.ID ) {
                        createLog.debug("Logging response for getIndividual PIID");
                        var piid = req.params.ID;
                        var data = await getPIData(piid,"individual");
                        createLog.debug(data);
                        res.status(200).json({'message':data});
                    } 
                } else if ( type == "IndividualID" ) {
                    //console.log("TYPE is "+type);
                    if ( req.params.ID ) {
                        createLog.debug("Logging response for getIndividual IndividualID");
                        //createLog.debug(`getIndividual IndID params ID is ${req.params.ID}`);
                        //console.log(" ID value is "+req.params.ID);
                        var indId = parseInt(req.params.ID);
                        var data = await readData(indId);
                        createLog.debug(data);
                        res.status(200).json({'message':data});
                    }
                }
            }
        } catch(err) {
            //res.status(400).json({'message':`failure:${err}`});
            next(`${err}`);
        }
    });

    app.route('/individual/:id')
    .get(loginRequired,async (req, res, next) => {
        var indId = req.params.id;
        //console.log("Argument Received is "+indId);
        try {
            if ( indId ) {
                indId = parseInt(indId);
                createLog.debug("Logging response for individual id");
            }
            var data = await readData(indId);
            createLog.debug(data);
            res.status(200).json({'message' : data});
        } catch(e) {
            //res.status(400).json({'message' : `failure-Error in retrieving data-${e}`});
            next(`${e}`);
        }
    });
    
    app.route('/getIndList/:type/:PIID')
    .get(loginRequired,async (req, res, next) => {
        console.log("************ Received request for unassigned Individuals");
        console.log(req.params);
        //var type = req.params.type;
        //console.log("TYPE is "+type);
        try {
            console.log("Received request for unassigned Individuals");
            console.log(req.params);
            //var reqBody = req.body;
            var reqBody = {};
            if ( req.params.PIID ) {
                reqBody['PIID'] = parseInt(req.params.PIID);
            }
            if ( req.params.Gender ) {
                reqBody['IndividualSex'] = parseInt(req.params.Gender);
            }
            console.log("Logging request params");
            console.log(req.params);
            var data = await getUnassignedInd(reqBody);
            createLog.debug("Logging response for getIndList PIID");
            createLog.debug(data);
            res.status(200).json({'message' : data});
        } catch(e) {
            //res.status(400).json({'message' : `failure-Error in retrieving data-${e}`});
            console.log("Logging the catch part");
            next(`${e}`);
        }
    });

    app.route('/getIndList/:type/:PIID/:Gender')
    .get(loginRequired,async (req, res, next) => {
        console.log("************ Received request for unassigned Individuals");
        console.log(req.params);
        //var type = req.params.type;
        //console.log("TYPE is "+type);
        try {
            console.log("Received request for unassigned Individuals");
            console.log(req.params);
            //var reqBody = req.body;
            var reqBody = {};
            if ( req.params.PIID ) {
                reqBody['PIID'] = parseInt(req.params.PIID);
            }
            if ( req.params.Gender ) {
                reqBody['IndividualSex'] = parseInt(req.params.Gender);
            }
            console.log("Logging request params");
            console.log(req.params);
            var data = await getUnassignedInd(reqBody);
            createLog.debug("Logging response for getIndList based on Gender and PIID");
            createLog.debug(data);
            res.status(200).json({'message' : data});
        } catch(e) {
            //res.status(400).json({'message' : `failure-Error in retrieving data-${e}`});
            console.log("Logging the catch part");
            next(`${e}`);
        }
    });

    // additional request can be chained for the specific endpoint if required.
    // POST endpoint
    //.post(addNewContact);

    // endpoint to get individuals based on the attributes provided as arguments
    app.route('/getInd/:attr/') 
    .get(loginRequired,async (req,res,next) => {
        var attr = req.params.attr;
        //console.log("Attributes to be checked are "+attr);
        // Assume attributes are supplied in the following format (curl http://127.0.0.1:8081/getInd/attr1=2-attr2=3-attr3=4)
    
        var attrV = attr.split('-');
        var filter1 = {};
        for ( var attrIdx in attrV ) {
            var attr1 = attrV[attrIdx];
            var re1 = /(.*?):(.*)/;
            var data = re1.exec(attr1);
            var name = data[1];
            var val = data[2];
            filter1[name] = val;
        }
    
        try {
            var data = await getAttrData(filter1);
            createLog.debug("Logging response for getInd");
            createLog.debug(data);
            res.status(200).json({'message':data});
        } catch(e) {
            //res.status(400).json({'message':`failure-Error in retrieving data-${e}`});
            next(`${e}`);
        }
    });

    // endpoint to add single or multiple individuals
    app.route('/addIndividuals')
    .post( loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        try {
            createLog.debug("Logging response for addIndividuals");
            var res1 = await storeMultiple(reqBody);
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                //res.status(400).json({'message':'failure'});
                next("Individuals could not be added");
            }
        } catch(err1) {
            //res.status(400).json({'message':`failure:${err1}`});
            next(`${err1}`);
        }
    });

    // endpoint to update individuals data
    app.route('/updateIndividuals')
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        try {
            var result = await updateData(reqBody);
            createLog.debug("Logging response for updateIndividuals");
            createLog.debug(result);
            if ( result != "Failure" ) {
                res.status(200).json({'message':'Success'});
            }
        } catch(err) {
            //res.status(400).json({'message':'failure'});
            next(`${err}`);
        }
    });

}

module.exports = { indRoutes };
//export default indRoutes;
