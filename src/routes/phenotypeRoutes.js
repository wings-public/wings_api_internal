
//const logger = require('../controllers/logger.js').logger;
const configData = require('../config/config.js');
const { app:{instance} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const addPhenTerms = require('../controllers/phenController.js').addPhenTerms;
const updatePhenTerms = require('../controllers/phenController.js').updatePhenTerms;
const delPhenTerms = require('../controllers/phenController.js').delPhenTerms;
const getHpoHist = require('../controllers/phenController.js').getHpoHist;
const getIndHPO = require('../controllers/phenController.js').getIndHPO;

// logger specific settings
var pid = process.pid;
var uDateId = new Date().valueOf();
//var logFile1 = `phen-routes-logger-${pid}-${uDateId}.log`;
//var logFile = loggerEnv(instance,logFile1);
//var createLog = logger(logFile);
var logFile = `phen-routes-logger-${pid}-${uDateId}.log`;
var createLog = logger('phenotype',logFile);

const phenotypeRoutes = (app) => {
    app.route('/addPhenotype') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("addPhenotype Route Endpoint");
        createLog.debug(reqBody);
        try {
            if ( ! reqBody.IndividualID || ! reqBody.HPOs ) {
                throw "JSON Structure Error";
            }
            if ( reqBody.HPOs.length > 1 ) {
                throw "Existing addPhenotype request format supports only 1 HPO at a time";
            }

            createLog.debug("Calling addPhenTerms");
            var res1 = await addPhenTerms(reqBody);
            createLog.debug("addPhenotype request completed");
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                next("Phenotype could not be added");
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });

    app.route('/updatePhenotype') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("updatePhenotype Route Endpoint");
        createLog.debug(reqBody);
        try {
            if ( ! reqBody.IndividualID || ! reqBody.HPOID ) {
                throw "JSON Structure Error";
            }

            createLog.debug("Calling updatePhenTerms");
            var res1 = await updatePhenTerms(reqBody);
            createLog.debug("updatePhenotype request completed");
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                next("Phenotype could not be updated");
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });

    app.route('/deletePhenotype') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("deletePhenotype Route Endpoint");
        createLog.debug(reqBody);
        try {
            if ( ! reqBody.IndividualID || ! reqBody.HPOs ) {
                throw "JSON Structure Error";
            }
            if ( reqBody.HPOs.length > 1 ) {
                throw "Existing deletePhenotype request format supports only 1 HPO at a time";
            }

            createLog.debug("Calling delPhenTerms");
            var res1 = await delPhenTerms(reqBody);
            createLog.debug("deletePhenotype request completed");
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                next("Phenotype could not be deleted");
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });


     app.route('/showHPOHistory/:IndividualID') 
    .get(loginRequired,async (req,res,next) => {
        try {
            var indId = parseInt(req.params.IndividualID);
            createLog.debug("showHPOHistory Route Endpoint");
            createLog.debug(indId);
        
            if ( ! indId ) {
                throw "JSON Structure Error";
            }

            createLog.debug("Calling getHpoHist");
            var hpoHistory = await getHpoHist(indId);
            createLog.debug("showHPOHistory request completed");
            createLog.debug(hpoHistory);
            res.status(200).json({'message':hpoHistory});
        } catch(err1) {
            next(`${err1}`);
        }
    });

    app.route('/getPhenotype/:IndividualID') 
    .get(loginRequired,async (req,res,next) => {
        try {
            var indId = parseInt(req.params.IndividualID);
            createLog.debug("getPhenotype Route Endpoint");
            createLog.debug(indId);
        
            if ( ! indId ) {
                throw "JSON Structure Error";
            }

            createLog.debug("Calling getHpoHist");
            var indHPOData = await getIndHPO(indId);
            createLog.debug("getPhenotype request completed");
            createLog.debug(indHPOData);
            res.status(200).json({'message':indHPOData});
        } catch(err1) {
            next(`${err1}`);
        }
    });

}

module.exports = { phenotypeRoutes };



