//const logger = require('../controllers/logger.js').logger;
const configData = require('../config/config.js');
const { app:{instance} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const createGenePanel = require('../controllers/genePanelController.js').createGenePanel;
const modifyGenepanel = require('../controllers/genePanelController.js').modifyGenepanel;
const selectGenepanel = require('../controllers/genePanelController.js').selectGenepanel;
const delGenepanel = require('../controllers/genePanelController.js').delGenepanel;

// logger specific settings
var pid = process.pid;
var uDateId = new Date().valueOf();

var logFile = `genepanel-routes-logger-${pid}-${uDateId}.log`;
var createLog = logger('genepanel',logFile);

const genepanelRoutes = (app) => {
    // Create a genepanel with a list of gene ids present in the request body.
    app.route('/addGenepanel') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("addGenePanel Route Endpoint");
        createLog.debug(reqBody);
        try {
            if ( ! reqBody.Genepanel ) {
                throw "JSON Structure Error";
            }

            var res1 = await createGenePanel(reqBody);
            createLog.debug("addGenePanel request completed");
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                next("Genepanel could not be added");
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });

    // Update the gene ids present in the genepanel.
    app.route('/updateGenepanel') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("updateGenepanel Route Endpoint");
        createLog.debug(reqBody);
        try {
            if ( ! reqBody.PanelID ) {
                throw "JSON Structure Error";
            }

            createLog.debug("Calling modifyGenepanel");
            var res1 = await modifyGenepanel(reqBody);
            createLog.debug("modifyGenepanel request completed");
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                next("Genepanel could not be updated");
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });

    // Delete the genepanel using the panel ID.
    app.route('/deleteGenepanel') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("deleteGenepanel Route Endpoint");
        createLog.debug(reqBody);
        try {
            if ( ! reqBody.PanelID ) {
                throw "JSON Structure Error";
            }

            createLog.debug("Calling delGenepanel");
            var res1 = await delGenepanel(reqBody);
            createLog.debug("delGenepanel request completed");
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                next("Genepanel could not be deleted");
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });

    // Get the gene ids based on the gene panel ID
    app.route('/getGenepanel/:genepanelID') 
    .get(loginRequired,async (req,res,next) => {
        try {
            var gpID = parseInt(req.params.genepanelID);
            createLog.debug("getGenepanel Route Endpoint");
            createLog.debug(gpID);
        
            if ( ! gpID ) {
                throw "JSON Structure Error";
            }

            createLog.debug("Calling selectGenepanel");
            var gpData = await selectGenepanel(gpID);
            createLog.debug("selectGenepanel request completed");
            createLog.debug(gpData);
            res.status(200).json({'message':gpData});
        } catch(err1) {
            next(`${err1}`);
        }
    });

}

module.exports = { genepanelRoutes };



