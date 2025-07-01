
//const logger = require('../controllers/logger.js').logger;
const configData = require('../config/config.js');
const { app: {tmpCenterID,tmpHostID},app:{instance} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const varPhenSearch = require('../controllers/varDisController.js').varPhenSearch;
const getVarPhenCount = require('../controllers/varDisController.js').getVarPhenCount;
const getGeneCount = require('../controllers/varDisController.js').getGeneCount;
const getRegionCount = require('../controllers/varDisController.js').getRegionCount;
const varCounts = require('../controllers/varDisController.js').varCounts
const varPhenAnalysis = require('../controllers/varDisController.js').varPhenAnalysis;
const fetchVarSamples = require('../controllers/varDisController.js').fetchVarSamples;
const contactPI = require('../controllers/varDisController.js').contactPI;

// logger specific settings
var pid = process.pid;
var uDateId = new Date().valueOf();
var logFile = `vardisc-routes-logger-${pid}-${uDateId}.log`;
var createLog = logger('var_disc',logFile);

const varDiscRoutes = (app) => {
    app.route('/varDiscReq') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("variant discovery request Endpoint");
        createLog.debug(reqBody);
        try {
            if ( ! reqBody.var_search || ! reqBody.centerId  || ! reqBody.hostId) {
                throw "JSON Structure Error";
            }

            var req_id = Date.now() + Math.floor((Math.random() * 100) + 1);
            var msg1 = {"centerId" : reqBody.centerId,"hostId" : reqBody.hostId, "reqId":req_id};

            if ( reqBody['var_search']['variant']) {
                if ( ! reqBody.seq_type ) {
                    throw "Sequence type required"
                }
                if ( ! reqBody.phen_term ) {
                    throw "phen_term required";
                }
                if ( ! reqBody['assembly'] ) {
                    throw "assembly type required for discovery based on variant";
                }
                var regex = /^[a-z0-9]+\-[0-9]+\-[AGCT]+\-[AGCT]+$/i;
                var var_key = reqBody['var_search']['variant'];
                if ( ! var_key.match(regex)) {
                    throw "Invalid variant format. Expected chr-pos-ref-alt";
                }
                res.status(200).json({"message":msg1});
                createLog.debug("Calling varPhenSearch");
                var res1 = await varPhenSearch(reqBody,req_id);
                createLog.debug("varPhenSearch request completed");
                createLog.debug(res1);
            } else if (reqBody['var_search']['var_key']) {
                console.log("Received request for variant ------");
                if ( ! reqBody['assembly'] ) {
                    throw "assembly type required for discovery based on variant";
                }
                var regex = /^[a-z0-9]+\-[0-9]+\-[AGCT]+\-[AGCT]+$/i;
                var var_key = reqBody['var_search']['var_key'];
                if ( ! var_key.match(regex)) {
                    throw "Invalid variant format. Expected chr-pos-ref-alt";
                }
                res.status(200).json({"message":msg1});
                createLog.debug("Calling varCounts");
                // function to perform the variant counts
                var res1 = await varCounts(reqBody,req_id);
                createLog.debug("varCounts request completed");
                createLog.debug(res1);
            } else {
                if ( reqBody['var_search']['region']) {
                    var region = reqBody['var_search']['region'];
                    var regex = /^[a-z0-9]+\-[0-9]+\-[0-9]+/i;
    
                    if ( ! region.match(regex)) {
                        throw "Invalid region format. Expected chr-startPos-stopPos";
                    }
                    res.status(200).json({"message":msg1});
                    var res1 = await getRegionCount(reqBody,req_id);

                } else if ( reqBody['var_search']['geneID']) {
                    res.status(200).json({"message":msg1});
                    var res1 = await getGeneCount(reqBody,req_id);
                    createLog.debug("getGeneCount request completed");
                    createLog.debug(res1);
                }
            }

        } catch(err1) {
            next(`${err1}`);
        }
    });

    app.route('/getVarPhenCount/:requestID') 
    .get(loginRequired,async (req,res,next) => {
        try {
            var reqID = parseInt(req.params.requestID);
            createLog.debug("getVarPhenCount Route Endpoint");
            createLog.debug(reqID);
        
            if ( ! reqID ) {
                throw "JSON Structure Error";
            }

            var varData = await getVarPhenCount(reqID);
            res.status(200).json({'message':varData});
        } catch(err1) {
            next(`${err1}`);
        }
    });
    
    app.route('/varPhenAnalysis') 
        .post(loginRequired,async (req,res,next) => {
            try {
                var reqBody = req.body;
                createLog.debug("variant discovery request Endpoint");
                createLog.debug(reqBody);
                var reqId = reqBody.req_id;
                var hpoList = reqBody.hpoList;
                var varList = [];
                var assemblyType = reqBody.assemblyType;
                var seqType = reqBody.seqType;

                var varPhen = await varPhenAnalysis(reqId,hpoList,varList,assemblyType,seqType);
                res.status(200).json({'message':varPhen});
            } catch(err) {
                next(`${err}`);
            }
    });

    // API to fetch the Individuals correspondingt the variant and specific request
    app.route('/varSamples') 
        .post(loginRequired,async (req,res,next) => {
            try {
                var reqBody = req.body;
                createLog.debug("variant samples request Endpoint");
                createLog.debug(reqBody);
                var reqID = reqBody.req_id;
                var variant = reqBody.var_key;
                var host_id = reqBody.host_id;
                var center_id = reqBody.center_id;
                var type = reqBody.type;
                var indList = "";
                if ( type == "association" ) {
                    var indList = await fetchInd(reqID,variant);
                } else {
                    indList = await fetchVarSamples(reqID,variant);
                }
                //var indList = await fetchVarSamples(reqID,variant);
                var indListStr = '';
                if ( indList.length > 0 ) {
                    indListStr = indList.join(',');
                }
                //res.status(200).json({'message': {"ID" : indListStr, "centerId" : center_id, "hostId" : host_id}});
                res.status(200).json({'message': {"ID" : indList, "centerId" : center_id, "hostId" : host_id}});
            } catch(err) {
                next(`${err}`);
            }
    });

    // API to contact the PI to request access
    app.route('/requestAccess') 
        .post(loginRequired,async (req,res,next) => {
            try {
                var reqBody = req.body;
                createLog.debug("variant samples request Endpoint");
                createLog.debug(reqBody);
                var reqID = reqBody.req_id;
                var piid = reqBody.piid;
                var pi_mail = reqBody.pi_mail;
                var host_id = reqBody.host_id;
                var center_id = reqBody.center_id;
                var variant = reqBody.variant;

                var status = await contactPI(reqBody);
                
                //res.status(200).json({'message': {"ID" : indListStr, "centerId" : center_id, "hostId" : host_id}});
                res.status(200).json({'message': {"status" : status, "centerId" : center_id, "hostId" : host_id}});
            } catch(err) {
                next(`${err}`);
            }
    });

}

module.exports = { varDiscRoutes };



