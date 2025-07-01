const createFamily = require('../controllers/entityController.js').createFamily;
const addPedigree = require('../controllers/entityController.js').addPedigree;
const showPedigree = require('../controllers/entityController.js').showPedigree;
const assignInd = require('../controllers/entityController.js').assignInd;
const checkProband = require('../controllers/entityController.js').checkProband;
const getUnassignedInd = require('../controllers/entityController.js').getUnassignedInd;
const updateRelative = require('../controllers/entityController.js').updateRelative;
const updateFamily = require('../controllers/entityController.js').updateFamily;
const getFamilyPIData = require('../controllers/entityController.js').getFamilyPIData;
const getFamily = require('../controllers/entityController.js').getFamily;
const checkIndFilter = require('../controllers/entityController.js').checkIndFilter;
//const logger = require('../controllers/logger.js').logger;
const configData = require('../config/config.js');
const { app:{instance,trioQCnt} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
const removeRelative = require('../controllers/entityController.js').removeRelative;
const checkTrioQueue = require('../controllers/entityController.js').checkTrioQueue;
const getTrioCodes = require('../controllers/entityController.js').getTrioCodes;
const getTrioFamily = require('../controllers/entityController.js').getTrioFamily;
const getDefinedRelatives = require('../controllers/entityController.js').getDefinedRelatives;
const getRelativeData = require('../controllers/entityController.js').getRelativeData;
const updateFamilySid = require('../controllers/entityController').updateFamilySid;
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const unassignRelative = require('../controllers/entityController.js').unassignRelative;
const getTrioMeta = require('../controllers/entityController.js').getTrioMeta;
const updateTrio = require('../controllers/entityController.js').updateTrio;
const trioVarAnno = require('../controllers/entityController.js').trioVarAnno;
const getFamAnalyseTypes = require('../controllers/entityController.js').getFamAnalyseTypes;
const familyAnalysisPrecomp = require('../controllers/entityController.js').familyAnalysisPrecomp;
const familyAnalysisPrecompStatus = require('../controllers/entityController.js').familyAnalysisPrecompStatus;
const getSVTrioFamily  = require('../controllers/entityController.js').getSVTrioFamily;
// logger specific settings
var pid = process.pid;
var uDateId = new Date().valueOf();
var logFile = `family-routes-logger-${pid}-${uDateId}.log`;
//var logFile = loggerEnv(instance,logFile1);
var createLog = logger('family',logFile);
// Initializing queue for family and trio based operations


/* ES6 Arrow functions format
const var = async function(arg1,arg2) {

} 

equivalent to 

const var = async (arg1,arg2) => {

}
*/

const familyRoutes = (app) => {
    // Insert the family document provided as input. Created an entry in family collection
    // ESAT URL https://wings.esat.kuleuven.be/PhenBook/Family --> Add New Family
    app.route('/addNewFamily') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("addNewFamily Route Endpoint");
        createLog.debug(reqBody);
        try {
            var res1 = await createFamily(reqBody);
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                next("Family could not be added");
                //res.status(400).json({'message':'failure'});
            }
        } catch(err1) {
            //res.status(400).json({'message':`failure:${err1}`});
            next(`${err1}`);
        }
    });

    app.route('/updateFamily') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug(`updateFamily Logging request`);
        createLog.debug(reqBody);
        console.dir(reqBody,{"depth":null});
        try {
            var res1 =  await updateFamily(reqBody);
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                //res.status(400).json({'message':'failure'});
                next("family could not updated");
            }
        } catch(err1) {
            //res.status(400).json({'message':`failure:${err1}`});
            next(`${err1}`);
        }
    });

    // Get Family data based on FamilyID or PIID
    // If PIID = -1, get all the families in the center
    // Else, get the families belonging to the specific PIID
    app.route('/getFamily/:type/:ID')
    .get(loginRequired,async (req,res,next) => {
        try {
            if ( req.params.type ) {
                var type = req.params.type;
                if ( type == "PIID" ) {
                    if ( req.params.ID ) {
                        createLog.debug(`getFamily PIID params ID is ${req.params.ID}`);
                        var piid = req.params.ID;
                        var data = await getFamilyPIData(piid);
                        //var data = await getPIFamilyData(piid);
                        createLog.debug("Logging response for getFamily PIID");
                        createLog.debug(data);
                        res.status(200).json({'message':data});
                    } 
                } else  if ( type == "FamilyID" ) {
                    console.log("TYPE is "+type);
                    if ( req.params.ID ) {
                        createLog.debug(`getFamily FamilyID params ID is ${req.params.ID}`);
                        console.log(" ID value is "+req.params.ID);
                        var data = await getFamily(req.params.ID);
                        createLog.debug("Logging response for getFamily FamilyID");
                        createLog.debug(data);
                        res.status(200).json({'message':data});
                    }
                }
            }
        } catch(err) {
            next(`${err}`); 
            //res.status(400).json({'message':`failure:${err}`});
        }
    });

    // Endpoint that will be called to store the pedigree that was created based on the questionaire(number of relations)
    app.route('/addPedigree')
    .post(loginRequired,async(req,res,next) => {
        var reqBody = req.body;
        try {
            if ( ! reqBody['_id'] || ! reqBody['update']) {
                throw "JSON Structure Error";
            }
            createLog.debug("Request received to addPedigree");
            var res1 = await addPedigree(reqBody);
            createLog.debug("Logging response for addPedigree");
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                res.status(400).json({'message':'failure'});
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });


    app.route('/indSampleUpdate')
    .post(loginRequired,async(req,res,next) => {
        var reqBody = req.body;
        try {
            /*res.status(200).json({'message':'trioSidUpdateTrigger:disabled for this release'});*/
            if ( !reqBody['IndividualID'] || !reqBody['SampleLocalID'] || !reqBody['action'] || !reqBody['SequenceType']) {
                throw "JSON Structure Error";
            }

            if ( reqBody['SequenceType'].toUpperCase() == "PANEL") {
                if ( ! reqBody['PanelType']) {
                    throw "PanelType mandatory for PANEL sequencing type";
                }
            }
            var res1 = await updateFamilySid(reqBody);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                res.status(400).json({'message':'failure'});
            }
        } catch(err) {
            next(`${err}`);
            //res.status(400).json({'message':'trioSidUpdateTrigger:disabled for this release'});
        }
    });

     // This endpoint will be called for Show Pedigree and Draw Pedigree 
    app.route('/showPedigree/:id')
    .get(loginRequired,async(req,res,next) => {
        var familyId = req.params.id;
        familyId = parseInt(familyId);
        try {
            createLog.debug("Received request for showPedigree");
            var response = await showPedigree(familyId);
            //var response = await showPedigree(familyId);
            createLog.debug("Logging response for showPedigree");
            createLog.debug(response);
            res.status(200).json({'message':response});
        } catch(err1) {
            next(`${err1}`);
        }
    });


    app.route('/showPedigree/proband/:id')
    .get(loginRequired,async(req,res,next) => {
        var probandId = req.params.id;
        console.log("proband id is "+probandId);
        if ( probandId == null ) {
            console.log("Do you come here ");
            next("invalid probandid");
        } else {  
            probandId = parseInt(probandId);
            try {
                createLog.debug("Received request for showPedigree based on proband");
                var familyId = await checkProband(probandId);
                var response = await showPedigree(familyId);
                createLog.debug("Logging response for showPedigree");
                createLog.debug(response);
                //console.log(response);
                if ( response['_id'] == null ) {
                    response['_id'] = probandId;
                }
                res.status(200).json({'message':response});
            } catch(err1) {
                next(`${err1}`);
            }
        }
    });    
    
    app.route('/assignProband')
    .post(loginRequired, async(req,res,next) => {
        try {
            if ( ! req.body['familyID'] || ! req.body['IndividualID']) {
                throw "Please provide:familyID and IndividualID";
            }
            createLog.debug("Received request for assignProband.Logging request body");
            createLog.debug(req.body);
            var familyID = req.body['familyID'];
            var indId = req.body['IndividualID'];
            var proband = 1;
            var res1 = await assignInd(familyID,indId,proband);
            createLog.debug(`Step1-Proband ${indId} assigned to family ${familyID}.Logging promise token below`);
            createLog.debug(res1);
            // call function to assign proband

            // set trio code = 1 for all files related to this Individual(proband)
            var res2 = await updateTrio(indId,1,"set");
            if ( req.body['pedigree']) {
                createLog.debug("Logging pedigree");
                createLog.debug(req.body['pedigree']);
                // let's define pedigree values in family 
                createLog.debug("Calling addPedigree to add pedigree of proband");
                var response = await addPedigree(req.body,'probandPedigree');
                createLog.debug("Logging response for assignProband Step2");
                createLog.debug(response);
                createLog.debug("Request assignProband Completed");
                res.status(200).json({'message':response});
            }
        } catch(err) {
            next(`${err}`);
        }
    })

    app.route('/assignIndividual') 
    .post(loginRequired,async(req,res,next) => {
        try {
            var familyId = req.body['familyID'];
            var indId = req.body['IndividualID'];
            // proband value will be passed as 1 when Individual is assigned as proband.
            // any other relation will have a default setting value of 0
            var proband = req.body['proband'] || 0;
            if ( !familyId || !indId ) {
                throw "Input parameters null";
                //res.status(400).json({'message':'failure-Input params null'});
            }
            familyId = parseInt(familyId);
            indId = parseInt(indId);
            createLog.debug(`familyID is ${familyId}`);
            createLog.debug(`Individual ID is ${indId}`);
            var res1 = await assignInd(familyId,indId,proband);
            createLog.debug("Logging response for assignIndividual");
            createLog.debug(res1);
            if ( res1 == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( res1 == "Failure" ) {
                next("family could not be assigned to Individual");
                //res.status(400).json({'message':'failure'});
            }
        } catch(err) {
            //res.status(400).json({'message':`failure-${err}`});
            next(`${err}`);
        }
    });
     
    app.route('/getRelativeData/:id/:nodekey')
    .get(loginRequired, async(req,res,next) => {
        // Inputs : family_id and nodeKey
        try {
            if ( ! req.params.id || ! req.params.nodekey ) {
                next("family ID and node key has to be provided as inputs");
            }
            var familyId = parseInt(req.params.id);
            var nodekey = parseInt(req.params.nodekey);

            console.log(`Request to get the relative data for ${familyId} and ${nodekey}`);
            var result = await getRelativeData(familyId,nodekey);
            res.status(200).json({'message':result});
        } catch(err) {
            next(`${err}`);
        }

    });


    app.route('/updateRelative') 
    .post(loginRequired,async(req,res,next) => {
        var reqBody = req.body; // json data
        try {
            if ( !reqBody['_id'] || (!reqBody['update'] && !reqBody['unassign'] ) ) {
                throw "JSON Structure Error";
            } 
            createLog.debug("Received request for updateRelative");
	        createLog.debug("Logging request body");
	        createLog.debug(reqBody);

            if ( reqBody['update']) {
                var type = 'relative';
                // Node_Key : 2 proband. to make sure we do not assign proband to family when update is requested.
                // This is allowed only for adding an Individual as Relative(not proband)
                // assigning proband will be a different workflow
                if ( reqBody['update']['relatives']['IndividualID'] && reqBody['update']['Node_Key'] != 2 ) {
                    createLog.debug("updateRelative-Individual-Not Proband");
                    var indId = reqBody['update']['relatives']['IndividualID'];
                    var famId = reqBody['_id'];
                    var proband = 0;
                    createLog.debug(`Check if IndividualID ${indId} has familyID ${famId}`);
                    var indInfo = await checkIndFilter({'_id':indId, 'familyId': famId});
                    createLog.debug("Logging individual info");
                    createLog.debug(indInfo);
                    console.log(`indInfo:${indInfo}Done`);
                    if ( ! indInfo ) {
                        createLog.debug(`Individual ${indId} is not assigned to family.Lets proceed and assign`);
                        var res1 = await assignInd(reqBody['_id'],indId,proband);
                        createLog.debug("Logging Response for updateRelative Step1 IndividualID Node_Key is not 2");
                        createLog.debug(res1);
                    } 
                    type = 'definedInd';
                } else if ( reqBody['update']['relatives']['IndividualID'] && reqBody['update']['Node_Key'] == 2 ) {
                    // initial proband assignment to family cannot be updated
                    createLog.debug("updateRelative-Individual-Proband");
                    type = 'definedInd';
                }
                // updates Relative data and also pedigree data
                createLog.debug(`Calling updateRelative function with type value ${type}`);
                var result = await updateRelative(type,reqBody);
                createLog.debug("Logging response for updateRelative Step2");
                createLog.debug(result);
                if ( result == "Success" ) {
                    res.status(200).json({'message':'Success'});
                } else if ( result == "Failure" ) {
                    next("relative could not be updated");
                }
            } else if ( reqBody['unassign']) {
                createLog.debug("Received request for updateRelative-unassign");
                var result = await unassignRelative(reqBody['_id'],reqBody['unassign']);
                createLog.debug("Logging response for updateRelative unassign");
                createLog.debug(result);
                if ( result == "Success" ) {
                    res.status(200).json({'message':'Success'});
                } else if ( result == "Failure" ) {
                    next("relative could not be updated");
                }
            }
        } catch(err) {
            next(`${err}`);
            //res.status(400).json({'message':'failure'});
        }
    });

    app.route('/removeRelative') 
    .post(loginRequired,async(req,res,next) => {
        var reqBody = req.body; // json data
        try {
            // updates Relative data and also pedigree data
            createLog.debug("Received request to removeRelative");
            createLog.debug(reqBody);
            var result = await removeRelative(reqBody);
            createLog.debug("Logging response for removeRelative");
            createLog.debug(result);
            if ( result == "Success" ) {
                res.status(200).json({'message':'Success'});
            } else if ( result == "Failure" ) {
                next("relative could not be removed");
                //res.status(400).json({'message':'failure'});
            }
        } catch(err) {
            next(`${err}`); 
            //res.status(400).json({'message':`failure:${err}`});
        }
    });

     // Endpoint that will be used to get the trio codes for the specific family id 
     app.route('/getTrioCodes/:localid')
     .get(loginRequired,async(req,res,next) => {
         var localid = req.params.localid;
         try {
             createLog.debug("Request received to getTrioCodes");
             var res1 = await getTrioCodes('trio_local_id',localid);
             res.status(200).json({'message':res1});
         } catch(err1) {
            next(`${err1}`);
         }
     });

     // Endpoint that will be used to get trio codes based on post request
     app.route('/getTrioCodesPost')
     .post(loginRequired,async(req,res,next) => {
         var reqBody = req.body;
         try {
             createLog.debug("Request received to getTrioCodes");
             var res1 = await getTrioCodes('post_req',reqBody);
             res.status(200).json({'message':res1});
         } catch(err1) {
            next(`${err1}`);
         }
     });

    // Endpoint that will be used to  getTrioFamilies based on proband
    app.route('/getTrioFamilies/:type/:id')
    .get(loginRequired,async(req,res,next) => {
        /*try {
            res.status(200).json({'message':'getTrioFamilies:disabled for this release'});
        } catch(err) {
            res.status(400).json({'message':'getTrioFamilies:disabled for this release'});
        }*/
        var type = req.params.type;
        var id = parseInt(req.params.id);
        console.log("type is "+type);
        console.log("id is "+id);
        if ( id == null ) {
            throw "invalid id value";
        }
        try {
            if ( ( type === "proband") || ( type === "family") || ( type === "piid" )) {
                createLog.debug(`Request received to getTrioFamilies based on ${type}`);
                var res1 = await getTrioFamily(type,id);
                res.status(200).json({'message':res1});
            } else {
                throw "Invalid type.supported types are proband or family or piid";
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });

    // SV - Endpoint that will be used to  getTrioFamilies based on proband
    app.route('/getTrioFamiliesSV/:type/:id')
    .get(loginRequired,async(req,res,next) => {
        /*try {
            res.status(200).json({'message':'getTrioFamilies:disabled for this release'});
        } catch(err) {
            res.status(400).json({'message':'getTrioFamilies:disabled for this release'});
        }*/
        var type = req.params.type;
        var id = parseInt(req.params.id);
        console.log("type is "+type);
        console.log("id is "+id);
        if ( id == null ) {
            throw "invalid id value";
        }
        try {
            if ( ( type === "proband") || ( type === "family") || ( type === "piid" )) {
                createLog.debug(`Request received to getTrioFamilies based on ${type}`);
                var res1 = await getSVTrioFamily(type,id);
                res.status(200).json({'message':res1});
            } else {
                throw "Invalid type.supported types are proband or family or piid";
            }
        } catch(err1) {
            next(`${err1}`);
        }
    });

    // Endpoint to get trio meta data based on trio id
    app.route('/getTrioMetaData/:id')
    .get(loginRequired,async(req,res,next) => {
        var trioId = req.params.id;
        if ( trioId == null ) {
            throw "invalid id value";
        }
        try {
            var res2 = await getTrioMeta(trioId);
            res.status(200).json({'message':res2});
        } catch(err) {
            next(`${err}`);
        }
    });

    // Endpoint that will be used to  getTrioFamilies based on proband
    app.route('/getTrioFamiliesOld/proband/:proband_id')
    .get(loginRequired,async(req,res,next) => {
        var probandId = req.params.proband_id;
        console.log("proband id is "+probandId);
        try {
            if ( probandId == null ) {
                next("invalid probandid");
            }
            createLog.debug("Request received to getTrioFamilies based on proband");
            var res1 = await getTrioFamilyOld('proband',probandId);
            res.status(200).json({'message':res1});
        } catch(err1) {
            next(`${err1}`);
        }
    });

    // Endpoint that will be used to get annotations for trio family
    app.route('/fetchTrioVariantAnno')
    .post(loginRequired,async(req,res,next) => {
        try {
            var reqBody = req.body;
            var res1 = await trioVarAnno(reqBody);
            res.status(200).json({'message':res1});
        } catch(err1) {
            next(`${err1}`);
        }
    });


    // Endpoint that will be used to to getTrioFamilies based on family
    app.route('/getTrioFamiliesPost')
    .post(loginRequired,async(req,res,next) => {
        try {
            res.status(200).json({'message':'getTrioFamiliesPost:disabled for this release'});
        } catch(err) {
            res.status(400).json({'message':'getTrioFamiliesPost:disabled for this release'});
        }
        /*var reqBody = req.body;
        try {
            if ( ! reqBody['FamilyID'] ) {
                next("invalid familyid");
            }
            createLog.debug("Request received to getTrioFamilies based on family");
            var res1 = await getTrioFamilyOld('family',reqBody);
            res.status(200).json({'message':res1});
        } catch(err1) {
            next(`${err1}`);
        }*/
    });

     // Endpoint that will be called to store the pedigree that was created based on the questionaire(number of relations)
     app.route('/scanTrioQueue')
     .get(loginRequired,async(req,res,next) => {
        try {
            res.status(200).json({'message':'scanTrioQueue:disabled for this release'});
        } catch(err) {
            res.status(400).json({'message':'scanTrioQueue:disabled for this release'});
        }
         /*var reqBody = req.body;
         try {
             createLog.debug("Request received to checkTrioQueue");
             var res1 = await checkTrioQueue();
             res.status(200).json({'message':res1});
         } catch(err1) {
             next(`${err1}`);
         }*/
     });

    app.route('/getInheritanceData')
    .get(loginRequired, async(req,res,next) => {
        try {
            var reqBody = req.body;
            createLog.debug("getInheritanceData");
            createLog.debug(reqBody);
            if ( ! req.body['proband'] || ! req.body['inheritance'] ) {
                next("Request body arguments not provided");
            }
            var result = await getDefinedRelatives(reqBody);
            res.status(200).json({'message' : result});
        } catch(err) {
            next(`${err}`);
        }
    });

    // Endpoint to get family analysis options for the provided Individuals
    app.route('/familyAnalysisTypes')
    .post(loginRequired,async(req,res,next) => {
        try {
            var reqBody = req.body;
            if ( ! reqBody.family_members ) {
                throw "family_members needed to process this request";
            }
        
            var fam_mem = reqBody.family_members;
            console.log(fam_mem);
            var res1 = await getFamAnalyseTypes(fam_mem);
            res.status(200).json({'message':res1});
        } catch(err) {
            next(`${err}`);
        }
    });


    // Endpoint to launch family analysis pre-computation
    app.route('/familyAnalysis/Precompute')
    .post(loginRequired,async(req,res,next) => {
        try {
            var reqBody = req.body;
            if ( ! reqBody.family_local_id || ! reqBody.affected_mem || ! reqBody.assembly_type ) {
                throw "Missing request body parameters";
            }
        
            var res1 = await familyAnalysisPrecomp(reqBody);
            res.status(200).json({'message':res1});
        } catch(err) {
            next(`${err}`);
        }
    });

    // Endpoint to get the status of family analysis precomputation
    app.route('/familyAnalysis/Precompute/status/:fam_code')
    .get(loginRequired,async(req,res,next) => {
        
        var fam_code = req.params.fam_code;
        if ( fam_code == null ) {
            throw "Missing family code";
        }

        try {
            var res1 = await familyAnalysisPrecompStatus(fam_code);
            res.status(200).json({'message':res1});
        } catch(err) {
            next(`${err}`);
        }
    });

}

module.exports = { familyRoutes };



