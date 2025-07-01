const createPopulation = require('../controllers/entityController.js').createPopulation;
const updatePopulation = require('../controllers/entityController.js').updatePopulation;
const assignInd = require('../controllers/entityController.js').assignInd;
const addIndividualsAndSamplesToPop = require('../controllers/entityController.js').addIndividualsAndSamplesToPop;
const removeIndividualsAndSamplesFromPop = require('../controllers/entityController.js').removeIndividualsAndSamplesFromPop;
const getUnassignedInd = require('../controllers/entityController.js').getUnassignedInd;
const getPopulationPIData = require('../controllers/entityController.js').getPopulationPIData;  
const getPopulation = require('../controllers/entityController.js').getPopulation;
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

const getRelativeData = require('../controllers/entityController.js').getRelativeData;
const updateFamilySid = require('../controllers/entityController').updateFamilySid;
const loginRequired = require('../controllers/userControllers.js').loginRequired;

const getTrioMeta = require('../controllers/entityController.js').getTrioMeta;
const updateTrio = require('../controllers/entityController.js').updateTrio;

const getFamAnalyseTypes = require('../controllers/entityController.js').getFamAnalyseTypes;
const familyAnalysisPrecomp = require('../controllers/entityController.js').familyAnalysisPrecomp;
const familyAnalysisPrecompStatus = require('../controllers/entityController.js').familyAnalysisPrecompStatus;

// logger specific settings
var pid = process.pid;
var uDateId = new Date().valueOf();
var logFile = `population-routes-logger-${pid}-${uDateId}.log`;
//var logFile = loggerEnv(instance,logFile1);
var createLog = logger('population',logFile);
// Initializing queue for family and trio based operations


/* ES6 Arrow functions format
const var = async function(arg1,arg2) {

} 

equivalent to 

const var = async (arg1,arg2) => {

}
*/

const populationRoutes = (app) => {
    // Insert the family document provided as input. Created an entry in family collection
    // ESAT URL https://wings.esat.kuleuven.be/PhenBook/Family --> Add New Family
    app.route('/addPopulation') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug("addPopulation Route Endpoint");
        createLog.debug(reqBody);
        try {
            var res1 = await createPopulation(reqBody);
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

    app.route('/updatePopulation') 
    .post(loginRequired,async (req,res,next) => {
        var reqBody = req.body;
        createLog.debug(`updatePopulation Logging request`);
        createLog.debug(reqBody);
        console.dir(reqBody,{"depth":null});
        try {
            var res1 =  await updatePopulation(reqBody);
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

    //maybe include sequencing type in the request ADD HOSTID CHECK
    app.route('/addIndividsSampsToPop')
    .post(loginRequired, async (req, res, next) => {
        var reqBody = req.body;
        createLog.debug("updatePopulation Route Endpoint");
        createLog.debug(reqBody);

        try {
            var populationId = reqBody.PopulationID;
            var individualsAndSamples = reqBody.individualsAndSamples;

            // Assuming you have a function to update the population
            var [result1, result2] = await addIndividualsAndSamplesToPop(populationId, individualsAndSamples);

            // Assuming result1 and result2 are boolean variables indicating whether certain conditions are met
            const message = {
                'message': 'Population updated successfully',
                'Already present in the population': result1,
                'Individual doesn not exists or doesnt have a SV_VCF sample': result2
            };

            res.status(200).json(message);

            
        } catch (err) {
            next(err);
        }
    });

    app.route('/removeIndividsSampsFromPop')
    .post(loginRequired, async (req, res, next) => {
        var reqBody = req.body;
        createLog.debug("removeIndividsSampsFromPop Route Endpoint");
        createLog.debug(reqBody);

        try {
            var populationId = reqBody.PopulationID;
            var individualsAndSamples = reqBody.individualsAndSamples;

            // Assuming you have a function to remove individuals and samples from the population
            var [result1, result2] = await removeIndividualsAndSamplesFromPop(populationId, individualsAndSamples);

            // Assuming result1 and result2 are boolean variables indicating whether certain conditions are met
            const message = {
                'message': 'Population updated successfully',
                'Removed individuals and samples from the population': result1,
                'Individuals or samples not found in the population': result2
            };

            res.status(200).json(message);

        } catch (err) {
            next(err);
        }
    });


    // Get Population data based on PopulationID or PIID
    // If PIID = -1, get all the populations in the center
    // Else, get the populations belonging to the specific PIID
    app.route('/getPopulation/:type/:ID')
    .get(loginRequired,async (req,res,next) => {
        try {
            if ( req.params.type ) {
                var type = req.params.type;
                if ( type == "PIID" ) {
                    if ( req.params.ID ) {
                        createLog.debug(`getPopulation PIID params ID is ${req.params.ID}`);
                        var piid = req.params.ID;
                        var data = await getPopulationPIData(piid);
                        //var data = await getPIFamilyData(piid);
                        createLog.debug("Logging response for Population PIID");
                        createLog.debug(data);
                        res.status(200).json({'message':data});
                    } 
                } else  if ( type == "PopID" ) {
                    console.log("TYPE is "+type);
                    if ( req.params.ID ) {
                        createLog.debug(`getPopulation PopulationID params ID is ${req.params.ID}`);
                        console.log(" ID value is "+req.params.ID);
                        var data = await getPopulation(req.params.ID);
                        createLog.debug("Logging response for getPopulation PopulationID");
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


}

module.exports = { populationRoutes };



