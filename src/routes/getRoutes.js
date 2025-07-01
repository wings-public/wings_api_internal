const SampleSV = require('../controllers/getController.js').SampleSV;
const SampleSNV = require('../controllers/getController.js').SampleSNV; 
const loginRequired = require('../controllers/userControllers.js').loginRequired;
//const logger = require('../controllers/logger.js').logger;
const configData = require('../config/config.js');
const { app:{instance,trioQCnt} } = configData;



// logger specific settings
var pid = process.pid;
var uDateId = new Date().valueOf();

// Initializing queue for family and trio based operations

// Changed POST to GET due to the restrictions for the response data size of POST request
const getRoutes = (app) => {
    // Insert the family document provided as input. Created an entry in family collection
    // ESAT URL https://wings.esat.kuleuven.be/PhenBook/Family --> Add New Family
    app.route('/getSVSamples/:piid/:host_id')
    .get(loginRequired,async (req,res,next) => {
    //app.route('/getSVSamples') 
    //.post(loginRequired,async (req,res,next) => {
        //const { host_id, piid } = req.body;
        var piid = req.params.piid;
        var host_id = req.params.host_id;

        
        try {
            const samples = await SampleSV(parseInt(host_id), parseInt(piid));
      
           
            res.json(samples);
        } catch(err1) {
            //res.status(400).json({'message':`failure:${err1}`});
            next(`${err1}`);
        }
    });

    app.route('/getSNVSamples') 
    .post(loginRequired,async (req,res,next) => {
        const { host_id, piid } = req.body;
        
        try {
            const samples = await SampleSNV(host_id, piid);
      
           
            res.json(samples);
        } catch(err1) {
            //res.status(400).json({'message':`failure:${err1}`});
            next(`${err1}`);
        }
    });





}

module.exports = { getRoutes };



