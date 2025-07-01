const spawn  = require('child_process');
const runningProcess = require('is-running');
var path = require('path');

const configData = require('../config/config.js');
const { db : {host,port,dbName,resultCollection,variantQueryCounts,reqTrackCollection} } = configData;

const getConnection = require('../controllers/dbConn.js').getConnection;
const getResColl = require('../controllers/entityController.js').getResultCollObj;
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const { createReadStream, createWriteStream } = require('fs');
const getVariantsByVectorAndMember = require('../controllers/getController.js').getVariantsByVectorAndMember;
const filterVariants = require('../controllers/SVqueryController.js').filterVariants;
const getResults = require('../controllers/SVqueryController.js').getResults;
const SVDiscVar = require('../controllers/SVqueryController.js').SVDiscVar;
const getDiscVarResults = require('../controllers/SVqueryController.js').getDiscVarResults;

var loggerMod = require('../controllers/loggerMod');

const queryRoutesSV = (app) => {
    app.route('/querySampleSV')
    .post( loginRequired,async (req,res,next) => {
        try {
            //const { TrioLocalID, vector, page, limit } = req.query;
            var TrioLocalID = req.body.TrioLocalID;
            var vector = req.body.vector;
            var page = parseInt(req.body.page) || 1;
            var limit = 20;
            console.log(TrioLocalID);
            console.log(vector);
            // Check if TrioLocalID and vector parameters are provided
            if (!TrioLocalID || !vector) {
                return res.status(400).json({ error: 'TrioLocalID and vector parameters are required' });
            }
    
            // Call the controller function to get the variants
            const variants = await getVariantsByVectorAndMember(TrioLocalID, vector, page, limit);
    
            res.json(variants);
        } catch(err) {
            //res.status(400).send("Failure-Error message "+err);
            console.log("******** Hey !! Did you find something here ");
            console.log(err);
            next(`${err}`);
        }

    });

    app.route('/queryVarDiscSV')
    .post( loginRequired,async (req,res,next) => {
        try {
            //const { TrioLocalID, vector, page, limit } = req.query;
            const { start_chr, end_chr ,sv_type, start_pos, sv_len, filter, gt, gene_name, OMIM_ID, ACMG_class, seq_type, ref_build_type, hpo_list,process_id} = req.body;
            const data = { start_chr, end_chr ,sv_type, start_pos, sv_len, filter, gt, gene_name, OMIM_ID, ACMG_class, seq_type, ref_build_type, hpo_list,process_id}; 

  
    
            // Call the controller function to get the variants
            const response = await SVDiscVar(data);
    
            res.json(response);
        } catch(err) {
            //res.status(400).send("Failure-Error message "+err);
            console.log("******** Hey !! Did you find something here ");
            console.log(err);
            next(`${err}`);
        }

    });
    app.route('/filterSVSample')
    .post(loginRequired, async (req, res, next) => {
        try {
        const { IndividualID, fileID, TrioLocalID, trio_filter_vector, PopulationID, fileID_to_filter, chr, sv_type, start_pos, sv_len, filter, gt, gene_name, OMIM_ID, ACMG_class } = req.body;

        const data = { IndividualID, fileID, TrioLocalID, trio_filter_vector, PopulationID, fileID_to_filter, chr, sv_type, start_pos, sv_len, filter, gt, gene_name, OMIM_ID, ACMG_class };

        // Call the controller function to filter the variants
        const response = await filterVariants(data);

        res.json(response);
        } catch (err) {
        console.log("******** Hey !! Did you find something here ");
        console.log(err);
        next(`${err}`);
        }
        });

    app.route('/SVresults/:processId')
    .get(loginRequired, async (req, res, next) => {
      try {
        const { processId } = req.params;
        const { page = 1, limit = 10 } = req.query;
  
        const result = await getResults(processId, page, limit);
  
        res.json(result);
      } catch (err) {
        console.log("******** Hey !! Did you find something here ");
        console.log(err);
        next(`${err}`);
      }
    });

   app.route('/SVresults_lr/:processId')
  .get(loginRequired, async (req, res, next) => {
    try {
      const { processId } = req.params;
      const { page = 1, limit = 10 } = req.query;

      const result = await getResults_lr(processId, page, limit, req);

      res.json(result);
    } catch (err) {
      console.log("******** Hey !! Did you find something here ");
      console.log(err);
      next(`${err}`);
    }
  });

  app.route('/VarDiscSV/:processId')
    .get(loginRequired, async (req, res, next) => {
      try {
        const { processId } = req.params;
     
  
        const result = await getDiscVarResults(processId);
  
        res.json(result);
      } catch (err) {
        console.log("******** Hey !! Did you find something here ");
        console.log(err);
        next(`${err}`);
      }
    });


}


module.exports = { queryRoutesSV };
