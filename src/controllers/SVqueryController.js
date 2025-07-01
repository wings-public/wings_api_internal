const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');
var bcrypt = require('bcrypt');
const Async = require('async');
const configData = require('../config/config.js');
var path = require('path');
const { app:{instance,logLoc} } = configData;
const {logger,loggerEnv} = require('../controllers/loggerMod');
//added importCollection3
const { db : {host,port,dbName,apiUserColl,apiUser,familyCollection,resultSV,resultsSVFreqDisc,populationSVCollection,individualCollection,resultCollection, phenotypeColl,phenotypeHistColl,fileMetaCollection,hostMetaCollection,indSampCollection,trioCollection,importCollection3} , app:{trioQCnt}} = configData;
const Result = require('../models/resultsSV');
const { fork } = require('child_process');
var createConnection = require('../controllers/dbConn.js').createConnection;
const { v4: uuidv4 } = require('uuid');

const baseUrl = 'http://localhost:8443/SVresults';

  const filterVariants = async (data) => {
    try {
      const processId = uuidv4();
      data.processId = processId;
      
      //console.log("Data is "+JSON.stringify(data)); 
      const Result = require('../models/resultsSV.js');
      // Create initial result document with status "computing"
      /*const result = new Result({
        process_id: processId,
        status: 'computing',
        filtered_variants: []
      });
  
      await result.save();*/
  
      const forked = fork(path.join(__dirname, '../query' ,'SVsampleFilterQuery.js'));
      //console.log(path.join(__dirname, '../query' ,'SVsampleFilterQuery.js'));
      forked.send(data);
      //console.log('Sent data to filterJob process');
      forked.on('message', (message) => {
        if (message.error) {
          console.error(message.error);
        }
      });
  
      // Return immediately with process ID and status
      return {
        process_id: processId,
        status: 'computing',
        next_page: `${baseUrl}/${processId}?page=1&limit=10`
      };
    } catch (error) {
      throw new Error(error.message);
    }
  };
  
  const getResults_old = async (processId, page = 1, limit = 10) => {
    try {
      const result = await Result.findOne({ process_id: processId });
  
      if (!result) {
        throw new Error('Result not found');
      }
  
      if (result.status !== 'complete') {
        return { process_id: processId, status: result.status };
      }
  
      const startIndex = (page - 1) * limit;
      const endIndex = startIndex + limit;
      const pagedVariants = result.filtered_variants.slice(startIndex, endIndex);
  
      const totalPages = Math.ceil(result.filtered_variants.length / limit);
      const nextPage = page < totalPages ? `${baseUrl}/${processId}?page=${parseInt(page) + 1}&limit=${limit}` : null;
  
      return {
        process_id: processId,
        status: result.status,
        filtered_variants: pagedVariants,
        current_page: parseInt(page),
        total_pages: totalPages,
        next_page: nextPage,
      };
    } catch (error) {
      throw new Error(error.message);
    }
  };

  const getResults = async (processId, page = 1, limit = 10) => {

    try {
      const client = await createConnection();
      
      const db = client.db(dbName);
      const Result = db.collection(resultSV); // Ensure the correct collection is used
  
      // Fetch all documents for the given processId with status 'complete'
      const results = await Result.find({ process_id: processId, status: 'complete' }).toArray();
      const computing = await Result.find({ process_id: processId, status: 'computing' }).toArray();
      if (computing.length) {
        return('Computing'); 
      } else if (!results.length) {
        throw new Error('Result not found');
        
      }
  
      // Aggregate all filtered_variants from the results
      const allVariants = results.reduce((acc, result) => acc.concat(result.filtered_variants), []);
      //console.log("All variants are "+allVariants.length);
      // Paginate the aggregated results
      const startIndex = (page - 1) * limit;
      const endIndex = startIndex + limit;
      const pagedVariants = allVariants.slice(startIndex, endIndex);
  
      const totalPages = Math.ceil(allVariants.length / limit);
      const nextPage = page < totalPages ? `${baseUrl}/${processId}?page=${parseInt(page) + 1}&limit=${limit}` : null;
  
      return {
        process_id: processId,
        status: 'complete',
        filtered_variants: pagedVariants,
        current_page: parseInt(page),
        total_pages: totalPages,
        next_page: nextPage,
      };
    } catch (error) {
      throw new Error(error.message);
    }
  };
  
  const SVDiscVar = async (data) => {
    try {
      const processId = data.process_id;
  
      
      //console.log("Data is "+JSON.stringify(data)); 
      // Create initial result document with status "computing"
      const result = new Result({
        process_id: processId,
        status: 'computing',
        all_cases: []
      });
  
      await result.save();
  
      const forked = fork(path.join(__dirname, '../query' ,'SVFreqVarDisc.js'));
      //console.log(path.join(__dirname, '../query' ,'SVsampleFilterQuery.js'));
      forked.send(data);
      //console.log('Sent data to filterJob process');
      forked.on('message', (message) => {
        if (message.error) {
          console.error(message.error);
        }
      });
  
      // Return immediately with process ID and status
      return {
        process_id: data.process_id,
        status: 'computing',
      };
    } catch (error) {
      throw new Error(error.message);
    }
  };

  const getDiscVarResults = async (processId) => {

    try {
      const client = await createConnection();
      
      const db = client.db(dbName);
      const Result = db.collection(resultsSVFreqDisc); // Ensure the correct collection is used
  
      // Fetch all documents for the given processId with status 'complete'
      const results = await Result.find({ process_id: processId, status: 'complete' }).toArray();
      const computing = await Result.find({ process_id: processId, status: 'computing' }).toArray();
      if (computing.length) {
        return computing; 
      } else if (!results.length) {
        throw new Error('Result not found');
        
      }
  
      
      return results;
    } catch (error) {
      throw new Error(error.message);
    }
  };
  module.exports = {
    filterVariants,
    getResults,
    SVDiscVar,
    getDiscVarResults
    // other exports
  };