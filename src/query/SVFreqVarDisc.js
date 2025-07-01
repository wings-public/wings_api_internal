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
const { db : {host,port,dbName,apiUserColl,apiUser,familyCollection,resultsSVFreqDisc,populationSVCollection,individualCollection,resultCollection, phenotypeColl,phenotypeHistColl,fileMetaCollection,hostMetaCollection,indSampCollection,trioCollection,importCollection3} , app:{trioQCnt}} = configData;
const Result = require('../models/resultsSV');
const fork  = require('child_process');
const getConnection = require('../controllers/dbConn.js').getConnection;
var createConnection = require('../controllers/dbConn.js').createConnection;
process.on('message', async (data) => {
    try {
      const client = await createConnection();
      
      const db = client.db(dbName);
      const indSamp = db.collection(indSampCollection);
      const indPhen = db.collection(phenotypeColl);
      const varColl = db.collection(importCollection3);
      const resultColfreq = require('../models/resultsSVFreqDisc.js');
      const queryIndividSample = {
        AssemblyType: data.ref_build_type,
        SeqTypeName: data.seq_type,
        FileType: 'SV_VCF'
      };

    // Find documents matching the query
      const documents = await indSamp.find(queryIndividSample).toArray();

      // Extract unique individualID values
      const uniqueIndividualIDs = new Set(documents.map(doc => doc.individualID));
      const tot_individ = uniqueIndividualIDs.size;
      const fileIDs = documents.map(doc => doc.fileID);

      console.log('Unique individualIDs:', uniqueIndividualIDs);
      console.log('FileIDs:', fileIDs);
       // Extract unique individualID values and create a mapping from fileID to individualID
       const fileToIndividualMap = {};
       documents.forEach(doc => {
           fileToIndividualMap[doc.fileID] = doc.individualID;
       });

      // Query the second collection for individuals with the specified HPO IDs
      const queryPhenotype = { 'hpo.HPOID': { $in: data.hpo_list } };
      const individualsWithHPOs = await indPhen.find(queryPhenotype).toArray();

      //const individualIDsWithPhenotype = new Set(individualsWithHPOs.map(individual => individual.IndividualID));
      //console.log('Individuals with HPOs:', individualIDsWithPhenotype);
      
       // Filter individuals to include only those who have all the HPO IDs in the list
       const individualHPOMap = new Map();
       individualsWithHPOs.forEach(individual => {
           if (!individualHPOMap.has(individual.IndividualID)) {
               individualHPOMap.set(individual.IndividualID, new Set());
           }
           individualHPOMap.get(individual.IndividualID).add(individual.hpo.HPOID);
       });

       const individualsWithAllHPOs = new Set(
           [...individualHPOMap.entries()]
               .filter(([_, hpoSet]) => data.hpo_list.every(hpo => hpoSet.has(hpo)))
               .map(([individualID, _]) => individualID)
       );

       console.log('Individuals with all HPOs:', individualsWithAllHPOs);

      // Split documents based on phenotype
      const individualsWithPhenotype = [];
      const individualsWithoutPhenotype = [];

      documents.forEach(doc => {
          if (individualsWithAllHPOs.has(doc.individualID)) {
              individualsWithPhenotype.push(doc);
          } else {
              individualsWithoutPhenotype.push(doc);
          }
      });
    

      console.log('Individuals with phenotype:', individualsWithPhenotype);
      console.log('Individuals without phenotype:', individualsWithoutPhenotype);  
      const fileIDsWithPhenotype = individualsWithPhenotype.map(doc => doc.fileID);
      const fileIDsWithNoPhenotype = individualsWithoutPhenotype.map(doc => doc.fileID);
      console.log('FileIDs with no phenotype:', fileIDsWithNoPhenotype);
      let filteredVariants;
      
      const initialQuery = buildFreqQuerry(data);
      const phen_query = { ...initialQuery, fileID: { $in: fileIDsWithPhenotype } };
      const no_phen_query =  { ...initialQuery, fileID: { $in: fileIDsWithNoPhenotype } };
      console.log('Phenotype query:', phen_query);  
      console.log('No phenotype query:', no_phen_query);
      
      const sampleDocs = await varColl.find({}).limit(5).toArray();
      console.log('Sample documents from varColl:', JSON.stringify(sampleDocs));

      const phenDoc = await varColl.find(phen_query).toArray();
      const noPhenDoc = await varColl.find(no_phen_query).toArray();
      console.log('Phenotype documents:', phenDoc);
      console.log('No phenotype documents:', noPhenDoc);  
      const uniquePhenIndividuals = new Set(phenDoc.map(doc => fileToIndividualMap[doc.fileID]));
      const uniqueNoPhenIndividuals = new Set(noPhenDoc.map(doc => fileToIndividualMap[doc.fileID]));

      const individualsWithPhenotypeCount = uniquePhenIndividuals.size;
      const individualsWithoutPhenotypeCount = uniqueNoPhenIndividuals.size;

      const phenIndividualsWithoutDocs = new Set([...individualsWithAllHPOs].filter(id => !uniquePhenIndividuals.has(id)));
      const noPhenIndividualsWithoutDocs = new Set([...uniqueIndividualIDs].filter(id => !individualsWithAllHPOs.has(id) && !uniqueNoPhenIndividuals.has(id)));

      
        
  
      const processId = data.processId;
      console.log(resultsSVFreqDisc);
      const Result = require('../models/resultsSVFreqDisc.js');
      console.log('Phenotype documents count:', phenDoc.length);
        console.log('Unique individuals with phenotype:', individualsWithPhenotypeCount);
        console.log('Individuals with phenotype but no documents:', phenIndividualsWithoutDocs.size);

        console.log('No phenotype documents count:', noPhenDoc.length);
        console.log('Unique individuals without phenotype:', individualsWithoutPhenotypeCount);
        console.log('Individuals without phenotype but no documents:', noPhenIndividualsWithoutDocs.size);

        await Result.create({
            process_id: data.process_id,
            status: 'computing',
            all_cases: {
                "var+phen": individualsWithPhenotypeCount,
                "var-phen": individualsWithoutPhenotypeCount,
                "-var+phen": phenIndividualsWithoutDocs.size,
                "-var-phen": noPhenIndividualsWithoutDocs.size
            }
          });
          await Result.updateOne({ process_id: data.process_id }, { $set: { status: 'completed' } });
          await Result.updateMany(
            { process_id: data.process_id },
            { $set: { status: 'complete'} }
          );
      process.send({ processId });
      process.exit();
    } catch (error) {
      await Result.updateOne(
        { process_id: data.processId },
        { $set: { status: 'error', error: error.message } }
      );
      process.send({ error: error.message });
      process.exit(1);
    }
  });


  





function buildFreqQuerry(data) {
  const query = {};

  // chr goes from 1 to 22 and we have X and Y, X we cast to int 23 and Y we cast to 24
  if (data.start_chr) {
    let chr = data.start_chr.toUpperCase();
    if (chr === 'X') {
      chr = 23;
    } else if (chr === 'Y') {
      chr = 24;
    } else {
      chr = parseInt(chr, 10);
    }
    query.chr = chr;
  }
  
  if (data.end_chr) {
    let end_chr = data.end_chr.toUpperCase();
    if (end_chr === 'X') {
        end_chr = 23;
    } else if (end_chr === 'Y') {
        end_chr = 24;
    } else {
        end_chr = parseInt(end_chr, 10);
    }
    query.end_chr = end_chr;
  }
  // sv_type filter
  if (data.sv_type) {
    query.sv_type = data.sv_type;
  }
  //query.start_pos = data.start_pos;
 
  query.start_pos = { $gte: (data.start_pos-(Math.abs(data.sv_len)/3)), $lte: (data.start_pos+(Math.abs(data.sv_len)/3)) };
  

  query.sv_len = { $gte: (data.sv_len-(Math.abs(data.sv_len)/2)), $lte: (data.sv_len+(Math.abs(data.sv_len)/2)) };

  // filter field
  if (data.filter) {
    query.filter = data.filter;
  }

  // gt field
  if (data.gt) {
    query.gt = data.gt;
  }

  // gene_name can be 'true' or a specific gene name
  if (data.gene_name) {
    if (data.gene_name === 'true') {
      query.gene_name = { $exists: true, $ne: '' };
    } else {
      query.gene_name = data.gene_name.toUpperCase();
    }
  }

  // OMIM_ID field
  if (data.OMIM_ID) {
    query.OMIM_ID = data.OMIM_ID;
  }

  // ACMG_class we always return variants that have major or equal to number present there
  if (data.ACMG_class) {
    const acmgClass = parseInt(data.ACMG_class, 10);
    query.ACMG_class = { $gte: acmgClass };
  }

  return query;
}


