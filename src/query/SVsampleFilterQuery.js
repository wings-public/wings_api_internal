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
const { db : {host,port,dbName,apiUserColl,apiUser,familyCollection,resultSV,populationSVCollection,individualCollection,resultCollection, phenotypeColl,phenotypeHistColl,fileMetaCollection,hostMetaCollection,indSampCollection,trioCollection,importCollection3} , app:{trioQCnt}} = configData;
const Result = require('../models/resultsSV');
const fork  = require('child_process');
const getConnection = require('../controllers/dbConn.js').getConnection;
var createConnection = require('../controllers/dbConn.js').createConnection;
process.on('message', async (data) => {
    try {
      const client = await createConnection();
      
      const db = client.db(dbName);

      // Validate the presence of IDs and their dependent fields
      const validation = await validateInput(db, data);
      if (validation.error) {
        // Update the process status with error and the error message
        const Result = db.collection(resultSV);
        await Result.updateOne(
          { process_id: data.processId },
          { $set: { status: 'error: ' + validation.error  } }
        );
        process.send({ error: validation.error });
        process.exit(1);
      }
      console.log("Validation is " + JSON.stringify(validation));
      const { useCase } = validation;
      console.log("Use case is " + useCase);
      let filteredVariants;
      if (useCase === 'PopulationID') {
        const initialQuery = buildPopulationQuery(data);
        // Call additional function for PopulationID and get initial results
        const initialResults = await handlePopulationID(db, data);
        // Apply further filtering to these initial results
        //console.log("Initial Results are " + JSON.stringify(initialResults));
        filteredVariants = await filterPopulationResults(db,initialResults, initialQuery,data.fileID_to_filter);
        //console.log("Filtered Variants are " + JSON.stringify(filteredVariants)); 
      } else {
        // Handle fileID and TrioLocalID use cases and build query
        const query = buildQuery(data, useCase);
        console.log("DO WE ARRIVE HERE");
        console.log("Query is " + JSON.stringify(query));
  
        const Variant = db.collection(importCollection3);
        filteredVariants = await Variant.find(query).toArray();
        //console.log("Filtered Variants are " + JSON.stringify(filteredVariants));
      }
  
      const processId = data.processId;
      //CHECK IF DATA DISAPEARS
      const Result = db.collection(resultSV);
     
      const fieldsToKeep = [
        '_id', 'svid', 'sv_type', 'svtool', 'vcf_chr', 'start_pos',
        'stop_pos', 'end_vcf_chr','sv_len', 'filter', 'gt', 'ACMG_class', 'AnnotSV_ranking_criteria',
        'ENCODE_blacklist_left', 'ENCODE_blacklist_right', 'GnomAD_pLI', 'Location', 'Location2',
        'OMIM_ID', 'SegDup_left', 'SegDup_right', 'TAD_coordinate', 'exon_count', 'gene_name',
        're_gene', 'repeat_type_left', 'repeat_type_right', 'tx'
      ];
  
      // Filter the fields of the variants
      const filterFields = (variant) => {
        const filteredVariant = {};
        fieldsToKeep.forEach(field => {
          if (variant.hasOwnProperty(field)) {
            filteredVariant[field] = variant[field];
          }
        });
        return filteredVariant;
      };

      const chunkSize = 5000; // Adjust chunk size as needed
       // Adjust chunk size as needed
      for (let i = 0; i < filteredVariants.length; i += chunkSize) {
        const chunk = filteredVariants.slice(i, i + chunkSize).map(filterFields);
        await Result.insertOne({
          process_id: processId,
          status: 'computing',
          filtered_variants: chunk,
          createdAt: new Date()
        });
      }

      await Result.updateMany(
        { process_id: processId },
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


  async function validateInput(db, data) {
    const Variant = db.collection(importCollection3);
    const Population = db.collection(populationSVCollection);
    //console.log("Data is " + JSON.stringify(data));
    // Check for fileID
    if (data.fileID) {
      const fileIDCount = await Variant.countDocuments({ fileID: data.fileID });
      if (fileIDCount === 0) {
        return { error: `File ID ${data.fileID} does not exist` };
      }
      return { useCase: 'fileID' };  // Valid fileID
    } else if (data.TrioLocalID) { // Check for TrioLocalID
        
        if (!data.trio_filter_vector) {
          return { error: 'trio_filter_vector must be provided when TrioLocalID is specified' };
        }
        if (data.sampleID) {
          return { error: 'sampleID must not be provided when TrioLocalID and trio_filter_vector are specified' };
        }
        const trioLocalIDQuery = {};
        trioLocalIDQuery[data.TrioLocalID] = { $exists: true };
        console.log("TrioLocalID Query is " + JSON.stringify(trioLocalIDQuery));
        const trioLocalIDCount = await Variant.countDocuments(trioLocalIDQuery);
        console.log("we arrived to count")
        if (trioLocalIDCount === 0) {
          return { error: `TrioLocalID ${data.TrioLocalID} does not exist` };
        }
        return { useCase: 'TrioLocalID' };  // Valid TrioLocalID
    } else if (data.PopulationID) { // Check for PopulationID
        console.log("do we enter population")
        if (!data.fileID_to_filter) {
          return { error: 'fileID_to_filter must be provided when PopulationID is specified' };
        }
        const populationIDCount = await Population.countDocuments({ _id: data.PopulationID });
        if (populationIDCount === 0) {
          return { error: `PopulationID ${data.PopulationID} does not exist` };
        }
        return { useCase: 'PopulationID' };  // Valid PopulationID
    } else {
      // If none of the IDs are provided
      return { error: 'At least one of fileID, TrioLocalID, or PopulationID must be provided' };
    }
  }
  

function buildQuery(data,useCase) {
  const query = {};

   // Use case specific additions
   if (useCase === 'fileID') {
    query.fileID = data.fileID;
  } else if (useCase === 'TrioLocalID') {
    query[data.TrioLocalID] = data.trio_filter_vector; // Set dynamic field value
  }

  // chr goes from 1 to 22 and we have X and Y, X we cast to int 23 and Y we cast to 24
  if (data.chr) {
    let chr = data.chr.toUpperCase();
    if (chr === 'X') {
      chr = 23;
    } else if (chr === 'Y') {
      chr = 24;
    } else {
      chr = parseInt(chr, 10);
    }
    query.chr = chr;
  }

  // sv_type filter
  if (data.sv_type) {
    query.sv_type = data.sv_type;
  }

  // start_pos_range indicates the start_pos range that has to be filtered for variants
  if (data.start_pos_range && Array.isArray(data.start_pos_range) && data.start_pos_range.length === 2) {
    query.start_pos = { $gte: data.start_pos_range[0], $lte: data.start_pos_range[1] };
  }

  // sv_len can be ">Number" or "<Number" and variants have to be filtered based on minor or major sign and number
  if (data.sv_len) {
    if (data.sv_len.startsWith('>')) {
      const len = parseInt(data.sv_len.substring(1), 10);
      query.sv_len = { $gte: len };
    } else if (data.sv_len.startsWith('<')) {
      const len = parseInt(data.sv_len.substring(1), 10);
      query.sv_len = { $lt: len };
    }
  }

  // filter field
  if (data.filter) {
    query.filter = data.filter;
  }

  // gt field
  if (data.gt) {
    query.gt = data.gt;
  }

  // gene_name can be or true or gene name
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

async function handlePopulationID(db, data) {
  try {
    const Population = db.collection(populationSVCollection);
    const Variant = db.collection(importCollection3);

    // Extract the sampleId of the proband
    const probandSampleId = data.fileID_to_filter;

    // Retrieve the population entry
    const populationEntry = await Population.findOne({ _id: data.PopulationID });

    if (!populationEntry) {
      throw new Error(`PopulationID ${data.PopulationID} does not exist`);
    }

    // Extract all samples from the population
    const populationSamples = populationEntry.individualsWithSamples;

    // Find variants for the proband sample
    const probandVariants = await Variant.find({ fileID: Number(probandSampleId) }).toArray();

    // Iterate over each sample in the population and compute the intersection
    let filteredVariants = [...probandVariants];

    for (const sample of populationSamples) {
      if (sample.fileID !== probandSampleId) {
        const sampleVariants = await Variant.find({ fileID: Number(sample.fileID) }).toArray();
        filteredVariants = computeIntersection(filteredVariants, sampleVariants);
      }
    }

    // Return the filtered variant _id's that are unique to the proband
    const uniqueVariants = filteredVariants.map(variant => variant._id);
    //console.log(uniqueVariants);
    return uniqueVariants;
  } catch (err) {
    console.log(err);
    throw err;
  }
}

function computeIntersection(probandVariants, sampleVariants) {
  const intersection = [];

  // Combine both proband and sample variants into a single array
  const mergedVariants = [...probandVariants, ...sampleVariants];

  for (const variant of mergedVariants) {
    if (variant.sv_len > 50) {
      matchSVCandidate(variant, intersection);
    }
  }

  // Filter out variants that are present in both sets
  const uniqueVariants = intersection.filter(group => group.count === 1 && group.files.includes(probandVariants[0].fileID));

  return uniqueVariants.map(group => ({
    _id: group.ids[0],
    chr: group.chr,
    sv_type: group.sv_type,
    start_pos: group.startposition,
    sv_len: group.svlenght,
    fileID: group.files[0]
  }));
}

function matchSVCandidate(candidate, groups, M = 500) {
  const { _id, chr, sv_type, start_pos, sv_len, fileID } = candidate;


  if (groups.length === 0) {
    // Create a new SV group
    const newGroup = {
      ids: [_id],
      chr,
      sv_type,
      svlenght: sv_len,
      startposition: start_pos,
      count: 1,
      files: [fileID],
    };
    groups.push(newGroup);
    return;
  }

  let matchedGroup = null;
  let smallestDeviation = Infinity;

  for (const group of groups) {
    const { svlenght: groupSVLength, startposition: groupStartPosition, files } = group;

    const deviationMetric = (Math.abs(start_pos + sv_len - (groupStartPosition + groupSVLength)) < M * Math.sqrt(Math.min(sv_len, groupSVLength)));

    if (sv_type === group.sv_type && chr === group.chr && deviationMetric) {
      const tmpDevMatric = (Math.abs(start_pos + sv_len - (groupStartPosition + groupSVLength)));
      if (tmpDevMatric < smallestDeviation) {
        smallestDeviation = tmpDevMatric;
        matchedGroup = group;
      }
    }
  }

  if (matchedGroup) {
    // Update the coordinates and file information of the selected group
    const { ids, count, svlenght, startposition, files } = matchedGroup;
    const newCount = count + 1;
    matchedGroup.svlenght = (svlenght * count + sv_len) / newCount;
    matchedGroup.startposition = (startposition * count + start_pos) / newCount;
    matchedGroup.count = newCount;
    // Check if the fileID already exists in the files array
    if (!files.includes(fileID)) {
      files.push(fileID);
    }
    if (!ids.includes(_id)) {
      ids.push(_id);
    }
  } else {
    // Create a new SV group
    const newGroup = {
      ids: [_id],
      chr,
      sv_type,
      svlenght: sv_len,
      startposition: start_pos,
      count: 1,
      files: [fileID],
    };
    groups.push(newGroup);
  }
}


function buildPopulationQuery(data) {
  const query = {};
  query.fileID = data.fileID_to_filter;

  // chr goes from 1 to 22 and we have X and Y, X we cast to int 23 and Y we cast to 24
  if (data.chr) {
    let chr = data.chr.toUpperCase();
    if (chr === 'X') {
      chr = 23;
    } else if (chr === 'Y') {
      chr = 24;
    } else {
      chr = parseInt(chr, 10);
    }
    query.chr = chr;
  }

  // sv_type filter
  if (data.sv_type) {
    query.sv_type = data.sv_type;
  }

  // start_pos_range indicates the start_pos range that has to be filtered for variants
  if (data.start_pos_range && Array.isArray(data.start_pos_range) && data.start_pos_range.length === 2) {
    query.start_pos = { $gte: data.start_pos_range[0], $lte: data.start_pos_range[1] };
  }

  // sv_len can be ">Number" or "<Number" and variants have to be filtered based on minor or major sign and number
  if (data.sv_len) {
    if (data.sv_len.startsWith('>')) {
      const len = parseInt(data.sv_len.substring(1), 10);
      query.sv_len = { $gte: len };
    } else if (data.sv_len.startsWith('<')) {
      const len = parseInt(data.sv_len.substring(1), 10);
      query.sv_len = { $lt: len };
    }
  }

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

async function filterPopulationResults_old(db, initialResults, query) {
  const Variant = db.collection(importCollection3);

  // Retrieve variants matching the initial results IDs
  const variants = await Variant.find({ _id: { $in: initialResults } }).toArray();
  //console.log("Variants are " + JSON.stringify(variants));
  // Apply the query to filter these variants
  const filteredVariants = variants.filter(variant => {
    for (const key in query) {
      if (query.hasOwnProperty(key) && variant[key] !== query[key]) {
        return false;
      }
    }
    return true;
  });


  return filteredVariants;
}

async function filterPopulationResults(db, initialResults, query, fileID) {
  const Variant = db.collection(importCollection3);

  // Retrieve variants matching the initial results IDs
  const initialVariants = await Variant.find({ _id: { $in: initialResults }, fileID: fileID }).toArray();

  // Retrieve the variants that match the updated query
  const queryVariants = await Variant.find(query).toArray();

  // Intersect the initially retrieved variants with those that matched the query
  const finalVariants = initialVariants.filter(variant => 
    queryVariants.some(qVariant => qVariant._id.toString() === variant._id.toString())
  );

  return finalVariants;
}