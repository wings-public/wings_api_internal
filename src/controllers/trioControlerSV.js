#!/usr/bin/env node
'use strict';
const spawn  = require('child_process');
//const runningProcess = require('is-running');
var path = require('path');
const promisify = require('util').promisify;
const { createReadStream, createWriteStream, stat ,unlink,existsSync} = require('fs');
var stats = promisify(stat);
const argParser = require('commander');
const {logger} = require('../controllers/loggerMod');
const configData = require('../config/config.js');
const { app:{instance,logLoc}, db:{sampleAssemblyCollection,familyCollection,importCollection3,trioCollection} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const trioQueueScraper = require('../routes/queueScrapper').SVtrioQueueScrapper;
var familyFormat = require('../config/familyConf.json');
const { update } = require('lodash');
//var db = require('../controllers/db.js');
(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-r, --request_json <request_json>', 'json request with trio sample details')
            .option('-p, --pid <pid>', 'pid')
        argParser.parse(process.argv);

        var trioReq1 = argParser.request_json;
        var pid = argParser.pid;

        if ( argParser.request_json == undefined ) {
            argParser.outputHelp();
            process.exit(1);
        }

        var month = new Date().getMonth();
        var year = new Date().getFullYear();
        var date1 = new Date().getDate()+'-'+month+'-'+year;
        var logFile = `trio-controller-SV-${pid}-${date1}.log`;
        //var logFile = loggerEnv(instance,logFile1);
        var trioCont = logger('trio',logFile);

        //console.log("************* TRIO CONTROLLER ");
        //console.dir(trioReq,{"depth":null});

        var db = require('../controllers/db.js');
        var queryCollection = "";
        //console.dir(trioReq,{"depth":null});

        var trioReq = JSON.parse(trioReq1);
        trioCont.debug(trioReq);

        // fetch details of trio using family ID

        var trioColl = db.collection(trioCollection);

        var familyId = parseInt(trioReq['family_id']);
        var localID = trioReq['localID'];
        await trioColl.updateOne({'TrioLocalID':localID,'TrioStatus':{$ne:'disabled'}},{$set : {'TrioStatus':'ongoing','TrioErrMsg':''}});

        if ( (trioReq['assembly_type'] == 'GRCh38')  || (trioReq['assembly_type'] == 'hg38') ) {
            queryCollection = importCollection3;
        }
        trioCont.debug("queryCollection is "+queryCollection);
        var queryCollObj = db.collection(queryCollection); 

        trioCont.debug("Calling scanTrioSample");
        
        var familyColl = db.collection(familyCollection);
        //await familyColl.updateOne({'_id':localID,'TrioStatus':{$ne:'disabled'}},{$set : {'TrioStatus' : 'ongoing', 'TrioErrMsg' : ""}});
        var result = await scanTrioSample(trioReq,trioColl,queryCollObj,trioCont);
        var stop_time = new Date();
        //console.log("Trio Controller stop time is "+stop_time);
        trioCont.debug("Result Done. Now Await timeout");
        // commenting debug timeout
        //await new Promise(resolve => setTimeout(resolve, 200000));
        //console.log("Timeout completed");

        trioCont.debug("Completed SV trio controller sample logic");
        process.exit(0);
    } catch(err) {
        console.log(err);
        //throw err;
        process.exit(1);
    }
}) ();

async function generateCodes(n){
    var states = [];
  
    // Convert to decimal
    var maxDecimal = parseInt("1".repeat(n),2);
    //console.log("maxDecimal is "+maxDecimal);
  
    // For every number between 1->decimal : we do not need code 000
    for(var i = 1; i <= maxDecimal; i++) {
      // Convert to binary, pad with 0, and add to final results
      states.push(i.toString(2).padStart(n,'0'));
    }
  
    return states;
  }

  
async function scanTrioSample(reqJson,trioColl,queryCollObj,trioCont) {
    try {
        
        var relations = Object.keys(reqJson['family']); // keys:Proband Father Mother
        var samples = Object.values(reqJson['family']); // values:samples
        var trioCodeHash = {};

        var localID = reqJson['localID'];
        trioCont.debug("Relations are "+relations);
        trioCont.debug("Samples are "+samples);
        
        // order in which the trio code is plotted. 111/011/101
        var order = familyFormat['trio']['order'];
        var maxCodes = await generateCodes(relations.length);
        for ( var idx in maxCodes ) {
            var c1 = maxCodes[idx];
            trioCodeHash[c1] = 0;
        }

        //console.log(trioCodeHash);

        var sampleCodes = familyFormat['trio']['sample_codes'];
        var sampleCodeKeys = Object.keys(sampleCodes);
        var proband_sampleId;
        var father_sampleId;
        var mother_sampleId;
        // trioCodeHash is populated with the trio sample counts
        //console.log("Time taken for Sample Count Start *********"+new Date());
        for (let i = 0; i < sampleCodeKeys.length ; i++ ) {
            var relation1 = sampleCodeKeys[i];
            trioCont.debug("Relation is "+relation1);
            var sampId = reqJson['family'][relation1];
            var trioCode = sampleCodes[relation1];
            trioCont.debug("Trio code is "+trioCode);
            trioCont.debug("Samp Id is "+sampId);
            var sampCount = await getSampleCount(queryCollObj,sampId);
            trioCont.debug(`Sample count for ${sampId} is ${sampCount}`);
            // unset trio_code to ensure there are no old traces of trio code for hom-ref variants
            trioCont.debug("Reset trio_code for hom-ref variants in sample  "+sampId);
            await queryCollObj.updateMany({'fileID':sampId},{$unset : {'trio_code':1}});
            trioCodeHash[trioCode] = sampCount;
            if (relation1 === 'Proband') {
                proband_sampleId = sampId;
                
            } else if (relation1 === 'Father') {
                father_sampleId = sampId;
                
            } else if (relation1 === 'Mother') {
                mother_sampleId = sampId;
            }
        }
        
        /*console.log("Time taken for Sample Count Stop *********"+new Date());
        console.log(proband_sampleId);
        console.log(father_sampleId);
        console.log(mother_sampleId);*/
        trioCont.debug(trioCodeHash);
     
        //var order = ['Proband','Father','Mother'];

        trioCont.debug("Trio code plot order is "+order);
        var sampleOrder = [];
        // traverse the order array. Example : Proband,Father,Mother
        for ( var idx1 in order ) {
            // fetch the familyType from order array
            var familyType = order[idx1];
            if ( reqJson['family'][familyType]) {
                // fetch the 'sid' corresponding to the family member type.
                // 1000001(Proband),1000002(Father),1000003(Mother)
                sampleOrder.push(reqJson['family'][familyType]);
            }
        }

        trioCont.debug(sampleOrder);

        var defaultSort = {'var_key':1};
        // hom-ref variants are excluded from trio analysis
        var filter = {'fileID':{$in:samples}};
        var familyCursor = await queryCollObj.find(filter);
        familyCursor.sort(defaultSort);
        
        familyCursor.project({'var_key':1,'fileID':1}); 
        
        
        
        console.log("Time taken for actual trio processing Start #############"+new Date());


        trioCont.debug(trioCodeHash);

         

        var updateFilter = await computeIntersection(proband_sampleId,father_sampleId,mother_sampleId);
        //console.log(updateFilter);
        //console.log("we returned");
        //console.log(updateFilter);
        // Assuming you have already defined proband_sampleId, father_sampleId, and mother_sampleId
        const bulkOps = [];
        //var updateObject = {};
        //updateObject[trioFieldName] = trioVector;
        // $set: { trio_sv_vector: trioVector }
        //console.log(proband_sampleId);
        updateFilter.forEach(item => {
            const trioVector = [
                item.files.includes(proband_sampleId) ? '1' : '0',
                item.files.includes(father_sampleId) ? '1' : '0',
                item.files.includes(mother_sampleId) ? '1' : '0'
            ].join('');
            //console.log(item.files);
            let updateObject = {}; 
            updateObject[localID] = trioVector;
            bulkOps.push({
                updateMany: {
                    filter: { _id: { $in: item.ids } },
                    update: {
                        $set: updateObject
                    }
                }
            });
        });
        //console.log(bulkOps);
        //console.log(localID);
        // Execute bulk write operation
        if (bulkOps.length > 0) {
            queryCollObj.bulkWrite(bulkOps, { 'ordered': false })
                .then(function (res) {
                    trioCont.debug("Executed bulkWrite and the results are ");
                    trioCont.debug(res.modifiedCount);
                })
                .catch((err1) => {
                    trioCont.debug("Error executing the bulk operations");
                    trioCont.debug(err1);
                });
        }
        await trioColl.updateOne({'TrioLocalID':localID,'TrioStatus':{$ne:'disabled'}},{$set : {'TrioCode' : trioCodeHash}});


        return "success";
    } catch(err) {
        console.log(err);
        throw err;
    }
}

async function computeIntersection(fileID_proband, fileID_father, fileID_mother) {
    var db = require('../controllers/db.js');
  
    try {
        const fileID1Number = Number(fileID_proband);
        const fileID2Number = Number(fileID_father);
        const fileID3Number = Number(fileID_mother)
        
        var collection = db.collection(importCollection3);
  
        const variants1 = await collection.find({ fileID: fileID1Number }).toArray();
        const variants2 = await collection.find({ fileID: fileID2Number }).toArray();
        const variants3 = await collection.find({ fileID: fileID3Number }).toArray();
  
        let mergedVariants = [...variants1, ...variants2, ...variants3]; // Combine all variants into a single array
  
        const intersection = [];
  
        for (const variant of mergedVariants) {
            const { _id,chr, start_pos, stop_pos, sv_type } = variant;
            if (variant.sv_len > 50){
                //matchSVCandidate_new(variant,intersection)
                // switched to sniffles
                matchSVCandidate(variant,intersection)
            }
        
        }
  
      
        //console.log(intersection);
        return intersection;
   
    } catch(err) {
        console.log(err);
        throw err;
    }
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
      const { svlenght: groupSVLength, startposition: groupStartPosition, count, files } = group;
  
      const deviationMetric = (Math.abs(start_pos + sv_len - (groupStartPosition + groupSVLength )) < M * Math.sqrt(Math.min(sv_len, groupSVLength)));
  
      if (sv_type === group.sv_type && chr === group.chr && deviationMetric  ) {
        const tmpDevMatric = (Math.abs(start_pos + sv_len - (groupStartPosition + groupSVLength )))
        if ( tmpDevMatric <  smallestDeviation){
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

  function matchSVCandidate_new_old(candidate, groups, M = 500) {
    const { _id, chr, sv_type, start_pos, sv_len, fileID } = candidate;
    
    
    //try {
    //    sv_len = parseInt(sv_len);
    //} catch (err) {
    //    sv_len = 0;  // Default length for unknown or invalid length
    //}

    const createNewGroup = (candidate) => {
      const { _id, chr, sv_type, start_pos, sv_len, fileID } = candidate;
      return {
        ids: [_id],
        chr,
        sv_type,
        svlenght: sv_len,
        startposition: start_pos,
        count: 1,
        files: [fileID],
      };
    };
  
    if (groups.length === 0) {
      groups.push(createNewGroup(candidate));
      return;
    }
  
    let matchedGroup = null;
    let smallestCombinedMetric = Infinity;
  
    for (const group of groups) {
      const { svlenght: groupSVLength, startposition: groupStartPosition } = group;
  
      const candidateEndPos = start_pos + sv_len;
      const groupEndPos = groupStartPosition + groupSVLength;
      const overlap = Math.max(0, Math.min(candidateEndPos, groupEndPos) - Math.max(start_pos, groupStartPosition));
      const positionDeviation = Math.abs(start_pos - groupStartPosition);
      const lengthDeviation = Math.abs(sv_len - groupSVLength);
  
      // Combined metric with different weights for each component
      const combinedMetric = positionDeviation * 0.4 + lengthDeviation * 0.4 - overlap * 0.2;
  
      if (sv_type === group.sv_type && chr === group.chr && combinedMetric < M) {
        if (combinedMetric < smallestCombinedMetric) {
          smallestCombinedMetric = combinedMetric;
          matchedGroup = group;
        }
      }
    }
  
    if (matchedGroup) {
      const { ids, count, svlenght, startposition, files } = matchedGroup;
      const newCount = count + 1;
      const weight = sv_len / (sv_len + svlenght);
  
      matchedGroup.svlenght = (svlenght * count + sv_len) / newCount;
      matchedGroup.startposition = (startposition * count + start_pos) / newCount;
      matchedGroup.count = newCount;
  
      if (!files.includes(fileID)) files.push(fileID);
      if (!ids.includes(_id)) ids.push(_id);
    } else {
      groups.push(createNewGroup(candidate));
    }
  }
  
  function matchSVCandidate_new(candidate, groups, baseM = 500) {
    const { _id, chr, sv_type, start_pos, sv_len, fileID } = candidate;
  
    const createNewGroup = (candidate) => {
      const { _id, chr, sv_type, start_pos, sv_len, fileID } = candidate;
      return {
        ids: [_id],
        chr,
        sv_type,
        svlenght: sv_len,
        startposition: start_pos,
        count: 1,
        files: [fileID],
      };
    };
  
    if (groups.length === 0) {
      groups.push(createNewGroup(candidate));
      return;
    }
  
    let matchedGroup = null;
    let smallestCombinedMetric = Infinity;
  
    for (const group of groups) {
      const { svlenght: groupSVLength, startposition: groupStartPosition } = group;
  
      const candidateEndPos = start_pos + sv_len;
      const groupEndPos = groupStartPosition + groupSVLength;
      const overlap = Math.max(0, Math.min(candidateEndPos, groupEndPos) - Math.max(start_pos, groupStartPosition));
      const positionDeviation = Math.abs(start_pos - groupStartPosition);
      const lengthDeviation = Math.abs(sv_len - groupSVLength);
  
      // Dynamic threshold M based on the size of the SV
      const dynamicM = baseM * Math.sqrt(Math.min(sv_len+1, groupSVLength));
  
      // Combined metric with different weights for each component
      const combinedMetric = positionDeviation * 0.4 + lengthDeviation * 0.4 - overlap * 0.2;
  
      if (sv_type === group.sv_type && chr === group.chr && combinedMetric < dynamicM) {
        if (combinedMetric < smallestCombinedMetric) {
          smallestCombinedMetric = combinedMetric;
          matchedGroup = group;
        }
      }
    }
  
    if (matchedGroup) {
      const { ids, count, svlenght, startposition, files } = matchedGroup;
      const newCount = count + 1;
  
      matchedGroup.svlenght = (svlenght * count + sv_len) / newCount;
      matchedGroup.startposition = (startposition * count + start_pos) / newCount;
      matchedGroup.count = newCount;
  
      if (!files.includes(fileID)) files.push(fileID);
      if (!ids.includes(_id)) ids.push(_id);
    } else {
      groups.push(createNewGroup(candidate));
    }
  }
  
async function getSampleCount(queryCollObj,sid) {
    try {
        sid = parseInt(sid);
        //console.log("Sample ID "+sid);
        // ignore hom-ref variants. only heterozygous, hom-alt variants are considered for counts.
        var sampCnt = await queryCollObj.find({'fileID':sid}).count();
        //console.log("Sample Count is "+sampCnt);
        return sampCnt;
    } catch(err) {
        throw err;
    }
}