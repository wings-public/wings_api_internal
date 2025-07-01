#!/usr/bin/env node
'use strict';


const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
const argParser = require('commander');
const colors = require('colors');
var urlLibraries = {
    "http:": require('http'),
    "https:": require('https')
};
// Promisify the fs.unlink function
//const http = require('http');
//const https = require('https');

const stream = require('stream');
const util = require('util');
var url = require('url');
const unlink = util.promisify(fs.unlink);
const pipeline = util.promisify(stream.pipeline);

//const mongoLogger = require('mongodb').Logger;

const { createLogger, format, transports } = require('winston');
const configData = require('../config/config.js');
const { db : {dbName,importCollection3,importStatsCollection,sampleSheetCollection,SVdockerImage, annotationDirSV}, app:{instance,logLoc} } = configData;
//console.log("Check the config data "+variantAnnoCollection);
const dockerImage = SVdockerImage; // Replace with the name of your Docker image
const annotationDir = annotationDirSV; // Replace with the path to your annotation data

var annoVar = process.env.ANNO_VAR;
var vcfVar = process.env.VCF_VAR;
var logVar = process.env.LOG_VAR;

const annotatioOutPath = path.join(logVar, 'svAnnot');
var loggerMod = require('../controllers/loggerMod');
var bson = require("bson");
var BSON = new bson.BSON();

var importCollection;
var db = require('../controllers/db.js');
var collection;
//const collection = db.collection(variantAnnoCollection);
//console.log("Collection is "+collection);

const createColIndex = require('../controllers/dbFuncs.js').createColIndex;

(async function () {
    argParser
        .version('0.1.0')
        .option('-s, --sample <vcf1>', 'sample_file or sample_url')
        .option('-vcf, --vcf_file_id <id1,id2,id3|id4>', 'commma separated vcf file ID. Pipe separated ID for multisample files')
        .option('--assembly, --assembly <GRCh38>', 'assembly type to decide the import collection to be used for import')
        .option('-p, --pid <pid>', 'id of the created request')
    argParser.parse(process.argv);

    // vcfformat UG/HC will be supported 
    // haplotype caller vcf format 

    /*if ( (! argParser.sample && ! argParser.vcf_file_id) || ( ! argParser.url || ! argParser.vcf_file_id) ) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }*/

    if ((!argParser.sample) || (!argParser.vcf_file_id) || (!argParser.assembly)) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }

    // Haplotype call format only supported now
    var vcfFormat = 'HC';
    var sample = argParser.sample;
    var vcfId = argParser.vcf_file_id;
    var assemblyType = argParser.assembly;
    var updateId = argParser.pid || '';



    var logFile = `annotationSampleSV-${vcfId}-${process.pid}.log`;
    var statsLogFile = `annotationStatsSampleSV-${vcfId}-${process.pid}.log`;

    //const filename = path.join(logDir, 'results.log');
    //var filename;

    // To be added to a separate library //////
    /*if ( instance == 'dev') {
        // Create the log directory if it does not exist
        const logDir = 'log';
        var tmpPath = path.parse(__dirname).dir;
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir);
        }
        filename = path.join(tmpPath,'import',logDir, logFile);
    } else {
        // env ( uat, prod ) will be using docker instance. Logs has to be created in the corresponding bind volumes defined in the docker-compose environment file
        // importLogPath will be defined in docker-compose environment variables section 
        filename = path.join(process.env.importLogPath,logFile);
   }*/

   console.log("******** Log File name is "+logFile);
   var createLog = loggerMod.logger('import',logFile);
   var statsLog = loggerMod.logger('import',statsLogFile);

   statsLog.verbose(`Sample : ${sample}`);
   statsLog.verbose(`Sample ID : ${vcfId}`);
   statsLog.verbose(`Assembly type : ${assemblyType}`);

   /////////////////////// Winston Logger ///////////////////////////// 


   //// Validate VCF File, ID and also ID for Multi-Sample VCF Files ///
   // Commenting Validation as we will not be supporting milti-sample VCF in this version
   // This will be added in upcoming versions.
   //validate(vcfFile, vcfFileId);

   // Check and create Index in VCF Collection and Annotation Collection
   //console.log("do we pass here");
   importCollection = importCollection3;
   //console.log("do yes");

   /*try {
       await createColIndex(db, importCollection,{"gene_annotations.gene_id" : 1});
   } catch (e) {
       console.log("gene_id index already exists. Proceed further"+e);
   }

   try {
       await createColIndex(db, variantAnnoCollection,{"annotated":1});
   } catch (e) {
        console.log("annotated index already exists. Proceed further"+e);
    }
    */
    //console.log("arriver here");
    var start = new Date();
    createLog.debug("Annotation Sample Program Started at "+start);
    createLog.debug(sample);
    createLog.debug(vcfId);
    createLog.debug(`sample ${sample} vcfId ${vcfId}`);
    try {
        if ( vcfFormat == "HC") {
            createLog.debug("Retrieving file for annotation");

            var urlReg = /^http/g;

            var filepath;
            if ( sample.match(urlReg) ) {
                createLog.debug(`calling retrieveFilePath for sample ${sample} VCF ${vcfId}`);
                filepath = await retrieveFilePath(sample,vcfId,createLog);
            } else {
                filepath = sample;
            }
            //console.log("Filepath is "+filepath);
            var val = await annotateVcfFile(filepath, dockerImage, annotationDir);
            var stop = new Date();
            var finish = await printTsvFileRowByRow(val,vcfId,createLog,db);
            //console.log("What was the return value sent by the above function ?");
            createLog.debug("Annotation Sample Program Completed at "+stop);
            if ( finish == "Success") {
                await deleteFiles(annotatioOutPath, val);
                createLog.debug("Received value "+finish);
                createLog.debug("*********** Right time to close the connection ");
                createLog.debug(" ************ TRACK CHECK & CLOSE ************ ");
                process.exit(0);
            }
        }
    } catch(err) {
        console.log("Catch Error is "+err);
        var errMsg = 'Annotation Sample Stage-failed due to path retrieve or parse errors '+err;
        var status = {'status_description' : 'Error','status_id':9, 'error_info':errMsg, 'finish_time': new Date()};
        createLog.debug('updateId is '+updateId);
        var pushSt = {'status_log': {status:"Error",log_time: new Date()}};
        if ( updateId ) {
            updateId = parseInt(updateId);
        }
        await updateStatus(db,updateId,status,pushSt);
        await updateSampleSheetStat(db,vcfId,"annotation failed");
        createLog.debug("Error resolving promise "+err);
        createLog.debug(`${err}`);
        createLog.debug("Exit spawned process ");
        await new Promise(resolve => setTimeout(resolve, 10000));
        /*createLog.on('finish', function (debug) {
            console.log("All debug log messages has now been logged");
        });*/
        console.log("Error here ********");
        console.log(err);
        process.exit(1);
    }

})();

function annotateVcfFile(filePath, dockerImage, annotationDir) {
    return new Promise((resolve, reject) => {
        const fileName = path.basename(filePath); // Extract filename from filePath
        const outputFile = `${fileName.replace(/\.vcf(\.gz)?$/, '')}.tsv`; // Construct output filename with .tsv extension
        const command = 'docker';
        const isGzipped = filePath.endsWith('.vcf.gz');


        const args = [
            'run',
            '-v', `${annoVar}:/AnnotSV_annotations`,
            '-v', `${vcfVar}:/input`, // Mount the directory containing the input file
            '-v', `${annotatioOutPath}:/data`,
            dockerImage,
            'AnnotSV',
            '-annotationsDir', '/AnnotSV_annotations',
            '-annotationMode', 'full',
            '-SVinputFile', isGzipped ? '/input/' + fileName : '/input/' + fileName,
            '-outputFile', `/data/${outputFile}` // Use outputFile as the output filename
        ];

        //console.log("Executing herer");
        //console.log(args);

        const child = spawn(command, args);

        child.stderr.on('data', (data) => {
            console.error(`Docker error: ${data}`);
        });

        child.on('error', (error) => {
            console.error(`Spawn error: ${error}`);
            reject(error);
        });
        /*child.on('exit', (code) => {
            if (code !== 0) {
                reject(new Error(`Annotation tool exited with code ${code}`));
            } else {
                resolve(outputFile); // Resolve with the output filename
            }
        }); */
        child.on('exit', (code) => {
            if (code !== 0) {
                reject(new Error(`Annotation tool exited with code ${code}`));
            } else {
                const outfileName=path.basename(outputFile);
                const origPath = path.dirname(filePath);
                //const returnPath = path.join(origPath,outfileName);
                //const returnPath = path.join(annotatioOutPath,outfileName);
                const returnPath = path.join('/logger/svAnnot',outfileName);
                resolve(returnPath); // Resolve with the output filename
            }
        });
    });
}

//add try catch block
async function printTsvFileRowByRow(fileName,vcfId,createLog,dbImport) {
    //console.log(fileName)
    var imprtUpdateAnnotation = dbImport.collection(importCollection3);
    importCollection = importCollection3;
    //const filePath = path.join(annotatioOutPath, fileName);
    const filePath = fileName;
    const fileStream = fs.createReadStream(filePath);

    createLog.debug("Function printTsvFileRowByRow called for parsing ----- " + fileName);
    createLog.debug("Sample ID ----- " + vcfId);
    var basePath = path.parse(__dirname).dir;
    var jsonP = path.join(basePath,'config','chrMap.json');

    var mapJson = fs.readFileSync(jsonP);
    var chrData = JSON.parse(mapJson);
    var chrMap = chrData['chromosomes'];

    var sId = parseInt(vcfId);
    var pid = process.pid;
    //var vcfIdList = vcfId.split('|');
    createLog.debug("Process ID is " + pid);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let isFirstLine = true;
    let headers = [];
    for await (const line of rl) {
        if (isFirstLine) {
            headers = line.split('\t');
            isFirstLine = false;
        } else {
            //console.log(line);
            var data = line.split('\t');
            var fieldVal = {};

            for (var idx in headers) {
                var field = headers[idx];
                // chromosome field ----------------
                if (idx == 1) {

                    var re = /chr/g;
                    var chr = data[idx];
                    var vcfChr = data[idx];
                    if (chr.match(re)) {
                        chr = data[idx].replace(re, '');

                    }

                    if ( chrMap[chr]) {
                        chr = chrMap[chr];
                    }

                    data[idx] = chr;
                    fieldVal['vcf_chr'] = vcfChr;

                } // Chr Field 

                //console.log("Value is------------------ "+field);
                fieldVal[field] = data[idx];
            }
            var uniqueKey = sId + '-' + fieldVal['SV_chrom'] + '-' + fieldVal['SV_start'] + '-' + fieldVal['SV_end'] + '-' + fieldVal['ID'] + '-' + fieldVal['SV_type'] + '-' + Math.abs(parseInt(fieldVal['SV_length']))  ;
            //console.log(uniqueKey)
            const updateData = {
                // Map field names to corresponding values from fieldVal, handling empty or missing values
                gene_name: fieldVal['Gene_name'] || '', // Assign empty string if gene_name is missing
                tx: fieldVal['Tx'] || '', // Assign empty string if tx is missing
                exon_count: !isNaN(parseInt(fieldVal['exon_count'])) ? parseInt(fieldVal['exon_count']) : null, // Check if exon_count is a valid number
                Location: fieldVal['Location'] || '', // Assign empty string if Location is missing
                Location2: fieldVal['Location2'] || '', // Assign empty string if Location2 is missing
                re_gene: fieldVal['RE_gene'] || '', // Assign empty string if re_gene is missing
                TAD_coordinate: fieldVal['TAD_coordinate'] || '', // Assign empty string if TAD_coordinate is missing
                repeat_type_left: fieldVal['Repeat_type_left'] || '', // Assign empty string if repeat_type_left is missing
                repeat_type_right: fieldVal['Repeat_type_right'] || '', // Assign empty string if repeat_type_right is missing
                SegDup_left: fieldVal['SegDup_left'] || '', // Assign empty string if SegDup_left is missing
                SegDup_right: fieldVal['SegDup_right'] || '', // Assign empty string if SegDup_right is missing
                ENCODE_blacklist_left: fieldVal['ENCODE_blacklist_left'] || '', // Assign empty string if ENCODE_blacklist_left is missing
                ENCODE_blacklist_right: fieldVal['ENCODE_blacklist_right'] || '', // Assign empty string if ENCODE_blacklist_right is missing
                OMIM_ID: fieldVal['OMIM_ID'] || '', // Assign empty string if OMIM_ID is missing
                GnomAD_pLI: !isNaN(parseFloat(fieldVal['GnomAD_pLI'])) ? parseFloat(fieldVal['GnomAD_pLI']) : null, // Check if GnomAD_pLI is a valid number
                AnnotSV_ranking_criteria: fieldVal['AnnotSV_ranking_criteria'] || '', // Assign empty string if AnnotSV_ranking_criteria is missing
                ACMG_class: !isNaN(parseInt(fieldVal['ACMG_class'])) ? parseInt(fieldVal['ACMG_class']) : null // Check if ACMG_class is a valid number
            };

            var filter = { _id: uniqueKey };

            var updateDoc = { $set: updateData };
            const result = await imprtUpdateAnnotation.updateOne(filter, updateDoc);

        }

    }
    return "Success";
    //console.log(headers);
}

async function retrieveFilePath(sample,vcfId,createLog) {
    return new Promise ( (resolve,reject) => {
        createLog.debug(`Received request for Sample ${sample}`);
        createLog.debug(`Received request for VCF file ID ${vcfId}`);

        var parsed = url.parse(sample);
        //console.dir(parsed,{"depth":null});
        var lib = urlLibraries[parsed.protocol || "http:"];
        var options = { 'agent' : false };
        //console.log("lib is "+parsed.protocol);
        if ( parsed.protocol == "https:" ) {
            options = {'rejectUnauthorized': false, 'requestCert': true, 'agent': false };
        }
        //console.log("options value is "+options);
        //var options = {'rejectUnauthorized': false, 'requestCert': true, 'agent': false }; //c1
        //const req = https.get(sample,options,async(response) => { //c1
        const req = lib.get(sample,options,async(response) => {
            if ( response.statusCode < 200 || response.statusCode >= 300 ) {
                var msg = sample+" URL-Not Valid. Response Status Code-"+response.statusCode;
                createLog.debug("MSG   "+msg);
                reject(msg);
            } else {
                //console.log("Check the response received from the request URL");
                //console.log(response);
                //var parsed = url.parse(sample); //c1
                createLog.debug("************* Logging URL PARSE DATA *****************");+
                createLog.debug(parsed);
                createLog.debug(path.basename(parsed.href));
                createLog.debug("************* Logging URL PARSE DATA *****************");

                var contentD = response.headers['content-disposition'];
                var reqFileName;
                var filename;
                if (contentD && /^attachment/i.test(contentD)) {
                    reqFileName = contentD.toLowerCase()
                        .split('filename=')[1]
                        .split(';')[0]
                        .replace(/"/g, '');
                    console.log("filename is "+reqFileName);
                } else {
                    //pass import PID
                    var tmpPid = process.pid;
                    var tmpFile = `sample-name-${tmpPid}`;
                    // if path cannot be parsed from requrl use tmpFile name
                    reqFileName = path.basename(url.parse(sample).path) || tmpFile;
                }

                createLog.debug(`reqFileName ${reqFileName}`);
                var serverRe = /^[\/a-zA-Z_0-9-\.]+\.(vcf|vcf.gz)$/i;
                if ( !reqFileName.match(serverRe) ) {
                    createLog.debug("VCFLocation: Invalid URL.Downloaded URL data did not match supported FileExtensions: .vcf or .vcf.gz");
                        reject("VCFLocation: Invalid URL.Downloaded URL data did not match supported FileExtensions: .vcf or .vcf.gz ");
                } else {
                    filename = path.join(logLoc,'import','samples',reqFileName);
                    resolve(filename);
                }

                /*if ( instance == 'dev') {
                    const logD = 'log';
                    filename = path.join(logD, reqFileName);
                } else {
                    // env ( uat, prod ) will be using docker instance. Logs has to be created in the corresponding bind volumes defined in the docker-compose environment file
                    // importLogPath will be defined in docker-compose environment variables section 
                    filename = path.join(process.env.importLogPath,reqFileName);
                }*/

            }
        });

        req.on('error' , (err) => {
            console.log("Error in rretriving filepath from Request URL");
            console.log(err);
            //createLog.debug(err);
            createLog.debug("Error in retriving filepath from Request URL "+sample);
            reject(err);
        });
        req.end();
    });
}





function getVarType(refA, altA) {
    var type = "";
    if ( (refA.length === 1) && (altA.length === 1) ) {
        type = "snv";
    } else if ( altA.startsWith(refA) && altA.split(refA).length > 1 ) {
        type = "ins";
    } else if ( refA.startsWith(altA) && refA.split(altA).length > 1 ) {
        type = "del";
    } else {
        type = "sub";
    }

    /*
    if (refA.length == altA.length) {
       type = "snv";
    } else if (refA.length > altA.length) {
        type = "del";
    } else if (refA.length < altA.length) {
        type = "ins";
    } */
    return type;
}

function getSampleNames(line1) {
    var data = [];
    data = line1.split('\t');
    //console.log("logging line "+line1);
    //console.log(data.length);
    if ( data.length < 2 ) {
        return ["Unknown"];
    }
    console.log("LINE CHECK "+line1);
    var regS = /FORMAT\t(.*)/;
    var sampInfo = regS.exec(line1);
    var sam1 = sampInfo[1].split('\t');
    return sam1;
}

function getBaseLog(x, y) {
    return Math.log(y) / Math.log(x);
}

function createDoc(fieldHeaders, printStr, insertId, pid) {
    //var fieldHeaders = ['sid','var_key','v_type','chr','start_pos','ref_all','alt_all','multi_allelic','phred_polymorphism','filter','alt_cnt','ref_depth','alt_depth','phred_genotype','mapping_quality','base_q_ranksum','mapping_q_ranksum','read_pos_ranksum','strand_bias','quality_by_depth','fisher_strand','vqslod','gt_ratio','ploidy','somatic_state','delta_pl','stretch_lt_a','stretch_lt_b'];
    var dataVal = printStr.split('|');
    //console.log("Logging printStr");
    //console.log(printStr);
    var mainDoc = {};
    var doc = {};
    var cntr = 0;
    for (var fIdx in fieldHeaders) {
        var fName = fieldHeaders[fIdx];
        var fVal = dataVal[cntr];
        if (fVal == "undefined") {
            fVal = "";
        // MQ=NaN in trio based samples. If GT is not ./. , then the vcf entry is considered valid. When we try to insert entry with value NaN, mongoose does not allow
        //ValidationError: mapping_quality: Cast to Number failed for value "NaN" at path "mapping_quality"
        // To handle this issue, we are setting value to null when field value is NaN
        } else if (fVal == "NaN") {
            fVal = "";
        }
        doc[fName] = fVal;
        doc['var_validate'] = 0;
        if (fName == 'var_key') {
            // Unique Key _id is set to SampleID-Chr-Position-RefAll-AltAll (4349-1-10041298-AT-A)
            // Added id to the field headers in the parsing section
            //doc['_id'] = fVal;
            doc['pid'] = pid;
        }
        cntr++;
    }
    //console.log("Document prepared ");
    //console.log(doc);
    mainDoc['document'] = doc;
    //console.log("Created document ");
    //console.dir(doc,{"depth":null});
    return mainDoc;
}
function normaliseVariant(start,stop,ref,alt) {
    var variant = {};
    // normalising from the right.
    while ( (ref.substr(0,-1) == alt.substr(0,-1) ) && ( (ref.length != 1) && (alt.length != 1)  ) ) {
        ref = ref.slice(0,-1);  // removes the last nucleotide
        alt = alt.slice(0,-1); // removes the last nucleotide
                stop--;
    }
    // if there is still scope for normalisation, do from left
    while ( (ref.substr(0,1) == alt.substr(0,1) ) && ( (ref.length != 1) && (alt.length != 1)  ) ) {
        ref.substr(0,1) = '';
        alt.substr(0,1) = '';

        //ref = ref.slice(0,1);  // removes the first nucleotide
        //alt = alt.slice(0,1); // removes the first nucleotide
                start++;
    }
    variant['start'] = start;
    variant['stop'] = stop;
    variant['refAll'] = ref;
    variant['altAll'] = alt;

    return variant;
}

async function updateStatus(dbStat,search,update,pushStat) {
    try {
        var id  = {'_id' : search};
        var set = {};
        if ( pushStat ) {
            set = {$set : update, $push : pushStat};
        } else {
            set = {$set : update };
        }
        var statsColl = dbStat.collection(importStatsCollection);
        var res = await statsColl.updateOne(id,set);
        return "success";
    } catch(err) {
        throw err;
    }
}

async function updateSampleSheetStat(db,id,stat) {
    try {
        var search = { fileID: id};
        var update = { $set : {status: stat}};
        var sampColl = db.collection(sampleSheetCollection);
        var res = await sampColl.updateOne(search,update);
        return "success";
    } catch(err) {
        throw err;
    }
}

function createAnnoDoc(annoKey,pid) {
    //var mainDoc = {};
    var doc = {};
    doc['_id'] = annoKey
    doc['annotated'] = 0;
    doc['pid'] = pid;
    //mainDoc['document'] = doc;
    return doc;
}

function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}

async function deleteFiles(directory, filename) {
  //const filePath = path.join(directory, filename);
  //const unannotatedFilePath = path.join(directory, filename.replace(/(\.[^.]+)$/, '.unannotated$1'));
  const filePath = filename;
  const unannotatedFilePath = filename.replace(/(\.[^.]+)$/, '.unannotated$1');
  try {
    // Delete the original file
    await unlink(filePath);
    console.log(`Deleted file: ${filePath}`);
  } catch (err) {
    console.error(`Error deleting file: ${filePath}, ${err}`);
  }

  try {
   // Delete the unannotated file
   await unlink(unannotatedFilePath);
   console.log(`Deleted file: ${unannotatedFilePath}`);
 } catch (err) {
   console.error(`Error deleting file: ${unannotatedFilePath}, ${err}`);
 }
}


