#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
const argParser = require('commander');
const colors = require('colors');
//const mongoLogger = require('mongodb').Logger;

const { createLogger, format, transports } = require('winston');
const configData = require('../config/config.js');
const { db : {dbName,variantAnnoCollection} } = configData;

var bson = require("bson");
var BSON = new bson.BSON();

// schema model for variants collection holding vcf data
//var variantSchema = require('../models/variantDataModel.js');
var varCol = require('../models/variantDataModel1.js');
console.log("Collection Object created ");

var db = require('../controllers/db.js');
const collection = db.collection(variantAnnoCollection);
console.log("Collection is "+collection);
//console.dir(collection);
/*
var getConnection = require('../controllers/dbConn.js').getConnection;
var client = getConnection();
const db = client.db(dbName);
const collection = db.collection(variantAnnoCollection);
*/
//var annoCol = require('../models/annotationDataModel.js');
//console.log("Variant Annotation Collection created");
//console.log(varCol);

// Adding changes for managing concurrent operations
// await/async modules to perform sequential VCF parsing

//const Schema = require('Schema');

(async function () {
    argParser
        .version('0.1.0')
        .option('-s, --sample <vcf1,vcf2,vcf3>', 'sample_file or sample_url')
        .option('-vcf, --vcf_file_id <id1,id2,id3|id4>', 'commma separated vcf file ID. Pipe separated ID for multisample files')
    argParser.parse(process.argv);

    // vcfformat UG/HC will be supported 
    // haplotype caller vcf format 

    if ((!argParser.sample) || (!argParser.vcf_file_id)) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }


    var vcfFormat = 'HC';
    var vcfFile = argParser.sample.split(',');
    var vcfFileId = argParser.vcf_file_id.split(',');
/*
    console.log("*************** LOGGING SPLIT VALUES ******************** ");
    console.log(vcfFile);
    console.log(vcfFileId);
    console.log("*************** LOGGING SPLIT VALUES ******************** ");


    //console.log("----------- EXPORT -------------------------");
    //console.dir(variantSchema,{depth:null});
*/
    ///////////////////// Winston Logger //////////////////////////////
    // To be added to a separate library //////
    const env = 'development';
    // Create the log directory if it does not exist
    const logDir = 'log';
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir);
    }

    var logFile = 'importDebug-'+process.pid+'.log';
    //const filename = path.join(logDir, 'results.log');
    const filename = path.join(logDir, logFile);

    const logger = createLogger({
        // change level if in dev environment versus production
        level: env === 'development' ? 'debug' : 'info',
        format: format.combine(
            format.timestamp({
                format: 'YYYY-MM-DD HH:mm:ss'
            }),
            format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
        ),
        transports: [
            new transports.Console({
                level: 'info',
                format: format.combine(
                    format.colorize(),
                    format.printf(
                        info => `${info.timestamp} ${info.level}: ${info.message}`
                    )
                )
            }),
            new transports.File({ filename })
        ]
    });
    /////////////////////// Winston Logger ///////////////////////////// 


    //mongoLogger.setLevel('debug');
    //// Validate VCF File, ID and also ID for Multi-Sample VCF Files ///
    validate(vcfFile, vcfFileId);
    var vcfFileCnt = vcfFile.length;
    var cntTrack = 0;
    for (var id in vcfFile) {
        //cntTrack++;
        var sample = vcfFile[id];
        var vcfId = vcfFileId[id];
        logger.debug("************ CHECK CALL *******************");
        logger.debug(sample);
        logger.debug(vcfId);
        logger.debug("************ CHECK CALL *******************");
        try {
            var val = await parseSample(sample, vcfId, vcfFormat, logger, varCol);
            if ( val == "Success") {
                cntTrack++;
                logger.debug("Received value "+val);
                logger.debug("Incremented count tracker");
            }
            if ( cntTrack == vcfFileCnt ) {
                logger.debug("vcfFileCnt "+vcfFileCnt);
                logger.debug("cntTrack "+cntTrack);
                logger.debug("looks like we have parsed the data for all the vcf files that was passed as input ");
                logger.debug("*********** Right time to close the connection ");
                logger.debug(" ************ TRACK CHECK & CLOSE ************ ");
                process.exit(0);
                //connection.close();
            }
        } catch(err) {
            logger.debug("Error resolving promise "+err);
            logger.debug(err);
            logger.debug("Exit spawned process ");
            //process.exit(1);
        }

    }
})();

function validate(vcfFile, vcfFileId) {
    // Throw Error if Number of samples and Number of sample IDs do not match. 
    if (vcfFile.length != vcfFileId.length) {
        argParser.outputHelp(applyFont);
        process.exit(0);
    }

    for (var fileIx in vcfFile) {
        var file1 = vcfFile[fileIx];
        // If file1 has multiple samples, the number of samples should match the number of IDs in fileId1
        // Multi-Sample File IDs separated by |
        var fileId1 = vcfFileId[fileIx];
        console.log("******* Concurrent Check for file id " + fileId1);

        var reFile = /\.gz/g;
        var readStr;
        if (file1.match(reFile)) {
            readStr = fs.createReadStream(file1, { encoding: 'utf-8' }).pipe(zlib.createGunzip());
        } else {
            readStr = fs.createReadStream(file1, { encoding: 'utf-8' });
        }

        var dataSamples;
        readStr.on('data', function (data) {
            //console.log("************** SCANNING ******************************* ");
            //console.log(data);
            var sampRe = /#CHROM/g;
            if (data.match(sampRe)) {
                //console.log("************* DATA MATCH FOUND ********************* ");
                dataSamples = getSampleNames(data);
                //console.log(" ****************** DATALINES ******************** ");
                //console.log(dataSamples);
                readStr.destroy();
            }
        });
        readStr.on('close', function () {
            if (dataSamples.length > 1) {
                var fileIds = fileId1.split('|');
                if (dataSamples.length != fileIds.length) {
                    console.log("Number of Samples and Number of IDs do not Match. Program will exit .... ");
                    argParser.outputHelp(applyFont);
                    process.exit(1);
                }
            }
        });

        readStr.on('end', function () {
            console.log("END event call received");
        });

        readStr.on('error', function () {
            console.log("ERROR event call received.Filehandle destroyed. Internal!!");
        });

    }
}

// For initial version,we are considering only haplotype caller based vcf files
async function parseSample(sample, vcfId, vcfFormat, logger, varCol) {
    const getSuccess = new Promise( ( resolve ) => resolve("Success") )
    console.log("Function parseSample ");
    // GATK haplotype caller VCF documentation - https://software.broadinstitute.org/gatk/documentation/article?id=11005
    if (vcfFormat == 'HC') {
        // add logic to check the format of the (g)vcf file
        var chrHash = {};
        // check for the number of samples in VCF/ cohort samples based on VCF
        logger.debug("********* Check for concurrent calls. Sample parsing done for " + sample);
        try {
            var value = await parseHCSample(sample, vcfId, logger, varCol);
            logger.debug("Await value received from Promise function parseHCSample is " + value);
            return await getSuccess;
        } catch(retErr) {
            logger.debug("Error loading final set of data .Promise failed ");
            logger.debug(retErr);
            console.log(retErr);
            console.dir(retErr,{"depth":null});
            reject(retErr);
        }
    }
}

// resolve/reject a promise when the VCF file parsing is completed
function parseHCSample(sample, vcfId, logger, varCol) {
    return new Promise((resolve, reject) => {
        logger.debug("Function parseHCSample called for parsing ----- " + sample);
        logger.debug("VCFID ----- " + vcfId);
        var rd;
        var reFile = /\.gz/g;
        if (sample.match(reFile)) {
            rd = readline.createInterface({
                input: fs.createReadStream(sample).pipe(zlib.createGunzip()),
                //output: process.stdout,
                console: false
            });
        } else {
            rd = readline.createInterface({
                input: fs.createReadStream(sample),
                //output: process.stdout,
                console: false
            });
        }

        var lineNo = 0;
        var variantHash = {};
        var printHeaderNo = 1;
        var sNames = [];
        var sampleName;
        var sData = {};

        var insertCtr = 0;
        var bulkOps = [];
        var annoData = [];

        var pid = process.pid;
        var vcfIdList = vcfId.split('|');
        logger.debug("Process ID is " + pid);
        rd.on('line', function (line) { // line input handler
            var nonVariant = 0;
            var vType = "";
            var nonRef = "";
            lineNo++;

            //console.log("Line "+line);
            var sampRe = /^#CHROM/g;

            // Ignore commented lines
            var commentReg = /^[^#]/g;
            if (line.match(commentReg)) { // VCF line
                //console.log("******************* LINE ************************ ");
                //console.log(line);
                var data = line.split('\t');
                var dataFields = ['chr', 'startPos', 'snpid', 'refall', 'altall', 'phredPoly', 'filter', 'infoStr', 'formatStr', 'sampleStr'];

                /* NON_REF SAMPLE
                chr22   23077521        .       C       <NON_REF>       .       .       END=23077554    GT:DP:GQ:MIN_DP:PL      0/0:18:48:16:0,45,618
                chr22   23077555        .       T       C,<NON_REF>     180.77  .       BaseQRankSum=-0.044;ClippingRankSum=-0.755;DP=18;MLEAC=1,0;MLEAF=0.500,0.00;MQ=68.29;MQ0=0;MQRankSum=0.133;ReadPosRankSum=-2.088        GT:AD:DP:GQ:PL:SB       0/1:10,8,0:18:99:209,0,252,239,276,515:4,6,3,5
                */
                var fieldVal = {};

                // expected number of fields for this format : 10
                for (var idx in dataFields) {
                    var field = dataFields[idx];
                    // chromosome field ----------------
                    if (idx == 0) {
                        //console.log("idx "+idx);
                        var re = /chr/g;
                        if (data[idx].match(re)) {
                            var chr = data[idx].replace(re, '');
                            //console.log("New String is "+chr);
                            if (chr > 22) {
                                // ignore this line and do not process further for this line
                            } else if (chr == 'X') {
                                chr = 23;
                            } else if (chr == 'Y') {
                                chr = 24;
                            } else if (chr == 'M') {
                                chr = 25;
                            }
                            data[idx] = chr;

                            //console.log("Chromosome pattern identified");
                            var checkVal = data[idx];
                            //console.log(checkVal);
                        }
                    } // Chr Field 

                    //console.log("Value is------------------ "+field);
                    fieldVal[field] = data[idx];
                }

                // ******************** Annotation Info **************************** //
                //           AC=1;AF=0.500;AN=2;BaseQRankSum=0.229                   //
                var infoStr = fieldVal['infoStr'];

                var infoSt = infoStr.split(';');
                var infoHash = {};
                for (var infoIdx in infoSt) {
                    var infoVal = infoSt[infoIdx];
                    var infoData = infoVal.split('=');
                    var text1 = infoData[0];
                    var text2 = infoData[1];
                    infoHash[text1] = text2;
                    //console.log("Text1 "+text1+" Text2 "+text2);
                }
                //console.dir(infoHash,{depth:null});

                // ********************** Annotation Info *****************************  //
                var refAll = fieldVal['refall'];
                var altAll = fieldVal['altall'];
                var altAlls = fieldVal['altall'].split(',');
                //console.log("Reference allele is " + refAll);
                var startPos = fieldVal['startPos'];
                var stopPos = parseInt(startPos);
                //console.log("Length of reference allele is " + refAll.length);
                if (refAll.length > 1) {
                    //console.log("Verify for a length greater than 2");
                    stopPos = parseInt(startPos) + refAll.length - 1;
                    //console.log("Stop Pos is " + stopPos);
                }
                if ( altAlls.length == 1 ) {
                    var normalised = normaliseVariant(startPos,stopPos,refAll,altAll);
                    var pattern = fieldVal['chr']+'\t'+normalised['start']+'\t'+'.'+'\t'+normalised['refAll']+'\t'+normalised['altAll'];
                    console.log(pattern);
                    //console.dir(normalised,{"depth":null});
                }

                
            } // if
        }); // file handler
    }); // Promise 
}

function getVarType(refA, altA) {
    var type = "";
    if (refA.length == altA.length) {
        type = "snv";
    } else if (refA.length > altA.length) {
        type = "del";
    } else if (refA.length < altA.length) {
        type = "ins";
    }
    return type;
}

function normaliseVariant(start,stop,refAll,altAll) {
    var normalisedVariant = {};
    start = parseInt(start);
    /*
    console.log("Org start "+start);
    console.log("Org stop "+stop);
    console.log("Org ref "+refAll);
    console.log("Org alt "+altAll);
    */

    // retrieve the last nucleotide and check if they match. Chop if they match.
    while ( (refAll.substr(0,-1) == altAll.substr(0,-1) ) && ( (refAll.length != 1) && (altAll.length != 1)  ) ) {
        refAll = refAll.slice(0,-1);  // removes the last allele
        altAll = altAll.slice(0,-1); // removes the last allele
		stop--;
		if (! refAll ) {
			refAll = '-';
			start--;		//now it is an insertion so the start should decrease by 1 (20150925)
			break;
		}
		if (! altAll) {
			altAll = '-';
			break;
		}
    }

/*
    while ( ( refAll.substr(0,1) == altAll.substr(0,1) ) &&  ( (refAll.length != 1 ) && (altAll.length != 1) ) ) {

        refAll.substr(0,1) = '';
        altAll.substr(0,1) = '';
		//substr (refAll,0,1) = '';
		//substr (altAll,0,1) = '';
		start++;
		if (! refAll) {
			refAll = '-';
			start--;		//now it is an insertion so the start should decrease by 1 (20150925)
			break;
		}
		if (! altAll ) {
			altAll = '-';
			break;
		}
    }
*/
    normalisedVariant['start'] = start;
    normalisedVariant['stop'] = stop;
    normalisedVariant['refAll'] = refAll;
    normalisedVariant['altAll'] = altAll;
    //console.log("Logging normalized variant below");
    //console.dir(normalisedVariant,{"depth":null});
    return normalisedVariant;
}

function getSampleNames(line1) {
    var data = line1.split('\t');
    //console.log("LINE CHECK "+line1);
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
    var mainDoc = {};
    var doc = {};
    var cntr = 0;
    for (var fIdx in fieldHeaders) {
        var fName = fieldHeaders[fIdx];
        var fVal = dataVal[cntr];
        if (fVal == "undefined") {
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


