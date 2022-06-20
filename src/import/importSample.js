#!/usr/bin/env node
'use strict';

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

//const http = require('http');
//const https = require('https');
const stream = require('stream');
const util = require('util');
var url = require('url');
const pipeline = util.promisify(stream.pipeline);

//const mongoLogger = require('mongodb').Logger;

const { createLogger, format, transports } = require('winston');
const configData = require('../config/config.js');
const { db : {dbName,importCollection1,importCollection2,variantAnnoCollection1,variantAnnoCollection2,importStatsCollection,sampleSheetCollection}, app:{instance,logLoc} } = configData;
//console.log("Check the config data "+variantAnnoCollection);
var loggerMod = require('../controllers/loggerMod');
var bson = require("bson");
var BSON = new bson.BSON();

var varCol;
var importCollection;
var db = require('../controllers/db.js');
var variantAnnoCollection;
var collection;
//const collection = db.collection(variantAnnoCollection);
//console.log("Collection is "+collection);

const createColIndex = require('../controllers/dbFuncs.js').createColIndex;

(async function () {
    argParser
        .version('0.1.0')
        .option('-s, --sample <vcf1>', 'sample_file or sample_url')
        .option('-vcf, --vcf_file_id <id1,id2,id3|id4>', 'commma separated vcf file ID. Pipe separated ID for multisample files')
        .option('--assembly, --assembly <GRCh37,GRCh38>', 'assembly type to decide the import collection to be used for import')
        .option('-p, --pid <pid>', 'id of the created request')
        .option('-b, --batch <batch>', 'batch size for bulkUpload')
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
    var batchSize = argParser.batch || 6000;
    batchSize = parseInt(batchSize);
    var updateId = argParser.pid || '';

    console.log("BATCH SIZE VALUE iS ******************* "+batchSize);
    ///////////////////// Winston Logger //////////////////////////////

    var logFile = `importSample-${vcfId}-${process.pid}.log`;
    var statsLogFile = `importStats-${vcfId}-${process.pid}.log`;
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

    try {
        if ( assemblyType == "GRCh37") {
            varCol = require('../models/variantDataModelGRCh37.js');
            console.log("Collection Object created ");
            importCollection = importCollection1;
            variantAnnoCollection = variantAnnoCollection1;
        } else if ( assemblyType == "GRCh38" ) {
            varCol = require('../models/variantDataModelGRCh38.js');
            console.log("Collection Object created ");
            importCollection = importCollection2;
            variantAnnoCollection = variantAnnoCollection2;
        }
        collection = db.collection(variantAnnoCollection);
    } catch(err) {
        createLog.debug("Error in importSample "+err)
    } 
    
    try {
        // 'sid' is changed to fileID. This is related to major design change done in UI
        var idx1 = {"fileID":1,"chr":1,"start_pos":1};
        var idx2 = {"var_key":1};
        var idx3 = {"chr":1,"start_pos":1,"stop_pos":1};

        await createColIndex(db,importCollection,idx1);
        await createColIndex(db,importCollection,idx2);
        await createColIndex(db,importCollection,idx3);
        
    } catch (err) {
        console.log("Index already exists.Just proceed"+err);
    }

    try {
        await createColIndex(db, importCollection,{"gene_annotations.gene_id" : 1});
    } catch (e) {
        console.log("gene_id index already exists. Proceed further"+e);
    }

    try {
        await createColIndex(db, variantAnnoCollection,{"annotated":1});
    } catch (e) {
        console.log("annotated index already exists. Proceed further"+e);
    }

    var start = new Date();
    createLog.debug("Import Sample Program Started at "+start);
    createLog.debug(sample);
    createLog.debug(vcfId);
    createLog.debug(`sample ${sample} vcfId ${vcfId}`);
    try {
        if ( vcfFormat == "HC") {
            createLog.debug("Calling createFileHandle function");

            var urlReg = /^http/g;
            var rd;
            var filename;
            if ( sample.match(urlReg) ) {
                createLog.debug(`calling downloadData for sample ${sample} VCF ${vcfId}`);
                filename = await downloadData(sample,vcfId,createLog);
                createLog.debug(`filename is ${filename}`);
                rd = await createZipInterface(filename,vcfId,createLog);
            } else {
                rd = await createZipInterface(sample,vcfId,createLog);
            }    
            //var rd = await createFileHandle(sample,vcfId,createLog);
            createLog.debug("handle returned from createFileHandle function "+rd);
            var val = await parseHCSample(rd,sample, vcfId,createLog, statsLog,varCol,batchSize);
            var stop = new Date();
            console.log("What was the return value sent by the above function ?");
            createLog.debug("Import Sample Program Completed at "+stop);
            if ( val == "Success") {
                createLog.debug("Received value "+val);
                createLog.debug("*********** Right time to close the connection ");
                createLog.debug(" ************ TRACK CHECK & CLOSE ************ ");
                process.exit(0);
            }
        }
    } catch(err) {
        console.log("Catch Error is "+err);
        var errMsg = 'Import Sample Stage-failed due to download or parse errors '+err;
        var status = {'status_description' : 'Error','status_id':9, 'error_info':errMsg, 'finish_time': new Date()};
        createLog.debug('updateId is '+updateId);
        var pushSt = {'status_log': {status:"Error",log_time: new Date()}};
        if ( updateId ) {
            updateId = parseInt(updateId);
        }
        await updateStatus(db,updateId,status,pushSt);
        await updateSampleSheetStat(db,vcfId,"import failed");
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

async function downloadData(sample,vcfId,createLog) {
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
                    var tmpPid = process.pid;
                    var tmpFile = `sample-name-${tmpPid}`;
                    // if path cannot be parsed from requrl use tmpFile name
                    reqFileName = path.basename(url.parse(sample).path) || tmpFile;
                }

                createLog.debug(`reqFileName ${reqFileName}`);
                var serverRe = /^[\/a-zA-Z_0-9-\.]+\.(vcf|vcf.gz|gvcf|gvcf.gz)$/i;
                if ( !reqFileName.match(serverRe) ) {
                    createLog.debug("VCFLocation: Invalid URL.Downloaded URL data did not match supported FileExtensions: .vcf or .vcf.gz or .gvcf or .gvcf.gz");
	                reject("VCFLocation: Invalid URL.Downloaded URL data did not match supported FileExtensions: .vcf or .vcf.gz or .gvcf or .gvcf.gz");
                } else {
                    filename = path.join(logLoc,'import','samples',reqFileName);
                    await pipeline(response,fs.createWriteStream(filename));
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
            console.log("Error in reading request data from Request URL");
            console.log(err);
            //createLog.debug(err);
            createLog.debug("Error in reading data from Request URL "+sample);
            reject(err);
        });
        req.end();
    });
}

async function createZipInterface(sample,vcfId,createLog) {
    var rd;
    var reFile = /\.gz/g;
    try {
        if ( ! fs.existsSync(sample) ) {
            throw `Sample ${sample} is not accessible`;
        }
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
        return rd;
    } catch(err) {
        console.log("Is the error caught here ?");
        throw err;
    }
    
}
// For initial version,we are considering only haplotype caller based vcf files
// resolve/reject a promise when the VCF file parsing is completed
async function parseHCSample(rd,sample, vcfId, createLog,statsLog,varCol,batchSize) {
    //return new Promise((resolve, reject) => { (async function)
    try {
        createLog.debug("Function parseHCSample called for parsing ----- " + sample);
        createLog.debug("Sample ID ----- " + vcfId);
        //var rd;
        var basePath = path.parse(__dirname).dir;
        var jsonP = path.join(basePath,'config','chrMap.json');

        var mapJson = fs.readFileSync(jsonP);
        var chrData = JSON.parse(mapJson);
        var chrMap = chrData['chromosomes'];

        //var variantHash = {};
        var printHeaderNo = 1;
        var sNames = [];
        var sampleName;
        var sData = {};

        var insertCtr = 0;
        var bulkOps = [];
        var annoData = [];
        var spanDelUpdates = [];

        var pid = process.pid;
        //var vcfIdList = vcfId.split('|');
        createLog.debug("Process ID is " + pid);
        var lineNo = 0;
        var importCollCnt = 0;
        var annoCollCnt = 0;
        var skippedEntries = 0;
        var uID;
        rd.on('line', function (line) { // line input handler
            var nonVariant = 0;
            var vType = "";
            var nonRef = "";
            lineNo++;

            //console.log("Line "+line);
            var sampRe = /^#CHROM/g;
            
            if (line.match(sampRe)) {
                sNames = getSampleNames(line); // list of samples present in this VCF file

                //console.log("Sample Names");
                //console.log(sNames);
                // traverse the sample names, generate sample ID. 
                // Sample ID generation : check for exists sample names in mongo collection and generate sample ID
                var cntIdx = 0;
                for (sIdx in sNames) {
                    var sNameTmp = sNames[sIdx];
                    //sData[sNameTmp] = vcfIdList[cntIdx];
                    sData[sNameTmp] = vcfId;
                    cntIdx++;
                }
                if (sNames.length > 1) {
                    //console.log("------------------ Multi-Sample VCF ------------------ ");
                }
                sampleName = sNames[0];
            } 

            // Ignore commented lines
            
            var commentReg = /^[^#]/g;
            if (line.match(commentReg) && line.split('\t').length > 9 ) { // VCF line
                //createLog.debug("Line is "+line);
                
                var data = line.split('\t');
                //createLog.debug(data);
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
                        // adding updates to support hg38 and GRCh38 vcf formats
                        var re = /chr/g;
                        var chr = data[idx];
                        var vcfChr = data[idx];
                        // chr:chr2 vcfChr:chr2 23 chrX
                        // chr:chr2_random vcfChr:chr2_random chr:30
                        // chr:2 vcfChr:2 
                        // chr:2_random vcfChr:2_random chr:30

                        // remove chr prefix if present from chr
                        // vcfChr has the original value present in vcf file
                        if (chr.match(re)) {
                            chr = data[idx].replace(re, '');
                            //createLog.debug("New String is "+chr);   
                        }

                        // non-canonical chromosomes,X,Y,M
                        // If any string value is present for chr, then retrieve the numerical counterpart 
                        if ( chrMap[chr]) {
                            chr = chrMap[chr];
                        }

                        data[idx] = chr;
                        fieldVal['vcf_chr'] = vcfChr;

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
                    //var re1 = /([^=]*)=([^=]*)/;
                    //var res = re1.exec(infoval);
                    var infoData = infoVal.split('=');
                    var text1 = infoData[0];
                    var text2 = infoData[1];
                    infoHash[text1] = text2;
                    //console.log("Text1 "+text1+" Text2 "+text2);
                }
                //console.dir(infoHash,{depth:null});

                // MQ BaseQRankSum MQRankSum ReadPosRankSum SB QD FS VQSLOD  -- Fetch from infoHash 
                var mq = infoHash['MQ'];
                var baseQRank = infoHash['BaseQRankSum'];
                var mqRank = infoHash['MQRankSum'];
                var readPosRank = infoHash['ReadPosRankSum'];
                var sb = infoHash['SB'];
                var qd = infoHash['QD'];
                var fs = infoHash['FS'];
                var vqslod = infoHash['VQSLOD'];

                // ********************** Annotation Info *****************************  //
                var refAll = fieldVal['refall'];
                var startPos = fieldVal['startPos'];
                var stopPos = parseInt(startPos);
                if (refAll.length > 1) {
                    //console.log("Verify for a length greater than 2");
                    stopPos = parseInt(startPos) + refAll.length - 1;
                    //console.log("Stop Pos is " + stopPos);
                }

                // ----------------------- Stretch ------------------------------------- // 
                // Based on data from RU & RPA
                var str = 0;
                var strA = 0;
                var strB = 0;
                var rpa = [];
                var strU;
                // tandem repeat unit
                if (infoHash['RU']) {
                    str = 1;
                    strU = infoHash['RU'];
                    // RPA : Number of times repeat unit is repeated for each allele, incl. REF allele
                    if (infoHash['RPA']) {
                        rpa = infoHash['RPA'].split(',');
                        //console.log("Stretch detected");
                    }
                }

                // -------------------------- Stretch -------------------------------- //
                var formatStr = fieldVal['formatStr'];
                var formats = formatStr.split(':');


                ////////////// Multiple Sample Specific parsing to parse Genotype fields /////////////////////////

                var vcfSampCnt = 8;
                // loop alignment has to be changed
                for (var sIdx in sNames) { // MultiSample VCF
                    //console.log("*********** SAMPLE PARSING STARTED FOR ******************** "+sIdx);
                    vcfSampCnt++;
                    var printHeader = "";
                    var printString = "";
                    var fieldHeaders = "";
                    var tmpS = sNames[sIdx];
                    var sId = sData[tmpS];
                    sId = parseInt(sId);
                    //console.log("*********** SAMPLE PARSING STARTED FOR ******************** "+sId);
                    fieldVal['sampleStr'] = data[vcfSampCnt];

                    ////////////////// Genotype fields for Sample //////////////////////////////////////////////
                    var sampleStr = fieldVal['sampleStr'];
                    //console.log("Format String **************** "+formatStr);
                    //console.log("Sample String **************** "+sampleStr);
                    var samples = sampleStr.split(':');
                    var formatHash = {};
                    for (var formatIdx in formats) {
                        var val1 = formats[formatIdx];
                        formatHash[val1] = samples[formatIdx];
                    }
                    //console.log("Dumping hash ------------ ");
                    //console.dir(formatHash,{depth:null});

                    // GT : './. GT : '.|.' --> Not enough information to call a variant. Skip them.
                    var gtRe1 = /\.\/\./g; // ./.
                    var gtRe2 = /\.\|\./g; // .|.
                    if ((formatHash['GT'].match(gtRe1)) || (formatHash['GT'].match(gtRe2))) {
                        //console.log("Skip variant not called GT - CHR" + chr);
                        //return;
                        createLog.debug("Skipping entry as GT is ./.");
                        createLog.debug(line);
                        skippedEntries = skippedEntries + 1;
                        continue;
                    }
                    var phredGeno = formatHash['GQ'];
                    //////////////////////////////////////////////////////////////////////////////////


                    ///////////////////// GVCF REFERENCE ///////////////////////////////////
                    //console.log("Checking for GVCF data if any ");
                    //console.dir(fieldVal,{"depth":null});
                    if (fieldVal['altall'] == '<NON_REF>') {
                        //console.log("Here is a NON_REF");
                        nonVariant = 1;
                        vType = "non_var";
                        var info = fieldVal['infoStr'];
                        var re1 = /END=([^=]*)/;
                        var res = re1.exec(info);
                        //console.log("********* NON_REF String Check ");
                        //console.log(res);
                        var stop = res[1];
                        //console.log("Stop position value is "+stop);
                        var dp = formatHash['DP'];
                        var minDp = formatHash['MIN_DP'];
                        var dpl = 0;
                        if (formatHash['PL']) {
                            var pl = formatHash['PL'];
                            var plArr = pl.split(',');
                            var sorted = plArr.sort();
                            dpl = sorted[1];
                        }
                        var gq = formatHash['GQ'];
                        var uniqueKey = sId + '-' + fieldVal['chr'] + '-' + startPos + '-' + fieldVal['refall'] + '-' + fieldVal['refall'];
                        var annoKey = fieldVal['chr'] + '-' + startPos + '-' + fieldVal['refall'] + '-' + fieldVal['refall'];
                        //nonRef = uniqueKey + '\t' + fieldVal['refall'] + '\t' + startPos + '\t' + stop + '\t' + dp + '\t' + gq + '\t' + minDp + '\t' + dpl;
                        nonRef = fieldVal['vcf_chr'] + '\t' + annoKey + '\t' + uniqueKey + '\t' + fieldVal['refall'] + '\t' + startPos + '\t' + stop + '\t' + dp + '\t' + gq + '\t' + minDp + '\t' + dpl;
                        //console.log("NON REF Positions Data ");
                        //console.log(nonRef);
                    }

                    ////////////////////  GVCF ////////////////////////////////////////////

                    var dpl = 0;
                    // Normalized phred-scaled genotype likelihoods rounded to the closest integer ( otherwise defined as GL field )
                    // How much less likely that genotype is compared to the best one
                    if (formatHash['PL']) {
                        var pl = formatHash['PL'];
                        var plArr = pl.split(',');
                        var sorted = plArr.sort();
                        dpl = sorted[1];
                        // GL : comma separated log10 scaled likelihood for all the possible genotypes
                    } else if (formatHash['GL']) {
                        var pvs = [];
                        var gl = formatHash['GL'].split(',');
                        for (var glIndex in gl) {
                            var glVal = gl[glIndex];
                            var p = (Math.pow(10, glVal));
                            pvs[glIndex] = p;
                        }
                        pvs = pvs.reverse(); //descending sort
                        var pls = [];
                        for (var glIndex in gl) {
                            var p = pvs[glIndex] / pvs[0];
                            // log with base 10
                            pls[glIndex] = -10 * (getBaseLog(10, p));
                        }
                        dpl = pls[1];
                    }


                    // ********************** Checks based on Allele & Genotype *********************** // 
                    // RefAll : TACACAC AltAll:TACAC,TACACACAC 
                    // RefAll : GA AltAll:GAA
                    var altAlls = fieldVal['altall'].split(',');

                    // GT: '0/1', GT: '1/1' , GT: '1/2'
                    
                    var genotype = formatHash['GT'].split(/\/|\|/);
                    //console.log("Checking for genotype");
                    //console.dir(genotype,{"depth":null});

                    // multiallelic : Two different alternate alleles are present
                    var mAllelic = 0;

                    var ploidy = 0;
                    var strA = 0;
                    var strB = 0;

                    //console.log(genotype);
                    //console.log(genotype.length);
                    if (genotype.length == 2) {
                        ploidy = 2;
                        var gt1 = genotype[0];
                        var gt2 = genotype[1];
                        var altCnt = parseInt(gt1) + parseInt(gt2);
                        //console.log("Test altCnt Value is "+altCnt);
                        var gtRatio = altCnt / 2;

                        //console.log("Test RPA length "+rpa.length);
                        if (rpa.length > 0) {
                            strA = rpa[gt1];
                            strB = rpa[gt2];
                        }

                        // Allelic Depths
                        var depths = [];
                        var refDepth = '';
                        if (formatHash['AD']) {
                            depths = formatHash['AD'].split(',');
                            refDepth = depths[0];
                        }

                        var spanningDeletion = 0;

                        // Refer WorkItem https://dev.azure.com/wingsorg/wings_api_dev/_workitems/edit/87 
                        // Spanning Deletion Example from GATK 4.0 VCF shown below 
                        //chrX    100675895       .       T       *,C     236.01  .       AC=1,1;AF=0.500,0.500;AN=2;DP=18;ExcessHet=3.0103;FS=0.000;MLEAC=1,1;MLEAF=0.500,0.500;MQ=60.00;QD=16.86;SOR=5.136      GT:AD:DP:GQ:PL  1/2:0,8,6:14:99:641,246,214,340,0,318

                        // chr1  11133956 . A AAA,AAAG AC=1,1;AF=0.500,0.500;AN=2;DP=1316;FS=0.000;InbreedingCoeff=0.9164;MA=multi;MLEAC=2;MLEAF=0.5;MQ=58.39;MQ0=0;QD=2.39;VQSLOD=4.53;culprit=FS;set=multiR-single GT:DP:GQ 1/2:70:84  
                        /* Modified section of code */
                        // more than one alternate allele
                        if (altAlls.length > 1) {
                            // decide if it is heterozygous-alt or homozygous alt
                            // GT : 1/2 or 0/3
                            if ((gt1 != gt2) && (altCnt > 2)) { // heterozygous-alt
                                for ( var idx in altAlls ) {
                                    // If there was a spanning deletion, GATK 4.0 plots the alternate allele as * 
                                    if ( altAlls[idx] == "*" ) {
                                        spanningDeletion = 1;
                                    }
                                }

                                mAllelic = 1;

                                //mAllelic = 1;
                                altCnt = 1; // heterozygous for two different alt alleles. eg: 1/2
                            } else if ( gt1 == gt2 ) { // homozygous-alt ( Same Alternate Alleles ) Not considered multiAllelic
                                //console.log("MultiAllelic Case--Alternate Count--Before Setting-- Homozygous "+altCnt);
                                // homozygous for the allele mentioned
                                altCnt = 2;
                            } else {
                                // normal heterozygous 0/1; 0/2
                                altCnt = 1;
                            }
                            gtRatio = altCnt / 2;
                        }

                        var sstate = 1; // set to germline

                        var altIdx = 0;
                        
                        for (var idx in altAlls) { // Alt All Loop
                            var altDepth = '';
                            altIdx++;
                            //console.log("altIdx is "+altIdx);
                            var altAll = altAlls[idx];
                            // Ignore alternate allele marked for spanning deletions
                            if ( altAll == "*" ) {
                                createLog.debug("Skipping spanning deletion allele marked with *");
                                createLog.debug(line);
                                continue;
                            }

                            //console.log("Now let us normalise for ");
                            //console.log(`startPos : ${startPos} stopPos: ${stopPos} refAll: ${refAll} altAll: ${altAll}`);
                            var normVariant = normaliseVariant(startPos,stopPos,refAll,altAll);
                            startPos = normVariant['start'];
                            stopPos = normVariant['stop'];
                            refAll = normVariant['refAll'];
                            altAll = normVariant['altAll'];

                            // substitute the values returned by normaliseVariant
                            var uniqueKey = sId + '-' + fieldVal['chr'] + '-' + startPos + '-' + refAll + '-' + altAll;
                            var annotationKey = fieldVal['chr'] + '-' + startPos + '-' + refAll + '-' + altAll;
                            //console.log('Check uniqueKey '+uniqueKey);

                            if ((refAll == altAll) && (altCnt == 0)) {
                                // Set the alt depth
                                altDepth = depths[1];
                            }
                            // Validate with some test data and also for NON_REF based positions
                            //console.log("altCnt check for nonVariant is "+nonVariant);
                            if (altCnt == 0) {
                                //console.log("altCnt is "+altCnt);
                                //console.log("Are we skipping the entry here ? ");
                                // GT : 0/0
                                // To be added later 
                                if (altAll == '<NON_REF>' && altAlls.length > 1) {
                                    createLog.debug("Skipping entry with NON_REF and having alternate alleles > 1");
                                    createLog.debug(line);
                                    continue;
                                }
                                altDepth = depths[1];
                                // 0 - ref 1-alt_all1 2-alt_all2 .... 
                                // skip the alleles that are not present
                            } else if ((gt1 != altIdx) && (gt2 != altIdx)) {
                                //console.log("Check here "+uniqueKey);
                                continue;
                            } else {
                                // set alt depth
                                altDepth = depths[altIdx];
                            }
                            var printCombination = uniqueKey;
                            //variantHash[uniqueKey] = 1;

                            // Update 26/04/2022 Commenting below section as this object is not used
                            /*
                            if (!variantHash[uniqueKey]) {
                                variantHash[uniqueKey] = 1;
                            } else {
                                variantHash[uniqueKey]++;
                            }*/

                            if ((refDepth == '') || (altDepth == '')) {
                                //console.log("fetch Missing Depth from IGV");
                            }

                            if (printHeaderNo == 1) {
                                printHeader = 'Sample_Id' + '|' + 'printCombination' + '|' + 'var_type' + '|' + 'Chr' + '|' + 'Start_Position' + '|' + 'Ref_All' + '|' + 'Alt_All' + '|' + '|' + 'Multi_Allelic' + '|' + 'Phred_Polymorphism' + '|' + 'filter' + '|' + 'Alt_Count' + '|' + 'Ref_Depth' + '|' + 'Alt_Depth' + '|' + 'Phred_Genotype' + '|' + 'Mapping_Quality' + '|' + 'Base_Q_RankSum' + '|' + 'Mapping_Q_RankSum' + '|' + 'Read_Pos_RankSum' + '|' + 'Strand_Bias' + '|' + 'Quality_by_Depth' + '|' + 'Phred_Scaled_Fisher' + '|' + 'VQSLOD' + '|' + 'Genotype_Ratio' + '|' + 'Ploidy' + '|' + 'Somatic_State' + '|' + 'Phred_Scaled' + '|' + 'Stretch_A' + '|' + 'Stretch_B';
                                printHeaderNo = 0; // Reset to 0
                            }

                            //console.log("nonVariant is "+nonVariant);
                            if (nonVariant == 1) {
                                // gq : phredGeno
                                //console.log("Check non_variant setting and log it");
                                //console.log("Log the non ref data and check");
                                //console.log(nonRef);
                                //nonRef : fieldVal['refall']+'\t'+startPos+'\t'+stop+'\t'+dp+'\t'+gq+'\t'+minDp+'\t'+dpl;
                                //nonRef = fieldVal['vcf_chr'] + '\t' + annoKey + '\t' + uniqueKey + '\t' + fieldVal['refall'] + '\t' + startPos + '\t' + stop + '\t' + dp + '\t' + gq + '\t' + minDp + '\t' + dpl;
                                var nonRefData = nonRef.split('\t');
                                var annoKey = nonRefData[1];
                                var var_key = nonRefData[2];
                                var stop = nonRefData[5];
                                var dp = nonRefData[6];
                                phredGeno = nonRefData[7];
                                var minDp = nonRefData[8];
                                dpl = nonRefData[9];
                                annotationKey = annoKey;
                                printString = sId + '|' + var_key + '|' + annoKey + '|' + vType + '|' + nonVariant + '|' + fieldVal['vcf_chr'] + '|' + fieldVal['chr'] + '|' + startPos + '|' + stop + '|' + refAll + '|' + refAll + '|' + mAllelic + '|' + altCnt + '|' + phredGeno + '|' + ploidy + '|' + sstate + '|' + dpl + '|' + dp + '|' + minDp;
                                fieldHeaders = ['fileID', '_id', 'var_key', 'v_type', 'non_variant', 'vcf_chr','chr', 'start_pos', 'stop_pos', 'ref_all', 'alt_all', 'multi_allelic', 'alt_cnt', 'phred_genotype', 'ploidy', 'somatic_state', 'delta_pl', 'dp', 'min_dp'];
                                
                            } else {
                                vType = getVarType(refAll, altAll);
                                printString = sId + '|' + printCombination + '|' + annotationKey + '|' + vType + '|' + nonVariant + '|' + fieldVal['vcf_chr'] + '|' + fieldVal['chr'] + '|' + startPos + '|' + stopPos + '|' + refAll + '|' + altAll + '|' + mAllelic + '|' + fieldVal['phredPoly'] + '|' + fieldVal['filter'] + '|' + altCnt + '|' + refDepth + '|' + altDepth + '|' + phredGeno + '|' + mq + '|' + baseQRank + '|' + mqRank + '|' + readPosRank + '|' + sb + '|' + qd + '|' + fs + '|' + vqslod + '|' + gtRatio + '|' + ploidy + '|' + sstate + '|' + dpl + '|' + strA + '|' + strB;
                                fieldHeaders = ['fileID', '_id', 'var_key', 'v_type', 'non_variant', 'vcf_chr', 'chr', 'start_pos', 'stop_pos', 'ref_all', 'alt_all', 'multi_allelic', 'phred_polymorphism', 'filter', 'alt_cnt', 'ref_depth', 'alt_depth', 'phred_genotype', 'mapping_quality', 'base_q_ranksum', 'mapping_q_ranksum', 'read_pos_ranksum', 'strand_bias', 'quality_by_depth', 'fisher_strand', 'vqslod', 'gt_ratio', 'ploidy', 'somatic_state', 'delta_pl', 'stretch_lt_a', 'stretch_lt_b'];

                                // store for variants that do not have spanning deletion and are not multi-allelic
                                if ( ! spanningDeletion && ! mAllelic ) {
                                    //console.log("Logging uID value "+uID);
                                    //console.log("Logging spanning deletion value "+spanningDeletion);
                                    uID = printCombination;
                                    //console.log(uID);
                                } else if ( spanningDeletion ) {
                                    createLog.debug("spanning deletion "+spanningDeletion);
                                    //console.log(uID);
                                    // When there is a position and then multiple subsequent spanning deletions(*), it is enough if we set the multi-allelic for previous position once.
                                    if ( uID != "" ) {
                                        var updateFilter = {};
                                        var filter = {};
                                        var setFilter = {};
                                        setFilter['multi_allelic'] = 1;
                                        filter['filter'] = {'_id':uID};
                                        //filter['update'] = {$set:{'multi_allelic':1}};
                                        filter['update'] = {$set: setFilter};
                                        updateFilter['updateOne'] = filter;
                                        spanDelUpdates.push(updateFilter);
                                        createLog.debug("Update multi-allelic Request for Unique ID "+uID);
                                        uID = "";
                                    }
                                } else {
                                    uID = "";
                                }
                                // log the previous variant if the current variant has spanning deletion
                            }

                            // Create document with Mongo and send the data using mongoose bulk write
                            if (printString != "") {
                                insertCtr++;
                                //console.log("Check for the document sent to createDoc function");
                                //console.log(printString);
                                var doc = createDoc(fieldHeaders, printString, insertCtr, pid); // variantSchema
                                //console.log("Log the document and check ");
                                //console.log(doc);
                                bulkOps.push({ "insertOne": doc });

                                var annoDoc = createAnnoDoc(annotationKey,pid); // Unique Variant Annotation Doc with the status "1/0" : Annotation
                                annoData.push({"insertOne": annoDoc});
                                //var docSize = BSON.calculateObjectSize(doc);
                                //console.log("Document Size is "+docSize);
                                if ((bulkOps.length % batchSize ) === 0) {
                                    //createLog.debug("Batch threshold Reached. Start with bulkLoading");
                                    var lgt = bulkOps.length;
                                    createLog.debug("Length of bulk Array "+bulkOps.length);
                                    varCol.bulkWrite(bulkOps, { 'ordered': false }).then(function (r) {
                                    //varCol.bulkWrite(bulkOps, { 'ordered': true }).then(function (r) {
                                        //console.log("Logging the bulkops result structure");
                                        //console.log(r,{"depth":null});
                                        //createLog.debug("Executed bulkWrite and the results are ");
                                        //createLog.debug("Import Collection-Count of data loaded given below");
                                        importCollCnt = importCollCnt + r.insertedCount;
                                        createLog.debug("Import-"+r.insertedCount);
                                        //createLog.debug("Then-Length of bulk Array "+bulkOps.length);
                                    }).catch((err1) => {
                                        //createLog.debug("Catch-import collection stats given below");
                                        if ( 'result' in err1 ) {
                                            importCollCnt = importCollCnt + err1.result.nInserted;
                                            createLog.debug("Import-Catch-"+err1.result.nInserted);
                                        } else {
                                            //console.dir(err1,{'depth':null});
                                            //console.log(`${err1}`);
                                        }
                                        
                                        //createLog.debug("Catch-Length of bulk Array "+bulkOps.length);
                                        //createLog.debug(err,{"depth":null});
                                    });
                                    bulkOps = [];
                                    

                                    collection.bulkWrite(annoData, { 'ordered': false }).then(function (res) {
                                            //createLog.debug("No. of VCF entries processed so far " + lineNo);
                                            //createLog.debug("Executed bulkWrite and the results are ");
                                            //createLog.debug("Annotation Collection-Count of data loaded given below");
                                            annoCollCnt = annoCollCnt + res.insertedCount;
                                            createLog.debug("Anno-"+res.insertedCount);
                                        }).catch((err2) => {
                                            //createLog.debug("Catch-annotation collection stats given below");
                                            if ( 'result' in err2 ) {
                                                annoCollCnt = annoCollCnt + err2.result.nInserted;
                                                createLog.debug("Anno-Catch-"+err2.result.nInserted);
                                            } else {
                                                console.log(`${err2}`);
                                            }
                                            
                                    });
                                    
                                    annoData = [];
                                }
                            }

                        } // Alt All Loop 
                    } 
                    // high ploidy section of code has been removed. We don't have any of these samples to test the related section of code.For this reason, we have taken a backup
                    // Filename : importSample_high_ploidy.js : This has importSample with the high-ploidy code section.
                    // This will be integrated and tested when we have relevant samples.

                } // MultiSample VCF

            } else { // VCF : commented lines
                if (line.match(commentReg) ) {
                    createLog.debug("Unexpected data format present in VCF file. This line will not be processed");
                    createLog.debug(line);
                }
                skippedEntries = skippedEntries + 1;
            }
                
        }); // line input handler

        // close handler
        return new Promise( resolve => {
            rd.on('close', async () => {
                console.log("CLOSE HANDLER");
                createLog.debug("Close Handler ---");
                if ( bulkOps.length > 0 ) {
                    try {
                        var importRes = await varCol.bulkWrite(bulkOps, {'ordered': false });
                        importCollCnt = importCollCnt + importRes.insertedCount;
                        createLog.debug("Close handler-import collection");
                        createLog.debug(importRes.insertedCount);
                    } catch(importErr) {
                        createLog.debug("importErr-catch block");
                        if ( 'result' in importErr ) {
                            importCollCnt = importCollCnt + importErr.result.nInserted;
                            createLog.debug(importErr.result.nInserted);
                        } else {
                            console.log(`${importErr}`);
                        }
                        
                    }
                }
                if ( annoData.length > 0 ) {
                    try {
                        var annoRes = await collection.bulkWrite(annoData, {'ordered': false });
                        annoCollCnt = annoCollCnt + annoRes.insertedCount;
                        createLog.debug("Close handler-anno collection");
                        createLog.debug(annoRes.insertedCount);
                    } catch(annoErr) {
                        createLog.debug("annoErr-catch block");
                        if ( 'result' in annoErr ) {
                            annoCollCnt = annoCollCnt + annoErr.result.nInserted;
                            createLog.debug(annoErr.result.nInserted);
                        } else {
                            console.log(`${annoErr}`);
                        }
                    }
                }
                if ( spanDelUpdates.length > 0 ) {
                    try {
                        var spanDelRes = await varCol.bulkWrite(spanDelUpdates,{'ordered' : false});
                        createLog.debug("Close handler-spanDel");
                        createLog.debug(spanDelRes.insertedCount);
                    } catch(spanUpdErr) {
                        createLog.debug("spanDelErr-catch block");
                        if ( 'result' in spanUpdErr ) {
                            createLog.debug(spanUpdErr.result.nInserted);
                        } else {
                            console.log(`${spanUpdErr}`);
                        }
                        
                    }
                }

                
                statsLog.verbose("Number of lines in the input vcf file are :"+lineNo);
                statsLog.verbose("importCollCount "+importCollCnt);
                statsLog.verbose("annoCollCount "+annoCollCnt);
                statsLog.verbose("Skipped entries "+skippedEntries);
                createLog.debug("Now we can resolve the promise. Await actions defined above have completed");
                createLog.debug("Number of lines in the input vcf file are :"+lineNo);
                await new Promise(resolve => setTimeout(resolve, 5000));
                resolve("Success");
            }); // close handler
        }); // close handler promise definition
    } catch(parseErr) {
        throw parseErr;
    }
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
    console.log("logging line "+line1);
    console.log(data.length);
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

async function updateStatus(dbStat,search,update,pushSt) {
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

// Enable and add additional settings when we create a code version for multi-sample VCF
/*function validate(vcfFile, vcfFileId) {
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
                dataSamples = getSampleNames(data);
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
}*/
// Adapt this function if required when multiple VCF formats are supported
/*async function parseSample(sample, vcfId, vcfFormat, createLog, varCol,batchSize) {
    const getSuccess = new Promise( ( resolve ) => resolve("Success") )
    console.log("Function parseSample ");
    // GATK haplotype caller VCF documentation - https://software.broadinstitute.org/gatk/documentation/article?id=11005
    if (vcfFormat == 'HC') {
        // add logic to check the format of the (g)vcf file
        var chrHash = {};
        // check for the number of samples in VCF/ cohort samples based on VCF
        createLog.debug("********* Check for concurrent calls. Sample parsing done for " + sample);
        try {
            var value = await parseHCSample(sample, vcfId, createLog, varCol,batchSize);
            createLog.debug("Await value received from Promise function parseHCSample is " + value + ' Import Program completed at ' + new Date);
            var stop = new Date();
            createLog.debug("Import Sample Program Completed at "+stop);
            return await getSuccess;
        } catch(retErr) {
            createLog.debug("Error loading final set of data .Promise failed ");
            createLog.debug(retErr);
            console.log(retErr);
            console.dir(retErr,{"depth":null});
            reject(retErr);
        }
    }
} */

