#!/usr/bin/env node
'use strict';

const dockerImage = 'quay.io/biocontainers/annotsv:3.4--py312hdfd78af_1'; // Replace with the name of your Docker image
const annotationDir = '/home/ben/AnnotSV_annotations'; // Replace with the path to your annotation data

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
const { db : {dbName,importCollection3,importStatsCollection,sampleSheetCollection}, app:{instance,logLoc} } = configData;
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

    var logFile = `importSampleSV-${vcfId}-${process.pid}.log`;
    var statsLogFile = `importStatsSV-${vcfId}-${process.pid}.log`;
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
        if ( assemblyType == "GRCh38" ) {
            varCol = require('../models/variantDataModelGRCh38SV.js');
            console.log("Collection Object created ");
            importCollection = importCollection3;
        }
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
                
                rd = await createZipInterface(filename,vcfId,createLog);
            } else {
                rd = await createZipInterface(sample,vcfId,createLog);
            }    
            //var rd = await createFileHandle(sample,vcfId,createLog);
            createLog.debug("handle returned from createFileHandle function "+rd);
            var val = await parseSVSample(rd,sample, vcfId,createLog, statsLog,varCol,batchSize);
            var stop = new Date();
            console.log("What was the return value sent by the above function ?");
            createLog.debug("Import Sample Program Completed at "+stop);
            console.log("checking value of val afer parsingSV");
            
            if ( val == "Success") {
                console.log("do we enter here SUCCESS");
                
                createLog.debug("Received value "+val);
                createLog.debug("*********** Right time to close the connection ");
                createLog.debug(" ************ TRACK CHECK & CLOSE ************ ");
                process.exit(0);
            }
        }
    } catch(err) {
        console.log("Catch Error is "+err);
        var errMsg = 'Import SV Sample Stage-failed due to download or parse errors '+err;
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
                var serverRe = /^[\/a-zA-Z_0-9-\.]+\.(vcf|vcf.gz)$/i;
                if ( !reqFileName.match(serverRe) ) {
                    createLog.debug("VCFLocation: Invalid URL.Downloaded URL data did not match supported FileExtensions: .vcf or .vcf.gz");
	                reject("VCFLocation: Invalid URL.Downloaded URL data did not match supported FileExtensions: .vcf or .vcf.gz ");
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

async function parseSVSample(rd,sample, vcfId, createLog,statsLog,varCol,batchSize) {
    //return new Promise((resolve, reject) => { (async function)
    try {
        createLog.debug("Function parseSVSample called for parsing ----- " + sample);
        createLog.debug("Sample ID ----- " + vcfId);
        //var rd;
        var basePath = path.parse(__dirname).dir;
        var jsonP = path.join(basePath,'config','chrMap.json');

        var mapJson = fs.readFileSync(jsonP);
        var chrData = JSON.parse(mapJson);
        var chrMap = chrData['chromosomes'];

        var variantHash = {};
        var printHeaderNo = 1;
        var sNames = [];
        var sampleName;
        var sData = {};

        var insertCtr = 0;
        var bulkOps = [];


        var pid = process.pid;
        //var vcfIdList = vcfId.split('|');
        createLog.debug("Process ID is " + pid);
        var lineNo = 0;
        var importCollCnt = 0;
        var annoCollCnt = 0;
        var skippedEntries = 0;
        var uID;
       
        //CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	P7_05092022_PAK16836_MG	SAMPLE	P7_05092022_PAK16836_MG_1
        rd.on('line', function (line) { // line input handler
            var nonVariant = 0;
            var vType = "";
            var nonRef = "";
            lineNo++;

            //console.log("Line "+line);
            var sampRe = /^#CHROM/g; //Detecth where starts
            
            if (line.match(sampRe)) {
                sNames = getSampleNames(line); // list of samples present in this VCF file

                //console.log("Sample Names");
                //console.log(sNames);
                // traverse the sample names, generate sample ID. 
                // Sample ID generation : check for exists sample names in mongo collection and generate sample ID
                var cntIdx = 0;
                for (var sIdx in sNames) {
                    var sNameTmp = sNames[sIdx];
                    //sData[sNameTmp] = vcfIdList[cntIdx];
                    sData[sNameTmp] = vcfId;
                    cntIdx++;
                }
                if (sNames.length > 1) {
                    //console.log("------------------ Multi-Sample VCF ------------------ ");
                }
                sampleName = sNames[0]; //pickFirst sample
            } 
            //check presence vec vec sv then if it's there add for every sample the sv
            //  
            
            var commentReg = /^[^#]/g;
            if (line.match(commentReg) && line.split('\t').length > 9 ) { // VCF line how to determin if its a good line
                
                var data = line.split('\t');
                //createLog.debug(data);
                ////CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	P7_05092022_PAK16836_MG
                var dataFields = ['chr', 'startPos', 'svid', 'refall', 'altall', 'quality', 'filter', 'infoStr', 'formatStr', 'sampleStr'];

                //var dataFields = ['chr', 'startPos', 'endPos', 'endchr', 'svid', 'refall', 'altall', 'qualitu', 'filter', 'infoStr', 'formatStr', 'sampleStr'];

                /* NON_REF SAMPLE
                #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE
                chr1	10305	Sniffles2.DEL.1627S0	accccaaccccaaccccaaccctaacccctaaccctaaccctaaccctaccctaaccctaaccctaaccctaac	N	51	PASS	
I               MPRECISE;SVTYPE=DEL;SVLEN=-74;END=10379;SUPPORT=3;COVERAGE=0,0,13,15,16;STRAND=+-;AF=0.333;PHASE=NULL,NULL,3,3,FAIL,FAIL;STDEV_LEN=23.516;STDEV_POS=19.425	
                GT:GQ:DR:DV	0/1:13:6:3*/
                var fieldVal = {};
                
                // expected number of fields for this format : 10
                for (var idx in dataFields) {
                    var field = dataFields[idx];
                    // chromosome field ----------------
                    if (idx == 0) {

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
                
                //extract GT
                //console.log("Field Value is " + fieldVal['formatStr'].indexOf('GT')); 
                //var gt = fieldVal['sampleStr'].substring(fieldVal['formatStr'].indexOf('GT'), (fieldVal['formatStr'].indexOf(':')+1)); 
                
                var gt = extractGTSubstring(fieldVal['formatStr'],fieldVal['sampleStr']);
                //console.log(gt)
                //take position of gt in string format strig
                //go to sampleStr extract same position pass to database
                
                // ********************  Info **************************** //
                //IMPRECISE;SVTYPE=DEL;SVLEN=-74;END=10379;SUPPORT=3;COVERAGE=0,0,13,15,16;STRAND=+-;AF=0.333;PHASE=NULL,NULL,3,3,FAIL,FAIL;
                //STDEV_LEN=23.516;STDEV_POS=19.425	

                var infoStr = fieldVal['infoStr'];
                
                var infoSt = infoStr.split(';');
                var infoHash = {};
                for (var infoIdx in infoSt) {
                    var infoVal = infoSt[infoIdx];
                    //var re1 = /([^=]*)=([^=]*)/;
                    //var res = re1.exec(infoval);
                    var infoData = infoVal.split('=');
                    if (infoData.length>1){
                        var text1 = infoData[0];
                        //console.log("END-"+endPos)
                        var text2 = infoData[1];
                        //console.log("END-"+endPos)
                        infoHash[text1] = text2;
                    }
                    else{
                        var text1 = infoData[0];
                        infoHash[text1] = text1;
                    }
                    //console.log("Text1 "+text1+" Text2 "+text2);
                }
                //console.dir(infoHash,{depth:null});

                // MQ BaseQRankSum MQRankSum ReadPosRankSum SB QD FS VQSLOD  -- Fetch from infoHash 
                var precise = infoHash['PRECISE'];
                var imprecise = infoHash['IMPRECISE'];
                var svtype = infoHash['SVTYPE'];
                var svlen = infoHash['SVLEN'];
                var endPos = infoHash['END'];
                var svcallers = infoHash['SVCALLERS'];
                //console.log(svcallers)
                var support = infoHash['SUPPORT'];
                // Manage different SVs BND DEL INS
                var re = /chr/g;
                var endChr = infoHash['CHR2'];
                var endvcfChr = infoHash['CHR2'];
                if (endChr && endChr.match(re)) {
                    endChr = infoHash['CHR2'].replace(re, '');       
                }
                else{
                    endChr = chr;
                    endvcfChr = vcfChr;
                }

                if ( chrMap[endChr]) {
                    endChr = chrMap[endChr];
                    infoHash['CHR2'] = endChr;   
                }
                
                fieldVal['end_vcf_chr'] = endvcfChr;

                var refAll = fieldVal['refall'];
                var startPos = fieldVal['startPos'];
                
                if (stopPos === parseInt(startPos)){
                    if (svlen === '.'){
                        svlen = 0;
                    } 
                    svlen = Math.abs(parseInt(svlen));
                    stopPos = stopPos //+ parseInt(svlen)

                }
                svlen = Math.abs(parseInt(svlen));
                var sId = sData[0];
                sId = parseInt(vcfId);

                var stopPos = parseInt(endPos);
                var altAll = fieldVal['altall'];

                //stopPos BND
                //add or TRA
                if (svtype === 'BND' || svtype === 'TRA'){
                    var chr_val = fieldVal['end_vcf_chr'];
                    var regex_stopPos = new RegExp( '.*?' + chr_val + ':(.*?)' + '\\[', 'g');
                    var new_endPos = line.matchAll(regex_stopPos);
                    for (const ep of new_endPos){
                        if (ep[1].length !== 0 ){
                            
                            stopPos = startPos;
                        }
                        else{
                            
                            stopPos = startPos;
                        }
                    }
                    //console.log(new_endPos);
                    svlen = 0;
                    svtype = 'TRA';
                    stopPos = startPos;
                }
                //if (refAll.length > 1) {
                    //console.log("Verify for a length greater than 2");
                //    stopPos = parseInt(startPos) + refAll.length - 1;
                    //console.log("Stop Pos is " + stopPos);
                //}

                

                // -------------------------- Stretch -------------------------------- //
                var formatStr = fieldVal['formatStr'];
                var formats = formatStr.split(':');
                
                //index of ID in bullshit string extract all for loop and substring for tool (SURVIVOR)
                var regex = new RegExp('.*?:' + svtype + ':(.*?)\.' + svtype + '.*?', 'g');
                //var matches = line.matchAll(/.*?:DEL:(.*?)\.DEL.*?/g);
                var matches = line.matchAll(regex);
                var fintools = ''
                //CHECK IF THIS WORKS
                for (const match of matches) {
                    fintools += match[1] + ', '
                    //console.log(match[1]);
                    //console.log("INDEX" +match.index)
                }
                
                //console.log(svcallers);
                if (fintools.length === 0 &&  (typeof svcallers == 'undefined') ){
                    var svtool = fieldVal['svid'].substring(0, fieldVal['svid'].indexOf('.')); 
                    //console.log(svtool);
                }
                else{
                    //console.log('ciao');
                    if (fintools !== '') {
                        var svtool = fintools;
                        //console.log('ciao1');
                        console.log(fintools);
                    }
                    else{
                        var svtool = svcallers;
                    }
                    
                }
                var printHeader = "";
                var printString = "";
                var fieldHeaders = "";
                var uniqueKey = sId + '-' + fieldVal['chr'] + '-' + startPos + '-' + stopPos + '-' + fieldVal['svid'] + '-' + svtype + '-' + svlen  ;
                var annotationKey = fieldVal['chr'] + '-' + startPos + '-' + stopPos + '-' + svtype +  '-' + refAll + '-' + altAll;
                
                
                
                
                if (printHeaderNo == 1) {
                    printHeader = 'Sample_Id' + '|' + 'printCombination' + '|' + 'var_type' + '|' + 'Chr' + '|' + 'Start_Position' + '|' + 'End_Position' + '|' + 'SVLEN' + '|' + 'End_Chr' + '|' + 'Ref_All' + '|' + 'Alt_All' + '|'  + 'filter' + '|'  + 'gt' ;
                    printHeaderNo = 0; // Reset to 0
                }
                printString = sId + '|' + uniqueKey + '|' + annotationKey + '|' + fieldVal['svid'] + '|' + svtype +  '|' + svtool +  '|' + fieldVal['vcf_chr'] + '|' + fieldVal['chr'] + '|' + startPos + '|' + stopPos + '|' + svlen + '|' + fieldVal['end_vcf_chr'] + '|' + endChr + '|' + refAll + '|' + altAll +   '|' + fieldVal['filter'] +   '|' + gt;
                fieldHeaders = ['fileID', '_id', 'var_key', 'svid', 'sv_type', 'svtool', 'vcf_chr', 'chr', 'start_pos', 'stop_pos','sv_len' , 'end_vcf_chr','end_chr', 'ref_all', 'alt_all', 'filter', 'gt'];
                
                if (printString != "") {
                    insertCtr++;
                    //console.log("Check for the document sent to createDoc function");
                    //console.log(printString);
                    var doc = createDoc(fieldHeaders, printString, insertCtr, pid); // variantSchema
                   
                    bulkOps.push({ "insertOne": doc });

                    
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
                                console.log(`${err1}`);
                            }
                            
                            //createLog.debug("Catch-Length of bulk Array "+bulkOps.length);
                            //createLog.debug(err,{"depth":null});
                        });
                        bulkOps = [];
        
                    }
                }
                ////////////// Multiple Sample Specific parsing to parse Genotype fields /////////////////////////

             

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
                
                statsLog.verbose("Number of lines in the input vcf file are :"+lineNo);
                statsLog.verbose("importCollCount "+importCollCnt);
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
// For initial version,we are considering only haplotype caller based vcf files
// resolve/reject a promise when the VCF file parsing is completed



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

function extractGTSubstring(formatStr, sampleStr) {
    const gtIndex = formatStr.indexOf('GT');
    
    if (gtIndex === -1) {
      // 'GT' not found in the string
      return null;
    }
  
    // Find the first colon after 'GT'
    const firstColonIndex = formatStr.indexOf(':', gtIndex);
  
    // If there's no colon after 'GT', extract to the end of the string
    if (firstColonIndex === -1) {
      return sampleStr.substring(gtIndex);
    }
  
    // Extract the substring from 'GT' to the position after the first colon
    return sampleStr.substring(gtIndex, firstColonIndex + 1);
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

