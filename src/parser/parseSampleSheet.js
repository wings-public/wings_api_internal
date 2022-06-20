#!/usr/bin/env node
'use strict';
var path = require('path');
const readline = require('readline');
const fs = require('fs');
//var multer = require('multer');
const argParser = require('commander');
const {logger,loggerEnv} = require('../controllers/loggerMod');
const configData = require('../config/config.js');
const { app:{instance,sampleSheet,centerName,logLoc}, db:{sampleSheetCollection,importStatsCollection} } = configData;
var sampleSheetFormat = require('../config/sampleSheet.json');
//const downloadData = require('../modules/requestFuncs.js').downloadData;
var sampColl = require('../models/sampleSheetModel.js');
const nodemailer = require("nodemailer");

// Test Funcs Start

//const https = require('https');
var urlLibraries = {
    "http:": require('http'),
    "https:": require('https')
};
var url = require('url');

// changes specific to streams backpressure, usage of pipeline
const stream = require('stream');
const util = require('util');
const pipeline = util.promisify(stream.pipeline);
const rename = util.promisify(fs.rename);
// Test Funcs End

(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-s, --sample_sheet <sample_sheet_location>', 'sample sheet location')
        argParser.parse(process.argv);

        var sampleSh = argParser.sample_sheet || sampleSheet;
        console.log(`sampleSh ${sampleSh}`);
        // Create Log File 
        var parsePath = path.parse(__dirname).dir;
        var pid = process.pid;
        //var logFile = path.join(parsePath,'import','log',`sample-sheet-logger-${pid}.log`);
        var logFile = `sample-sheet-logger-${pid}.log`;
        //var logFile = loggerEnv(instance,logFile1);
        
        var createLog = logger('samplesheet',logFile);
        console.log(`logFile is ${logFile}`);
        createLog.debug("Sample Sheet Parsing Started");
        //var db = require('../controllers/db.js');

        // Step 1 
        var arrData = await readFile(sampleSh,createLog);
        //console.log(arrData);
        //console.log(arrData.length);

        if ( arrData.length > 0 ) {
            // Start playing with the array.
            // Step 2 : Perform Validation
            var res = await consolidateValidation(arrData,createLog);
            //console.dir(res,{"depth":null});

            // Step 3 : Process Validation Results
            var res = await processValResults(res,createLog);
            process.exit;
        }
    } catch(err) {
        // Trigger e-mail to the center code admins
        console.log(err);
        process.exit(1);
    } finally {
        console.log("Over to finally section");
        process.exit();
    }
                
}) ();


async function readFile(sampleSh,createLog) {
    var reFile = /\.csv/g;
    var archId = new Date().valueOf();
    var renameFileName = sampleSh + '_archived_'+archId;
    var rd;
    try {

        createLog.debug(`Parsing filename ${sampleSh}`);
        if (sampleSh.match(reFile)) {
            rd = readline.createInterface({
                input: fs.createReadStream(sampleSh),
                //output: process.stdout,
                console: false
            });
            var lineCnt = 0;
            var lineRe = /^#/g;
            var blankLine = /^\s+$/g;
            // lock file and remove write acess for group
           
            //console.log("Collection Object created ");
            var headerArr = [];
            var lineArr = [];
            //console.log("Line Count is *************"+lineCnt);
            rd.on('line', (line) => {
                //if ( ! line.match(lineRe)  && ! line.match(blankLine) ) {
                if ( ! line.match(lineRe) && ! line.match(blankLine)) {
                    createLog.debug(line);
                    lineArr.push(line);
                } else {
                    createLog.debug(line);
		            headerArr.push(line);
		        }
            });

            return new Promise( resolve => {
                rd.on('close',  async () => {
                    //console.log("Reading Data Completed Start Traversing Array");
                    if ( lineArr.length > 0 ) {
                        await rename(sampleSh, renameFileName);
                        createLog.debug("File renamed to "+renameFileName);    
                        var headers = headerArr.join('\n');
                        var res = await writeFile(sampleSh,headers);
                    }
                    //var fstream = fs.createWriteStream(sampleSh);
                    //console.log("Logging header and checking for data being written to the file");
                    //console.log(headers);
                    createLog.debug("Created new file "+sampleSh);
                    resolve(lineArr);
                }); 
            }, reject => {

            });
        } else {
            createLog.debug("sample sheet format not recognized-not csv format*********---------------------");
            throw "sample sheet format not recognized-not csv format";
        }
        
    } catch(err) {
        //console.log("Error $$$$$$$$$$$$$$$$$$");
        console.log(err);
        createLog.debug(err);
        throw err;
    }
}


const consolidateValidation = async (fileArr,createLog) =>
  {
    createLog.debug("consolidateValidation function");
    var valArr = [];
    var patternKey = {};
    var SidIndIDMap = {};
    // It is important to perform promise related operations in a 'for' loop or 'map'.
    // Reason for not traversing performValidation from filehandle. Loop has to be promise aware to perform in the expected sequence to await response from the initiated operation
    for (let i = 0; i < fileArr.length ; i++ ) {
        var line = fileArr[i];
        //console.log(line);
        //var res = await performValidation(line);
        //console.log (i, res)
        var uDateId = new Date().valueOf() + i;
        var valResult = await performValidation(line,uDateId,createLog);
        //console.log("Data sent by Validator- Start processing these information");
        //console.dir(valResult);
        if ( valResult['verify_key']) {
            var key = valResult['verify_key'];
            //console.log("KEY is "+key);
            if ( key in patternKey ) {
                //console.log("Logging duplicate message");
                // same key combination during the same import run will not be allowed. Reject this entry
                var errLog = [];
                var msg = `Error-Detected duplicate entry for the same SampleLocalID#IndLocalID#AssemblyType#fileType ${key}`;
                errLog.push(msg);
                valResult['type'] = "Error";
                valResult['msg'] = errLog;
            } else {
                // just store this entry in patternKey
                patternKey[key] = 1;
                //console.log("Logging additional log data here ");
                //console.dir(valResult,{"depth":null});
                if ( valResult['SampleLocalID']) {
                    var sID = valResult['SampleLocalID'];
                    var indID = valResult['IndLocalID'];
                    //console.log(`Current data ${sID} ${indID}`);
                    if ( sID in SidIndIDMap ) {
                        // same sample local ID but different individual ID
                        var storedInd = SidIndIDMap[sID];
                        //console.log(`Stored ${sID} ${storedInd}`);
                        if ( indID != storedInd ) {
                            //console.log(`Logging-Mentioned SampleLocalID ${sID} was already added for Individual ${storedInd}. Add a different SampleLocalID for ${indID}`);
                            var errLog = [];
                            var msg = `Mentioned SampleLocalID ${sID} was already added for Individual ${storedInd}. Add a different SampleLocalID`;
                            errLog.push(msg);
                            valResult['type'] = "Error";
                            valResult['msg'] = errLog;
                        } 
                    } else { 
                        //console.log("No information on this sample ID. Proceed further");
                        SidIndIDMap[sID] = indID;
                    }
                }
            }
        }
        createLog.debug("Logging validation Results");
        createLog.debug(valResult);
        
        //console.log(i);
        //console.log(valResult);
        valArr.push(valResult);
    }
    //return 'done'
    //console.dir(valArr,{"depth":null});
    return valArr;
  }

  async function performValidation(line,id,createLog) {
    try {
        createLog.debug("performValidation Started for  line "+line);
        var msgLog = {};
        var dataFields = [];
        var allFields = [];
        var optFields = [];
        var optObj = {};
        var minCnt = 0;
        var validation = {};
        if ( sampleSheetFormat['required_fields']) {
            dataFields = sampleSheetFormat['required_fields'];
            minCnt = dataFields.length;
            //console.log(dataFields);
        }

        if ( sampleSheetFormat['optional_fields']) {
            optFields = sampleSheetFormat['optional_fields'];
            for ( var idx in optFields ) {
                var key = optFields[idx];
                optObj[key] = 1;
            }
            allFields = dataFields.concat(optFields);
        }

        if ( sampleSheetFormat['validation'] ) {
            validation = sampleSheetFormat['validation'];
        }
        
        var data = []; 
        data = line.split(',');
        //console.log("CSV LINE LENGTH "+data.length);
        //console.log("MINIMUM Count "+minCnt);
        // looks like we have the required fields.we will perform data validation
        var errLog = [];
        var warnLog = [];
        var mainDoc = {};
        var doc = {};
        var fieldMap = {};
        if ( data.length >= minCnt ) {
            doc['_id'] = id;
            for ( var fieldIdx in data ) {
                // required_fields in sampleSheet.json has been defined in the same order of csv. This way, we could map the field name in json and csv field value. We have to update sampleSheet.json if there is any order shift or any fields are added
                var jsonDataField = allFields[fieldIdx]; // field name
                var fieldVal = data[fieldIdx]; // value present in csv
                var csvFieldName = validation[jsonDataField]['csv_field']; // check and add undefined validation here
                //console.log("*****************************************");
                fieldMap[jsonDataField] = fieldVal;
                //console.log(`jsonDataField : ${jsonDataField}`);
                //console.log(`fieldVal : ${fieldVal}`);
                //console.log(`csvFieldName : ${csvFieldName}`);

                if ( validation[jsonDataField]['data_type'] ) {
                    var dType = validation[jsonDataField]['data_type'];
                    if ( dType == "location" ) {
                        var locPath;
                        locPath = path.join(logLoc,'import','samples');

                        // check if it is http based url or it is a server location file with /
                        var urlRe = /https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)/i;
                        var serverRe = /^[\/a-zA-Z_0-9-\.]+\.(vcf|vcf.gz|bam|idx|bai|bam.gz|gvcf|gvcf.gz|fastq|cram)$/i;

                        if ( fieldVal.match(urlRe) ) {
                            // download and check.if not valid, set to invalid
                            try {
                                    var res1 =  await downloadData(fieldVal,locPath);
                                    //console.log("----------------Received Response from download data *************** ");
                                    //console.log(res1);
                                    doc[csvFieldName] = res1;
                                    if ( csvFieldName.toUpperCase() == "FILELOCATION" ) {
                                        //console.log("**********File Source type is URL\n");
                                        doc['FileSourceType'] = 'url';
                                    } 
                                 } catch(err) {
                                    //console.log("%%%%%%%%%%%% Error received in download data "+err);
                                    if ( ! optObj[jsonDataField] ) {
                                        err = " Field Name "+csvFieldName + ' '+err;
                                        errLog.push(err);
                                    } else {
                                        // optional field. just add a warning
                                        var warn = `FieldName:${csvFieldName} <b>Warning:</b>${serverFileLoc} is not accessible.Invalid Location.`;
                                        warnLog.push(warn);
                                    }
                                }
                        } else if ( fieldVal.match(serverRe) ) {
                            // check if file path is accessible. if not, set to invalid
                            //var basename = path.basename(fieldVal);
                            //var serverFileLoc = path.join(locPath,basename);
                            var serverFileLoc = fieldVal;
                            //doc[csvFieldName] = fieldVal;
                            if ( ! fs.existsSync(serverFileLoc) ) {
                                if ( ! optObj[jsonDataField] ) {
                                    var msg = `FieldName:${csvFieldName} <b>Error:</b>${serverFileLoc} is not accessible.Invalid Location.`;
                                    errLog.push(msg);
                                } else {
                                    var msg = `FieldName:${csvFieldName} <b>Warning:</b>${serverFileLoc} is not accessible.Invalid Location.`;
                                    warnLog.push(msg);
                                }
                            } else {
                                //console.log("Logging ###### test infor here \n");
                                //console.log(csvFieldName);
                                doc[csvFieldName] = fieldVal;
                                if ( csvFieldName.toUpperCase() == "FILELOCATION" ) {
                                    //console.log("***** File source type is Server\n");
                                    doc['FileSourceType'] = 'server';
                                } 
                            }
                        } else { // this would handle file-name convention issues 
                            if ( ! optObj[jsonDataField] ) {
                                var msg = `FieldName:${csvFieldName} <b>Error:</b>Invalid Location Path`;
                                errLog.push(msg);
                            } else {
                                var msg = `FieldName:${csvFieldName} <b>Warning:</b>Invalid Location Path`;
                                warnLog.push(msg);
                            }
                        }
                    } else if ( dType == "date" ) {
                        //doc[csvFieldName] = fieldVal;
                        var retVal = isDate(fieldVal);
                        if ( ! retVal ) {
                            if (! optObj[jsonDataField]) {
                                var msg = `FieldName:${csvFieldName} <b>Error:</b>${fieldVal} Invalid Date format`;
                                errLog.push(msg);
                            } else {
                                var msg = `FieldName:${csvFieldName} <b>Warning:</b>${fieldVal} Invalid Date format`;
                                warnLog.push(msg);
                            }
                        } else {
                            doc[csvFieldName] = fieldVal;
                        }
                    } else {
                        //console.log(`Data Type defined in JSON ${dType}`);
                        //console.log("CSV Entry Data type "+typeof(fieldVal));
                        var re;
                        doc[csvFieldName] = fieldVal;
                        if ( dType == "char" ) {
                            re = /^[a-z\s]+$/i;
                        } else if ( dType == "varchar" ) {
                            re = /^[\w-]+$/i;
                        } else if ( dType == "number" ) {
                            re = /^\d+$/i;
                        } else if ( dType = "freetext") {
                            re = /^[\w-\s]+$/i;
                        }
                        //First check: Check the data type
                        if ( ! fieldVal.match(re) ) {
                            // not an optional field. validation has to pass.
                            if (! optObj[jsonDataField]) {
                                var msg = `FieldName:${csvFieldName} Error:Invalid Datatype`;
                                errLog.push(msg);
                                createLog.debug(msg);
                            } else { 
                                // optional field. set only warning message.
                                var msg = `FieldName:${csvFieldName} Warning:Invalid Value`;
                                warnLog.push(msg);
                                createLog.debug(msg);
                            }
                        } else {
                            if ( jsonDataField == "indLocalID" ) {
                                var filter = {};
                                // sLocalID+indLocalID+fileType+assemblyType -> has to be unique within the same import run.Do not check in database.Create a unique hash and compare against that.
                                // sLocalID assigned to a indLocalID cannot be assigned to another indLocalId
                                // check if this sLocalID is present in DB , fetch the record. Compare this indLocalID with the indLocalID present in the record. Raise error, if they differ.

                                // csfFieldName fieldVal
                                var currSLocalID = doc['SampleLocalID'] || '';
                                filter['SampleLocalID'] = currSLocalID;
                                createLog.debug("Logging filter below");
                                // search filter using sample ID in database
                                var sampDoc = await sampColl.findOne(filter);

                                //console.log("Logging document present in database");
                                //console.log(sampDoc);
                                //console.log("Logging search filter here");
                                //console.log(filter);

                                // if entry already present in database                     
                                if ( (sampDoc) && (sampDoc['IndLocalID'] )) {
                                    var dbIndID = sampDoc['IndLocalID'];
                                    if ( dbIndID != fieldVal ) {
                                        var msg = `FieldName-DB:SampleLocalID <b>Error:</b>Mentioned SampleLocalID ${currSLocalID} was already added for Individual ${dbIndID}. Add a different SampleLocalID`;
                                        errLog.push(msg);
                                    } 
                                } else {
                                    doc[csvFieldName] = fieldVal;
                                }
                            }
                        }
                    }
                } else if ( validation[jsonDataField]['allowed_values'] ) {
                    //doc[csvFieldName] = fieldVal;
                    var val = validation[jsonDataField]['allowed_values'];
                    var chk = 0;
                    for ( var vidx in val ) {
                        if ( fieldVal.toUpperCase() == val[vidx].toUpperCase() ) {
                            chk = 1;
                        }
                    }
                    if ( !chk ) {
                        //console.dir(fieldMap,{"depth":null});
                        if (  jsonDataField == "assemblyType") {
                            var fastqRe = /^fastq/i;
                            // fastq is build independent
                            if ('fileType' in fieldMap ) {
                                var ftVal = fieldMap['fileType'];
                                //console.log("Logging file type value "+ftVal);
                                if ( ftVal.match(fastqRe) ) {
                                    doc[csvFieldName] = "na";
                                } else {
                                    var msg = `FieldName:${csvFieldName} <b>Error:</b>Unsupported Option.Choose within ${val}`;
                                    errLog.push(msg);
                                }
                            }
                        } else {
                            var msg = `FieldName:${csvFieldName} <b>Error:</b>Unsupported Option. Choose within ${val}`;
                            errLog.push(msg);
                        }
                    } else {
                        if ( fieldVal.toUpperCase() == "GRCH37" ) {
                            fieldVal = "hg19";
                        } else if ( fieldVal.toUpperCase() == "GRCH38" ) {
                            fieldVal = "hg38";
                        }

                        doc[csvFieldName] = fieldVal;
                    }
                } else if (validation[jsonDataField]['dependency'] ) {
                    if ( doc['SeqTypeName'].toUpperCase() == "PANEL") {
                        var val = validation[jsonDataField]['dependency'];
                        var chk = 0;
                        for ( var vidx in val ) {
                            if ( fieldVal.toUpperCase() == val[vidx].toUpperCase() ) {
                                chk = 1;
                            }
                        }
                        if ( !chk ) {
                            var msg = `FieldName:${csvFieldName} <b>Error:</b>Unsupported Option. Choose within ${val}`;
                            errLog.push(msg);
                        } else {
                            doc[csvFieldName] = fieldVal;
                        } 
                    }

                }
            }
            msgLog['line'] = line;
            if (warnLog.length > 0 ) {
                msgLog['Warning'] = warnLog;
            }
            if ( errLog.length > 0 ) {
                msgLog['type'] = "Error";
                msgLog['msg'] = errLog;
            } else {
                //console.log("*********** Logging a valid document entry and checking ***********");
                //console.log(doc);
                //var utcTime = new Date().toUTCString();
                doc['statMsg'] = 'sample sheet entry loaded';
                //doc['processed'] = 0;
                //doc['loadedTime'] = utcTime;
                mainDoc['document'] = doc;
                msgLog['type'] = "Valid";
                msgLog['doc'] = mainDoc;
                msgLog['verify_key'] = doc['SampleLocalID']+'#'+doc['IndLocalID']+'#'+doc['AssemblyType']+'#'+doc['FileType'];
                msgLog['SampleLocalID'] = doc['SampleLocalID'];
                msgLog['IndLocalID'] = doc['IndLocalID'];
            }
            return msgLog;
        } else {
            // log error: required number of fields not present 
            var msg = "Required Number of fields not present";
            errLog.push(msg);
            msgLog['msg'] = errLog;
            msgLog['type'] = "Error";
            msgLog['line'] = line;
            createLog.debug("Required number of fields not present");
            return msgLog;
        }
    } catch(err) {
        console.log("Error here is this **********");
        console.log(err);
        createLog.debug(err);
        throw err;
    }
}

const writeFile = (path, data, opts = 'utf8') =>
  new Promise((resolve, reject) => {
    fs.writeFile(path, data, opts, (err) => {
      if (err) reject(err)
      else resolve()
    })
})

async function processValResults(valResultsArr,createLog) {
    try {
        var mailQueue = [];
        var bulkOpsQueue = [];

        var smtpHost = process.env.SMTP_SERVER || 'smtp.uantwerpen.be';
        var smtpPort = process.env.SMTP_PORT || 25;
        var smtpFrom = process.env.SAMPLE_SHEET_FROM_MAIL || "nishkala.sattanathan@uantwerpen.be";
        var smtpTo = process.env.SAMPLE_SHEET_TO_LIST || "nishkala.sattanathan@uantwerpen.be";

        let transporter = nodemailer.createTransport({
            host: smtpHost, // smtp server
            port: smtpPort,
            auth: false,
            tls: {
                  // if we are doing from the local host and not the actual domain of smtp server
                  rejectUnauthorized: false
                 }
          });


        /*let transporter = nodemailer.createTransport({
            host: "smtp.uantwerpen.be", // smtp server
            port: 25,
            auth: false,
            tls: {
                  // if we are doing from the local host and not the actual domain of smtp server
                  rejectUnauthorized: false
                 }
          });*/

        createLog.debug("processValResults function");
        for ( let j = 0; j < valResultsArr.length; j++ ) {
            var mailErr = {};
            var arrIdxVal = valResultsArr[j];
            if ( arrIdxVal['Warning'] ) {
                mailErr['Warning'] = arrIdxVal['Warning'];
            }
            if ( arrIdxVal['type'] == "Error"  ) {
                mailErr['line'] = arrIdxVal['line'];
                mailErr['msg'] = arrIdxVal['msg'];
                //mailQueue.push(arrIdxVal);
            } else if ( arrIdxVal['type'] == "Valid" ) {
                mailErr['line'] = arrIdxVal['line'];
                bulkOpsQueue.push({ "insertOne": arrIdxVal['doc'] });
            }
            mailQueue.push(mailErr);
        }

        if ( bulkOpsQueue.length > 0 ) {
            createLog.debug(bulkOpsQueue);
            var bRes = await sampColl.bulkWrite(bulkOpsQueue);
        }

	    var mailMsg = "<h2>Validation Results for "+centerName+" Center Sample Sheet scanned at "+new Date()+"</h2>";
        for ( var k = 0; k < mailQueue.length; k++ ) {
            var queueItem = mailQueue[k];
            //console.log("Logging for k "+k);
            //console.log(queueItem);
            var line = queueItem['line'];
            var msgs = "<ol>";
            var setMsg = "<b>Conclusion</b>: <b>Warning-</b>Sample Sheet Entry <b>will be loaded</b>. Fields having Validation Warnings will be skipped<br>";
            if ( queueItem['Warning'] ) {
                msgs = msgs + '<li>' + queueItem['Warning'].join('</li><li>') + '</li>';
            }
            if ( queueItem['msg']) {
                msgs = msgs + '<li>' + queueItem['msg'].join('</li><li>') + '</li>';
                setMsg = "<b>Conclusion</b>: <span style='color:red'><b>Error-</b></span>Sample Sheet Entry <b>will not be loaded</b>.Please fix  mentioned errors in the sample sheet entry and add it to the Sample Sheet<br>";
            }
            mailMsg = mailMsg + "<h3>Sample Sheet Entry : </h3>";
            mailMsg = mailMsg + line + "<br>";
            mailMsg = mailMsg + "<u><i>Validation Warnings and Errors are shown below</i></u>" + "<br>";
            mailMsg = mailMsg + msgs + "</ol><br>";
            mailMsg = mailMsg + setMsg + "<br>";
            //console.log("Sample Sheet Entry : "+line);
            //console.log(line);
            //console.log("\n");
            createLog.debug("                  Validation Errors below                    ");
            //console.log("                  Validation Errors below                    ");
            //console.log("------------------------------------------------------------------------------------------");
            //console.log(msgs);
            createLog.debug(msgs);
            //console.log("\n");
        }

        //console.log(mailMsg);
        createLog.debug(mailMsg);

          let info = await transporter.sendMail({
            from: smtpFrom, // sender address
            to: smtpTo, // list of receivers
            subject: "WiNGS "+centerName+" Center Sample Sheet Validation Results ", // Subject line
            //subject: "WiNGS Uantwerp Center Sample Sheet Validation Results ", // Subject line
            //text: mailMsg, // plain text body
            html: mailMsg // html body
          });

        console.log(info);
        createLog.debug("Logging the info of the mail message transporter");
        createLog.debug(info);
        createLog.debug("Message sent ID "+info.messageId);
        //console.log("Message sent: %s", info.messageId);
        return "success";
    } catch(err) {
        throw err;
    }
}


function downloadData(reqUrl,serverLoc) {
    return new Promise ( (resolve,reject) => {
        console.log(`Received request for Sample ${reqUrl}`);
        var parsed = url.parse(reqUrl);
        //console.dir(parsed,{"depth":null});
        var lib = urlLibraries[parsed.protocol || "http:"];
        var options = { 'agent' : false };
        //console.log("lib is "+parsed.protocol);
        if ( parsed.protocol == "https:" ) {
            options = {'rejectUnauthorized': false, 'requestCert': true, 'agent': false };
        }
        const req = lib.get(reqUrl,options,async(response) => {
            if ( response.statusCode < 200 || response.statusCode >= 300 ) {
                var msg = reqUrl+" URL-Not Valid. Response Status Code-"+response.statusCode;
                console.log("MSG   "+msg);
                reject(msg);
            } else {
                //console.log("Check the response received from the request URL");
                //console.log("************* Logging URL PARSE DATA *****************");
                //console.log(parsed);
                //console.log(path.basename(parsed.href));
                //console.log("************* Logging URL PARSE DATA *****************");

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
                    reqFileName = path.basename(url.parse(reqUrl).path) || tmpFile;
                }

                console.log(`reqFileName ${reqFileName}`);
                filename = path.join(serverLoc,reqFileName);
                console.log("Downloading data to the file "+filename);
                try {
                    await pipeline(response,fs.createWriteStream(filename));
                    resolve(filename);
                } catch(err) {
                    reject(err);
                }
            }
        });
                
        req.on('error' , (err) => {
            console.log("Error in reading request data from Request URL");
            console.log(err)
            //createLog.debug("Error in reading data from Request URL "+reqUrl);
            reject(err);
        });
        req.end();
    });
}

function isDate(str) {    
    var parms = str.split(/[\.\-\/]/);
    var yyyy = parseInt(parms[0],10);
    var mm   = parseInt(parms[1],10);
    var dd   = parseInt(parms[2],10);
    var date = new Date(yyyy,mm-1,dd,0,0,0,0);
    return mm === (date.getMonth()+1) && dd === date.getDate() && yyyy === date.getFullYear();
}
