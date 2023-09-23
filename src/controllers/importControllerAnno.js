#!/usr/bin/env node
'use strict';
const spawn  = require('child_process');
//const runningProcess = require('is-running');
var path = require('path');
const promisify = require('util').promisify;
const { createReadStream, createWriteStream, stat ,unlink,existsSync} = require('fs');
var stats = promisify(stat);
//var multer = require('multer');
const argParser = require('commander');
var loggerMod = require('../controllers/loggerMod');
const configData = require('../config/config.js');
const { app:{instance,logLoc}, db:{importStatsCollection} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
const getAnnoApiToken = require('../controllers/annotationController.js').getAnnoApiToken;
const triggerAnnotationStreamPipeline = require('../controllers/annotationController.js').triggerAnnotationStreamPipeline;
const getAnnotationStatus = require('../controllers/annotationController.js').getAnnotationStatus;
const downloadData = require('../controllers/annotationController.js').downloadData;
const verifyCS = require('../controllers/annotationController.js').verifyCS;
const updateAnnotations = require('../controllers/annotationController.js').updateAnnotations;

//var db = require('../controllers/db.js');
(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-vcf, --vcf_file_id <id1>', 'vcf file ID')
            .option('--import_id, --import_id <uDateId>', 'import id for the request')
            .option('-p, --pid <pid>', 'batch size for bulkUpload')
            .option('-m, --anno_type <VEP or CADD>', 'type of annotation')
            .option('-f, --field_anno <field>', 'annotation field type')
            .option('-a, --assemblyType <assemblyType>', 'assembly type or reference build used')
        argParser.parse(process.argv);

        var fileId = argParser.vcf_file_id;
        var pid = argParser.pid;
        var assemblyType = argParser.assemblyType;
        var uDateId = parseInt(argParser.import_id);
        
        console.log("importControllerAnno.js");
        console.log(`fileId:${fileId} uDateId:${uDateId} assemblyType:${assemblyType}`);
        // annotation type - high level (VEP or CADD)
        var annoType = argParser.anno_type;
        // comma separated field type
        var annoFields = argParser.field_anno;

        if ( assemblyType == "hg19" ) {
            assemblyType = "GRCh37";
        } else if ( assemblyType == "hg38" ) {
            assemblyType = "GRCh38";
        }
        
        pid = parseInt(pid);

        // Level 1 Processing
        var parsePath = path.parse(__dirname).dir;

        //var logFile = path.join(parsePath,'import','log',`import-controllers-logger-${fileId}-${pid}.log`);
        var logFile = `import-control-reanno-logger-${fileId}-${pid}.log`;
        var createLog = loggerMod.logger('import',logFile);

        // IMPORT

        var db = require('../controllers/db.js');
                
        // Existing Annotations
        // may be not required : check
        // useful if the variant already has updated fields. ( add criteria in script )
        // for now : comment the below section of code

        var annoScript = path.join(parsePath,'import','updateExistingAnnotations.js');
        createLog.debug("Annotation Script Path is now "+annoScript);
        var subprocess2 = spawn.fork(annoScript, ['--sid',fileId,'--assembly', assemblyType] );
        var procPid = subprocess2.pid;
        createLog.debug("Annotation Update Process now trigerred with process ID "+procPid);
        await closeSignalHandler(subprocess2);

        //var status = {'status' : 'Update Existing Annotations - Completed'};
        //await updateStatus(db,pid,status);
        createLog.debug("Annotation Update Process Completed");

        // criteria to get novel variants has to be changed.
        // Novel Variants. include type of annotation
        createLog.debug("Starting getNovelVariants");
        var nScript = path.join(parsePath,'import','getNovelVariants.js');
        var subprocess3 = spawn.fork(nScript, ['--sid',fileId,'--assembly', assemblyType] ); 

        subprocess3.on('message', async(data) => { // data has the absolute path of the novel  variants vcf 
            try {
                // Responding data/VCF variants as a tab file
                createLog.debug("Message received from child_process "+data);
                var sizeObj = await stats(data);
                //console.log(sizeObj);
                var size1 = sizeObj['size'];
                createLog.debug("Logging the size value below");
                createLog.debug(size1); 

                createLog.debug(`Retrieved Reannotate Unique Variants - Size ${size1}`);
                var status = {'status_description' : 'Retrieved Unique Variants','status_id':4,'finish_time': new Date()};
                var pushSt = {'status_log': {status:"Retrieved Reannotate Unique Variants",log_time: new Date()}};
                await updateStatus(db,uDateId,status,pushSt);

                // sleep value will be configured based on the size of novel variants file
                if ( size1 ) {
                    var zipFile = data+'.gz';
                    createLog.debug("Proceed to Annotation Swarm Process "); 

                    // Connect and get token
                    // Execute process
                    var token = await getAnnoApiToken();
                    createLog.debug("Response received from token creation request");
                    //createLog.debug(token);
                    // Execute Annotation based process.

                    //var response = await triggerAnnotationStream(token,fileId,zipFile);
                    
                    var defaultAnno = 'false';
                    createLog.debug("Received token from the API Call. Trigger Process to start the Annotations");
                    var response = "";
                    if ( annoType == "Def" ) {
                        response = await triggerAnnotationStreamPipeline(token,db,uDateId,data,zipFile,fileId,assemblyType,createLog);
                    } else {
                        response = await triggerAnnotationStreamPipeline(token,db,uDateId,data,zipFile,fileId,assemblyType,createLog,defaultAnno,annoType,annoFields);
                    }
                    
                    createLog.debug("What is the response sent by this function");
                    createLog.debug(response);
                    var status = {'status_description' : 'ReAnnotation Process Started','status_id':5,'finish_time': new Date()};
                    var pushSt = {'status_log': {status:"ReAnnotation Process Started",log_time: new Date()}};
                    await updateStatus(db,uDateId,status,pushSt);

                    createLog.debug("Logging the response received from the triggerAnnotationStream function");
                    createLog.debug(response);
                    // sleep for sometime and keep checking for intermittent Annotation Status
                    var intervalTime;
                    // set interval time based on the size of file
	
	                //intervalTime = 60000;
                    if ( size1 <= 2097152 ) { // Test Comment Start
                        // exome data
                        intervalTime = 600000; // check the Annotation status every 10 minutes

                        //intervalTime = 60000 // testing purpose
                    } else {
                        intervalTime = 1200000; // check the Annotation status every 20 minutes
                        //intervalTime = 60000
                    }  // Test Comment End
                    createLog.debug(`Size of the novel Variants file is ${size1} and the interval time has been set to value ${intervalTime}`);

                    //var intervalTime = 900000; // milliseconds(15 minutes)
                    var intervalObj = setInterval( () => {
                        getAnnotationStatus(token,fileId,db,uDateId).then( (resp) => {
                            //console.log("Checking for the Annotation Status");
                            console.log("Logging the response from annotation status")
                            console.log(resp);
                            createLog.debug("Checking for Annotation Status");
                            // remove file only if it exists
                            if ( existsSync(data)) {
                                unlink(data,(err) => {
                                    if (err) {
                                        createLog.debug("Error in removing novel variants file");
                                        createLog.debug(err);
                                    }
                                    createLog.debug("Removing the novel variants file ");
                                    createLog.debug(data);
                                });
                            }
                            // check response status and clear timer interval
                            // for testing purpose only
                            //if ( resp ) {
                            var respObj = JSON.parse(resp);
                            console.log(respObj);
                            
                            if ( respObj && (respObj['message']['status'] == "Annotation Completed" ) ) {
                                // update current annotation version
                                var annoVer = "";
                                if ( process.env.CURR_ANNO_VER ) {
                                    annoVer = process.env.CURR_ANNO_VER;
                                }

                                var status = {'status_description' : 'Re-Annotation Process Completed','status_id':7,'finish_time': new Date(), "anno_ver": annoVer};
                                var pushSt = {'status_log': {status:"Re-Annotation Process Completed",log_time: new Date()}};
                                updateStatus(db,uDateId,status,pushSt).then ( () => {});

                                createLog.debug("Logging the response from Annotation Status codeblock");
                                var respJson = JSON.stringify(respObj);
                                createLog.debug(respJson);
                                //createLog.debug(respObj);
                                //console.dir(respObj,{"depth":null});
                                clearInterval(intervalObj);
                                // Download the Annotation file and the checksum file
                                var annoFile = respObj['message']['annotation_file'];
                                var csFile = respObj['message']['checksum'];
                                createLog.debug(`annoFile ${annoFile}`);
                                createLog.debug(`csFile ${csFile}`);
                                var dest1;
                                var dest2;
                                dest1 = path.join(logLoc,'import','samples','tmp',path.basename(annoFile));
                                dest2 = path.join(logLoc, 'import', 'samples','tmp',path.basename(csFile));
                                /*if ( instance == 'dev' ) {
                                    dest1 =  path.join(parsePath,'import','log',path.basename(annoFile));
                                    dest2 = path.join(parsePath, 'import', 'log', path.basename(csFile));
                                } else {
                                    dest1 =  path.join(process.env.importLogPath,path.basename(annoFile));
                                    dest2 = path.join(process.env.importLogPath,path.basename(csFile));
                                }*/
                                createLog.debug("Initiate download process for the below files");
                                createLog.debug(`annoFile ${annoFile}`);
                                createLog.debug(`dest1 ${dest1}`);
                                createLog.debug(`dest2 file to be checked ${dest2}`);
                                downloadData(token, annoFile, dest1).then( (resp) => {
                                    createLog.debug("1:Annotation file data downloaded");
                                    downloadData(token,csFile,dest2).then( (resp) => {
                                        createLog.debug("2:checksum data downloaded");
                                        // revoke the Annotation API token: to be discussed if this is needed
                                        verifyCS(dest1,dest2).then( (status) => {
                                            createLog.debug("3:checksum validated. Proceed to update novel Annotations");
                                            // update Annotations
                                            var newAnno = "reannotate"
                                            if ( annoType == "Def" ) {
                                                newAnno = "";
                                            }
                                            updateAnnotations(fileId,dest1,assemblyType,newAnno).then((status) => {
                                                createLog.debug("4:novel Annotations updated");
                                                process.exit(0);
                                            })
                                            .catch( (err) => {
                                                createLog.debug("Error4:Could not update Novel Annotations");
                                                process.exit(1);
                                            })
                                        }).catch( (err) => {
                                            createLog.debug("Error3:Invalid checksum");
                                            createLog.debug(err);
                                            process.exit(1);
                                            //next(`${err}`);
                                        })
                                    }).catch( (err) => {
                                        createLog.debug("Error2:Could not download checksum data"+err);
                                        process.exit(1);
                                    })
                                }).catch( (err) => {
                                    createLog.debug("Error1:Could not download Annotation Data"+err);
                                    process.exit(1);
                                })
                            } else if ( respObj && (respObj['message']['status'] == "Annotation InProgress" ) ) {
				                createLog.debug(respObj['message']['stdout']);
                                createLog.debug("Annotation InProgress");
                                var status = {'status_description' : 'ReAnnotation InProgress','status_id':6,'finish_time': new Date()};
                                var pushSt = {'status_log': {status:"ReAnnotation InProgress",log_time: new Date()}};
                                updateStatus(db,uDateId,status,pushSt).then ( () => {});
                            } else if (respObj && (respObj['message']['status'] == "Annotation Error" ) ) {
                                var errInfo = respObj['message']['errInfo'];
                                var appendErr = errInfo || "failed due to unknown reasons.Check Annotation logs for further details";
                                var errInfo1 = "Annotation Process Error "+appendErr;
                                var status = {'status_description':'Error','status_id': 9, 'error_info' : errInfo1,'finish_time': new Date()};
                                var pushSt = {'status_log': {status:"Error",log_time: new Date()}};
                                createLog.debug("Clearing the Interval as the Annotation Status need not be monitored further");
                                createLog.debug("Annotation Error "+errInfo1);
                                createLog.debug(respObj['message']['stdout']);
                                updateStatus(db,uDateId,status,pushSt).then ( (resp) => {
                                    clearInterval(intervalObj);
                                    process.exit(1);
                                }).catch( (err) => {
                                    createLog.debug("Error in updating Annotation error status "+err);
                                    clearInterval(intervalObj);
                                    process.exit(1);
                                })
                            }
                        }).catch ( (err) => {
			                console.log("Logging the error in Annotation catch err block- added now");
                            console.log(err);
                            var appendErr = err || "failed due to unknown reasons.Check Annotation logs for further details";
                            var errInfo = "Annotation Process Error "+appendErr;
                            var status = {'status_description':'Error','status_id': 9, 'error_info' : errInfo,'finish_time': new Date()};
                            var pushSt = {'status_log': {status:"Error",log_time: new Date()}};
                            createLog.debug("Clearing the Interval as the Annotation Status need not be monitored further");
                            createLog.debug("Annotation Error "+err);
                            updateStatus(db,uDateId,status,pushSt).then ( (resp) => {
                                clearInterval(intervalObj);
                                process.exit(1);
                            }).catch( (err) => {
                                createLog.debug("Error in updating Annotation error status "+err);
                                clearInterval(intervalObj);
                                process.exit(1);
                            })
                        })
                    },intervalTime);                       
                } else {
                    createLog.debug("Size of Novel Variants is 0");
                    process.exit(0);
                }
            } catch(err1) {
                    console.log("Check if the error sent by the Annotation token process has been caught");
                    console.log(err1);
                    createLog.debug("Error caught "+err1);
		            var errInfo = "Annotation Process Error "+err1;
		            var status = {'status_description':'Error','status_id': 9, 'error_info' : errInfo,'finish_time': new Date() };
		            var pushSt = {'status_log': {status:"Error",log_time: new Date()}};
                    updateStatus(db,uDateId,status,pushSt).then ( (resp) => {
                        process.exit(1);
	                }).catch( (err) => {
	                    createLog.debug("Error in updating Annotation error status "+err);
			            process.exit(1);
	                })
            } 
        });
    } catch(err) {
        console.log("*************Looks like we have received an error message");
        console.log(err);
        createLog.debug("Error caught "+err);
        //throw err;
        process.exit(1);
        //next(`${err}`);
    } finally {
        // nodejs request module and trigger https Annotation Request
        // handler specific to parent process
        process.on('beforeExit', (code) => {
            console.log(`--------- About to exit PARENT PROCESS with code: ${code}`);
            console.log("%%%%%%%%%%% BEFORE EXIT ********************** ");
        });

        console.log("************* Final Section of code block ************************ ");
        console.log("Do you pass by here in all scenarios ");
    }
}) ();

async function updateStatus(db,search,update,pushStat) {
    try {
        var id  = {'_id' : search};
        var set = {};
        if ( pushStat ) {
            set = {$set : update, $push : pushStat};
        } else {
            set = {$set : update };
        }
        
        console.log("##############################################");
        console.log("Request to update the status for the Import");
        console.log(id);
        console.log(set);
        console.log("##############################################");
        var statsColl = db.collection(importStatsCollection);
        var res = await statsColl.updateOne(id,set);
        return "success";
    } catch(err) {
        throw err;
    }
}
