const spawn  = require('child_process');
const runningProcess = require('is-running');
var path = require('path');

const configData = require('../config/config.js');
const { db : {host,port,dbName,resultCollection,variantQueryCounts} } = configData;

const getConnection = require('../controllers/dbConn.js').getConnection;
const getResColl = require('../controllers/entityController.js').getResultCollObj;
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const { createReadStream, createWriteStream } = require('fs');
var loggerMod = require('../controllers/loggerMod');

const queryRoutes = (app) => {
    app.route('/querySample')
    .post( loginRequired,async (req,res,next) => {
        try {

           //console.log(req);
           //console.log("Query Sample Route Point ");
           var pid = process.pid;
           var basePath = path.parse(__dirname).dir;
           //var jsonFile = path.join(basePath,'query','json',`queryInput_${pid}.json`);
           var jsonData = req.body;

           if ( !jsonData['trio']) {
               if ( (!jsonData['condition']) || (!jsonData['invoke_type']) || (!jsonData['assembly_type'])) {
                    throw "JSON Structure Error- invoke_type or assembly_type missing";
                }
                if (['hg19','hg38','GRCh37','GRCh38'].indexOf(jsonData['assembly_type']) < 0 ) {
                    throw "assemblyType: Supported options are hg19/hg38/GRCh37/GRCh38";
                }  
            }

           if ( jsonData['var_disc']) {
                if ( !jsonData['var_disc']['geneID'] && !jsonData['var_disc']['region']) {
                    throw "geneID or region values expected for var_disc";
                }
                if ( (! jsonData['seq_type'])|| (! jsonData['centerId']) || ( ! jsonData['hostId']) ) {
                    throw "Required parameters missing for var_disc request type";
                }
            } else if ( jsonData['trio']) {
                if ( !jsonData['trio']['trioLocalID'] || !jsonData['trio']['trio_code'] ) {
                    throw "trioLocalID and trio_code are expected for trio filter queries";
                } 
            } else {
                if ( ! jsonData['fileID']) {
                    throw "fileID required";
                }
            }

           var queryScript = path.join(basePath,'query','sampleFilterQuery.js');           
           req.on('error', (err) => {
               //next(err);
               //res.status(400).send("FAILURE Error Message "+err);
               next(`${err}`);
           }); 

           // Updating code to support json input in body of request instead of attaching the json to the request
           /*
           var wFd = req.pipe(createWriteStream(jsonFile));
           wFd.on('error', (err) => {
               //next(err);
               //res.status(400).send("Failure-Error message "+err);
               next(`${err}`);
           }); */

           var parsePath = path.parse(__dirname).dir;
           //var logFile = path.join(parsePath,'query','log',`sample-query-route-logger-${pid}.log`);
           //var createLog = loggerMod.logger(logFile);
           var logFile = `sample-query-route-logger-${pid}.log`;
           var createLog = loggerMod.logger('query',logFile);
           createLog.debug("Query Request Received");
           createLog.debug(jsonData);

           const client = getConnection();
           const db = client.db(dbName);
           var resColl = db.collection(resultCollection);

           var jsonReqData = JSON.stringify(jsonData);

           console.log("queryRoutes - Logging json data");
           console.log(jsonReqData);
           console.log(queryScript);
           var subprocess = spawn.fork(queryScript,['--post_json',jsonReqData]);

           //var subprocess = spawn.fork(queryScript,['--json',jsonFile]);

           subprocess.on('message', async(data) =>  {
               if ( data.err ) {
                   next(data.err);
               } else if ( data.count ) {
                   //res.status(200).json({"message":"0 variants"});
                   //res.status(200).json({"pid":-1});
                   if ( jsonData['var_disc']) {
                       res.status(200).json({"pid":-1,'centerId':jsonData['centerId'], 'hostId': jsonData['hostId']});
                   } else {
                       res.status(200).json({"pid":-1});
                   }
                   
               } else if ( data.debug ) {
                   res.status(200).json(data);
               }  else {
                    createLog.debug("************* IPC RECEIVED MSG FOR ****************"+data);
                    
                    var str = JSON.parse(data);
                    var pid = str['pid'];
                    var batch = str['batch'];
                    var req = str['req'];   

                    if ( batch == 1 ) {
                        if ( jsonData['var_disc']) {
                            //console.log("Actual PID "+pid)
                            //console.log("PID After calculations "+pid)
                            res.status(200).json({'pid':pid,'centerId':jsonData['centerId'], 'hostId': jsonData['hostId']});
                        } else {
                            res.status(200).json({'pid':pid});
                        }
                    }
                    //if ( batch === "batch1" ) {
                    /*if ( batch == 1 ) {
                        //var firstStream = {'batch':'batch1','pid':pid};
                        var firstStream = {'batch': 1,'pid':pid};
                        var stream = resColl.find(firstStream,{'projection': {'_id':0,'pid':1,'batch':1,'lastBatch':1,'documents':1 } }).stream();

                        //res.status(200);
                        //res.write(`QueryID:${pid}\n`);
                        stream.on('data',(data) => {
                            res.write(JSON.stringify(data));
                        });

                        stream.on('end',() => {
                            res.end();
                            //client.close(); // Do not close the client here
                        });
                    }*/
                }
           });

         setTimeout(() => {
            createLog.debug("Done with TIMEOUT ****************");
            subprocess.kill()
          }, 300000 );

           subprocess.on('error', (err) => {
               console.log(`Caught a error signal. Anything to log ${err}`);
               next(`${err}`);
           });

           subprocess.on('data',function(data) {
               console.log(`subprocess stderr:${data}`);
           });

           // handler to listen when the child exits or has completed
           subprocess.on('close',function(code) {
               if ( code != 0 ) {
                   console.log(`Child process exited with code ${code}`);
               } 
               console.log("*************** CHILD Process exited ************* ");
           });

           subprocess.on('exit', function(exCode) {
               console.log("Child Process exited. We can close the IPC Channel ");
               //client.close();
           });

           process.on('close', (code) => {
               console.log("************ PARENT Process will exit now. Do we have to complete any processing *************** ");
           });

           process.on('beforeExit', (code) => {
               console.log(`--------- About to exit PARENT PROCESS with code: ${code}`);
           });

        } catch(err) {
            //res.status(400).send("Failure-Error message "+err);
            console.log("******** Hey !! Did you find something here ");
            console.log(err);
            next(`${err}`);
        }

    });

    app.route('/queryBatch/:queryID/:batchID')
    .get( loginRequired,async (req,res,next) => {
        try {
            if ( ! req.params.queryID || ! req.params.batchID ) {
                next('Failure-Error message Request parameters null');
                //res.status(400).send("Failure-Error message Request parameters null");
            }

            //console.log('I am at queryBatch');
            var queryID = parseInt(req.params.queryID);
            var batchID = parseInt(req.params.batchID);

            var client = getConnection();
            const db = client.db(dbName);
            var resColl = db.collection(resultCollection);

            var filter = {"pid":queryID, "batch" : batchID};
            //console.dir(filter);
            var stream = resColl.find(filter,{'projection': {'_id':0,'batch':1,'lastBatch':1,'hostId' : 1,'centerId' : 1,'documents':1,'assembly_type':1,'result_type':1 } } ).stream();

            stream.on('data', (data) => {
                //console.log(data);
                res.write(JSON.stringify(data));
            });

            stream.on('end', (data) => {
                res.end();
            });

        } catch(err) {
            //console.log("Error in route endpoint query");
            next(`${err}`);
        }
    });

    
    app.route('/getCountPost')
    .post( loginRequired,async (req,res,next) => {
        try {

           //console.log(req);
           console.log("Query Sample Route Point ");
           var pid = process.pid;
           var basePath = path.parse(__dirname).dir;
           //var jsonFile = path.join(basePath,'query','json',`queryInput_${pid}.json`);
           var jsonData = req.body;
           console.log("Logging the JSON data received as input");
           console.log(jsonData);
           if ( !jsonData['trio']) {
               if ( (!jsonData['invoke_type']) || (!jsonData['assembly_type'])) {
                   throw "JSON Structure Error- invoke_type or assembly_type missing";
               }
               if (['hg19','hg38','GRCh37','GRCh38'].indexOf(jsonData['assembly_type']) < 0 ) {
                   throw "assemblyType: Supported options are hg19/hg38/GRCh37/GRCh38";
               }  
            }          

           if ( jsonData['var_disc']) {
               console.log("Detected var_disc request type");
               if ( !jsonData['var_disc']['geneID'] && !jsonData['var_disc']['region']) {
                   throw "geneID or region values expected for var_disc";
               }
               if ( (! jsonData['seq_type'])|| (! jsonData['centerId']) || ( ! jsonData['hostId']) ) {
                   throw "Required parameters missing for var_disc request type";
                }
           } else if ( jsonData['trio']) {
               if ( !jsonData['trio']['trioLocalID'] || !jsonData['trio']['trio_code']) {
                   throw "trioLocalID and trio_code are expected for trio filter queries";
               } 
           } else {
               if ( ! jsonData['fileID']) {
                   throw "fileID required";
               }
           }
           //console.dir(jsonData,{"depth":null});
           var queryScript = path.join(basePath,'query','sampleFilterQuery.js');           
              
           req.on('error', (err) => {
               next(`${err}`);
               console.log("Error in request ***********************");
               console.log(err);
               console.log("Error in request ***********************");
               //res.status(400).json({"message":`failure-Error ${err}`});
           }); 

           var jsonReqData = JSON.stringify(jsonData);
           //console.log("Logging json Data received ******************");
           //console.log(jsonData);
           //console.log("Launching fork request ");
           var subprocess = spawn.fork(queryScript,['--post_json',jsonReqData]);

           subprocess.on('message', (msg) => {
               //console.log(`Message received from child process is ${msg}`);
               //console.dir(msg,{"depth":null});
               // Below message format will be updated for async approach.
               //res.status(200).json({"message":msg});
               // Additional details added to response format of var_disc requests
               //console.log("Logging json data inside subprocess");
               //console.dir(jsonData,{"depth":null});
               if ( jsonData['var_disc']) {
                   //console.log("Detected var_disc type. Diff response");
                   res.status(200).json({"message":{"req_id":msg, 'centerId':jsonData['centerId'], 'hostId': jsonData['hostId']}});
               } else {
                   //console.log("Usual response");
                   res.status(200).json({"req_id":msg});
               }
               
               //res.send(msg);
           });
        } catch(err) {
            console.log("Error in route endpoint query");
            //res.status(400).json({"message":err});
            next(`${err}`);
        }    
    });

    app.route('/fetchCounts/:reqID')
    .get( loginRequired,async (req,res,next) => {
        try {
            if ( ! req.params.reqID  ) {
                next('Failure-Error message Request parameters null');
                //res.status(400).send("Failure-Error message Request parameters null");
            }

            //console.log('I am at queryBatch');
            var reqID = parseInt(req.params.reqID);

            var client = getConnection();
            const db = client.db(dbName);
            var resColl = db.collection(variantQueryCounts);

            var filter = {"_id": reqID};
            var projection = {'loadedTime' : 0, 'processedTime' : 0};
            
            var statusMsg = await resColl.findOne(filter, {'projection':projection});
            if ( ! statusMsg ) {
                next("invalid id");
            } else {
                if ( statusMsg['status'] == "completed" ) {
                    var msg = statusMsg['result'];
                    if ( statusMsg['req_type'] == "var_disc" ) {
                        var loadRes = {};
                        if (msg['fid']) {
                            loadRes = {'centerId': statusMsg['centerId'], 'hostId' : statusMsg['hostId'], 'fid': msg['fid'], 'documents': msg['documents']};
                        } else {
                            loadRes = {'centerId': statusMsg['centerId'], 'hostId' : statusMsg['hostId'],'result': statusMsg['result']};
                        }
                        res.status(200).json({"message":loadRes});
                    } else {
                        res.status(200).json({"message":msg});
                    }
                    
                } else if ( statusMsg['status'] == "inprogress" ) {
                    var msg = statusMsg['status'];
                    if ( statusMsg['req_type'] == "var_disc" ) {
                        var loadRes = {};
                        if (msg['fid']) {
                            loadRes = {'centerId': statusMsg['centerId'], 'hostId' : statusMsg['hostId'],'status': msg};
                        } else {
                            loadRes = {'centerId': statusMsg['centerId'], 'hostId' : statusMsg['hostId'],'status': msg};
                        }
                        res.status(200).json({"message":loadRes});
                    } else {
                        res.status(200).json({"message":msg});
                    }
                }
            }

        } catch(err) {
            //console.log("Error in route endpoint query");
            next(`${err}`);
        }
    });

    app.route('/getSavedFilterCount')
    .post( loginRequired,async (req,res,next) => {
        try {

           //console.log(req);
           //console.log("Query Sample Route Point ");
           var pid = process.pid;
           var basePath = path.parse(__dirname).dir;
           //var jsonFile = path.join(basePath,'query','json',`queryInput_${pid}.json`);
           var jsonData = req.body;
           //console.log("Logging the JSON data received as input");
           if ( (!jsonData['condition']) || (!jsonData['invoke_type']) || (!jsonData['fileID']) || (!jsonData['assembly_type'])) {
               throw "JSON Structure Error";
           }

           if (['hg19','hg38','GRCh37','GRCh38'].indexOf(jsonData['assembly_type']) < 0 ) {
               throw "assemblyType: Supported options are hg19/hg38/GRCh37/GRCh38";
           }
           //console.dir(jsonData,{"depth":null});
           var queryScript = path.join(basePath,'query','sampleFilterQuery.js');           
              
           req.on('error', (err) => {
               next(`${err}`);
               //res.status(400).json({"message":`failure-Error ${err}`});
           }); 

           jsonData = JSON.stringify(jsonData);
           var subprocess = spawn.fork(queryScript,['--post_json',jsonData]);

           subprocess.on('message', (msg) => {
               console.log(`Message received from child process is ${msg}`);
               res.status(200).json({"message":msg});
               //res.send(msg);
           });
        } catch(err) {
            console.log("Error in route endpoint query");
            //res.status(400).json({"message":err});
            next(`${err}`);
        }    
    });
}


module.exports = { queryRoutes };
