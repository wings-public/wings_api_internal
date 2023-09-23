var rp = require('request-promise');
var fs = require('fs');
const spawn  = require('child_process');
// request module has been deprecated from Feb 11 2020
//var request = require('request');
var path = require('path');
var zlib = require('zlib');
const configData = require('../config/config.js');
const https = require('https');
const querystring = require('querystring');

// changes specific to streams backpressure, usage of pipeline
const stream = require('stream');
const util = require('util');
//const { initParams } = require('request');
const pipeline = util.promisify(stream.pipeline);
// stream pipeline

const { db : {annotationEngine,annotationEngineUrl,annoApiUser,importStatsCollection} ,app : {logLoc}} = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;
//var logFile = `import-controllers-logger-${fileId}-${pid}.log`;
//var createLog = loggerMod.logger('import',logFile);

const getAnnoApiToken = () => {
        return new Promise( (resolve,reject) => {
            var apiPwd = annoApiUser+'@A#01';
            var userObj = { 'user' : annoApiUser, 'password': apiPwd };
            var url1  = annotationEngineUrl;
            if ( process.env.ANNOTATION_REVERSE_PATH ) {
                url1 = url1 + process.env.ANNOTATION_REVERSE_PATH;
            }

            //var url = `https://${annotationEngineUrl}/auth/login`
            var url = `https://${url1}/auth/login`
	    console.log(`Annotation url:${url}`);
            
            var options = { 'method': 'POST', 'url': url, 'headers' : {'Content-Type': "application/json"}, 'body': userObj, json:true };
            rp(options).then( (response) => {
                //console.log("Token created");
                //console.log(response);
                resolve(response);
            }).catch( (err) => {
                console.log("Logging token error here ");
                console.log(err.message);
                reject(err.message);
            });
        })    
}

// include request body 
const triggerAnnotationStreamPipeline = async(tokenH,db,uDateId,novelVariants,zipFile,sid,assemblyType,logger,defaultAnno = true,annoType = false ,annoFields = false ) => {
    try {
            var token = tokenH['token'];

            // Note : config file has to be included if defaultAnno = false
            // request body included to support specific annotation request
            //const data = {'fileID': sid,'assembly': assemblyType,'annotations': {'VEP' : 1, 'CADD' : 0}, 'config' : defaultAnno};
            //const reqBody = {"req" : "teststr"};

            //var annoObj = { assembly : assemblyType, VEP : 1, CADD : 1 , config : defaultAnno};
            // if we send 0, it is sent as "0", parseInt required at client code

            //var annoObj = { assembly : assemblyType, VEP : 1, config : defaultAnno};
            var annoObj = { assembly : assemblyType,config : defaultAnno };

            console.log(`tokenH:${tokenH} uDateId:${uDateId} novelVariants:${novelVariants} zipFile:${zipFile} sid:${sid} assemblyType:${assemblyType} defaultAnno:${defaultAnno} annoType:${annoType} annoFields:${annoFields}`);

            // customized annotation
            if ( annoType != false ) {
                // VEP,CADD or VEP
                var anno = annoType.split(',');
                for ( var idx in anno ) {
                    var type = anno[idx];
                    annoObj[type] = 1;
                }
                // temp setting for testing
                //annoObj['VEP'] = 0;
            // default annotations
            } else {
                // default annotations that will be launched
                annoObj['VEP'] = 1;
                // commented CADD for testing purpose
                annoObj['CADD'] = 1;
            }
            if ( annoFields != false ) {
                annoObj['annoFields'] = annoFields;
            }

            const params = querystring.stringify(annoObj);
            console.log("*******************************")

            console.log("Logging the params");
            console.log(params);

            var apipath = `/deployStack/${sid}`;

            //var path = `/deployStack/${sid}/${assemblyType}`;
            if ( process.env.ANNOTATION_REVERSE_PATH ) {
                apipath = process.env.ANNOTATION_REVERSE_PATH + apipath;
            }

            const re = /.*?:(\w+)/;
            console.log(re);
            var matches = annotationEngineUrl.match(re);
            var annoPort = 0;

            if (annotationEngineUrl.match(re)) {
                console.log("Logging anno port");
                console.log(matches[1]);
                annoPort = matches[1];
            }

            var updHostName = annotationEngineUrl;
            // port number is included if there is no reverse proxy for annotation URL
            updHostName = annotationEngineUrl.replace(/:\d+/,"");

            var options = {
                'method': 'POST',
                //'hostname' : annotationEngineUrl,
                'hostname' : updHostName,
                //'path': path,
                'path': apipath+'?' + params,
                'headers': {
                  //'Content-Type': 'application/x-www-form-urlencoded',
                  //'Content-Length': data.length,
                  'authorization': ' JWT '+token,
                  
                },
                //'body': data,
                //json: true
            };
            console.log(options);

            // If annotation port is present, include to option.
            // present only if reverse proxy is not setup for anno server.
            if ( annoPort ) {
                options['port'] = annoPort;
            }

            //console.log("Logging the option of deploy stack------");
            //console.dir(options,{"depth":null});

            logger.debug("Now i am here ");
            console.log("sending request for deploy stack")
            const reqObj = https.request(options, async(response) => {    
                logger.debug("Looks like i've received response- inside response callback");
                response.setEncoding('utf8');
                logger.debug("Any Status Code ??? "+response.statusCode);
                //console.log("Logging the response below to check the resp received---");
                //console.log(response);
                
                if ( response.statusCode < 200 || response.statusCode >= 300 ) {
                    // test this section of code: is return required?
                    //throw "Annotation Deployment Error";
                    // only log the error and do not throw it.
                    logger.debug("failure");
                } else if ( response.statusCode == 200 ) {
                    //return "stack_deployed";
                    // just log the status code with a message
                    logger.debug("success");
                }
                // timeout setting for testing purpose    
                // handler to process the response sent by /deployStack API
                response.on('data', (d) => {
                    console.log("data signal handler - received response from Annotation Server");
                    console.log(d);
                    var parsedData = JSON.parse(d);
                    var tmpDir = parsedData.tmp_loc || "";
                    
                    console.log(tmpDir);

                    console.log("Logging the id field "+uDateId);
                    console.log(`tmpDir:${tmpDir}`);
                    var statsColl = db.collection(importStatsCollection);
                    var setCmd = {$set:{"tmp_dir": tmpDir}};
                    console.log(`uDateId:${uDateId}`);
                    console.log(setCmd);
                    console.log("triggerAnnotationStreamPipeline");
                    uDateId = parseInt(uDateId);
                    statsColl.updateOne({_id:uDateId},setCmd,{upsert:true}).then( (resp) => {
                        console.log("Updated tmp_dir -----")
                    }).catch( (err) => {
                        console.log("could not update tmp dir")
                        console.log(err);
                    })
                });         
            });
            
            //reqObj.end();
            reqObj.setTimeout(200000000);
            
            /*req.on('error', async(err) => {
                
                console.log("Error is ---- "+err);
                return;
                //throw err;
            });*/
             
            // trigger pipeline request and resolve promise after the await has completed

            await pipeline(fs.createReadStream(novelVariants),zlib.createGzip(),fs.createWriteStream(zipFile));
            logger.debug("zip file creation - pipeline succeeded");
            await pipeline(fs.createReadStream(zipFile),reqObj);
            logger.debug("request has been streamed - pipeline succeeded");
            // stream customized VEP config if defaultConfig option is false
            
            //reqObj.write("dummy request");
            //reqObj.write(dataString); // request body
            
            //reqObj.write(postData);
            reqObj.end();
            console.log("message before return ")
            return "stack_deployed";
    } catch(err) {
        logger.debug("Error in triggerAnnotationStreamPipeline");
        console.log("Logging error in triggerAnnotationStreamPipeline ***************************");
        console.log(err);
        throw err;
    }
}

// Request Annotation Server to trigger Annotation process for the streamed Novel Variants

const triggerAnnotationStream =  (tokenH,sid,novelVariants) => {
        return new Promise( (resolve,reject) => {
            var token = tokenH['token'];

            var path = `/deployStack/${sid}`;

            var options = {
                'method': 'POST',
                'hostname': annotationEngineUrl,
                'path': path,
                'headers': {
                  'authorization': ' JWT '+token
                }
              };

            const req = https.request(options, (response) => {    
                response.setEncoding('utf8');                   
                response.on('data', (d) => {
                    console.log("data signal handler - received response from Annotation Server");
                    console.log(d);
                });
                response.on('end', (chunk) => {
                    console.log('end signal handler - received response from Annotation Server');
                    console.log(chunk);
                });
                response.on('error', (error) => {
                    console.log('error signal handler - received response from Annotation Server');
                    console.log(error);
                });

                if ( response.statusCode == 200 ) {
                    console.log("Log the status code received from the response");
                    console.log(response.statusCode);
                    resolve(response.statusCode);
                } else {
                    console.log("status code is not 200. error response");
                    reject(error);
                }
            });

            fs.createReadStream(novelVariants).pipe(req)
            .on('error', (err1) => {
                console.log("Error in piping data to request stream");
                reject(err1);
                //next(`${err1}`);
            })
            .on('end', () => {
                console.log('There will be no more data');
                // end request once the request data has been streamed.
                req.end();
            })
        });
 }
 
const getAnnotationStatus = (tokenH,sid,db,uDateId) => {
    return new Promise ( ( resolve,reject) => {
        //var url = `https://${annotationEngineUrl}/annotationStatus/${sid}`;
        
        console.log(`sid:${sid} uDateId:${uDateId}`);
        var statsColl = db.collection(importStatsCollection);
        console.log("uDateId"+uDateId);
        statsColl.findOne({_id:parseInt(uDateId)},{projection:{'tmp_dir':1}}).then( (resp) => {
            //statsColl.findOne({_id:parseInt(uDateId)}).then( (resp) => {
            console.log("Logging response of find request ")
            console.log(resp);
            var tmpDir = "";
            tmpDir = resp.tmp_dir;
            console.log("Logging temp directory below -------");
            console.log(tmpDir);

            //
            var token = tokenH['token'];
            var path = `/annotationStatus/${sid}/${tmpDir}`;
            console.log(`Path is ${path}`);
            
            if ( process.env.ANNOTATION_REVERSE_PATH ) {
                path = process.env.ANNOTATION_REVERSE_PATH + path;
            }

            const re = /.*?:(\w+)/;
            //console.log(re);
            var matches = annotationEngineUrl.match(re);
            var annoPort = 0;

            if (annotationEngineUrl.match(re)) {
                //console.log("Logging anno port");
                //console.log(matches[1]);
                annoPort = matches[1];
            }

            var updHostName = annotationEngineUrl;
            // port number is included if there is no reverse proxy for annotation URL
            updHostName = annotationEngineUrl.replace(/:\d+/,"");

            //var options = { 'method': 'GET','hostname': annotationEngineUrl,'path': path,'headers': {'authorization': ' JWT '+token } };

            var options = { 'method': 'GET','hostname': updHostName,'path': path,'headers': {'authorization': ' JWT '+token } };

            // If annotation port is present, include to option.
            // present only if reverse proxy is not setup for anno server.
            if ( annoPort ) {
                options['port'] = annoPort;
            }

            //var options = { 'method' : 'GET', 'url': url, 'headers': { 'authorization':' JWT '+token } };

            console.log("Sending request for getAnnotationStatus");
            console.log(`annotationEngineUrl:${annotationEngineUrl} path:${path}`);
            const req = https.request(options, (response) => {
                if ( response.statusCode < 200 || response.statusCode >= 300 ) {
                    // test this section of code: is return required?
                    reject("Annotation Error");
                }  

                const data = [];
                response.on('data', (chunk) => {
                    data.push(chunk);
                });
                response.on('end', () => resolve(Buffer.concat(data).toString() ));
            });
            req.on('error',(error) => { return reject("request error") });
            //req.on('error',(error) => {  reject("request error") });
            req.end();
            //
        }).catch( (err) => {
                console.log("getAnnotationStatus - Error ");
                console.log(err);
        })

    });
}

const downloadData = async(tokenH,filename,dest) => {
    return new Promise( ( resolve,reject) => {
        var token = tokenH['token'];
        var path = `/downloadData?annotation=${filename}`;
        if ( process.env.ANNOTATION_REVERSE_PATH ) {
            path = process.env.ANNOTATION_REVERSE_PATH + path;
        }

        const re = /.*?:(\w+)/;
        console.log(re);
        var matches = annotationEngineUrl.match(re);
        var annoPort = 0;

        // Fetch the annotation port
        if (annotationEngineUrl.match(re)) {
            console.log("Logging anno port");
            console.log(matches[1]);
            annoPort = matches[1];
        }

        var updHostName = annotationEngineUrl;
        // port number is included if there is no reverse proxy for annotation URL
        updHostName = annotationEngineUrl.replace(/:\d+/,"");

        //var options = { 'gzip' : true, 'method' : 'GET', 'hostname': annotationEngineUrl, 'path' : path , 'headers': { 'authorization':' JWT '+token } };

        var options = { 'gzip' : true, 'method' : 'GET', 'hostname': updHostName, 'path' : path , 'headers': { 'authorization':' JWT '+token } };

        // If annotation port is present, include to option.
        // present only if reverse proxy is not setup for anno server.
        if ( annoPort ) {
            options['port'] = annoPort;
        }

        //console.log("Logging the options value for download data");
        //console.log(filename);
        //console.log(dest);
        //console.log(options);
        const req = https.request( options, async(response) => {
            if ( response.statusCode == 500  ) {
                // test this section of code: is return required?
                reject("Could not download data "+filename);
            }  

            try {
                await pipeline(response,fs.createWriteStream(dest));
                resolve("data downloaded");    
            } catch(err) {
                reject(err);
            }
        });
        req.on('error', (err) => {
            console.log("Error is "+err);
            return reject(err);
        })
        req.end();
    });
}


const verifyCS = async (anno,cs) => {
    try {
        var parsePath = path.parse(__dirname).dir;
        var cScript = path.join(parsePath,'import','verifySha256.js');
        var subprocess = spawn.fork(cScript,[anno,cs]);
        subprocess.on('message', (data) => {
            if ( data === "valid" ) {
                return "valid";
            }
        })
    } catch(err) {
        throw "checksum error";
    }
}

const updateAnnotations = async(sid,anno,assemblyType,type = "def") => {
    try {
        var parsePath = path.parse(__dirname).dir;
        var novelScript = path.join(parsePath,'parser','novelAnnotationUpload.js');
        var annoScript = path.join(parsePath,'import','updateExistingAnnotations.js');
        var maxEntScript = path.join(parsePath,'import','updateTranscriptAnno.js');

        // Upload Novel Annotations
        var subprocess = "";
        if ( type == "reannotate" ) {
            subprocess = spawn.fork(novelScript, ['--parser',"Novel",'--input_file', anno,'--assembly', assemblyType,'--type',type]);
        } else {
            subprocess = spawn.fork(novelScript, ['--parser',"Novel",'--input_file', anno,'--assembly', assemblyType]);
        }
        var procPid = subprocess.pid;
        await closeSignalHandler(subprocess);

        // required only for maxent annotations
        // code block commented for RNACentral reanno process
        /*if ( type == "reannotate" ) {
            var criteria = 2;
            var subprocess2 = spawn.fork(maxEntScript, ['--sid',sid,'--condition', criteria,'--assembly', assemblyType]);
            await closeSignalHandler(subprocess2);
        }*/

        // Update Annotations : annotation collection ==> import collection
        //if ( type == "def" ) {
            var subprocAnno = spawn.fork(annoScript, ['--sid',sid,'--assembly', assemblyType] );
            await closeSignalHandler(subprocAnno);
        //}
        return "novel Annotations updated";
        /*
        subprocess.on('close', (code) => {
            var subprocAnno = spawn.fork(annoScript, ['--sid',sid] );
            subprocAnno.on('close', () => {
                return "novel Annotations updated";
            })
        }) */
    } catch(err) {
        throw err;
    }
}

const updateAnnotationsCustom = async(anno,assemblyType,type = "def") => {
    try {
        var parsePath = path.parse(__dirname).dir;
        var novelScript = path.join(parsePath,'parser','novelAnnotationUpload.js');
        var vcfAnno = path.join(parsePath,'parser','novelAnnoVcf.js');
        
        console.log("Anno file is "+anno);
        console.log(assemblyType);

        // Upload Novel Annotations
        var subprocess = "";
        
        subprocess = spawn.fork(novelScript, ['--parser',"Novel",'--input_file', anno,'--assembly', assemblyType]);
        
        var procPid = subprocess.pid;
        await closeSignalHandler(subprocess);

        var subprocess1 = spawn.fork(vcfAnno, ['--parser',"Novel",'--input_file', anno,'--assembly', assemblyType]);
        await closeSignalHandler(subprocess1);
        
        return "novel Annotations updated";
    } catch(err) {
        console.log(err);
        throw err;
    }
}

const writeFile = (pathNew, data, opts = 'utf8') => 
    new Promise((resolve, reject) => {
        fs.writeFile(pathNew, data, opts, (err) => {
            if (err) reject(err)
            else resolve()
        })
})

module.exports = {getAnnoApiToken,triggerAnnotationStream, triggerAnnotationStreamPipeline, getAnnotationStatus, downloadData, verifyCS, updateAnnotations,updateAnnotationsCustom};
