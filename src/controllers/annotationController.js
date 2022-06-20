var rp = require('request-promise');
var fs = require('fs');
const spawn  = require('child_process');
// request module has been deprecated from Feb 11 2020
//var request = require('request');
var path = require('path');
var zlib = require('zlib');
const configData = require('../config/config.js');
const https = require('https');

// changes specific to streams backpressure, usage of pipeline
const stream = require('stream');
const util = require('util');
const pipeline = util.promisify(stream.pipeline);
// stream pipeline

const { db : {annotationEngine,annotationEngineUrl,annoApiUser} } = configData;
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

const triggerAnnotationStreamPipeline = async(tokenH,novelVariants,zipFile,sid,assemblyType,logger) => {
    try {
            var token = tokenH['token'];

            var path = `/deployStack/${sid}/${assemblyType}`;
            if ( process.env.ANNOTATION_REVERSE_PATH ) {
                path = process.env.ANNOTATION_REVERSE_PATH + path;
            }

            var options = {
                'method': 'POST',
                'hostname' : annotationEngineUrl,
                'path': path,
                'headers': {
                  'authorization': ' JWT '+token
                }
            };

            logger.debug("Now i am at triggerAnnotationStreamPipeline");
            const reqObj = https.request(options, async(response) => {    
                logger.debug("Looks like i've received response- inside response callback");
                response.setEncoding('utf8');
                logger.debug("Any Status Code ??? "+response.statusCode);
                
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
            });
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
            reqObj.end();
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
 
const getAnnotationStatus = (tokenH,sid) => {
    return new Promise ( ( resolve,reject) => {
        //var url = `https://${annotationEngineUrl}/annotationStatus/${sid}`;
          
        var token = tokenH['token'];
        var path = `/annotationStatus/${sid}`;
        if ( process.env.ANNOTATION_REVERSE_PATH ) {
            path = process.env.ANNOTATION_REVERSE_PATH + path;
        }
        var options = { 'method': 'GET','hostname': annotationEngineUrl,'path': path,'headers': {'authorization': ' JWT '+token } };

        //var options = { 'method' : 'GET', 'url': url, 'headers': { 'authorization':' JWT '+token } };

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
    });
}

const downloadData = async(tokenH,filename,dest) => {
    return new Promise( ( resolve,reject) => {
        var token = tokenH['token'];
        var path = `/downloadData?annotation=${filename}`;
        if ( process.env.ANNOTATION_REVERSE_PATH ) {
            path = process.env.ANNOTATION_REVERSE_PATH + path;
        }
        var options = { 'gzip' : true, 'method' : 'GET', 'hostname': annotationEngineUrl, 'path' : path , 'headers': { 'authorization':' JWT '+token } };
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

const updateAnnotations = async(sid,anno,assemblyType) => {
    try {
        var parsePath = path.parse(__dirname).dir;
        var novelScript = path.join(parsePath,'parser','novelAnnotationUpload.js');
        var annoScript = path.join(parsePath,'import','updateExistingAnnotations.js');

        var subprocess = spawn.fork(novelScript, ['--parser',"Novel",'--input_file', anno,'--assembly', assemblyType]);
        var procPid = subprocess.pid;
        await closeSignalHandler(subprocess);

        var subprocAnno = spawn.fork(annoScript, ['--sid',sid,'--assembly', assemblyType] );
        await closeSignalHandler(subprocAnno);
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

module.exports = {getAnnoApiToken,triggerAnnotationStream, triggerAnnotationStreamPipeline, getAnnotationStatus, downloadData, verifyCS, updateAnnotations};
