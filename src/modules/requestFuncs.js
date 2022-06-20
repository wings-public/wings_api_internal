var rp = require('request-promise');
var fs = require('fs');
const spawn  = require('child_process');
// request module has been deprecated from Feb 11 2020
//var request = require('request');
var path = require('path');
var zlib = require('zlib');
const configData = require('../config/config.js');
const https = require('https');
var url = require('url');

// changes specific to streams backpressure, usage of pipeline
const stream = require('stream');
const util = require('util');
const pipeline = util.promisify(stream.pipeline);
// stream pipeline

const { db : {annotationEngine,annotationEngineUrl,annoApiUser} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;


async function downloadData(reqUrl,serverLoc) {
    return new Promise ( (resolve,reject) => {
        console.log(`Received request for Sample ${reqUrl}`);
        var options = { 'rejectUnauthorized': false, 'requestCert': true, 'agent': false };
        const req = https.get(reqUrl,options,async(response) => {
            if ( response.statusCode < 200 || response.statusCode >= 300 ) {
                var msg = reqUrl+" URL-Not Valid. Response Status Code-"+response.statusCode;
                console.log("MSG   "+msg);
                reject(msg);
            } else {
                console.log("Check the response received from the request URL");
                var parsed = url.parse(reqUrl);
                console.log("************* Logging URL PARSE DATA *****************");
                console.log(parsed);
                console.log(path.basename(parsed.href));
                console.log("************* Logging URL PARSE DATA *****************");

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
                    reqFileName = path.basename(url.parse(reqUrl).path);
                }

                console.log(`reqFileName ${reqFileName}`);
                filename = path.join(serverLoc,reqFileName);
                var response = await pipeline(response,fs.createWriteStream(filename));
                console.log(response);
                resolve(filename);
            }
        });
                
        req.on('error' , (err) => {
            console.log("Error in reading request data from Request URL");
            console.log(err)
            createLog.debug("Error in reading data from Request URL "+reqUrl);
            reject(err);
        });
        req.end();
    });
}

module.exports = {downloadData};
