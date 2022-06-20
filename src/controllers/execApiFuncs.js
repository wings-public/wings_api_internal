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
const closeSignalHandler = require('./execChildProcs.js').closeSignalHandler;
const { reject } = require('async');

const getAnnoApiToken = () => {
        return new Promise( (resolve,reject) => {
            // using test user name and password
            var apiPwd = 'testnewpwd';
            var userObj = { 'user' : 'UAApiUserTest', 'password': apiPwd };
            var url1  = "wings-ua-dev.biomina.be/anno/"
            var url = `https://${url1}/auth/login`
            
            var options = { 'method': 'POST', 'url': url, 'headers' : {'Content-Type': "application/json"}, 'body': userObj, json:true };
            rp(options).then( (response) => {
                resolve(response);
            }).catch( (err) => {
                console.log("Logging token error here ");
                console.log(err.message);
                reject(err.message);
            });
        })    
}

const assignProband = async(tokenH,famId,indId) => {
    try {
            var token = tokenH['token'];

            var path = `wings-ua-dev.biomina.be/anno/assignProband/`;
            var hName = "wings-ua-dev.biomina.be";

            var jsonReq = {"familyID":famId,"IndividualID":indId,"pedigree":[{"FamilyMemberTypeName":"Proband","FamilyID":famId,"IndividualID":indId,"FamilyMemberTypeID":15,"Family_side":-1,"Node_Key":2,"RelativeName":"jj jj","RelativeBirthdate":null,"RelativeGender":1,"RelativeStatus":0,"UserID":1}]};

            var postData = JSON.stringify(jsonReq);
            var options = {
                'method': 'POST',
                'hostname' : hName,
                'path': path,
                'headers': {
                  'authorization': ' '+token
                }
            };

            console.log("Now i am here ");
            return new Promise((resolve,reject) => {
                const reqObj = https.request(options, async(response) => {    
                    response.setEncoding('utf8');
                    
                    if ( response.statusCode < 200 || response.statusCode >= 300 ) {
                        console.log("failure");
                    } else if ( response.statusCode == 200 ) {
                        console.log("success");
                    }
                    const data = [];
                    response.on('data', (chunk) => {
                        data.push(chunk);
                    });
                    response.on('end', () => resolve(Buffer.concat(data).toString() ));             
                });
            
                reqObj.on('error', (err) => {
                    reject(err);
                })
              
                reqObj.on('timeout', () => {
                    req.destroy();
                    reject(new Error('Request time out'));
                })

                reqObj.write(postData);
                reqObj.end();
            })
    } catch(err) {
        console.log("Logging error in triggerAnnotationStreamPipeline ***************************");
        console.log(err);
        throw err;
    }
}

// call this function with required arguments for indId,famId and nodekey
// 
const updateRelative = async(tokenH,famId,indId,nodekey) => {
    try {
            var token = tokenH['token'];

            var path = `wings-ua-dev.biomina.be/anno/updateRelative/`;
            var hName = "wings-ua-dev.biomina.be";

            var jsonReq = {"_id":famId,"update":{"Node_Key":nodekey,"relatives":{"IndividualID":indId,"RelativeName":"esdfcsdf e","RelativeBirthdate":"","RelativeGender":null,"RelativeStatus":null,"Disease_TL":"","Code_TL":"DTL","Disease_TR":"","Code_TR":"JTR NEW","Disease_BL":"","Code_BL":"JTR Updated","Disease_BR":"","Code_BR":"JTR","Hex_BR":"","Hex_BL":"","Hex_TR":"#7FFF00","Hex_TL":"#00FFFF","DateAdd":"Jun 18 2020  9:58AM","UserID":1}}};

            var postData = JSON.stringify(jsonReq);
            var options = {
                'method': 'POST',
                'hostname' : hName,
                'path': path,
                'headers': {
                  'authorization': ' '+token
                }
            };

            console.log("Now i am here ");
            return new Promise((resolve,reject) => {
                const reqObj = https.request(options, async(response) => {    
                    response.setEncoding('utf8');
                    
                    if ( response.statusCode < 200 || response.statusCode >= 300 ) {
                        console.log("failure");
                    } else if ( response.statusCode == 200 ) {
                        console.log("success");
                    }
                    const data = [];
                    response.on('data', (chunk) => {
                        data.push(chunk);
                    });
                    response.on('end', () => resolve(Buffer.concat(data).toString() ));             
                });
            
                reqObj.on('error', (err) => {
                    reject(err);
                });
              
                reqObj.on('timeout', () => {
                    req.destroy();
                    reject(new Error('Request time out'));
                });

                reqObj.write(postData);
                reqObj.end();
            });
    } catch(err) {
        console.log("Logging error in triggerAnnotationStreamPipeline ***************************");
        console.log(err);
        throw err;
    }
}
