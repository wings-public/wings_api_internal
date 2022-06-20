const spawn  = require('child_process');
const path = require('path');
var zlib = require('zlib');
const closeSignalHandler = async (childProc) => {
    return new Promise( (resolve,reject) => {
        //console.log("Received request for childProc");
        childProc.on('close', async(code) => {
            if ( code == 0 ) {
                //console.log("code is 0 and closed");
                resolve('closed');
            } else {
                reject("error");
            }
        });
        childProc.on('error', async(err) => {
            reject(err);
        })
    })
}

module.exports = {  closeSignalHandler };