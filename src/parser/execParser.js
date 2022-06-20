const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const Async = require('async');
const spawn  = require('child_process');
const runningProcess = require('is-running');
const configData = require('../config/config.js');
const { db : {host,port,dbName,variantAnnoCollection} } = configData;

//var createConnection = require('../controllers/dbConn.js').createConnection;
//const getConnection = require('../controllers/dbConn.js').getConnection;

var args = process.argv.slice(2);
var input = args[0];
console.log("input is "+input);

var subprocess;
// VEP and CADD Annotation Parsing 
console.log("Program Started at "+new Date());
var pid = process.pid;
if ( input === "parse_annotations" ) {
    var vepData = args[1];
    var caddData = args[2];
    console.log("VEP Data is "+vepData);
    console.log("CADD Data is "+caddData);
    const logDir = 'log';
    if ( !fs.existsSync(logDir) ) {
        fs.mkdirSync(logDir);
    }
    vepProcess = spawn.fork('./vepParser.js', ['--parser','VEP','--input_file', vepData] );
    // handler to listen for close events on child process
    vepProcess.on('close', function(code) {
        console.log("VEP Process completed. Proceed to CADD Annotations ");
        caddProcess = spawn.fork('./caddParser.js', ['--parser','CADD','--input_file', caddData] );
        caddProcess.on('close',function(closeCode) {
            console.log("Annotations updated for VEP and CADD. Check and verify the Mongo Collections ");
            console.log("Begin to export data ");
            var jsonFile = "./log/variantAnnotations.json."+pid;
            spawn.exec('mongoexport --db '+dbName+' --host '+host+' --port '+port+' --collection '+variantAnnoCollection+' --out '+jsonFile, (error,stdout,stderr) => {
                if (error) {
                    console.log(error);
                    return;
                }
                console.log("mongo Data exported to "+jsonFile);
                spawn.exec('gzip '+jsonFile, (err,stdout,stderr) => {
                    console.log("gzip done");
                    var jsonZip = jsonFile+'.gz';
                    spawn.exec('shasum -a 256 '+jsonZip+ ' > variantAnnotations.json.sha256', (err2,stdout,stderr) => {
                        if ( err2 ) {
                            console.log(err2);
                            return;
                        }
                        console.log("Checksum Generated ");
                    });
                });
            });
        });
    });
}

