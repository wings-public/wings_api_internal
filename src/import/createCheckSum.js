#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
const argParser = require('commander');
const colors = require('colors');
const crypto = require('crypto');
const { createLogger, format, transports } = require('winston');
const configData = require('../config/config.js');
//const { db : {dbName,variantAnnoCollection} } = configData;


var bson = require("bson");
var BSON = new bson.BSON();

(async function () {
    argParser
        .version('0.1.0')
        .option('-i, --input_file <file>', 'filename for which checksum has to be created')
    argParser.parse(process.argv);


    if (!argParser.input_file) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }
    
    var inputFile = argParser.input_file;
    ///////////////////// Winston Logger //////////////////////////////
    // To be added to a separate library //////
    const env = 'development';
    // Create the log directory if it does not exist
    const logDir = 'log';
    if (!fs.existsSync(logDir)) {
        fs.mkdirSync(logDir);
    }

    var logFile = 'checksum-'+process.pid+'.log';
    //const filename = path.join(logDir, 'results.log');
    const filename = path.join(logDir, logFile);

    const logger = createLogger({
        // change level if in dev environment versus production
        level: env === 'development' ? 'debug' : 'info',
        format: format.combine(
            format.timestamp({
                format: 'YYYY-MM-DD HH:mm:ss'
            }),
            format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
        ),
        transports: [
            new transports.Console({
                level: 'info',
                format: format.combine(
                    format.colorize(),
                    format.printf(
                        info => `${info.timestamp} ${info.level}: ${info.message}`
                    )
                )
            }),
            new transports.File({ filename })
        ]
    });
    /////////////////////// Winston Logger ///////////////////////////// 
    
    //// Validate VCF File, ID and also ID for Multi-Sample VCF Files ///
    
    try {
        var val = await parseFile(inputFile,logger);
        //console.log("Log the promise value received from parseFile");
        //console.log(val);
        if ( val == "Success" ) {
            logger.debug("Checksum process completed");
        }
    } catch(err) {
        console.log("Error is " +err);
    }

})();

async function parseFile(file,logger) {
    //const getSuccess = new Promise( ( resolve ) => resolve("Success") );
    var reFile = /\.gz/g;
    const hash = crypto.createHash('sha256');
    //const verifyCrypto = crypto.createVerify('RSA-SHA256');
    //var rd;
    var rd = fs.createReadStream(file);
    /*
    if (file.match(reFile)) {
        rd = readline.createInterface({
            input: fs.createReadStream(file).pipe(zlib.createGunzip()),
            //output: process.stdout,
            console: false
        });
    } else {
        rd = readline.createInterface({
            input: fs.createReadStream(file),
            //output: process.stdout,
            console: false
        });
        //console.log("Created Read Stream");
        //console.log(rd);
    } */

    //rd.on('readable', function (line) {
    rd.on('readable',  () => {
        var line = rd.read();
        console.log("Line is "+line);
        hash.update(line);
    })
        //console.log("Line is "+line);
        //hash.update(line);
        //console.dir(hash);
    //});

    rd.on('close', async  () => {
        //console.log("*************close handler");
        //hash.digest('hex');
        console.log(`${hash.digest('hex')} ${file}`);
        //var hashSign = hash.digest('hex');
        //const result = verifyCrypto.verify(hashSign);
        //console.log("Result is "+result);
        process.exit(0);
    });

    rd.on('end', function () {
        console.log("END event call received");
    });

    rd.on('error', function () {
        console.log("ERROR event call received.Filehandle destroyed. Internal!!");
    });
}

function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


