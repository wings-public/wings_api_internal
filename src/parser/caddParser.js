#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const readline = require('readline');
const argParser = require('commander');
const colors = require('colors');

const { createLogger, format, transports } = require('winston');
const configData = require('../config/config.js');
const { db : {dbName,variantAnnoCollection} } = configData;

var createConnection = require('../controllers/dbConn.js').createConnection;
const getConnection = require('../controllers/dbConn.js').getConnection;

var bson = require("bson");
var BSON = new bson.BSON();
var client;

(async function () {
    argParser
        .version('0.1.0')
        .option('-p, --parser <CADD>', 'Annotation Source')
        .option('-i, --input_file <file1>', 'cadd Annotation file to be parsed and loaded')
    argParser.parse(process.argv);


    if ((!argParser.parser) || (!argParser.input_file)) {
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

    var logFile = 'caddParse-'+process.pid+'.log';
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
    try {
        await createConnection();
    } catch(e) {
        logger.debug("Error is "+e);
    }

    client = getConnection();
    const db = client.db(dbName);
    logger.debug("VariantAnnoCollection is "+variantAnnoCollection);
    var annoCollection = db.collection(variantAnnoCollection);

    //// Validate VCF File, ID and also ID for Multi-Sample VCF Files ///
    try {
        var val = await parseFile(inputFile,logger,annoCollection);
        logger.debug("Check the value returned from the promise of parseFile");
        logger.debug(val);
        if ( ( val == "Success" ) || ( val == "Duplicate" ) ) {
            logger.debug("Hey !!!!!!!!! I am going to exit the process ");
            process.exit(0);
        }
    } catch(err) {
        logger.debug("Error is "+err);
        process.exit(0);
    }
})();

async function parseFile(file,logger,annoCollection) {
    //const getSuccess = new Promise( ( resolve ) => resolve("Success") );
    var reFile = /\.gz/g;
    var rd;
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
    }

    var bulkOps = [];
    rd.on('line', function (line) {
        var updateDoc = {};

        var inputD = line.split('\t');
        var id = inputD[0]+'-'+inputD[1]+'-'+inputD[2]+'-'+inputD[3];
        var caddPhredScore = parseFloat(inputD[5]);
        
        var filter = {};
        var setFilter = {};
        var updateFilter = {};
        
        setFilter['CADD_PhredScore'] = caddPhredScore;
        
        setFilter['annotated'] = 1;
        filter['filter'] = {'_id' : id};
        filter['update'] = {$set : setFilter};

        updateFilter['updateOne'] = filter;
        updateFilter['updateOne']['upsert'] = 1;
        
        bulkOps.push(updateFilter);
        if ( bulkOps.length === 100 ) {
            logger.debug("Execute the bulk update ");
            logger.debug(bulkOps.length);
            //console.dir(bulkOps,{"depth":null});
            annoCollection.bulkWrite(bulkOps,{'ordered':false}).then( function(res) {
                logger.debug(res.insertedCount,res.modifiedCount,res.deletedCount);
            }).catch( (err) => {
                logger.debug("Error executing bulk operations "+err);
            });
            logger.debug("Initializing bulkOps to 0");
            bulkOps = [];
            logger.debug("Initialized bulkOps to 0");
            logger.debug(bulkOps.length);
        }
    });

    // resolving promise from the async function handler of close signal
    // promise returned by the close handler has to be processed and returned by the function parseFile
    return  new Promise( resolve => {
        rd.on('close', async () => {
            if ( bulkOps.length > 0 ) {
                try {
                    var res1 = await annoCollection.bulkWrite(bulkOps,{'ordered':false});
                    resolve("Success");        
                } catch(err2) {
                    //logger.debug(err2);
                    resolve("Duplicate");
                }
           } else {
               // This condition is required to handle the case when the size of bulkOps data was loaded in the previous modulus 
               // When there is not enough data to be loaded to mongo db, we have to resolve the promise to ensure that it is resolved at the calling await
               // exit condition of the process is performed on the resolved promise
               resolve("Success");
           }
        })
    });

    rd.on('end', function () {
        logger.debug("END event call received");
    });

    rd.on('error', function () {
        logger.debug("ERROR event call received.Filehandle destroyed. Internal!!");
    });
}

function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


