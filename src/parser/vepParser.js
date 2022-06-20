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
var initialize = require('../controllers/entityController.js').initialize;

var bson = require("bson");
var BSON = new bson.BSON();
var client;
var annoFields = ['gene_id','fathmm-mkl_coding_pred','fathmm-mkl_coding_score','provean_score','polyphen2_hdiv_pred','metalr_score','impact','polyphen2_hvar_score','metasvm_pred','metasvm_score','cadd_phred','codons','mutationassessor_pred','loftool','consequence_terms','csn','spliceregion'];
// consequence terms & spliceregion will have array values

(async function () {
    argParser
        .version('0.1.0')
        .option('-p, --parser <VEP>', 'Annotation Source')
        .option('-i, --input_file <file1>', 'Ensembl VEP Generated JSON file')
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

    var logFile = 'annoParse-'+process.pid+'.log';
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
        logger.debug("Calling initialize to create the initial collections");
        var data = await initialize();
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
        if ( ( val == "Success" ) || ( val == "Duplicate" ) ) {
            logger.debug("Hey !! exit the process ");
            process.exit(0);
        }
    } catch(err) {
        logger.debug("Error is "+err);
        process.exit(0);
    }

})();

async function parseFile(file,logger,annoCollection) {
    var reFile = /\.gz/g;
    var rd;
    if (file.match(reFile)) {
        rd = readline.createInterface({
            input: fs.createReadStream(file).pipe(zlib.createGunzip()),
            console: false
        });
    } else {
        rd = readline.createInterface({
            input: fs.createReadStream(file),
            console: false
        });
    }

    var bulkOps = [];
    rd.on('line', function (line) {
        var document = {};
        var updateDoc = {};
        //logger.debug("************** SCANNING ******************************* ");
        //logger.debug(line);
        var parsedJson = JSON.parse(line);
        //console.dir(parsedJson);
        var input = parsedJson['input'];
        var inputD = input.split('\t');
        logger.debug(inputD);
        var id = inputD[0]+'-'+inputD[1]+'-'+inputD[3]+'-'+inputD[4];
        //logger.debug(id);
        var asn = parsedJson['assembly_name'];
        var start = parsedJson['start'];
        var end = parsedJson['end'];
        var transcripts = parsedJson['transcript_consequences']; // array of hashes. Each hash holds transcript details.
        var customAnno = {};
        var customAnnoData = {};
        if ( parsedJson['custom_annotations'] ) {
            customAnno = parsedJson['custom_annotations'];
            //logger.debug("Logging custom Annotations");
            //console.dir(customAnno,{"depth":null});
            //console.dir(customAnno,{"depth":null});

            if ( customAnno['gnomADg']) {
                var gArr = customAnno['gnomADg'];
                for (var idx in gArr) {
                    var tmpArr = gArr[idx];
                    //logger.debug("Check the structure of gnomad fields array");
                    //console.dir(tmpArr,{"depth":null});
                    customAnnoData['gnomADg'] = tmpArr['fields'];
                    //logger.debug("Check the added gnomADg key to customAnnoData");
                    //console.dir(customAnnoData,{"depth":null});
                }
            }
            if ( customAnno['ClinVar'] ) {
                var cArr = customAnno['ClinVar'];
                for ( var idx1 in cArr ) {
                    var tmpArr = cArr[idx1];
                    customAnnoData['ClinVar'] = tmpArr['fields'];
                }
            }
        }
        console.dir(customAnnoData,{"depth":null});
        var storeAnno = [];
        for ( var idx in transcripts ) {
            var anno = transcripts[idx];
            var tId = anno['transcript_id'];
            var transcriptAnno = {};
            for ( var idx1 in annoFields ) {
                var field = annoFields[idx1];
                if ( anno[field] ) {
                    transcriptAnno[field] = anno[field];
                }
            }
            transcriptAnno['transcript_id'] = anno['transcript_id'];
            storeAnno.push(transcriptAnno);
            //storeAnno[tId] = transcriptAnno;
        }
        var re = /chr/g;
        var variant;
        logger.debug("ID is "+id);
        // chr prefix is present in the input tag in the generated json file based on the input file used for generating vep annotations
        if (id.match(re)) {
            variant = id.replace(re, '');
            //logger.debug("New String is "+chr);
        } else {
            variant = id;
        }

        //logger.debug("Variant after replacing chr prefix is "+variant);
        var filter = {};
        var setFilter = {};
        var updateFilter = {};
        
        setFilter['annotation'] = storeAnno;
        setFilter['ClinVar'] = customAnnoData['ClinVar'];
        setFilter['gnomAD'] = customAnnoData['gnomADg'];
        setFilter['annotated'] = 1;
        filter['filter'] = {'_id' : variant};
        filter['update'] = {$set : setFilter}

        updateFilter['updateOne'] = filter;
        // by default upsert is false. Setting it to true below
        updateFilter['updateOne']['upsert'] = 1;
        
        bulkOps.push(updateFilter);
        logger.debug(bulkOps.length);
        if ( bulkOps.length  === 1000 ) {
            logger.debug("Execute the bulk update ");
            //console.dir(bulkOps,{"depth":null});
            annoCollection.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                logger.debug(res.insertedCount, res.modifiedCount, res.deletedCount);
            }).catch((err1) => {
                logger.debug("Error executing the bulk operations");
                logger.debug(err1);
            });
            logger.debug("Initializing bulkOps to 0");
            bulkOps = [];
        }
    });

    return new Promise( resolve => {
        rd.on('close', async () => {
            if ( bulkOps.length > 0 ) {
                try {
                    var res1 = await annoCollection.bulkWrite(bulkOps,{'ordered':false});
                    resolve("Success");
                } catch(err1) {
                    // duplicate key issue when the key is present in the existing mongo collection
                    //logger.debug(err1);
                    resolve("Duplicate");
                }
            } else {
                // This condition is required to handle the case when the size of bulkOps data was loaded in the previous modulus 
                // When there is not enough data to be loaded to mongo db, we have to resolve the promise to ensure that it is resolved at the calling await
                // exit condition of the process is performed on the resolved promise
                resolve("Success");
            }
        });
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


