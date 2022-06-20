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
const { db : {dbName,variantAnnoCollection1,variantAnnoCollection2,importCollection1,importCollection2} , app : {instance,logLoc}} = configData;
var loggerMod = require('../controllers/loggerMod');

// needed only when the connection is trigerred by the script
//var createConnection = require('../controllers/dbConn.js').createConnection;
//const getConnection = require('../controllers/dbConn.js').getConnection;

/* Replace the database connection once the script is connected to the Router
Connection has to be created by Router
var db = require('../controllers/db.js');
*/

var bson = require("bson");
var BSON = new bson.BSON();
var client;

(async function () {
    argParser
        .version('0.1.0')
        .option('-i, --sid <sid>', 'sample for which novel variants are retrieved')
        .option('--assembly, --assembly <GRCh37,GRCh38>', 'assembly type to decide the import collection to be used for import')
    argParser.parse(process.argv);


    if ((!argParser.sid) || (!argParser.assembly)) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }
    
    var sid = argParser.sid;
    var assemblyType = argParser.assembly;
    //sid = parseInt(sid);
    console.log("Sample ID received as input is "+sid);
    ///////////////////// Winston Logger //////////////////////////////
    // To be added to a separate library //////

    var logFile = `novelVariants-${sid}-${process.pid}.log`;
    //const filename = path.join(logDir, 'results.log');
    /*var filename;

    // To be added to a separate library //////
    console.log("****************** In getNovelVariants Script ****************");
    console.log("Instance value is "+instance);
    if ( instance == 'dev') {
        // Create the log directory if it does not exist
        const logDir = 'log';
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir);
        }
        filename = path.join(__dirname,logDir, logFile);
    } else {
        // env ( uat, prod ) will be using docker instance. Logs has to be created in the corresponding bind volumes defined in the docker-compose environment file
        // importLogPath will be defined in docker-compose environment variables section 
        filename = path.join(process.env.importLogPath, logFile);
    }*/

    var createLog = loggerMod.logger('tmp',logFile);

    /////////////////////// Winston Logger ///////////////////////////// 
    /*
    try {
        await createConnection();
    } catch(e) {
        console.log("Error is "+e);
    } */


    //client = getConnection();
    //const db = client.db(dbName);
    //console.log("VariantAnnoCollection is "+variantAnnoCollection);
    //var annoCollection = db.collection(variantAnnoCollection);

    //// Validate VCF File, ID and also ID for Multi-Sample VCF Files ///
    var db = require('../controllers/db.js');
    try {
        //const logDir = 'log';
        var vFile = 'novelVariants-'+sid+'.vcf';
        var variantsFile;
        variantsFile = path.join(logLoc,'import','samples','tmp',vFile);
        /*if ( instance == "dev" ) {
            variantsFile = path.join(__dirname,logDir, vFile);
        } else {
            variantsFile = path.join(process.env.importLogPath,vFile);
        }*/
        var val = await getVariantsToAnnotate(db,sid,createLog,variantsFile,assemblyType);
        createLog.debug("Log the promise value received from parseFile");
        createLog.debug(val);
        if ( val == "success") {
            createLog.debug("What is the value that gets returned from the main func");
            createLog.debug("Incremented count tracker");
            // Send the variants file location to the parent process
            process.send(variantsFile);
            process.exit(0);
        }
    } catch(err) {
        createLog.debug("Error is " +err);
        process.exit(0);
    }

})();

async function getVariantsToAnnotate(db,sid,createLog,variantsFile,assemblyType) {
    //const getSuccess = new Promise( ( resolve ) => resolve("Success") );

    createLog.debug("db is "+db);
    sid = parseInt(sid);
    var variantAnnoCollection;
    var importCollection;
    if ( assemblyType == "GRCh37" ) {
        importCollection = importCollection1;
        variantAnnoCollection = variantAnnoCollection1;
    } else if ( assemblyType == "GRCh38" ) {
        importCollection = importCollection2;
        variantAnnoCollection = variantAnnoCollection2;
    }

    var importColl = db.collection(importCollection);
    createLog.debug("importCollection is "+importColl);
    
    // Update 26/04/2022.Excluding NON_REF positions from Annotations
    var matchFilter =  {$match : {'fileID' : {$eq:sid }, 'non_variant' : 0} } ;

    var lookupFilter = { $lookup : { 'from':variantAnnoCollection,'localField':'var_key','foreignField': '_id', 'as':'annotation_data' } };
    var annoMatch = { $match: {'annotation_data.annotated' : 0} };


    // Sample Aggregation
    // db.getCollection('wingsVcfData').aggregate([{$match: {sid: {$eq:930 }}},   {$lookup: { from: "variantAnnotations",localField: "var_key",foreignField: "_id", as: "annotation_data"     }   } , {$match :{'annotation_data.annotated':0} }])

    var wFd = fs.createWriteStream(variantsFile);
    var data = await importColl.aggregate([ matchFilter,lookupFilter,annoMatch]);

    var bulkOps = [];
    while ( await data.hasNext() ) {
        const doc = await data.next();
        var id = doc._id;
        var var_key = doc.var_key; // chr-pos-ref-alt
        var arr = var_key.split('-');
        var re = /chr/g;

        var vcf_chr = doc.vcf_chr;
        var chr = vcf_chr;
        if ( vcf_chr.match(re)) {
            chr = vcf_chr.replace(re, '');
        }
        //var chr = vcf_chr.replace(re, '');
        //console.log(`chr is ${chr}`);
        //var chr = arr[0];
        if ( chr == "23" ) {
            chr = 'X';
        } else if ( chr == "24" ) {
            chr = 'Y';
        } else if (chr == "25" ) {
            chr = 'M';
        }

        // CADD expects the VCF Input format to be CHROM POS ID REF ALT(ID column can be empty but cannot be missing)
        var vcfData = chr+'\t'+arr[1]+'\t'+'.'+'\t'+arr[2]+'\t'+arr[3];
        //console.log("VCF Data "+vcfData);
        wFd.write(vcfData+'\n');
        // VCF chr\tpos\t.\tref\alt
        // vcf format 
    }
    wFd.end();
    // listen for finish event and then resolve the promise.
    // Reason : await is performed on the data read from database, we have to also check if the data has been written to file completely.
    return  new Promise( resolve => {
        wFd.on('finish', async () => {
            //console.log("******************** Are we here ");
            resolve("success");
        } );
    } , reject => {
        wFd.on('error', async () => {
            //console.log("******************** Are we here ----------- ");
            reject("error");
        });
    });
}

function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


