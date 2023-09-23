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
const { db : {dbName,variantAnnoCollection1,variantAnnoCollection2,importCollection1,importCollection2}, app:{instance} } = configData;
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
var annoFields = ['gene_id','fathmm-mkl_coding_pred','fathmm-mkl_coding_score','provean_score','polyphen2_hdiv_pred','metalr_score','impact','polyphen2_hvar_score','metasvm_pred','metasvm_score','cadd_phred','codons','mutationassessor_pred','loftool','consequence_terms','csn','spliceregion'];
// consequence terms & spliceregion will have array values

(async function () {
    argParser
        .version('0.1.0')
        .option('-i, --sid <sid>', 'sample to be annotated')
        .option('--assembly, --assembly <GRCh37,GRCh38>', 'assembly type to decide the import collection to be used for import')
    argParser.parse(process.argv);


    if ((!argParser.sid) || (!argParser.assembly)) {
        argParser.outputHelp(applyFont);
        process.exit(1);
    }
    
    var sid = argParser.sid;
    var assemblyType = argParser.assembly;
    ///////////////////// Winston Logger //////////////////////////////

    var logFile = `updateExistAnno-${sid}-${process.pid}.log`;
    //const filename = path.join(logDir, 'results.log');
    //var filename;

    // To be added to a separate library //////
    /*if ( instance == 'dev') {
        // Create the log directory if it does not exist
        const logDir = 'log';
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir);
        }
        filename = path.join(__dirname,logDir,logFile);
    } else {
        // env ( uat, prod ) will be using docker instance. Logs has to be created in the corresponding bind volumes defined in the docker-compose environment file
        // importLogPath will be defined in docker-compose environment variables section 
        filename = path.join(process.env.importLogPath,logFile);
    } */

    var createLog = loggerMod.logger('import',logFile);

    console.log("************** Updates are logged at "+logFile);

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
        var val = await updateAnnotations(db,sid,createLog,assemblyType);
        if ( val == "success" ) {
            process.exit(0);
        }
    } catch(err) {
        createLog.debug("Error is "+err);
        process.exit(0);
    }
})();

async function updateAnnotations(db,sid,createLog,assemblyType) {
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
    
    // Update 26/04/2022. Excluding NON_REF positions from Annotations
    var matchFilter =  {$match : {'fileID' : {$eq:sid }, 'non_variant' : 0} } ;

    var lookupFilter = { $lookup : { 'from':variantAnnoCollection,'localField':'var_key','foreignField': '_id', 'as':'annotation_data' } };
    var annoMatch = { $match: {'annotation_data.annotated' : 1} };

    var data = await importColl.aggregate([ matchFilter,lookupFilter,annoMatch]);

    var bulkOps = [];
    while ( await data.hasNext() ) {
        const doc = await data.next();
        var setFilter = {};
        var filter = {};
        var updateFilter = {};
        if ( doc['annotation_data'] ) {
            var variant = doc['_id'];
            var annotations = doc['annotation_data'][0];
            var annoId = annotations['_id'];
            //console.log("variant" + variant);
            //console.log("Anno ID "+annoId);
            // Check for curated transcripts
            var geneAnnotations = annotations['annotation'];

            var gnomadObj = {};
            // All the fields of gnomAD will be added to the core fields for filtering.
            if ( annotations['gnomAD'] ) {
                //console.log("gnomAD present")
                //console.log(doc);
                //gnomadObj['AF_all'] = gnomad['AF'];
                //gnomadObj['AN_all'] = gnomad['AN'];
                gnomadObj = annotations['gnomAD'];
            }
            setFilter['gnomAD'] = gnomadObj;
            //console.dir(gnomadObj,{"depth":null});
            
            //console.dir(gnomad);
            
            if ( annotations['CADD_PhredScore'] ) {
                //console.log("CADD score present")
                setFilter['phred_score'] = annotations['CADD_PhredScore'];
            }

            if ( annotations['ClinVar'] ) {
                //console.log("ClinVar present");
                var clinVar = annotations['ClinVar'];
                //console.log("Logging Clinvar Annotations ");
                //console.dir(clinVar,{"depth":null});
                setFilter['CLNSIG'] = clinVar['CLNSIG'];
                setFilter['GENEINFO'] = clinVar['GENEINFO'];
            }

            // check and add RNA Central annotations
            if ( annotations['RNACentral'] ) {
                //console.log("ClinVar field present");
                setFilter['RNACentral'] = annotations['RNACentral'];
            }

            if ( annotations['regulatory_feature_consequences'] ) {
                //console.log("regulatory_feature_consequences present")
                setFilter['regulatory_feature_consequences'] = annotations['regulatory_feature_consequences'];
            }
    
            if ( annotations['motif_feature_consequences'] ) {
                //console.log("motif_feature_consequences present")
                setFilter['motif_feature_consequences'] = annotations['motif_feature_consequences'];
            }

            
            //console.dir(setFilter,{"depth":null});

            var coreGeneAnno = [];
            for ( var idx in geneAnnotations ) {
                var coreGeneObj = {};
                var geneObj = geneAnnotations[idx];
                var transcript = geneObj['transcript_id'];
                //console.log("Transcript is "+transcript);
                // transcripts to be added to core Annotations
                var transcriptRe = /^NM|^NR|^NP/g;
                // consider only NM, NR and NP transcripts for core filtering
                if ( transcript.match(transcriptRe) ) {
                    //console.log("MATCH FOUND "+transcript);
                    //console.dir(geneObj);
                    coreGeneObj['transcript_id'] = geneObj['transcript_id'];
                    coreGeneObj['impact'] = geneObj['impact'];
                    coreGeneObj['consequence_terms'] = geneObj['consequence_terms'];
                    coreGeneObj['codons'] = geneObj['codons'];
                    coreGeneObj['gene_id'] = geneObj['gene_id'];
                    coreGeneObj['csn'] = geneObj['csn'];
                    if ( 'maxentscan_ref' in geneObj ) {
                        coreGeneObj['maxentscan_ref'] = geneObj['maxentscan_ref'];
                    }
                    if ( 'maxentscan_alt' in geneObj ) {
                        coreGeneObj['maxentscan_alt'] = geneObj['maxentscan_alt']
                    }
                    if ( 'maxentscan_diff' in geneObj ) {
                        coreGeneObj['maxentscan_diff'] = geneObj['maxentscan_diff']
                    }
                    //console.dir(coreGeneObj);
                    //setFilter['gene_annotations'] = coreGeneObj;
                    // store the transcript Annotations
                    coreGeneAnno.push(coreGeneObj);
                } else if ( transcript == "" ) {
                    var consTerms = geneObj['consequence_terms'];
                    var intergenic = 0;
                    for ( var idx in consTerms ) {
                        var consTerm = consTerms[idx];
                        if ( consTerm == "intergenic_variant" ) {
                            intergenic = 1;
                        }
                    }
                    // required to handle intergenic_variants with empty transcript_id
                    if ( intergenic == 1 ) {
                        coreGeneObj['transcript_id'] = geneObj['transcript_id'];
                        coreGeneObj['impact'] = geneObj['impact'] || "";
                        coreGeneObj['consequence_terms'] = geneObj['consequence_terms'] || [];
                        coreGeneObj['codons'] = geneObj['codons'] || null;
                        coreGeneObj['gene_id'] = geneObj['gene_id'] || "";
                        coreGeneObj['csn'] = geneObj['csn'] || "";
                    }
                    coreGeneAnno.push(coreGeneObj);

                }
                
            }
            //console.dir(coreGeneAnno,{"depth":null});
            // Array of transcript-gene Annotations
            setFilter['gene_annotations'] = coreGeneAnno;

            //console.dir(setFilter,{"depth":null});
            //console.dir(setFilter,{"depth":null});
            filter['filter'] = {'_id' : variant};
            filter['update'] = {$set : setFilter}

            updateFilter['updateOne'] = filter;
            
            bulkOps.push(updateFilter);
            if ( (bulkOps.length % 5000 ) === 0 ) {
                //console.log("Execute the bulk update ");
                importColl.bulkWrite(bulkOps, { 'ordered': false }).then(function (res) {
                    createLog.debug("Executed bulkWrite and the results are ");
                    createLog.debug(res.insertedCount, res.modifiedCount, res.deletedCount);
                    }).catch((err1) => {
                        createLog.debug("Error executing the bulk operations");
                        createLog.debug(err1);
                });
                bulkOps = [];
            }
        
        } // annotation if 
    } // while

    // load the remaining annotations present in bulkOps
    if ( bulkOps.length > 0 ) {
        createLog.debug("Update the remaining Annotations");
        try {
            var result = await importColl.bulkWrite(bulkOps, { 'ordered': false });
            return "success";
        } catch(errLog) {
            createLog.debug("Error Log is "+errLog);
            throw errLog;
        }
    } else {
        // This condition is required to handle the case when the size of bulkOps data was loaded in the previous modulus 
        // When there is not enough data to be loaded to mongo db, we have to resolve the promise to ensure that it is resolved at the calling await
        return "success";
    }
}

function applyFont(txt) {
    return colors.red(txt); //display the help text in red on the console
}


