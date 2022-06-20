#!/usr/bin/env node
'use strict';
const spawn  = require('child_process');
//const runningProcess = require('is-running');
var path = require('path');
const promisify = require('util').promisify;
const { createReadStream, createWriteStream, stat ,unlink,existsSync} = require('fs');
var stats = promisify(stat);
//var multer = require('multer');
const argParser = require('commander');
var loggerMod = require('../controllers/loggerMod');
const configData = require('../config/config.js');
const { app:{instance,logLoc}, db:{importStatsCollection} } = configData;
const getAnnoApiToken = require('../controllers/execApiFuncs.js').getAnnoApiToken;
//var db = require('../controllers/db.js');
(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-r, --req_type <req_type>', 'sample_file or sample_url')
            .option('-j, --json_format <json_format>', 'vcf file ID')
        argParser.parse(process.argv);

        var reqType = argParser.req_type;
        var jsonFormat = argParser.json_format;
        
        var token = await getAnnoApiToken();

        await assignProband(token,famId,indId);
        await updateRelative(token,famId,indId,nodekey);

    } catch (err) {

    }
}) ();