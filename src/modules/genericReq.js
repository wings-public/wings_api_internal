#!/usr/bin/env node
'use strict';
var path = require('path');
const configData = require('../config/config.js');
const { app:{instance,logLoc}, db:{importCollection1,importCollection2,trioCollection} } = configData;

async function getQueryCollName(assemblyType) {
    try {
        var queryCollection = "";
        if ( (assemblyType == 'GRCh37') || (assemblyType == 'hg19') ) {
            queryCollection = importCollection1;
        } else if ( (assemblyType == 'GRCh38')  || (assemblyType == 'hg38') ) {
            queryCollection = importCollection2;
        }
        return queryCollection;
    } catch(err) {
        throw err;
    }
}

module.exports = {getQueryCollName};
