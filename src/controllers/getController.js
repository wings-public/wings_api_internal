
const { sample } = require('lodash');
const configData = require('../config/config.js');
//added importCollection3
const { db : {host,port,dbName,apiUserColl,apiUser,familyCollection,populationSVCollection,individualCollection,variantAnnoCollection1,variantAnnoCollection2,resultCollection, revokedDataCollection,importStatsCollection,sampleSheetCollection,phenotypeColl,phenotypeHistColl,sampleAssemblyCollection, genePanelColl,fileMetaCollection,hostMetaCollection,indSampCollection,trioCollection,importCollection1,importCollection2,importCollection3,annoHistColl,famAnalysisColl} , app:{trioQCnt}} = configData;


const getConnection = require('../controllers/dbConn.js').getConnection;
//const createApiUser = require('../controllers/userControllers.js').createApiUser;
const createTTLIndexExpire = require('../controllers/dbFuncs.js').createTTLIndexExpire;
const createColIndex = require('../controllers/dbFuncs.js').createColIndex;
//const { create } = require('domain');
var pid = process.pid;
var uDateId = new Date().valueOf();





const SampleSV = async (hostId, piid) => {
    var client = getConnection();
    const db = client.db(dbName);
    const indColl = db.collection(individualCollection);
    const indSampColl = db.collection(indSampCollection);
    const importStatsColl = db.collection(importStatsCollection);
    var formattedSamples = [];
    var filteredSamples =[]
    try {
        

        
        // Retrieve individuals based on HostID and PIID
        const individualsCursor = indColl.find({ HostID: hostId, PIID: piid });
        const individuals = await individualsCursor.toArray();

        // Extract IndividualIDs from the retrieved individuals
        const individualIds = individuals.map(individual => individual._id);

        // Retrieve samples associated with the retrieved individuals and FileType equal to SV_VCF
        const samplesCursor = indSampColl.find({
            individualID: { $in: individualIds },
            FileType: "SV_VCF"
        });
       
        const samples = await samplesCursor.toArray();
        

        if (samples.length == 0) {
            return formattedSamples;
        }

         // Filter samples based on wingsImportStats
         const validSamples = await Promise.all(samples.map(async (sample) => {
            const importStats = await importStatsColl.findOne({ fileID: sample.fileID.toString(), status_id: 8 });
            if (importStats) {
                return sample;
            }
            return null;
        }));

        if (validSamples.length == 0) {
            return formattedSamples;
        }
        // Remove null entries from validSamples
        filteredSamples = validSamples.filter(sample => sample !== null);

        // Format the response
        formattedSamples = filteredSamples.map(sample => ({
            SampleLocalID: sample.SampleLocalID,
            IndividualID: sample.individualID,
            IndividualLocalID: sample.IndLocalID,
            SampleFileID: sample.fileID,
            ReferenceBuildName: sample.AssemblyType
        }));

        return formattedSamples;
    } catch (error) {
        throw error;
    } 
};



const SampleSNV = async (hostId, piid) => {
    var client = getConnection();
    const db = client.db(dbName);
    const indColl = db.collection(individualCollection);
    const indSampColl = db.collection(indSampCollection);
    const importStatsColl = db.collection(importStatsCollection);
    var formattedSamples = [];
    var filteredSamples =[]
    try {
        

        
        // Retrieve individuals based on HostID and PIID
        const individualsCursor = indColl.find({ HostID: hostId, PIID: piid });
        const individuals = await individualsCursor.toArray();

        // Extract IndividualIDs from the retrieved individuals
        const individualIds = individuals.map(individual => individual._id);

        // Retrieve samples associated with the retrieved individuals and FileType equal to SV_VCF
        const samplesCursor = indSampColl.find({
            individualID: { $in: individualIds },
            $or: [
                { FileType: "VCF" },
                { FileType: "gVCF" }
            ]
        });
       
        const samples = await samplesCursor.toArray();
        

        if (samples.length == 0) {
            return formattedSamples;
        }

         // Filter samples based on wingsImportStats
         const validSamples = await Promise.all(samples.map(async (sample) => {
            const importStats = await importStatsColl.findOne({ fileID: sample.fileID.toString(), status_id: 8 });
            if (importStats) {
                return sample;
            }
            return null;
        }));

        if (validSamples.length == 0) {
            return formattedSamples;
        }
        // Remove null entries from validSamples
        filteredSamples = validSamples.filter(sample => sample !== null);

        // Format the response
        formattedSamples = filteredSamples.map(sample => ({
            SampleLocalID: sample.SampleLocalID,
            IndividualID: sample.individualID,
            IndividualLocalID: sample.IndLocalID,
            SampleFileID: sample.fileID,
            ReferenceBuildName: sample.AssemblyType
        }));

        return formattedSamples;
    } catch (error) {
        throw error;
    } 
};



const getVariantsByVectorAndMember = async (TrioLocalID, vector, page = 1, limit) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const trioColl = db.collection(trioCollection);
        const variantCollection = db.collection(importCollection3);
        

        // Retrieve trio information based on TrioLocalID
        const trio = await trioColl.findOne({ TrioLocalID });

        // Determine which member's fileID to use based on the vector
        let fileID;
        if (vector[0] === '1') {
            fileID = trio.trio.find(member => member.relation === 'Proband').fileID;
        } else if (vector[1] === '1') {
            fileID = trio.trio.find(member => member.relation === 'Father').fileID;
        } else if (vector[2] === '1') {
            fileID = trio.trio.find(member => member.relation === 'Mother').fileID;
        } else {
            throw new Error('Invalid vector. At least one member (Proband, Father, or Mother) must be selected.');
        }
        console.log(fileID)
       // , [TrioLocalID]: vector
        // Query variants based on the selected fileID and TrioLocalID
        const filter = {
            fileID: fileID // Fixed field name with a variable value
        };
        filter[TrioLocalID] = vector; // Dynamic field name with a variable value
        
        // Querying the collection
        console.log(filter)
        const variants = await variantCollection
        .find(filter)
        .skip((page - 1) * limit)
        .limit(limit)
        .toArray();
        
        

        return variants;
    } catch (error) {
        throw error;
    }
};



module.exports = {SampleSV,SampleSNV,getVariantsByVectorAndMember};