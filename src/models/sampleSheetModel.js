const configData = require('../config/config.js');
const { db: { host, port, dbName, sampleSheetCollection } } = configData;
var db = require('../controllers/db.js');

// family:4 // use IPV4, skip trying IPV6
var sampleSchema = {
//var variantSchema = new Schema({
    _id : String,
    statMsg : String,
    loadedTime : {type: Date, default: Date.now},
    PIName : String,
    Description : String,
    FileLocation : String,
    FileType : String,
    FileSourceType: String,
    AssemblyType : String,
    SampleLocalID : String,
    IndLocalID : String,
    SampleTypeName : String,
    SeqMachineName : String,
    SeqKitModelName : String,
    SeqTargetCov : Number,
    SeqTypeName : String,
    SeqTargetReadLen : Number,
    SeqDate : Date,
    PanelTypeName : String,
    IndividualFName : String,
    IndividualLName : String,
    IndividualSex : String,
    IndividualBirthDate : Date,
    status : {type: String, default: 'queued'}
};
    
var sampShColl = db.model('sampleSheetImports',sampleSchema,sampleSheetCollection);

//module.exports = variantSchema;
module.exports = sampShColl;
    
    


