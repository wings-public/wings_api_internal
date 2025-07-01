const configData = require('../config/config.js');
console.log("File path used in variantDataModel is "+__filename);
console.log("File path used in variantDataModel is "+__dirname);
const { db: { host, port, dbName, importCollection3 } } = configData;
var db = require('../controllers/db.js');

// family:4 // use IPV4, skip trying IPV6
var variantSchema = {
//var variantSchema = new Schema({
    _id : String,
    var_key : String,
    fileID : Number,
    pid : Number,
    svid : String,
    svtool : String,
    start_pos : Number,
    stop_pos : Number,
    sv_len : Number,
    dp : Number,
    min_dp : Number,
    vcf_chr : String,
    chr : Number,
    end_vcf_chr : String,
    end_chr : Number,
    ref_all : String,
    alt_all : String,
    sv_type : String,
    filter : String,
    gt : String,
    gene_name : String,
    tx : String,
    exon_count : Number,
    Location : String,
    Location2 : String,
    re_gene : String,
    TAD_coordinate : String,
    repeat_type_left : String,
    repeat_type_right : String,
    SegDup_left : String,    
    SegDup_right : String,   
    ENCODE_blacklist_left : String,
    ENCODE_blacklist_right : String,
    OMIM_ID : String,
    GnomAD_pLI : Number,
    AnnotSV_ranking_criteria : String,
    ACMG_class : Number
//});
};
    
var varCol = db.model('vcfSVimports38',variantSchema,importCollection3);

//module.exports = variantSchema;
module.exports = varCol;
    
    


