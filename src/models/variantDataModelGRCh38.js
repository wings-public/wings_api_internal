const configData = require('../config/config.js');
console.log("File path used in variantDataModel is "+__filename);
console.log("File path used in variantDataModel is "+__dirname);
const { db: { host, port, dbName, importCollection2 } } = configData;
var db = require('../controllers/db.js');

// family:4 // use IPV4, skip trying IPV6
var variantSchema = {
//var variantSchema = new Schema({
    _id : String,
    var_key : String,
    var_validate : Number,
    fileID : Number,
    pid : Number,
    start_pos : Number,
    stop_pos : Number,
    dp : Number,
    min_dp : Number,
    vcf_chr : String,
    chr : Number,
    ref_all : String,
    alt_all : String,
    v_type : String,
    non_variant : Number,
    multi_allelic : Number,
    alt_cnt : Number,
    gt_ratio : Number,
    ploidy : Number,
    somatic_state : Number,
    stretch : Number,
    stretch_unit : Number,
    stretch_lt_a : Number,
    stretch_lt_b : Number,
    inheritance : Number,
    inheritance_mode : Number,
    class_v : Number, // classifier data
    autoclassified : Number, // classifier data
    validation : String, // sequencer(Sanger/Not Validated)
    validation_details : String,
    filter : String,
    ref_depth : Number,
    alt_depth : Number,
    phred_genotype : Number,
    phred_polymorphism : Number,
    vqslod : Number,
    delta_pl : Number,
    mapping_quality : Number,
    base_q_ranksum : Number,
    mapping_q_ranksum : Number,
    read_pos_ranksum : Number,
    strand_bias : Number,
    quality_by_depth : Number,
    fisher_strand : Number,
    phred_fisher_pval : Number,
    ref_base_q : Number,
    alt_base_q : Number
//});
};
    
var varCol = db.model('vcfimports38',variantSchema,importCollection2);

//module.exports = variantSchema;
module.exports = varCol;
    
    


