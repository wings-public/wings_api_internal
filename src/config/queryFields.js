var queryFields = {
   "ClinVar_Class" : "CLNSIG",
   "ClinVar_GeneInfo" : "GENEINFO",
   "Cadd_Score" : "phred_score",
   "Zygosity" : "alt_cnt",
   "Call_Status_Filter" : "filter",
   "Call_Status_Mutation_Type" : "v_type", 
   "Allele_Frequencies_gnomAD_frequency" : "gnomAD.AF_all",
   "Allele_Number_gnomAD" : "gnomAD.AN_all",
   "chr" : "chr",
   "gt_ratio" : "gt_ratio",
   "start_pos" : "start_pos",
   "stop_pos" : "stop_pos",
   "transcript_id" : "gene_annotations.transcript_id",
   "gene_id" : "gene_annotations.gene_id",
   "Effect" : "gene_annotations.consequence_terms",
   "Impact" : "gene_annotations.impact",
   "Codons" : "gene_annotations.codons",
   "CSN" : "gene_annotations.csn"
};

module.exports = queryFields;
