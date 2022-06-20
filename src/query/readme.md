- [Query Execution](#query-execution)
## Query Execution
* Sample Execution Commands

  * **MultiAllelic**
  ```test1
  node sampleFilterQuery.js --invoke_type sample_based --json ./json/chrexome_test_mallelic.json --sid 5759 --relation --filter_option_clause --projections 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type' --batchRange  500
  ```
  * **NotMatch Exome**
  ```test1
  node sampleFilterQuery.js --invoke_type sample_based --json ./json/chrexome_NotMatch.json --sid 9360 --filter_option_clause --projections 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type' --batchRange  5000
  ```
  
  * **NotMatch Family Exome**
  ```test1
  node sampleFilterQuery.js --invoke_type sample_based --json ./json/chrexomeFamily_NotMatch.json --sid 9360 --relation --filter_option_clause --projections 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type' --batchRange  5000
  ```
  ```test1
  node sampleFilterQuery.js --invoke_type sample_based --json ./json/chrexomeFamily_NotMatch_type1.json --sid 134 --relation --filter_option_clause --projections 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type' --batchRange  5000
  ```

  * **NotMatch Genome**
  ```test1
  node sampleFilterQuery.js --invoke_type sample_based --json ./json/chrgenome_NotMatch.json --sid 161621 --filter_option_clause --projections 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type' --batchRange  10000
  ```
  * **NotMatch Family Genome**
  ```test1
  node sampleFilterQuery.js --invoke_type sample_based --json ./json/chrgenomeFamily_NotMatch.json --sid 161617 --relation --filter_option_clause --projections 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type' --batchRange  10000
  ```
  ```test1
  node sampleFilterQuery.js --invoke_type sample_based --json ./json/chrgenomeFamily_NotMatch_type1.json --sid 18318 --relation --filter_option_clause --projections 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type' --batchRange  550
  ```
  * **Position Query1**
  ```test1
  node sampleFilterQuery.js --position 768118 --chr 1
  ```
  * **Position Query 2**
  ```test1
  node sampleFilterQuery.js --position 3219072 --chr 1
  ```
    * **Position Query 3**
  ```test1
  node sampleFilterQuery.js --position 103976 --chr 2
  ```
    * **Position Query 4**
  ```test1
  node sampleFilterQuery.js --position 207890866 --chr 1
  ```