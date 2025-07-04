- [Import Commands](#import-commands)
## Import Commands
* Sample Execution Commands

   * **Exome Samples - Single Sample**
```
  * time node  importSample.js -s "<file_path>/927.vcf.gz" --vcf_file_id 927 
  * time node  importSample.js -s "<file_path>/929.vcf.gz" --vcf_file_id 929
  * time node  importSample.js -s "<file_path>/979.vcf.gz" --vcf_file_id 979
  * time node  importSample.js -s "<file_path>/9351.vcf.gz" --vcf_file_id 9351
```

* **Exome Samples - Multiple Samples**
```
time node importSample.js -s "<file_path>/9360.vcf.gz,<file_path>/9361.vcf.gz,<file_path>/9362.vcf.gz,<file_path>/9363.vcf.gz,<file_path>/9364.vcf.gz,<file_path>/9365.vcf.gz,<file_path>/9366.vcf.gz,<file_path>/9367.vcf.gz,<file_path>/9368.vcf.gz,<file_path>/9369.vcf.gz,<file_path>/9370.vcf.gz" --vcf_file_id "9360,9361,9362,9363,9364,9365,9366,9367,9368,9369,9370" > dataBulkLoad
```

* **Genome Samples - Single Sample**
```
time node importSample.js -s "<file_path>/161619.snp.vcf.gz" --vcf_file_id "161619"
```