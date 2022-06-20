**Import Process Workflow** 

* **ESAT creates request to Center with the VCF location , fileID : generated at ESAT and batchSize**
1. curl --cacert /variantdb_mongo/ssl/cert.pem -X POST  https://143.169.15.106:80/importSample -H 'Content-Type: application/json' -d '{ "sample" : "/home/nsattanathan/VCF_data/hg38_samples/11231_hg38.vcf.gz","fileId":"11232", "batchSize" : 1000 }'
   
   _Provides pid that can be used to track the import Status_
2. curl --cacert /variantdb_mongo/ssl/cert.pem -X GET https://143.169.15.106:80/importStatus/32350
   
   _Output: Provides Status of import process_
* **Once the import has completed, request for the novel Variants from Center**
1. curl -X GET  https://143.169.15.106:80/novelVariantsStreamZip/11232 -O -J
   _Output : Generates zip file with novel variants_
* **Send Request to Annotation Docker with novelVariants zip file, fileID**
2. curl --cacert /variantdb_mongo/ssl/cert1.pem -X POST  https://143.169.238.133:80/deployStack/11232 -H 'Content-Type: application/x-www-form-urlencoded' --data-binary "@/variantdb_mongo/dump/novelVariants-11232.vcf.gz"
   
   _Output : Deploys the Docker swarm and triggers the Annotation Engines(VEP,CADD)_
3.  curl --cacert /variantdb_mongo/ssl/cert1.pem -X GET  https://143.169.238.133:80/annotationStatus/11232
   
   _Output: Annotation Inprogress or Annotation Error or Annotation Completed_
   _If Annotation has completed, provides location of Annotation data and corresponding checksum file. These files has to be used for the next set of requests
* **Download the Annotation file and checksum file**_
4.  curl --cacert /variantdb_mongo/ssl/cert1.pem  -X GET  "https://143.169.238.133:80/downloadData?annotation=/mnt/data/parseLogs/variantAnnotations.json.11232.gz" -O -J
5.  curl --cacert /variantdb_mongo/ssl/cert1.pem  -X GET  "https://143.169.238.133:80/downloadData?annotation=/mnt/data/parseLogs/variantAnnotations.json.11232.gz.sha256" -O -J
* **Send Request to Center with the Annotations for the Novel Variants and the checksum file**
6.  curl --cacert /variantdb_mongo/ssl/cert.pem -X POST -F 'checksum=@/variantdb_mongo/dump/variantAnnotations.json.11232.gz.sha256' -F 'annotation=@/variantdb_mongo/dump/variantAnnotations.json.11232.gz' -F 'sid=11232' https://143.169.15.106:80/updateNovelAnnotations
   
   _Output : If checksum is valid, updates the Annotations for the Novel Variants_
   

