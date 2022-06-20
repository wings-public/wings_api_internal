#!/usr/bin/env node
'use strict';
const spawn  = require('child_process');
const readline = require('readline');
var path = require('path');
const stream = require('stream');
const util = require('util');
const promisify = require('util').promisify;
const { createReadStream, createWriteStream, stat ,unlink,existsSync} = require('fs');
var stats = promisify(stat);
const fs = require('fs');
//var multer = require('multer');
const argParser = require('commander');
var loggerMod = require('../controllers/loggerMod');
const pipeline = util.promisify(stream.pipeline);
const configData = require('../config/config.js');
const { app:{instance,logLoc,liftMntSrc,liftMntDst,gatkImg,liftChain1,liftChain2,liftFasta1,liftFasta2,liftoverDocker,gatkLoc,ucscMntDst,uliftChain1,uliftChain2,uliftFasta1,uliftFasta2}, db:{importStatsCollection} } = configData;
const closeSignalHandler = require('../controllers/execChildProcs.js').closeSignalHandler;

//var db = require('../controllers/db.js');
(async function () {
    try {
        argParser
            .version('0.1.0')
            .option('-v, --variant <variant>', 'variant')
            .option('-r, --req_id <request id>', 'batch size for bulkUpload')
            .option('-a, --assembly <assembly>', 'assembly type or reference build used')
        argParser.parse(process.argv);


        var variant = argParser.variant;
        var reqId = argParser.req_id;
        var assembly = argParser.assembly;

        // split the variant and write to a file
        // Note: Also include the template
        
        // based on assembly setup fasta and chain file.
        var assemblyDir;
        var inputFile;
        var outputFile;
        var chainFile;
        var fastaFile;
        var rejectFileTmp;
        var variantFile = liftMntSrc+'/'+reqId+'-variant.bed';
        console.log("********** VARIANT FILE is "+variantFile);
        var chainurl;
        var fastaurl;
        var dictFile;
        
        var chainurl1 = process.env.CHAIN_URL1 || "https://hgdownload.soe.ucsc.edu/goldenPath/hg19/liftOver/hg19ToHg38.over.chain.gz";
        var chainurl2 = process.env.CHAIN_URL2 || "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/liftOver/hg38ToHg19.over.chain.gz";
        var fastaurl1 = process.env.CHAIN_FA1 || "https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/latest/hg19.fa.gz";
        var fastaurl2 = process.env.CHAIN_FA2 || "https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/latest/hg38.fa.gz";

        
        // create required directories
        var basePath = "";
        var standalone = 1;
        if ( standalone ) { 
            // gatk standalone/installed version will be used.
            // create folders under liftMntDst
            console.log("False section");
            basePath = ucscMntDst;
        } 
        

        var path1 = path.join(basePath,"hg19");
        var path2 = path.join(basePath,"hg38");
        if (!fs.existsSync(path1)) {
            fs.mkdirSync(path1);
            console.log("Created required path "+path1);
        }
        if (!fs.existsSync(path2)) {
            fs.mkdirSync(path2);
            console.log("Created required path "+path2);
        }


        console.log("assembly is *****************"+assembly);
        if ( (assembly == "hg19") || (assembly == "GRCh37") ) {
            assemblyDir = "hg19";
            inputFile = '/'+"hg19"+'/'+reqId+'.bed';
            outputFile = '/'+"hg38"+'/'+reqId+'.bed';
            chainFile = `${uliftChain1}`;
            fastaFile = `${uliftFasta2}`;
            rejectFileTmp = '/'+"hg38"+'/'+reqId+'unlifted.bed';
            chainurl = chainurl1;
            fastaurl = fastaurl2;
        } else if ( (assembly == "hg38") || (assembly == "GRCh38") ) {
            assemblyDir = "hg38";
            inputFile = '/'+"hg38"+'/'+reqId+'.bed';
            outputFile = '/'+"hg19"+'/'+reqId+'.bed';
            chainFile = `${uliftChain2}`;
            fastaFile = `${uliftFasta1}`;
            rejectFileTmp = '/'+"hg19"+'/'+reqId+'unlifted.bed';
            chainurl = chainurl2;
            fastaurl = fastaurl1;
        }
        dictFile = fastaFile+'.dict';

        // check if files for liftover exists
        // Note : The section below has to be customized for development
        // Chain files are sequence dictionary are available in development
        // Below section, checks and downloads these files in production and also in centers.

        //var stat1 = await checkDownloadFile(chainurl,chainFile);
        //var stat2 = await checkDownloadFile(fastaurl,fastaFile);

        //console.log("CALL create sequence dictionary");
        //var getSeqDict = await execSeqDict(fastaFile,dictFile);
        
        
        // split variant
        var data = variant.split('-');
        var chrReg = /^chr/i;
        var chrom = data[0];
        if ( ! chrom.match(chrReg)) {
            chrom = 'chr'+data[0];
        }
        var startPos = data[1];
        var stopPos = data[2];

        var hostLoc = liftMntSrc+inputFile;
        var dockLoc = ucscMntDst+inputFile;
        var hostOpFile = liftMntSrc+outputFile;
        var dockOpFile = ucscMntDst+outputFile;
        var rejectFile = ucscMntDst+rejectFileTmp;
        var hostRejectFile = liftMntSrc+rejectFileTmp;
        //chainFile = liftMntSrc+chainFile;
        console.log(`hostLoc : ${hostLoc}`);
        //console.log(`dockLoc ${dockLoc}`);
        var stream;
        var cmd = "";

        try {
            stream = createWriteStream(hostLoc);

            var bed_data = `${chrom}:${startPos}-${stopPos}\n`;
            //await pipeline(template,fs.createWriteStream(hostLoc));
            stream.on('error',(error) => {
                console.log("Looks like some issue in writing to file");
            })
            stream.write(bed_data);
            stream.end(); 

            // call the docker command to perform liftover
            
            //console.log(liftoverDocker);

            // gatk docker will be used

            if ( standalone ) {
                console.log(`hostLoc ${hostLoc}`);
                console.log(`chainFile ${chainFile}`);
                console.log(`hostOpFile ${hostOpFile}`);
                console.log(`hostRejectFile ${hostRejectFile}`);
                //var ucscLoc = "/home/nsattanathan/UCSC";
                var ucscLoc = ".";
                var currPath = path.parse(__dirname).dir;
                var ucscLoc = path.join(currPath,'modules','liftOver');

                console.log("Current Path is "+currPath);
                cmd = `${ucscLoc} -positions ${hostLoc} ${chainFile} ${hostOpFile} ${hostRejectFile}`;
            } 

        } catch(err2) {
            console.log(err2);
            console.log("Error in this block");
            //process.exit(1);
        }
        // write the data to a file
  
        spawn.exec(cmd, async(err, stdout, stderr) => {
            //console.log("Executing command");
            //console.log(stdout);
            if (err) {
                console.log(err);
                console.log("Logging error message below spawn error section: ");
            } else {
                console.log("liftover performed");
                //console.log(stdout);
                // read the file

                //console.log("Checking output file "+hostOpFile);
                var lineArr = [];
                try {
                    //console.log("Starting with getting the file handle");
                    var rd = await getFileHandle(hostOpFile);
                    //console.log("Got file handle");
                    if ( rd ) {
                        lineArr = await getFileData(rd,hostOpFile);
                    }
                } catch(err) {
                    //process.exit(1);
                }

                
                if ( lineArr.length > 0 ) {
                    console.log("liftover performed and there is some liftover data");
                    //console.log(lineArr);
                    var liftedData = lineArr[0];
                    var chrRegion = liftedData.split(':');
                    var re = /chr/g;
                    var chr = chrRegion[0].replace(re, '');
                    var region = chrRegion[1].split('-');
                    var liftedPos = chr+'-'+region[0]+'-'+region[1];
                    console.log(`Lifted Region is ${liftedPos}`);
                
                    try {
                        var res = await writeFile(variantFile,liftedPos);
                        console.log(`Variant data has been written to the file ${variantFile}`);
                    } catch(err) {
                        console.log(err);
                        console.log("Error in writing data to the variant file");
                    }
                    //var liftedVar = 
                } else {
                    var rejectData = "";
                    try {
                        var rd2 = await getFileHandle(hostRejectFile);
                        var rejectArr = await getFileData(rd2,hostRejectFile);
                        if ( rejectArr.length > 0 ) {
                            var parseStr = rejectArr[0];
                            //var msg = parseStr.split('\t');
                            //var reason = msg[6];
                            var rejReason = "Variant could not be lifted over to the other assembly.Reason:";
                            rejectData = rejReason;
                        }
                        console.log("Logging reject array");
                        console.log(rejectArr);
                    } catch(err) {

                    }
                    console.log("No data lifted over");    
                    var res = await writeFile(variantFile,rejectData);
                    console.log("Checking output file "+hostOpFile);
                }
                    
                
    
            }
        });

        
    } catch(err) {
        //throw err;
        console.log(err);
        console.log("Last catch block error");
    }
}) ();


async function execSeqDict(faFile,dictFile) {
    try {
        var cmd1;
        if ( liftoverDocker == "false" ) {
            cmd1 = `${gatkLoc}/gatk --java-options "-Xmx6G"  CreateSequenceDictionary -R ${faFile} -O ${dictFile}`;
        } else {
            cmd1 = `docker run  --rm -v ${liftMntSrc}:${ucscMntDst}   ${gatkImg}  gatk --java-options "-Xmx6G"  CreateSequenceDictionary -R ${faFile} -O ${dictFile}`;
        }
        console.log(`Sequence dictionary command is ${cmd1}`);

        if ( ! fs.existsSync(dictFile)) {
            return new Promise( (resolve,reject) => {
                spawn.exec(cmd1, (error,stdout,stderr) => {
                    if(error) {
                        console.log(error);
                        console.log("Logging error message below spawn error section: ");
                        reject(error);
                        //return;
                    }
                    console.log("Sequence dictionary created");
                    resolve(stdout);
                });
            })
        }
        return "success";
    } catch(err) {
        console.log(err);
        throw err;
    }
}

async function checkDownloadFile(uri,filename) {
    try {
        console.log("Trying with the following for checkDownloadFile");
        console.log(uri);
        console.log(filename);
        if ( ! fs.existsSync(filename)) {
            var cmd = `curl -o ${filename} ${uri}`;
            var res = spawn.execSync(cmd);
            console.log("Downloaded file"+filename);
            return "success";
        } else {
            console.log("File already exists "+filename);
            return "success";
        }
    } catch(err) {
        console.log("Logging error below");
        console.log(err);
        throw err;
    }
}

async function getFileHandle(file) {
    var rd = false;
    var reFile = /\.gz/g;
    try {
        console.log("*********************************");
        if ( ! fs.existsSync(file)) {
            return rd;
        } else {
            if (file.match(reFile)) {
                rd = readline.createInterface({
                    input: fs.createReadStream(file).pipe(zlib.createGunzip()),
                    console: false
                });
            } else {
                rd = readline.createInterface({
                    input: fs.createReadStream(file),
                    console: false
                });
            }
            rd.on('error', () => console.log('errr rd stream'));
            return rd;
        }
        
    } catch(err) {
        console.log("Is the error caught here ?");
    }
    
}

async function getFileData(rd,filename) {
    try {
        var lineArr = [];

        if ( ! fs.existsSync(filename)) {
            resolve(lineArr);
        } else {
            var lineCnt = 0;
            var lineRe = /^#/g;
            var blankLine = /^\s+$/g;
        
            //console.log("Collection Object created ");
            var headerArr = [];
            
            //console.log("Line Count is *************"+lineCnt);
            rd.on('line', (line) => {
                if ( ! line.match(lineRe) && ! line.match(blankLine)) {
                    //createLog.debug(line);
                    lineArr.push(line);
                    console.log(line);
                } else {
                    //createLog.debug(line);
                    headerArr.push(line);
                    //console.log(line);
                }
            });
            rd.on('error',(error) => {
                console.log("Looks like some issue in reading file");
            });

            return new Promise( resolve => {
                rd.on('close', async () => {
                    resolve(lineArr);
                }); // close handler
            }); // close handler promise definition
        }
    } catch(err) {

    }
}

const writeFile = (pathNew, data, opts = 'utf8') => 
    new Promise((resolve, reject) => {
        fs.writeFile(pathNew, data, opts, (err) => {
            if (err) reject(err)
            else resolve()
        })
})

