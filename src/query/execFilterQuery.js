const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const Async = require('async');
const spawn  = require('child_process');


//const mongoHost = process.env.MONGODB_HOST;
var mongoHost = 'localhost';

//const mongoDB = process.env.MONGODB_DATABASE;
//const mongoPort = process.env.MONGODB_PORT;

var args = process.argv.slice(2);
var input = args[0];

var subprocess;
// EXOME
if ( input === "exome1" ) {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--invoke_type','sample_based','--json', './json/chrexome_test_mallelic.json', '--sid' ,'5759', '--filter_option_clause','--projections', 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type','--batchRange','5000'] );
} else if ( input === "exome2") {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--invoke_type','sample_based','--json', './json/chrexome_NotMatch.json', '--sid' ,'9360', '--filter_option_clause','--projections', 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type','--batchRange','5000'] );
} else if ( input === "exome_fam1" ) {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--invoke_type','sample_based','--json', './json/chrexomeFamily_NotMatch.json', '--sid' ,'9360', '--relation', '--filter_option_clause','--projections', 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type', '--batchRange','5000'] );
} else if ( input === "exome_fam2") {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--invoke_type','sample_based','--json', './json/chrexomeFamily_NotMatch_type1.json', '--sid' ,'134', '--relation', '--filter_option_clause','--projections', 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type', '--batchRange','5000'] );
} else if ( input === "genome1" ) {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--invoke_type','sample_based','--json', './json/chrgenome_NotMatch.json', '--sid' ,'161621', '--filter_option_clause','--projections', 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type', '--batchRange','10000'] );
} else if ( input === "genome2") {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--invoke_type','sample_based','--json', './json/chrgenome_NotMatch.json', '--sid' ,'18328', '--filter_option_clause','--projections', 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type', '--batchRange','10000'] );
} else if ( input === "genome_fam1" ) {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--invoke_type','sample_based','--json', './json/chrgenomeFamily_NotMatch.json', '--sid' ,'161617', '--relation', '--filter_option_clause','--projections', 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type', '--batchRange','10000'] );
} else if ( input === "genome_fam2") {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--invoke_type','sample_based','--json', './json/chrgenomeFamily_NotMatch_type1.json', '--sid' ,'18318', '--relation', '--filter_option_clause','--projections', 'chr,alt_cnt,start_pos,sid,ref_all,alt_all,read_pos_ranksum,vqslod,phred_genotype,v_type', '--batchRange','10000'] );
} else if ( input === "pos1" ) {
    // Insertions-Deletions
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--position','768118','--chr','1'] );
} else if ( input === "pos2" ) {
    // snv
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--position','3219072','--chr','1'] );
} else if ( input === "pos3" ) {
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--position','103976','--chr','2'] );
} else if ( input === "pos4" ) {
    // snv
    subprocess = spawn.fork('./sampleFilterQuery.js', ['--position','207890866','--chr','1'] );
}

console.log("Program Started at "+new Date());
var collObj;
var client;

// IIFE to await and get the collection object
( async function() {
    collObj = await getColObj();
    //console.log(collObj);
} ) ();

subprocess.on('message',function(data) {
    //console.log("Received data in stdout handle");
    console.log(`Got data: ${data}`);
    //console.log("End of data -------");

    // Issue : When two batches of data is available and the data is sent by the child process for say two batches together, parse functionality does not work as expected
    /* Got data: {"batch":"batch7","pid":5464,"req":"done"}
        {"batch":"batch8","pid":5464,"req":"done"}
        End of data -------
    */

    var str = JSON.parse(data);
    //console.log(str);
    var pid = str['pid'];
    var val = str['batch'];
    var req = str['req'];
    //console.log("Data loaded for "+val);
    //console.log("Batch is now ------------ "+req);
    furtherProcess(str,val,pid,req);
    //console.log("Batch is ------------ "+req);
});

subprocess.on('data',function(data) {
    console.log(`subprocess stderr:${data}`);
});

// handler to listen when the child exits or has completed
subprocess.on('close',function(code) {
    if ( code != 0 ) {
        console.log(`Child process exited with code ${code}`);
    } 
});

// handler to receive IPC signals. messages sent by child using process.send can be received with this handler
subprocess.on('message',function(message) {
    console.log(`Parent Received the following message ${message}`);
});

process.on('beforeExit', (code) => {
    console.log(`--------- About to exit PARENT PROCESS with code: ${code}`);
});

async function getColObj() {
    const url = 'mongodb://'+mongoHost+':27017';
    client = await MongoClient.connect(url,{ useNewUrlParser : true });
    const db = client.db('VariantDB');
    const collection = db.collection('variantQueryResults_tmp');
    return Promise.resolve(collection);
}

// Operations related to Annotation filtering and adding data to UI can be handled here.
// To Fetch the data, it will filter data with fields pid, batch from variantQueryResults table
function furtherProcess(obj,batch,pid,req) {
    //console.log("Received object - func furtherProcess");
    console.dir(obj,{depth:null});
    filter = {'batch':batch,'pid':pid};
    //console.log("Filter has been set");
    //console.dir(filter,{"depth":null});

    // Execute the filter and get the stream
    var stream = collObj.find(filter).stream();

    stream.on('end', function() {
       //console.log("Completed the current stream operation");
       console.log("Done at time "+new Date());
       //console.log("Checking for batch req to end the connection ****************** "+req);
       // close the client when the last batch of stream has been processed
       if ( req === "last" ) {
           client.close();
       }
    });

    stream.on('data', function(data) {
      //console.log(data);
      //test.ok(data != null);
    });
}

subprocess.on('data',function(data) {
    console.log(data);
});
