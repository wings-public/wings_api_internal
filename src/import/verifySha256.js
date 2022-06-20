const filename = process.argv[2];
const checkSumFile = process.argv[3];
const crypto = require('crypto');
const fs = require('fs');
const hash = crypto.createHash('sha256');

( async () => {
    try {
        if ( process.argv.length < 4 ) {
            //console.log("Length is "+process.argv.length);
            console.log("node verifySha256.js <filename> <checksum_file>")
            process.exit(-1);
        }
        var computedCS =  await computeCheckSum(filename);
        //console.log(computedCS);
        fs.readFile(checkSumFile,'utf8', (err,data) => {
            if (err) throw err;
            //console.log(data);
            var content = data.split(' ');
            var storedCheckSum = content[0];
            //console.log("Storedchecksum is "+storedCheckSum);
            //console.log("Calculated checksum is "+computedCS);
            if ( computedCS == storedCheckSum ) {
                //console.log("Checksum Validated");
                process.send('valid');
            } else {
                //console.log("invalid checksum");
                process.send('invalid');
            }
        })
    } catch (err) {
         console.log(err);
    } 
} ) ();


async function computeCheckSum(filename) {
    const input = fs.createReadStream(filename);
    //const getSuccess = new Promise( ( resolve ) => resolve("Success") );
    input.on('readable', async () => {
        const data = input.read();
        //console.log("************************************DATA is "+data);
        if (data)
            hash.update(data);
    });

    // resolving promise from the async function handler of close signal
    return  new Promise( resolve => {
        input.on('close', async() => {
            resolve(await hash.digest('hex'));
        })
    })
}
