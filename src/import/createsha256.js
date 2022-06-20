// Creates sha256 crypto checksum for the filename provided as input
// Calculated checksum is equivalent to shasum -a 256 <file_name>
const crypto = require('crypto');
const fs = require('fs');
const filename = process.argv[2];
if ( process.argv.length < 3 ) {
    console.log("node createsha256.js <filename>");
    process.exit(-1);
}
try {
    const hash = crypto.createHash('sha256');
    const input = fs.createReadStream(filename);
    input.on('readable', () => {
        const data = input.read();
        if (data)
            hash.update(data);
        else {
            console.log(`${hash.digest('hex')} ${filename}`);
        }
    }); 
} catch(err) {
    console.log(err);
}
