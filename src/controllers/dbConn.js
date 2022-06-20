const MongoClient = require('mongodb').MongoClient;
const configData = require('../config/config.js');
const { db: { host, port, dbName, importCollection } } = configData;
const url = 'mongodb://' + host + ':' + port + '/' + dbName;

var client;
async function createConnection() {
    const url = `mongodb://${host}:${port}`;
    //var options = { useUnifiedTopology : true, useNewUrlParser : true };
    var opts = { useNewUrlParser : true , useUnifiedTopology: true };
    client = await MongoClient.connect(url,opts);
    //client = await MongoClient.connect(url,{ useNewUrlParser : true });
    //client = await MongoClient.connect(url,options);
    return client;
}

const getConnection = () => {
    if(!client) {
        throw new Error('Call connect first!');
        //console.log('Call connect first!');
    }

    return client;
}

module.exports = { createConnection, getConnection };
