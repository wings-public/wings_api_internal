const configData = require('../config/config.js');
const { db: { host, port, dbName, importCollection } } = configData;
const url = 'mongodb://' + host + ':' + port + '/' + dbName;
var mongoose = require('mongoose');

// Below options are set for mongoose based on the standard mongoose doc
// https://mongoosejs.com/docs/deprecations.html
//mongoose.set('useNewUrlParser',true);
mongoose.set('useCreateIndex',true);
//mongoose.set('debug', true);
//mongoose.set('useUnifiedTopology',true);

// family:4 // use IPV4, skip trying IPV6
// connection Object can be used to create or retrieve models

var conn = mongoose.createConnection(url, {useNewUrlParser:true,family:4,useUnifiedTopology:true});

module.exports = conn;

//module.exports.connect = async dsn => mongoose.connect(url,{useNewUrlParser:true, family:4});
