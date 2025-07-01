const configData = require('../config/config.js');
console.log("File path used in resultSchema is " + __filename);
console.log("File path used in resultSchema is " + __dirname);

const { db: { host, port, dbName, resultsSVFreqDisc } } = configData;
var db = require('../controllers/db.js');


var resultSchema = {
  process_id: String,
  status: { type: String, default: 'computing' },
  all_cases: Object,
  error: String,
  createdAt: { type: Date, expires: '60m', default: Date.now }
};

var resultColfreq = db.model('ResultsSVFreqDisc', resultSchema, resultsSVFreqDisc);

module.exports = resultColfreq;
