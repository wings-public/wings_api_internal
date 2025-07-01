const configData = require('../config/config.js');
console.log("File path used in resultSchema is " + __filename);
console.log("File path used in resultSchema is " + __dirname);

const { db: { host, port, dbName, resultSV } } = configData;
var db = require('../controllers/db.js');


var resultSchema = {
  process_id: String,
  status: { type: String, default: 'computing' },
  filtered_variants: Array,
  error: String,
  createdAt: { type: Date, expires: '30m', default: Date.now }
};

var resultCol = db.model('ResultsSV', resultSchema, resultSV);

module.exports = resultCol;
