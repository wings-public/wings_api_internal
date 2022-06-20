var fs = require('fs');
var express = require('express');
var indRoutes = require('./src/routes/individualRoutes').indRoutes;
var familyRoutes = require('./src/routes/familyRoutes').familyRoutes;
var importRoutes = require('./src/routes/importRoutes').importRoutes;
var createConnection = require('./src/controllers/dbConn.js').createConnection;
//var client;

const configData = require('./src/config/config.js');
const { app : {expressPort} } = configData;

var initialize = require('./src/controllers/entityController.js').initialize;

console.log("Variable to resolve the path");
console.log("file path is  "+__filename);
console.log("Dir Path is "+__dirname);
// import/ES6 not supported by Nodejs
/*
import { 
    initialize
} from './src/controllers/entityController.js';
*/

const app = express();
// EXPRESS_PORT will be provided from environment file for docker setup
//const EXPRESS_PORT = 8081;
app.use(express.json());

// Specific endpoint in the route gets called based on the URL.
// Pass express app to Individual Routes

indRoutes(app);
familyRoutes(app);
importRoutes(app);
//famRoutes(app);

//routes(app);


var server = app.listen(expressPort, async () => {
    var host = server.address().address;
    var port = server.address().port;
    //server.setTimeout();
    try {
        // check and setup database collections
        //client = await createConnection();
        await createConnection();
        console.log("Creating Main Client Connection");
        //console.log(client);
        // Initializing database Collections
        console.log("Calling initialize to create initial collections ");
        var data = await initialize();
    } catch (e) {
        console.log("Error is "+e);
        //process.exit(1);
    }
    console.log(`Individual Express app listening http://${host}:${port}`);
 });
 
  // Handle server errors
  server.on('error', (error) => {
    if (error.syscall !== 'listen') {
      throw error;
    }
    port = expressPort;
    const bind = typeof port === 'string'
      ? `Port ${port}`
      : `Port ${port}`;
  
    // handle specific listen errors with friendly messages
    switch (error.code) {
      case 'EACCES':
        console.log(`${bind} requires elevated privileges`);
        process.exit(1);
        break;
      case 'EADDRINUSE':
        console.log(`${bind} is already in use`);
        process.exit(1);
        break;
      default:
        console.log(error);
      // throw error;
    }
  });

app.get('/', (req, res) =>
    res.send('Node and express server is running on port '+expressPort)
);

