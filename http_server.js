var fs = require('fs');
var express = require('express');
var http = require('http');
var https = require('https');
const jwt = require('jsonwebtoken');
const addRequestId = require('express-request-id')();
const morgan = require('morgan');

var loginRoutes = require('./src/routes/loginRoutes').loginRoutes;
var indRoutes = require('./src/routes/individualRoutes').indRoutes;
var familyRoutes = require('./src/routes/familyRoutes').familyRoutes;
var importRoutes = require('./src/routes/importRoutes').importRoutes;
var queryRoutes = require('./src/routes/queryRoutes').queryRoutes;
var createConnection = require('./src/controllers/dbConn.js').createConnection;
//const logger = require('./src/log/logger.js').logger;
const {requestLogger, errorLogger}  = require('./src/controllers/loggerMiddleware.js');

const configData = require('./src/config/config.js');
const { app : {expressPort, privkey, cert, certAuth} } = configData;

var initialize = require('./src/controllers/entityController.js').initialize;

console.log("Variable to resolve the path");
console.log("file path is  "+__filename);
console.log("Dir Path is "+__dirname);

const app = express();
// EXPRESS_PORT will be provided from environment file for docker setup
//const EXPRESS_PORT = 8081;
//The order of middleware loading is important: middleware functions that are loaded first are also executed first

app.use(express.json());

// START Morgan Logger
app.use(addRequestId);

morgan.token('id', function getId(req) {
    return req.id
});

//var loggerFormat = ':id [:date[web]] ":method :url" :status :response-time';
var loggerFormat = ':remote-addr - :remote-user [:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length]';


app.use(morgan(loggerFormat, {
    skip: function (req, res) {
        return res.statusCode < 400
    },
    stream: process.stderr
}));

app.use(morgan(loggerFormat, {
    skip: function (req, res) {
        return res.statusCode >= 400
    },
    stream: process.stdout
}));


// END Morgan Logger

// JWT setup
// If header authorization token is present in request, token is validated and jwtid is set
// If header is not present, jwtid will be reset.
// If loginRequired middleware is added to the endpoint, it checks for the jwtid 
app.use((req, res, next) => {
   
  if (req.headers && req.headers.authorization) {
    var tokenIp = req.headers.authorization;
  //if (req.headers && req.headers.authorization && req.headers.authorization.split(' ')[0] === 'JWT') {
     //jwt.verify(req.headers.authorization.split(' ')[1], 'RESTFULAPIs', (err, decode) => {
     jwt.verify(tokenIp, 'RESTFULAPIs', (err, decode) => {
         console.log(err);
         console.log(decode);
         if (err) {
           console.log("Encountered error in JWT Verification");
           req.jwtid = undefined;
           // pass the error to express.
           next(err);
         }
         console.log("JWT Decoded.Process to next function");
         req.jwtid = decode;
         next();
     }); 
  } else {
      req.jwtid = undefined;
      next();
  }
});



// Specific endpoint in the route gets called based on the URL.
// Pass express app to Individual Routes
try {
    loginRoutes(app);
    app.use(requestLogger);
    indRoutes(app);
    familyRoutes(app);
    importRoutes(app);
    queryRoutes(app);
} catch(err) {
    console.log("Error in routes "+err);
}

var httpsPort = expressPort;

var server = http.createServer(app).listen(httpsPort,async () => {
    var host = server.address().address;
    var port = server.address().port;
    //server.setTimeout();
    try {
        // check and setup database collections        
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
  
    // handle specific listen errors
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

// error-handling middleware should be defined last, after the other app.use and route calls.

app.use(errorLogger);
app.get('/', (req, res) =>
    res.send('Node and express server is running on port '+expressPort)
);

