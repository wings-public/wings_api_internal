const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
var test = require('assert');
const jwt = require('jsonwebtoken');
var bcrypt = require('bcrypt');
var request = require('request');
const configData = require('../config/config.js');
const { db : {host,port,dbName,apiUser,apiUserColl,revokedDataCollection,fileMetaCollection,hostMetaCollection} } = configData;

const getConnection = require('../controllers/dbConn.js').getConnection;
const checkApiUser = require('../controllers/entityController.js').checkApiUser;
const registerAPIUser = async(req, res,next)  => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const collection = db.collection(apiUserColl);
        var loginDetails = req.body;
        console.dir(loginDetails,{"depth":null});
        var action = loginDetails['action'];
        var apiUserArg = loginDetails['apiUser'];
        var pwd = loginDetails['pwd'];
        var hashPassword = bcrypt.hashSync(`${pwd}`, 10);

        // Auto generate a salt and hash
        //var hashPassword = bcrypt.hashSync(`${apiUser}@C#01`, 10);
        var data = {'_id':apiUserArg, 'user':apiUserArg, 'hashPassword' : hashPassword,'createDate': Date.now()};
        
        if ( action == "create" ) {
            var result = await collection.insertOne(data);
        } else if ( action == "update" ) {
            var checkUser = await checkApiUser(apiUserArg);
            if ( checkUser == "not_exists") {
                throw `${apiUserArg} does not exist`;
            }
            var result = await collection.updateOne({'_id':apiUserArg},{$set:{'hashPassword':hashPassword}});
        } else if ( action == "delete" ) {
            var result = await collection.deleteOne({'_id':apiUserArg});
        }
        return res.json({"message":"Success"});
    } catch(err) {
        next(err);
    }
}

const createApiUser = async (apiUser) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const collection = db.collection(apiUserColl);
        // Auto generate a salt and hash
        var hashPassword = bcrypt.hashSync(`${apiUser}@C#01`, 10);
        var data = {'_id':apiUser,'user':apiUser, 'hashPassword' : hashPassword,'createDate': Date.now()};
        
        var result = await collection.insertOne(data);
        test.equal(1,result.insertedCount);
        return "Success";
    } catch(err) {
        throw err;
    }
};

const login = async (req, res,next) => { 
    try {
        var client = getConnection();
        const db = client.db(dbName);
        //console.log("****Printing request body ******");
        //console.log(req.body);
        const collection = db.collection(apiUserColl);
        //var un = req.params.un;
        //var pw = req.params.pw;
        var result = await collection.findOne({'user':req.body.user});
        //var result = await collection.findOne({'user':un});
        if ( result ) {
            if ( ! comparePassword(req.body.password,result.hashPassword) ) {
                //return res.status(401).json({ message: 'Authentication failed. Wrong password!'});
                next("Authentication failed.Wrong password!");
            } else {
               //return res.json({token: jwt.sign({ 'user': req.body.user, 'expiresIn': 20 , 'audience': "auth_user"}, 'RESTFULAPIs')});            
               // setting expiry of one day for the token
               //console.log("****** Preparing to send the response token for ESAT ");   
               // Update and set the expiration time of token to 4hours.           
               return res.json({token: jwt.sign({ 'user': req.body.user, exp: Math.floor( Date.now() / 1000 ) + ( 24 * 60 * 60 ), 'audience': "auth_user"}, 'RESTFULAPIs')});
            }
        } else {
            //return res.status(401).json({ message: 'Authentication failed. No user found!'});
            next("Authentication failed.No user found!");
        }
    } catch(err) {
        //res.status(401).json({ message: 'Login failed!'}); 
        next("Login failed!");
    }

}

const comparePassword = (pwd,hashPwd) => {
    return bcrypt.compareSync(pwd,hashPwd);
};

// middleware validation. function added to all the route endpoints that has to be validated with a token
const loginRequired = async(req, res, next) => {
    //console.log(req);
    try {
        if ( req.jwtid && req.jwtid.audience === "auth_user" ) {
            // check if token is not revoked
            var token = req.headers.authorization;
            var client = getConnection();
            const db = client.db(dbName);
            const collection = db.collection(revokedDataCollection);
            var result = await collection.findOne({'ack': token});
            if ( result ) {
                // invalid token. return error
                next("blacklisted token");
            } else {
                // valid token. Proceed to the next function
                next();
            }     
        } else {
            console.log("************ Here !!!");
            var jsonErr = {};
            jsonErr['message'] = 'Unauthorized User';
    
            if ( req.headers ) {
                jsonErr['logMsg'] = req.headers;
            }
            next("Unauthorized User!");
        }
    } catch(err) {
        next(err);
    }

}

const tokenStatus = (req,res,next) => {
    if (req.jwtid) {
        try {
            const dateexp = new Date(req.jwtid.exp * 1000);
            const datenow = Math.floor(Date.now() / 1000);
            const expiry = req.jwtid.exp - datenow;
            /*var jsonObj = {};
            jsonObj['iat'] = req.jwtid.iat;
            jsonObj['exp'] = req.jwtid.exp;
            jsonObj['expiresInSec'] = expiry; */
            return res.status(200).json({message: expiry});
            //return res.status(200).json({message: `iat(epoch):${req.jwtid.iat}, exp(epoch):${req.jwtid.exp}, exp(date):${dateexp}, expiresIn(sec):${expiry}`});
        } catch(err) {
            res.status(401).json({ message: err });
            //next(err);
        }
    }
}

const revokeToken = async(req,res,next) => {
    if (req.jwtid) {
        try {
            var tokenIp = req.headers.authorization; 
            var expiry = req.jwtid.exp;
            //console.log(`token IP is ${tokenIp}`);
            //console.log(`expiry set in the token IP is ${expiry}`);

            var client = getConnection();
            const db = client.db(dbName);
            const collection = db.collection(revokedDataCollection);

            var data = {'ack':tokenIp, 'expireAt': new Date(expiry * 1000)};
            var result = await collection.insertOne(data);
            test.equal(1,result.insertedCount);
            return res.status(200).json({message: 'revoked token'});
        } catch(err) {
            next(err);
        }
    }
}

const registerHost = async (req,res,next) => {
    try {
        var client = getConnection();
        const db = client.db(dbName);
        const collection = db.collection(hostMetaCollection);
        var data = req.body;
        var url = data['API_URL'];
        var hostID = data['HOST_ID'];
        var data = {'_id': hostID, 'url':url};
        var storedData = await collection.findOne({'url':url});
        if ( storedData ) {
            throw "Host is already registered";
        } else {
            var result = await collection.insertOne(data);
            test.equal(1,result.insertedCount);
            //next("Success");
            return res.status(200).json({message: "Success"});
        }
    } catch(err) {
        //throw err;
        next(err);
    }
};

module.exports = { createApiUser, registerAPIUser, login, loginRequired, tokenStatus, revokeToken, registerHost };
