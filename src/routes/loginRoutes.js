const login = require('../controllers/userControllers.js').login;
const tokenStatus = require('../controllers/userControllers.js').tokenStatus;
const revokeToken = require('../controllers/userControllers.js').revokeToken;
const loginRequired = require('../controllers/userControllers.js').loginRequired;
const registerAPIUser = require('../controllers/userControllers.js').registerAPIUser;
const registerHost = require('../controllers/userControllers.js').registerHost;

const loginRoutes = (app) => {
    app.route('/manageAPIUser')
        .post(registerAPIUser);

    app.route('/auth/login')
        .post(login);

    app.route('/registerHostID')
        .post(registerHost);
    // Only authorized token holders can check token status
    app.route('/jwtStatus')
        .get(loginRequired,tokenStatus); 

    // Only authorized token holders can execute revoke token
    app.route('/revokeToken')
        .post(loginRequired,revokeToken);
}

module.exports = {loginRoutes}; 
