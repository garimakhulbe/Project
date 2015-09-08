var adal = require('adal-node'),
    fs = require("fs"),
    https = require("https"),
    resourceManagement = require('azure-arm-resource'),
    common = require('azure-common'),
    logger = require('./logger.js');

function downloadJson(url, callback) {
    //console.log(url);
    https.get(url, function (res) {
        body = '';
        
        res.on('data', function (data) {
            body += data;
        });
        
        res.on('end', function () {
            callback(null, body);
        });
        
        res.on('error', function () {
            return callback(error, null);
        });

    }).on('error', function (e) {
        callback(e, null);
    });
}

var getToken = function (callback) {
    var AuthenticationContext = adal.AuthenticationContext;
    var authorityHostUrl = 'https://login.windows.net';
    var authorityUrl = authorityHostUrl + '/' + process.env.TENANT;
    var resource = 'https://management.azure.com/';
    var context = new AuthenticationContext(authorityUrl);
    context.acquireTokenWithClientCredentials(resource, process.env.CLIENT_ID, process.env.CLIENT_SECRET, function (err, tokenResponse) {
        if (err)
            return callback(err, null);
        callback(null, tokenResponse.accessToken);
    });
}

function getResourceManagementClient(subscriptionId, token) {
    var resourceManagementClient = resourceManagement.createResourceManagementClient(new common.TokenCloudCredentials({
        subscriptionId: subscriptionId,
        token: token
    }));
    return resourceManagementClient;
}


module.exports.getToken = getToken;
module.exports.downloadJson = downloadJson;
module.exports.getResourceManagementClient = getResourceManagementClient;
