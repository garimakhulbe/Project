var fs = require("fs"),
    utils = require('./autoscaleUtils'),
    logger = require('./logger');

var log = logger.log;
var deploymentTemplate;

/* This script includes all the template operations */

function getTemplateForDeployment(resourceGroup, callback) {
    utils.getToken(function (err, token) {
        if (err) {
            return callback(err);
        }
        
        log.info('Resource group: ' + resourceGroup);
        
        var armClient = utils.getResourceManagementClient(process.env.SUBSCRIPTION, token);
        
        armClient.deployments.get(resourceGroup, "SwarmSlaveNodes", function (err, result) {
            if (err) {
                console.log(err);
                return callback(err);
            }
            
            var deployment = result.deployment.properties;
            
            if (deployment.provisioningState !== 'Succeeded') {
                return callback(new Error('Failed status for slaves deployment.'));
            }
            
            if (deployment.templateLink.uri === null || deployment.templateLink.uri === undefined) {
                return callback(new Error('TemplateLink can not be null.'));
            }
            
            
            var parametersArray = Object.keys(deployment.parameters);
            for (var i = 0; i < parametersArray.length; i++) {
                delete deployment.parameters[parametersArray[i]].type;
            }
            
            deploymentTemplate = {
                "properties": {
                    "mode": "Incremental",
                    "parameters": deployment.parameters
                }
            }
            
            utils.downloadJson(deployment.templateLink.uri, function (err, result) {
                if (err) {
                    return callback(err);
                }
                
                template = JSON.parse(JSON.stringify(result, null, 4).replace(/copyIndex\(\)/gi, 'copyIndex(INDEX)'));
                deploymentTemplate.properties.template = JSON.parse(template);
                callback(null, deploymentTemplate);
            });
        });
    });
}

module.exports.getTemplateForDeployment = getTemplateForDeployment;
