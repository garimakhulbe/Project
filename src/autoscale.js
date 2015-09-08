var fs = require('fs'),
    events = require('events'),
    os = require('os'),
    path = require('path');

var templateOperations = require('./templateOperations'),
    storageOperations = require('./storageOperations'),
    utils = require('./autoscaleUtils'),
    logger = require('./logger'),
    constants = require('./autoscaleConstants');


process.env.FILE_DIRECTORY = path.join('.', 'files');
var log = logger.log;
var tableName = 'diagnosticsTable';


/*
 * Autoscale agent class - Includes function for the monitoring the CPU usage of the cluster and for scaling up action
 * in high load scenario. This agent runs on the Swarm master node to monitor and scale up the cluster.
 * **/
var AutoscaleAgentOperations = (function () {
    
    var self = null;
    var i = 0;
    var timerId = null;
    var intervalIdDeploymentStatus = null;
    var intervalId = null;
    
    /* Constructor - To initialize the environment and class variables from the Autoscale config file. */
    function AutoscaleAgentOperations(resourceGroup) {
        
        log.info('Initilizing the Autoscale agent...');
        try {
            self = this;
            if (!fs.existsSync(process.env.FILE_DIRECTORY)) {
                fs.mkdirSync(process.env.FILE_DIRECTORY);
            }
            
            this.deploymentTemplateFilePath = path.join(process.env.FILE_DIRECTORY, 'deploymentTemplate.json');
            if (!resourceGroup) {
                throw new Error('Resource group can not be null');
            }
            this.resourceGroup = resourceGroup;

        } catch (e) {
            throw e;
        }
    }
    
    /**
     * Start funtion for the Autoscale agent. It checks the deployment status of the slaves before starting the agent.
     * Download the template and save it locally for redeployment. Start monitoring the CPU usage of the slaves.
     **/
    AutoscaleAgentOperations.prototype.startAgent = function (callback) {
        
        log.info('Starting autoscale agent..');
        if (!fs.existsSync(this.deploymentTemplateFilePath)) {
            
            /* Download the template for the later re-deployments. */
            templateOperations.getTemplateForDeployment(this.resourceGroup, function (err, deploymentTemplate) {
                if (err) {
                    return callback(err);
                }
                try {
                    // Set the upper threshold(%) of CPU usage for scaling up.
                    self.upperThreshold = deploymentTemplate.properties.parameters.upperThresholdForCPUUsage.value;
                    
                    // Slave count to now set to node count for scaling up.
                    deploymentTemplate.properties.parameters.slaveCount.value = deploymentTemplate.properties.parameters.nodeCount.value;
                    fs.writeFileSync(self.deploymentTemplateFilePath, JSON.stringify(deploymentTemplate, null, 4));
                    self.storageOperations = new storageOperations.StorageOperations(process.env.STORAGE_ACCOUNT,
                        process.env.STORAGE_KEY, tableName);
                    monitorCPUUsage(function (err, result) {
                        if (err) {
                            if (intervalId)
                                clearInterval(intervalId);
                            if (intervalIdDeploymentStatus)
                                clearInterval(intervalIdDeploymentStatus);
                            callback(err);
                        }
                    });
                } catch (e) {
                    callback(e);
                }
            });

        } else {
            
            // If the template already exists locally, start monitoring right away.
            monitorCPUUsage(function (err, result) {
                if (err) {
                    if (intervalId)
                        clearInterval(intervalId);
                    if (intervalIdDeploymentStatus)
                        clearInterval(intervalIdDeploymentStatus);
                    callback(err);
                }
            });
        }

    }
    
    /*
     * Monitor the cluster for CPU usage and call the scaling operation. 
     * */
    function monitorCPUUsage(callback) {
        intervalId = setInterval(function () {
            self.storageOperations.readTable(function (err, percentage) {
                if (err) {
                    return callback(err);
                }
                try {
                    /* Check for constant high usage before starting the scale up operation. */
                    var p = calculateAverageCpuLoad(percentage);
                    if (p > self.upperThreshold) {
                        log.warn('Cluster CPU usage(%): ' + p);
                        i++;
                    } else {
                        log.info('Cluster CPU usage(%): ' + p);
                        i--;
                        if (i < 0)
                            i = 0;
                    }
                    
                    if (i >= 3) {
                        i = 0;
                        log.info('Scaling up the swarm cluster.');
                        clearInterval(intervalId);
                        scaleUp(function (err) {
                            if (err) {
                                return callback(err);
                            }
                        });
                    }
                } catch (e) {
                    callback(e);
                }
            });
        }, constants.STORAGE_READ_INTERVAL * 1000); /* Monitoring interval */
    }
    
    /*
     * It does ARM API calls for scaling up and for creating new resources. Also, keep track of new deployment.
     * */
    function scaleUp(callback) {
        utils.getToken(function (err, token) {
            if (err) {
                return callback(err);
            }
            try {
                var armClient = utils.getResourceManagementClient(process.env.SUBSCRIPTION, token); /* resourceManagementClient */
                var parameters = {
                    resourceGroupName: self.resourceGroup,
                    resourceType: "Microsoft.Compute/virtualMachines/extensions"
                }
                
                /* Check the slave count to set name index for the next slave e.g. Slave1, Slave2. */
                armClient.resources.list(parameters, function (err, response) {
                    if (response.statusCode !== 200)
                        return callback(response.statusCode);
                    
                    var deploymentTemplate = fs.readFileSync(self.deploymentTemplateFilePath, 'utf8');
                    var template = JSON.parse(deploymentTemplate.replace(/\(INDEX\)/g, '(' + (response.resources.length - 1) + ')'));
                    self.deploymentName = "Deployment-" + new Date().getTime();
                    
                    armClient.deployments.createOrUpdate(self.resourceGroup, self.deploymentName, template, function (err, result) {
                        if (err) {
                            return callback(err);
                        }
                        
                        log.info('Starting ' + self.deploymentName + ', Status code: ' + result.statusCode);
                        intervalIdDeploymentStatus = setInterval(function () {
                            
                            /* Check deployment status on regular interval */
                            checkDeploymentStatus(function (err, result) {
                                if (err) {
                                    return callback(err);
                                }
                                
                                /* If deployment succeeds, start the timeout to stablize the CPU load across the nodes */
                                if (result === 'Succeeded') {
                                    clearInterval(intervalIdDeploymentStatus);
                                    log.info(self.deploymentName + ' Succeeded');
                                    setTimeout(function () {
                                        log.info("Set timeout after scaling up operation.");
                                        self.startAgent();
                                    }, constants.TIMEOUT * 1000);
                                }

                            });
                        }, constants.CHECK_STATUS_INTERVAL * 1000);
                    });
                });
            } catch (e) {
                return callback(e);
            }
        });
    }
    
    /* 
     * Calculate Average CPU usage of the cluster. 
     * */
    function calculateAverageCpuLoad(percentageArray) {
        var percentage = [];
        for (var i = 0; i < percentageArray.length; i++) {
            var x = JSON.stringify(percentageArray[i].CPUUsagePercentage);
            var str = x.replace(/['"]+/g, '').toString();
            percentage[i] = parseFloat(str.split(':')[1].replace("}", ""));
        }
        var sum = 0.0;
        for (var j = 0; j < percentage.length; j++) {
            sum += percentage[j];
        }
        
        return (sum / percentage.length);
    }
    
    
    /* 
     * Check deployment status of the scaling up deployment.
     **/
    function checkDeploymentStatus(callback) {
        utils.getToken(function (err, token) {
            if (err) {
                return callback(err);
            }
            try {
                var armClient = utils.getResourceManagementClient(process.env.SUBSCRIPTION, token);
                armClient.deployments.get(self.resourceGroup, self.deploymentName, function (err, data) {
                    if (err) {
                        return callback(err);
                    }
                    if (data.deployment.properties.provisioningState === 'Running' || data.deployment.properties.provisioningState === 'Accepted') {
                        log.info('Deployment status:' + data.deployment.properties.provisioningState);
                    } else if (data.deployment.properties.provisioningState === 'Failed') {
                        return callback(new Error('Deployment Failed'));
                    } else {
                        return callback(null, data.deployment.properties.provisioningState);
                    }
                });
            } catch (e) {
                callback(e);
            }
        });
    }
    
    return AutoscaleAgentOperations;
})();


/*
 * Autoscale Client class - includes function for slaves to record their CPU usage to the storage. 
 * This class is to initilize the autoscale at the swarm slave nodes.  
 * **/
var AutoscaleNodeOperations = (function () {
    var self = null;
    /* Constructor - To initialize the environment and class variables from the Autoscale config file. */
    function AutoscaleNodeOperations(resourceGroup) {
        
        self = this;
        try {
            
            this.resourceGroup = resourceGroup;
            this.storageOperations = new storageOperations.StorageOperations(process.env.STORAGE_ACCOUNT,
                process.env.STORAGE_KEY, tableName);
        } catch (e) {
            throw e;
        }

    }
    
    /**
     * Get the CPU process details from the /proc/stat file in linux.
     * */
    function getStats() {
        var statFile = fs.readFileSync('/proc/stat', 'utf8');
        var arr = statFile.split(os.EOL);
        var stats = arr[0].split(/\s+/g, 5);
        return stats;
    }
    
    /*
     * Calculate usage for a slave and write to the stroage.
     * **/
    function writeUsageToStorage(callback) {
        try {
            
            var stat1 = getStats();
            timerId = setTimeout(function () {
                try {
                    clearTimeout(timerId);
                    var stat2 = getStats();
                    var total1 = 0,
                        total2 = 0,
                        usage1 = 0,
                        usage2 = 0;
                    for (var i = 1; i <= 4; i++) {
                        total1 += parseInt(stat1[i]);
                        total2 += parseInt(stat2[i]);
                        if (i != 4) {
                            usage1 += parseInt(stat1[i]);
                            usage2 += parseInt(stat2[i]);
                        }
                    }
                    
                    self.cpuUsage = ((100 * (usage2 - usage1)) / (total2 - total1));
                    self.storageOperations.writeTable(self.cpuUsage, self.resourceGroup, function (err, result) {
                        if (err) {
                            callback(err);
                        }
                    });
                } catch (e) {
                    callback(e);
                }
            }, 5000);
        } catch (e) {
            callback(e);
        }
    }
    
    /**
     * Start funtion for the Autoscale client.
     **/
    AutoscaleNodeOperations.prototype.startClient = function (callback) {
        try {
            log.info('Starting autoscale client..');
            log.info('Start storing monitoring data in table - ' + self.storageOperations.tableName);
            intervalId = setInterval(function () {
                writeUsageToStorage(function (err) {
                    if (err) {
                        if (timerId)
                            clearTimeout(timerId);
                        if (intervalId)
                            clearInterval(intervalId);
                        callback(err);
                    }
                });
            }, constants.STORAGE_WRITE_INTERVAL * 1000);
        } catch (e) {
            if (timerId)
                clearTimeout(timerId);
            if (intervalId)
                clearInterval(intervalId);
            callback(err);
        }
    }
    
    return AutoscaleNodeOperations;
})();


function start() {
    var autoscale;
    
    try {
        if (process.argv[2] === 'agent') {
            autoscale = new AutoscaleAgentOperations(process.argv[3]);
            autoscale.startAgent(function (err) {
                if (err) {
                    console.log("startagent");
                    log.error(err.message);
                }
            });
        } else if (process.argv[2] === 'client') {
            autoscale = new AutoscaleNodeOperations(process.argv[3]);
            autoscale.startClient(function (err) {
                if (err) {
                    console.log("startclient");
                    log.error(err.message);
                }
            });
        } else {
            //TODO - create usage option.
            log.info("Argument missing:\n\t Argument 1 - agent or client. \n\t Argument 2 - <resource group name>.");
        }
    } catch (e) {
        log.error(e.message);
    }
}


start();
