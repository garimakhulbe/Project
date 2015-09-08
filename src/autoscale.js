var fs = require('fs'),
    events = require('events'),
    os = require('os'),
    path = require('path'),
    uuid = require('node-uuid');

var templateOperations = require('./templateOperations'),
    storageOperations = require('./storageOperations'),
    utils = require('./autoscaleUtils'),
    logger = require('./logger'),
    constants = require('./autoscaleConstants');

var tableName = 'diagnosticsTable';

process.env.FILE_DIRECTORY = path.normalize('.//files');

var intervalId;
var intervalIdDeploymentStatus;
var timerId;
var self;
var log = logger.LOG;
var i;


/*
 * Autoscale agent class - Includes function for the monitoring the CPU usage of the cluster and for scaling up action
 * in high load scenario. This agent runs on the Swarm master node to monitor and scale up the cluster.
 * **/
var AutoscaleAgentOperations = (function (configFile) {
    
    /* Constructor - To initialize the environment and class variables from the Autoscale config file. */
    function AutoscaleAgentOperations(configFile) {
        
        self = this;
        i = 0;
        timerId = null;
        intervalIdDeploymentStatus = null;
        intervalId = null;
        
        if (configFile === null || configFile === undefined) {
            log.error('Configuration file cannot be null.');
            return;
        }
        
        try {
            
            var inputJson = JSON.parse(configFile);
           
            if (inputJson.parameters.Credentials.TenantId === null || inputJson.parameters.Credentials.TenantId === undefined) {
                log.error('TenantId cannot be null.');
                return;
            }
            if (inputJson.parameters.Credentials.ClientId === null || inputJson.parameters.Credentials.ClientId === undefined) {
                log.error('clientId cannot be null.');
                return;
            }
            if (inputJson.parameters.Credentials.ClientSecret === null || inputJson.parameters.Credentials.ClientSecret === undefined) {
                log.error('clientSecret cannot be null.');
                return;
            }
            if (inputJson.parameters.Credentials.SubscriptionId === null || inputJson.parameters.Credentials.SubscriptionId === undefined) {
                log.error('subscriptionId cannot be null.');
                return;
            }
            if (inputJson.parameters.Autoscale.ResourceGroup === null || inputJson.parameters.Autoscale.ResourceGroup === undefined) {
                log.error('resourceGroup cannot be null.');
                return;
            }
            if (inputJson.parameters.Autoscale.ThresholdPercentage.Upper === null || inputJson.parameters.Autoscale.ThresholdPercentage.Upper === undefined) {
                log.error('upperThreshold cannot be null.');
                return;
            }
            if (inputJson.parameters.Autoscale.NodeCount === null || inputJson.parameters.Autoscale.NodeCount === undefined/*|| inputJson.parameters.Credentials.count === 0*/) {
                log.error('NodeCount cannot be null or 0');
                return;
            }
            
            process.env.CLIENTID = inputJson.parameters.Credentials.ClientId;
            process.env.AZURE_SUBSCRIPTION = inputJson.parameters.Credentials.SubscriptionId;
            process.env.TENANT = inputJson.parameters.Credentials.TenantId; 
            process.env.CLIENTSECRET = inputJson.parameters.Credentials.ClientSecret;
            
            this.deploymentTemplate = path.normalize(process.env.FILE_DIRECTORY+'//deploymentTemplate.json');
            this.resourceGroup = inputJson.parameters.Autoscale.ResourceGroup;
            this.upperThreshold = inputJson.parameters.Autoscale.ThresholdPercentage.Upper;
            this.nodeCount = inputJson.parameters.Autoscale.NodeCount;
            
            /* Create directory to save the downloaded templates. */
            try {
                fs.mkdirSync(process.env.FILE_DIRECTORY);
            } catch (e) {
                if (e.code != 'EEXIST')
                    callback(e, null);
            }
            
            /* StroageOperations class object for calling stroage operations. */
            this.storageOperations = new storageOperations.StorageOperations(inputJson.parameters.Credentials.StorageAccountName,
                                inputJson.parameters.Credentials.StorageAccessKey,
                                tableName);
            
        } catch (e) {
            log.error(e.message);
            return;
        }
      
    }
    
    /**
     * Start funtion for the Autoscale agent. It checks the deployment status of the slaves before starting the agent.
     * Download the template and save it locally for redeployment. Start monitoring the CPU usage of the slaves.
     **/
    AutoscaleAgentOperations.prototype.init = function () {
        log.info('Starting autoscale agent');
        if (!fs.existsSync(this.deploymentTemplate)) {

            /* Download the template for the later re-deployments. */
            templateOperations.getTemplate(self.resourceGroup, self.deploymentTemplate, function (err, deploymentTemplate) {
                if (err) {
                    log.error(err.message);
                    return;
                }
                try {
                    
                    deploymentTemplate.properties.parameters.slaveCount.value = self.nodeCount;
                    fs.writeFileSync(self.deploymentTemplate, JSON.stringify(deploymentTemplate, null, 4));
                    monitorStorage(function (err, result) {
                        if (err) {
                            log.error(err.message);
                            
                            if (intervalId)
                                clearInterval(intervalId);
                            if (intervalIdDeploymentStatus)
                                clearInterval(intervalIdDeploymentStatus);
                            return;
                        }
                    });
                } catch (e) {
                        callback(e, null);
                }
            });
        } else {

            /* If already template already exists locally, start monitoring right away */
            monitorStorage(function (err, result) {
                if (err) {
                    log.error(err.message);
                    
                    if (intervalId)
                        clearInterval(intervalId);
                    if (intervalIdDeploymentStatus)
                        clearInterval(intervalIdDeploymentStatus);
                    return;
                }
            });
        }

    }
    
    /*
     * Monitor the cluster for CPU usage and call the scaling operation. 
     * */
    function monitorStorage(callback) {
        var intervalId = setInterval(function () {
            self.storageOperations.readTable(function (err, percentage) {
                if (err) {
                    clearInterval(intervalId);
                    return callback(err, null);
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
                    
                    var scaling = new events.EventEmitter();
                    if (i >= 3) {
                        i = 0;
                        log.info('Scaling up the swarm cluster.');
                        clearInterval(intervalId);  
                        scaling.on('scaleup', function () {
                            scaleUp(function (err, result) {
                                if (err) {
                                    return callback(err, null);
                                }
                            });
                        });  
                        scaling.emit('scaleup');
                    }
                } catch (e) {
                    callback(e, null);
                }
            });
        }, constants.STORAGE_READ_INTERVAL*1000);  /* Monitoring interval */
    }
    
    /*
     * It does ARM API calls for scaling up and for creating new resources. Also, keep track of new deployment.
     * */
    function scaleUp(callback) {
        utils.getToken(function (err, token) {
            if (err) {
                return callback(err, null);
            }
            try {
                
                var armClient = utils.armClient(process.env.AZURE_SUBSCRIPTION, token); /* resourceManagementClient */
                var parameters = {
                    resourceGroupName: self.resourceGroup,
                    resourceType : "Microsoft.Compute/virtualMachines/extensions"
                }
                
                /* Check the slave count to set name index for the next slave e.g. Slave1, Slave2. */
                armClient.resources.list(parameters, function (err, response) {
                    if (response.statusCode !== 200)
                        return callback(response.statusCode);
                    
                    var deploymentTemplate = fs.readFileSync(self.deploymentTemplate, 'utf8');
                    var template = JSON.parse(deploymentTemplate.replace(/\(INDEX\)/g, '(' + (response.resources.length - 1) + ')'));
                    self.deploymentName = "Deployment-" + uuidGen();
                    
                    armClient.deployments.createOrUpdate(self.resourceGroup, self.deploymentName, template , function (err, result) {
                        if (err) {
                            return callback(err, null);
                        }
                        
                        log.info('Starting ' + self.deploymentName + ', Status code: ' + result.statusCode);
                        intervalIdDeploymentStatus = setInterval(function () {
                            
                            /* Check deployment status on regular interval */
                            checkDeploymentStatus(function (err, result) {
                                if (err) {
                                    return callback(err, null);
                                }
                                
                                /* If deployment succeeds, start the timeout to stablize the CPU load across the nodes */
                                if (result === 'Succeeded') {
                                    clearInterval(intervalIdDeploymentStatus);
                                    log.info(self.deploymentName + 'Succeeded');
                                    setTimeout(function () {
                                        log.info("Setting timeout for stablizing the cluster CPU usage after scaling up operation.");
                                        self.init();
                                    }, constants.TIMEOUT * 1000);
                                }

                            });
                        }, 60000);
                    });
                });
            } catch (e) {
                return callback(e, null);
            }
        });
    }
    
    /* 
     * Calculate Average CPU usage of the cluster. 
     * */
    function calculateAverageCpuLoad(percentageArray) {
        var percentage = [];
        for (var i = 0; i < percentageArray.length; i++) {
            var x = JSON.stringify(percentageArray[i].Percentage);
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
     * Creates unique deployment identifier.
     * */
    function uuidGen() {
        return uuid.v4();
    }
    
    /* 
     * Check deployment status of the scaling up deployment.
     **/
    function checkDeploymentStatus(callback) {
        utils.getToken(function (err, token) {
            if (err) {
                return callback(err, null);
            }
            try {
                var armClient = utils.armClient(process.env.AZURE_SUBSCRIPTION, token);
                armClient.deployments.get(self.resourceGroup, self.deploymentName, function (err, data) {
                    if (err) {
                        return callback(err, null);
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
                callback(e, null);
            }
        });
    }

    return AutoscaleAgentOperations;
})();


/*
 * Autoscale Client class - includes function for slaves to record their CPU usage to the storage. 
 * This class is to initilize the autoscale at the swarm slave nodes.  
 * **/
var AutoscaleNodeOperations = (function (configFile) {
    
    /* Constructor - To initialize the environment and class variables from the Autoscale config file. */
    function AutoscaleNodeOperations(configFile) {
        
        if (configFile === null || configFile === undefined) {
            throw new Error('Configuration file cannot be null.');
        }
        self = this;
        try {
            var inputJson = JSON.parse(configFile);
            this.resourceGroup = inputJson.parameters.Autoscale.ResourceGroup;
            this.storageOperations = new storageOperations.StorageOperations(inputJson.parameters.Credentials.StorageAccountName,
                                inputJson.parameters.Credentials.StorageAccessKey,
                                tableName);
        } catch (e) {
            log.error(e);
            return;
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
            var stat2;
            timerId = setTimeout(function () {
                try {
                    clearTimeout(timerId);
                    stat2 = getStats();
                   
                    var total1 = 0, total2 = 0, usage1 = 0, usage2 = 0;
                    for (var i = 1; i <= 4 ; i++) {
                     
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
                            log.error(err);
                            return callback(err, null);
                        }
                    });
                } catch (e) {
                    return callback(e, null);
                }
            }, 5000);
        } catch (e) {
            return callback(e, null);
        }
    }
    
    /**
     * Start funtion for the Autoscale client.
     **/
    AutoscaleNodeOperations.prototype.init = function () {
        try {
            log.info('Starting autoscale client..');
            log.info('Start recording CPU usage for slaves in Table - ' + self.storageOperations.tableName);
            intervalId = setInterval(function () {
                writeUsageToStorage(function (err, result) {
                    if (err) {
                        if (timerId)
                            clearTimeout(timerId);
                        if (intervalId)
                            clearInterval(intervalId);
                        return;
                    }
                });
            }, constants.STORAGE_WRITE_INTERVAL*1000);
        } catch (e) {
            if (timerId)
                clearTimeout(timerId);
            if (intervalId)
                clearInterval(intervalId);
            log.error(e);
        }
    }
    
    return AutoscaleNodeOperations;
})();


function start() {
    var autoscale;
    utils.download(process.argv[3], function (err, configFile) {
        if (err) {
            log.error(err);
            return;
        }
       
        if (process.argv[2] === 'agent') {
            autoscale = new AutoscaleAgentOperations(configFile);
            autoscale.init();
        }
        else {
            autoscale = new AutoscaleNodeOperations(configFile);
            autoscale.init();
        }
    });
}


start();
