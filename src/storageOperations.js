var fs = require('fs'),
    azureStorage = require('azure-storage'),
    exec = require('child_process').exec,
    logger = require('./logger.js');

var log = logger.log;
var self;
/*
 * This script includes all the storage related operations.
 * */
function StorageOperations(accountName, accessKey, tableName) {
    self = this;
    if (!accountName) {
        throw new Error('Stroage account name cannot be null.');
    }
    
    if (!accessKey) {
        throw new Error('Storage access key cannot be null.');
    }
    
    if (!tableName) {
        throw new Error('Table name cannot be null.');
    }
    
    this.tableSvc = azureStorage.createTableService(accountName, accessKey);
    this.tableName = tableName;
};

StorageOperations.prototype.readTable = function (callback) {
    
    try {
        var query = new azureStorage.TableQuery().select(['RowKey', 'CPUUsagePercentage']).where('PartitionKey eq ?', 'CPUUsage');
        this.tableSvc.queryEntities(this.tableName, query, null, function (error, result, response) {
            if (error) {
                return callback(error);
            } else if (result.entries.length === 0) {
                return callback(new Error("No nodes to monitor."));
            }
            callback(null, result.entries);
        });
    } catch (e) {
        callback(e);
    }
}

StorageOperations.prototype.writeTable = function (usage, resourceGroup, callback) {
    try {
        this.tableSvc.createTableIfNotExists(this.tableName, function (error, result, response) {
            if (error) {
                return callback(error);
            }
            
            if (response.statusCode === 200 || response.statusCode === 204) {
                
                /* check if the hostname set in the environment variables, else run the 'hostname' command. */
                if (process.env.HOST_VM !== undefined) {
                    insertEntity(usage, process.env.HOST_VM, resourceGroup, function (err, result) {
                        if (err) {
                            callback(err);
                        }
                    });
                } else {
                    exec('hostname', function (error, stdout, stderr) {
                        if (error) {
                            return callback(error);
                        }
                        insertEntity(usage, stdout, resourceGroup, function (err, result) {
                            if (err) {
                                callback(err);
                            }
                        });
                    });
                }

            } else {
                err = new Error('Stroage error: ' + response.message + '(' + response.statusCode + ')');
                callback(err);
            }
        });
    } catch (e) {
        callback(e);
    }
}

var insertEntity = function (usage, hostname, resourceGroup, callback) {
    try {
        var host = hostname.replace(/\n|\r/g, '');
        log.info('Entry for the host: ' + host + ', CPU Usage: ' + usage);
        
        var entGen = azureStorage.TableUtilities.entityGenerator;
        var entity = {
            PartitionKey: entGen.String('CPUUsage'),
            RowKey: entGen.String(host),
            CPUUsagePercentage: entGen.Double(usage),
            ResourceGroup: entGen.String(resourceGroup),
            complexDateValue: entGen.DateTime(new Date(Date.now()))
        };
        
        self.tableSvc.insertOrReplaceEntity(self.tableName, entity, function (error, result, response) {
            if (error) {
                return callback(error);
            }
            return callback(null, response.statusCode);

        });
    } catch (e) {
        callback(e);
    }
}

exports.StorageOperations = StorageOperations;
