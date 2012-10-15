var events = require("events");
var util = require("util");

exports.debug_mode = true;

var RedisSingleClient = require("./index");

function reply_to_object(reply) {
    var obj = {},
        j, jl, key, val;

    if(reply.length === 0) {
        return null;
    }

    for(j = 0, jl = reply.length; j < jl; j += 2) {
        key = reply[j].toString();
        val = reply[j + 1];
        obj[key] = val;
    }

    return obj;
}

function RedisMetaClient(masterName, startingSentinels) {
    this.healthy = false;
    this.master = null;
    this.masterClientId = 0;
    this.masterClients = [];
    this.masterName = masterName;
    this.messagesReceived = {};
    this.sentinels = [];
    this.slaves = [];

    var self = this;

    this.cleanMessagesReceived = setInterval(function() {
        var now = +(new Date());
        var newLastMessages = [];
        for(var i in self.messagesReceived) {
            if(self.messagesReceived[i] > now - 4000) {
                newLastMessages[i] = self.messagesReceived[i];
            }
        }
        self.messagesReceived = newLastMessages;


        console.log(self.masterClients.length);
    }, 2000);

    this.getSentinels(startingSentinels, function(error, sentinelsConfig) {
        if(error) {
            console.log(error);
            return;
        }

        self.initSentinelsConnection(sentinelsConfig);
    });

    this.on('healthyChange', function(newState) {
        this.healthy = newState;
    });

    this.on('masterAvailable', function(availableMaster) {
        self.masterAvailable(availableMaster);
    });

    this.on('sentinelsConfigured', function() {
        self.sentinelsConfigured();
    });

}

util.inherits(RedisMetaClient, events.EventEmitter);

RedisMetaClient.prototype.getSentinels = function(startingSentinels, cb) {
    if(!Array.isArray(startingSentinels)) {
        return cb("Invalid argument");
    }

    if(startingSentinels.length === 0) {
        return cb("Error in initializing sentinels");
    }

    var i = 0;
    var self = this;

    var onSentinelResultsGotten = function(error, sentinels) {
            if(sentinels) {
                return cb(null, sentinels);
            }

            if(error) {
                if(exports.debug_mode) {
                    console.log(error);
                }
            }

            if(++i >= startingSentinels.length) {
                return cb("Error in initializing sentinels");
            } else {
                self.getSentinelsFromOne(startingSentinels[i], onSentinelResultsGotten);
            }
        };

    this.getSentinelsFromOne(startingSentinels[i], onSentinelResultsGotten);
};

RedisMetaClient.prototype.getSentinelsFromOne = function(sentinelConfig, cb) {
    var sentinelClient = RedisSingleClient.createClient(sentinelConfig.port, sentinelConfig.host);
    var hasResponded = false;
    sentinelClient.on('error', function(error) {
        if(!hasResponded){
            hasResponded = true;
            return cb(error);
        }
    });
    sentinelClient.on('ready', function() {
        sentinelClient.send_command('SENTINEL', ["sentinels", "mymaster"], function(error, sentinels) {
            sentinelClient.end();
            if(hasResponded){
                return;
            }
            hasResponded = true;
            if(error) {
                return cb(error);
            }
            var sentinelsConfigs = [];
            sentinelsConfigs.push(sentinelConfig);
            sentinels.forEach(function(otherSentinel) {
                otherSentinel = reply_to_object(otherSentinel);
                sentinelsConfigs.push({
                    host: otherSentinel.ip,
                    port: otherSentinel.port
                });
            });
            return cb(null, sentinelsConfigs);
        });
    });
};

RedisMetaClient.prototype.initSentinelsConnection = function(sentinelsConfig) {
    var self = this;

    sentinelsConfig.forEach(function(sentinelConfig) {
        self.initSentinelConnection(sentinelConfig);
    });

    this.emit('sentinelsConfigured');
};

RedisMetaClient.prototype.addSentinel = function(sentinelConfig) {
    this.initSentinelConnection(sentinelConfig);
};

RedisMetaClient.prototype.removeSentinel = function(sentinelConfig) {
    for(var i = 0; i < this.sentinels.length; i++) {
        var existingSentinel = this.sentinels[i];
        var existingSentinelConfig = existingSentinel.config;
        if(existingSentinelConfig.host === sentinelConfig.host && existingSentinelConfig.port === sentinelConfig.port) {
            if(existingSentinel.client && existingSentinel.client.end){
                existingSentinel.client.end();
            }
            this.sentinels.splice(i, 1);
            break;
        }
    }
};

RedisMetaClient.prototype.initSentinelConnection = function(sentinelConfig) {
    var self = this;

    this.removeSentinel(sentinelConfig);

    var sentinelClient = RedisSingleClient.createClient(sentinelConfig.port, sentinelConfig.host);
    this.sentinels.push({
        client: sentinelClient,
        config: sentinelConfig
    });

    sentinelClient.on('pmessage', function(pattern, channel, message) {
        var messageHash = channel + ":" + message;
        var previousSameMessage = self.messagesReceived[messageHash];
        var now = +(new Date());
        if(previousSameMessage) {
            if(previousSameMessage > now - 4000) {
                return;
            }

        }

        self.messagesReceived[messageHash] = now;

        var parts = message.split(' ');
        switch(channel) {
        case '+reset-master':
            console.log(channel);
            console.log(message);
            break;

        case '+slave':
            break;

        case '+failover-state-reconf-slaves':
            break;

        case '+slave-reconf-sent':
            break;

        case '+slave-reconf-inprog':
            break;

        case '+slave-reconf-done':
            break;

        case '-dup-sentinel':
            break;

        case '+sentinel':
            var sentinelConfig = {
                host: parts[2],
                port: parts[3]
            };
            self.addSentinel(sentinelConfig);
            break;

        case '+sdown':
            break;

        case '-sdown':
            break;

        case '+odown':
            if(parts[0] === 'master') {
                self.emit('healthyChange', false);
            }
            break;

        case '-odown':
            if(parts[0] === 'master') {
                self.emit('healthyChange', true);
            }
            break;

        case '+failover-takedown':
            break;

        case '+failover-triggered':
            if(parts[0] === 'master') {
                self.emit('healthyChange', false);
            }
            break;

        case '+failover-state-wait-start':
            break;

        case '+failover-state-select-slave':
            break;

        case 'no-good-slave':
            break;

        case '+selected-slave':
            break;

        case '+promoted-slave':
            break;

        case '+failover-state-wait-promotion':
            break;
        
        case '+failover-state-send-slaveof-noone':
            break;

        case 'failover-end-for-timeout':
            break;

        case '+failover-detected':
            break;

        case '+failover-end':
            break;

        case '+switch-master':
            self.emit('masterAvailable', {
                host: parts[3],
                port: parts[4]
            });
            break;

        case 'failover-abort-x-sdown':
            break;

        case '-slave-reconf-undo':
            break;

        case '+tilt':
            break;

        case '-tilt':
            break;

        case '-failover-abort-master-is-back':
            // Undocumented event as of October 15th, 2012
            break;

        case '+redirect-to-master':
            // Undocumented event as of October 15th, 2012
            self.emit('masterAvailable', {
                host: parts[3],
                port: parts[4]
            });
            break;

        case '+reboot':
            // Undocumented event as of October 15th, 2012
            if(parts[0] === 'master') {
                self.emit('masterAvailable', {
                    host: parts[2],
                    port: parts[3]
                });
            }
            break;


        default:
            console.log(channel + "::" + message);
            break;
        }
    });

    sentinelClient.on('error', function() {
        self.removeSentinel(sentinelConfig);
    });

    sentinelClient.psubscribe('*');
};

RedisMetaClient.prototype.quit = function(){
    clearTimeout(this.cleanMessagesReceived);
};

RedisMetaClient.prototype.sentinelsConfigured = function() {
    this.setupMasterConnection();
};

RedisMetaClient.prototype.setupMasterConnection = function() {
    var self = this;
    var waitingResults = 0;
    var results = [];

    var onAllResultsFromSentinel = function() {
            var detectedMasters = {};
            var neededForMajority = Math.floor(1 + (self.sentinels.length / 2));
            var selectedMaster;
            results.forEach(function(result) {
                if(result.masterUp === true) {
                    var masterConfig = result.host + ":" + result.port;
                    if(detectedMasters[masterConfig]) {
                        detectedMasters[masterConfig].ups += 1;
                    } else {
                        detectedMasters[masterConfig] = {
                            host: result.host,
                            port: result.port,
                            ups: 1
                        };
                    }
                }
            });

            for(var i in detectedMasters) {
                if(detectedMasters.hasOwnProperty(i)) {
                    var master = detectedMasters[i];
                    if(master.ups >= neededForMajority) {
                        selectedMaster = master;
                        break;
                    }
                }
            }

            if(selectedMaster) {
                self.emit('masterAvailable', selectedMaster);
            }
        };

    var onResultFromSentinel = function(error, result) {
        if(error && exports.debug_mode) {
            console.log(error);
        }
        if(result) {
            results.push(result);
        }
        if(--waitingResults === 0) {
            onAllResultsFromSentinel();
        }
    };

    this.sentinels.forEach(function(sentinel) {
        waitingResults++;
        var sentinelConfig = sentinel.config;
        var sentinelClient = RedisSingleClient.createClient(sentinelConfig.port, sentinelConfig.host);
        var hasResponded = false;

        sentinelClient.on('error', function(error) {
            if(!hasResponded){
                hasResponded = true;
                return onResultFromSentinel(error);
            }
        });

        sentinelClient.send_command('SENTINEL', ['get-master-addr-by-name', self.masterName], function(error, masterConfig) {
            if(hasResponded){
                return;
            }
            if(error) {
                sentinelClient.quit();
                hasResponded = true;
                return onResultFromSentinel(error);
            }

            var masterHost = masterConfig[0];
            var masterPort = masterConfig[1];
            sentinelClient.send_command('SENTINEL', ['is-master-down-by-addr', masterHost, masterPort], function(error, masterUpResult) {
                sentinelClient.quit();

                if(hasResponded){
                    return;
                }

                if(error) {
                    hasResponded = true;
                    return onResultFromSentinel(error);
                }

                if(masterUpResult[1] === "?") {
                    // We thought we had a master, but not really
                    return onResultFromSentinel("Master unknown");
                }
                var masterUp = masterUpResult[0] === 0;

                hasResponded = true;
                return onResultFromSentinel(null, {
                    host: masterHost,
                    port: masterPort,
                    masterUp: masterUp
                });
            });
        });
    });

};

RedisMetaClient.prototype.masterAvailable = function(availableMaster) {
    this.master = availableMaster;
    this.masterClients.forEach(function(masterClient){
        masterClient.client.host = availableMaster.host;
        masterClient.client.port = availableMaster.port;
    });

    this.emit('healthyChange', true);
};

RedisMetaClient.prototype.createMasterClient = function(options) {
    var client = RedisSingleClient.createClient(this.master.port, this.master.host, options);
    var id = ++this.masterClientId;
    var self = this;
    client.on('error', function(){});
    client.on('end', function(){
        for (var i = 0, len = self.masterClients.length; i < len; i++) {
            var cl = self.masterClients[i];
            if(cl.client.closing === true && id === cl.id){
                self.masterClients = self.masterClients.slice(i, 1);
            }
        }
    });
    this.masterClients.push({config: this.master, client: client, id: id});
    return client;
};

// RedisMetaClient.prototype.getMasterClient = function(options, cb) {
    
//     if(this.healthy){
//         return RedisSingleClient.createClient(this.master.port, this.master.host, options);
//     }
//     else {
//         var client = {};
//         for (var i in RedisSingleClient.RedisClient.prototype) {
//             console.log(i);

//         }
//     }
//     // for (var i in RedisSingleClient.RedisClient.prototype) {
//     //     console.log(i);
//     //     if(healthy){

//     //     }
//     //     else {

//     //     }
//     // }
//     // if(this.healthy && this.masterClient) {
//     //     return cb(null, this.masterClient);
//     // } else {
//     //     if(this.healthy && !this.masterClient){
//     //         console.log("this.masterClient not available");
//     //     }
//     //     return cb("Unhealthy");
//     // }
//     // console.log(this.healthy);
//     // console.log(this.master.port);
//     // if(this.masterClient) {
//     //     return this.masterClient;
//     // } else {
//     //     return false;
//     // }
// };

exports.RedisMetaClient = RedisMetaClient;