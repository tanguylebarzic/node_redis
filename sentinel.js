var events = require("events");
var reply_to_object = require("./lib/utils").reply_to_object;
var util = require("util");

var RedisSingleClient = require("./index");

exports.debug_mode = false;

var states = {
    NOT_INITIALIZED: 0,
    HEALTHY: 1,
    UNHEALTHY: 2
};

function RedisMetaClient(masterName, startingSentinels, options) {
    events.EventEmitter.call(this);

    this.options = options || {};
    this.options.pingPeriod = this.options.pingPeriod || 500;
    this.options.quorum = this.options.quorum || Math.floor(1 + (startingSentinels.length / 2));

    this.state = states.NOT_INITIALIZED;

    this.master = {
        host: null,
        port: null
    };
    
    this.masterClients = [];
    this.masterName = masterName;
    this.sentinels = [];

    var self = this;
    this.on('stateChange', function() {
        if(exports.debug_mode){
            console.log("stateChange detected");
        }

        self.assessState();
    });

    this.on('masterAvailable', function(availableMaster) {
        this.state = states.HEALTHY;
        self.masterAvailable(availableMaster);
    });

    this.on('masterUnavailable', function() {
        this.state = states.UNHEALTHY;
        self.masterUnavailable();
    });

    startingSentinels.forEach(function(sentinel){
        self.initSentinel(sentinel);
    });

    setTimeout(function() {
        self.assessState();
    }, 5000);
}

util.inherits(RedisMetaClient, events.EventEmitter);

RedisMetaClient.prototype.assessState = function(){
    var detectedMasters = {};
    var currentMasterConfig = (this.master && this.master.host + ":" + this.master.port) || "";
    var currentMasterIsOdown = false;
    var selectedMaster;
    
    this.sentinels.forEach(function(sentinel) {
        var sentinelMaster = sentinel.master;
        var masterConfig;
        if(sentinelMaster && sentinelMaster.state === 'odown') {
            masterConfig = sentinelMaster.host + ":" + sentinelMaster.port;
            if(currentMasterConfig === masterConfig){
                currentMasterIsOdown = true;
            }
            if(detectedMasters[masterConfig]) {
                detectedMasters[masterConfig].odown = true;
            } else {
                detectedMasters[masterConfig] = {
                    host: sentinelMaster.host,
                    port: sentinelMaster.port,
                    odown: true
                };
            }
        }
        if(sentinelMaster && sentinelMaster.state === 'ok') {
            masterConfig = sentinelMaster.host + ":" + sentinelMaster.port;
            if(detectedMasters[masterConfig]) {
                detectedMasters[masterConfig].oks += 1;
            } else {
                detectedMasters[masterConfig] = {
                    host: sentinelMaster.host,
                    port: sentinelMaster.port,
                    oks: 1
                };
            }
        }
    });

    for(var i in detectedMasters) {
        if(detectedMasters.hasOwnProperty(i)) {
            var master = detectedMasters[i];
            if(!master.odown && master.oks >= this.options.quorum) {
                selectedMaster = master;
                break;
            }
        }
    }

    if(selectedMaster) {
        if(selectedMaster.host !== this.master.host || selectedMaster.port !== this.master.port){
            this.emit('masterAvailable', selectedMaster);
        }
    }
    else {
        // There is no master for sure. However, if there was one previously, mark it as non unavailable only if we have an odown for it
        if(currentMasterIsOdown && this.state === states.HEALTHY){
            this.emit('masterUnavailable');
        }
        else if (this.state === states.NOT_INITIALIZED){
            this.emit('masterUnavailable');
        }
    }
};

RedisMetaClient.prototype.initSentinel = function(sentinelConfig){
    // Check the sentinel doesn't exist yet
    var alreadyExists = false;
    sentinelConfig.port = parseInt(sentinelConfig.port, 10);
    for (var i = 0; i < this.sentinels.length; i++) {
        var thisSentinel = this.sentinels[i];
        var thisSentinelConfig = thisSentinel.config;
        if(thisSentinelConfig.host === sentinelConfig.host && thisSentinelConfig.port === sentinelConfig.port){
            alreadyExists = true;
            break;
        }
    }
    if(!alreadyExists){
        if(exports.debug_mode){
            console.log("Sentinel added, config: " + JSON.stringify(sentinelConfig));
        }
        var sentinelIndex = this.sentinels.length;
        this.sentinels.push({
            config: sentinelConfig
        });
        this.pingSentinel(sentinelIndex);
    }
};

RedisMetaClient.prototype.masterAvailable = function(availableMaster) {
    if(exports.debug_mode){
        console.log("Master available, config: " + JSON.stringify(availableMaster));
    }
    this.master = availableMaster;
    this.masterClients.forEach(function(masterClient){
        masterClient.client.host = availableMaster.host;
        masterClient.client.port = availableMaster.port;
        masterClient.client.forceReconnectionAttempt();
    });
};

RedisMetaClient.prototype.masterUnavailable = function() {
    if(exports.debug_mode){
        console.log("masterUnavailable");
    }
    
    this.masterClients.forEach(function(masterClient){
        masterClient.client.flush_and_error("Master not available");
        masterClient.client.enable_offline_queue = false;
    });
};

RedisMetaClient.prototype.pingSentinel = function(sentinelIndex){
    var sentinel = this.sentinels[sentinelIndex];
    var sentinelConfig = sentinel.config;
    var sentinelMaster = sentinel.master;
    var masterChanged = false;

    var self = this;

    var onFinish = function(){
        self.sentinels[sentinelIndex].nextPing = setTimeout(function() {
            self.pingSentinel(sentinelIndex);
        }, self.options.pingPeriod);
    };

    var sentinelClient = RedisSingleClient.createClient(sentinelConfig.port, sentinelConfig.host, {max_attempts: 1, socket_timeout: 3000});

    sentinelClient.on('error', function(error) {
        if(exports.debug_mode){
            console.log(error);
        }
    });

    sentinelClient.on('end', function(){
        onFinish();
    });

    sentinelClient.on('ready', function() {
        sentinelClient.send_command('INFO', [], function(error, info) {
            if(error) {
                if(exports.debug_mode){
                    console.log(error);
                }
                sentinelClient.quit();
                return;
            }

            var regex = /master(?:\d)*:name=([^,]*),status=([^,]*),address=([^:]*):([^,]*)/g;
            var match = regex.exec(info);
            while(match){
                var masterName = match[1];
                if(masterName !== self.masterName){
                    match = regex.exec(info);
                    continue;
                }
                var masterState = match[2];
                var masterHost = match[3];
                var masterPort = match[4];

                if(!sentinelMaster ||
                    sentinelMaster.host !== masterHost ||
                    sentinelMaster.port !== masterPort ||
                    sentinelMaster.state !== masterState){
                        masterChanged = true;

                        self.sentinels[sentinelIndex].master = {
                            host: masterHost,
                            port: masterPort,
                            state: masterState
                        };

                        self.emit('stateChange');
                }
                break;
            }

            sentinelClient.send_command('SENTINEL', ["sentinels", self.masterName], function(error, sentinels) {
                sentinelClient.quit();
                if(error) {
                    if(exports.debug_mode){
                        console.log(error);
                    }
                    return;
                }

                sentinels.forEach(function(otherSentinelConfig) {
                    otherSentinelConfig = reply_to_object(otherSentinelConfig);
                    self.initSentinel({
                        host: otherSentinelConfig.ip,
                        port: otherSentinelConfig.port
                    });
                });
            });
        });
    });
};

RedisMetaClient.prototype.quit = function(){
    this.sentinels.forEach(function(sentinel){
        if(sentinel.nextPing){
            clearTimeout(sentinel.nextPing);
        }
    });

    this.masterClients.forEach(function(masterClient){
        masterClient.client.end();
    });
};

// Return a RedisSingleCLient pointing to the master (or to nothing if there is no master yet)
RedisMetaClient.prototype.createMasterClient = function(options) {
    options = options || {};
    options.allowNoSocket = true;
    options.no_ready_check = true;

    var client = RedisSingleClient.createClient(this.master.port, this.master.host, options);
    var self = this;
    var connection_id = client.connection_id;

    client.on('end', function(){
        for (var i = 0, len = self.masterClients.length; i < len; i++) {
            var cl = self.masterClients[i];
            if(cl.client.closing === true && connection_id === cl.client.connection_id){
                self.masterClients = self.masterClients.slice(i, 1);
            }
        }
    });

    this.masterClients.push({config: this.master, client: client});
    
    return client;
};

exports.RedisMetaClient = RedisMetaClient;