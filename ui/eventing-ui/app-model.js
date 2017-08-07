function Application(data) {
    for (var key of Object.keys(data)) {
        this[key] = data[key];
    }
}

Application.prototype.actionsVisible = false;

// Actions are Export, Enable/Disable, Delete, Deploy, Edit
Application.prototype.toggleActionsVisibility = function() {
    this.actionsVisible = this.actionsVisible ? false : true;
};

// TODO : May deprecate this soon.
Application.prototype.enforceSchema = function() {
    if (typeof this.depcfg === 'string') {
        this.depcfg = JSON.parse(this.depcfg);
    }
};

// ApplicationManager manages the list of applications in the front-end.
function ApplicationManager() {
    var applications = {};

    this.getApplications = function() {
        return applications;
    };
}

// This contains each of the applications' states - enabled/disable or deployed/undeployed.
ApplicationManager.prototype.appState = {};

// Creates a new app in the front-end.
ApplicationManager.prototype.createApp = function(appModel) {
    if (!appModel instanceof ApplicationModel) {
        throw 'parameter must be an instance of ApplicationModel';
    }

    var appList = this.getApplications();
    var app = new Application(appModel);

    app.id = appList.length;
    app.enforceSchema();

    this.appState[app.appname] = {
        deployed: false,
        enabled: false
    };

    // Store the app - appname is the key for the application.
    appList[app.appname] = app;
};

ApplicationManager.prototype.pushApp = function(app, state) {
    if (!app instanceof Application) {
        throw 'Parameter must be an instance of Application';
    }

    if (!state) {
        throw 'State is missing for ' + app.appname;
    }

    this.appState[app.appname] = state;
    app.enforceSchema();

    this.getApplications()[app.appname] = app;
};

ApplicationManager.prototype.getAppByName = function(appName) {
    var appList = this.getApplications();

    if (appList[appName]) {
        return appList[appName];
    } else {
        throw appName + ' does not exist';
    }
};

ApplicationManager.prototype.deleteApp = function(appName) {
    var appList = this.getApplications();

    // Remove the app's state.
    delete this.appState[appName];

    if (appList[appName]) {
        delete appList[appName];
    } else {
        throw appName + ' does not exist';
    }
};

// ApplicationModel is the model for exchanging data between server and the UI.
function ApplicationModel(app) {
    if (app) {
        for (var key of Object.keys(app)) {
            this[key] = app[key];
        }
    }
}

ApplicationModel.prototype.getDefaultModel = function() {
    var code = 'function OnUpdate(doc, meta){} function OnDelete(doc){}';
    return {
        appname: 'Application name',
        appcode: formatCode(code),
        depcfg: {
            buckets: [],
            metadata_bucket: 'eventing',
            source_bucket: 'default'
        },
        settings: {
            log_level: 'TRACE',
            dcp_stream_boundary: 'everything',
            sock_batch_size: 1,
            tick_duration: 5000,
            checkpoint_interval: 10000,
            worker_count: 1,
            cleanup_timers: false,
            timer_worker_pool_size: 3,
            skip_timer_threshold: 86400,
            timer_processing_tick_interval: 500,
            rbacuser: 'eventing',
            rbacpass: 'asdasd',
            rbacrole: 'admin'
        }
    }
};

// Fills the Missing parameters in the model with default values.
ApplicationModel.prototype.fillWithMissingDefaults = function() {
    function setIfNotExists(source, target, key) {
        target[key] = target[key] ? target[key] : source[key];
    }

    function fillMissingWithDefaults(source, target) {
        for (var key of Object.keys(source)) {
            setIfNotExists(source, target, key);
        }
    }

    var defaultModel = this.getDefaultModel();
    this.depcfg = this.depcfg ? this.depcfg : {};
    this.settings = this.settings ? this.settings : {};

    setIfNotExists(defaultModel, this, 'appname');
    setIfNotExists(defaultModel, this, 'appcode');
    fillMissingWithDefaults(defaultModel.depcfg, this.depcfg);
    fillMissingWithDefaults(defaultModel.settings, this.settings);
};

ApplicationModel.prototype.initializeDefaults = function() {
    this.depcfg = this.getDefaultModel().depcfg;
    this.settings = {};
    this.settings.checkpoint_interval = 10000;
    this.settings.sock_batch_size = 1;
    this.settings.worker_count = 1;
    this.settings.skip_timer_threshold = 86400;
    this.settings.tick_duration = 5000;
    this.settings.timer_processing_tick_interval = 500;
    this.settings.timer_worker_pool_size = 3;
};

// Prettifies the JavaScript code.
function formatCode(code) {
    var ast = esprima.parse(code, {
        sourceType: 'script'
    });
    var formattedCode = escodegen.generate(ast);
    return formattedCode;
}