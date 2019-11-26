function Application(data) {
    for (var key of Object.keys(data)) {
        this[key] = data[key];
    }
}

Application.prototype.actionsVisible = false;

// Actions are Export, Enable/Disable, Delete, Deploy, Edit
Application.prototype.toggleActionsVisibility = function() {
    this.actionsVisible = !this.actionsVisible;
};

// TODO : May deprecate this soon.
Application.prototype.enforceSchema = function() {
    if (typeof this.depcfg === 'string') {
        this.depcfg = JSON.parse(this.depcfg);
    }
};

Application.prototype.getProcessingStatus = function(inverted) {
    // Inverted case is used for the button.
    if (inverted) {
        return this.status === 'paused' ? 'Resume' : 'Pause';
    }

    return this.status === 'paused' ? 'paused' : 'running';
};

Application.prototype.getDeploymentStatus = function(inverted) {
    // Inverted case is used for the button.
    if (inverted) {
        return this.status === 'deployed' || this.status === 'paused' ? 'Undeploy' : 'Deploy';
    }

    return this.status === 'deployed' || this.status === 'paused' ? 'deployed' : 'undeployed';
};

Application.prototype.clone = function() {
    return JSON.parse(JSON.stringify(this));
};

// ApplicationManager manages the list of applications in the front-end.
function ApplicationManager() {
    var applications = {};

    this.getApplications = function() {
        return applications;
    };
}

// Creates a new app in the front-end.
ApplicationManager.prototype.createApp = function(appModel) {
    if (!appModel instanceof ApplicationModel) {
        throw 'parameter must be an instance of ApplicationModel';
    }

    var appList = this.getApplications();
    var app = new Application(appModel);

    app.id = appList.length;
    app.enforceSchema();

    // Store the app - appname is the key for the application.
    appList[app.appname] = app;
};

ApplicationManager.prototype.pushApp = function(app) {
    if (!app instanceof Application) {
        throw 'Parameter must be an instance of Application';
    }

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
    var code = 'function OnUpdate(doc, meta){log(\'docId\', meta.id);} function OnDelete(meta){}';
    return {
        appname: 'Application name',
        appcode: formatCode(code),
        depcfg: {
            buckets: [],
            metadata_bucket: 'eventing',
            source_bucket: 'default'
        },
        version: getVersion(),
        settings: {
            log_level: 'INFO',
            dcp_stream_boundary: 'everything',
            processing_status: false,
            deployment_status: false,
            description: '',
            worker_count: 3,
            execution_timeout: 60,
            user_prefix: 'eventing',
            n1ql_consistency: 'none',
            language_compatibility: '6.5.0'
        }
    };
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
    this.settings.worker_count = 3;
    this.settings.execution_timeout = 60;
    this.settings.user_prefix = 'eventing';
    this.settings.n1ql_consistency = 'none';
    this.version = getVersion();
};

// Prettifies the JavaScript code.
function formatCode(code) {
    var ast = esprima.parse(code, {
        sourceType: 'script'
    });
    var formattedCode = escodegen.generate(ast);
    return formattedCode;
}

function determineUIStatus(status) {
    switch (status) {
        case 'deployed':
            return 'healthy';
        case 'undeployed':
        case 'paused':
            return 'inactive';
        case 'pausing':
        case 'undeploying':
        case 'deploying':
            return 'warmup';
        default:
            console.error('Abnormal case - status can not be', status);
            return '';
    }
}

function getWarnings(app) {
    if (app.settings.language_compatibility &&
        app.version.startsWith('evt-6.0') &&
        app.settings.language_compatibility === '6.0.0') {
        return ['Running in compatibility mode. Please make the necessary changes and update the language compatibility to latest.']
    }
    return [];
}
