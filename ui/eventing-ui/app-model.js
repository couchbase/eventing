import _ from "lodash";
import getVersion from "../../../version.js";
export {
  Application,
  ApplicationManager,
  ApplicationModel,
  determineUIStatus,
  getWarnings,
  getAppLocation,
  getReverseAppLocation
};

function Application(data) {
  for (var key of Object.keys(data)) {
    if (key == "function_scope") {
      this[key] = {"bucket": "*", "scope": "*"};
      if (data[key].bucket == undefined && data[key].scope == undefined) {
        this[key].bucket = data[key].bucket_name;
        this[key].scope = data[key].scope_name;
        return
      }
      this[key].bucket = data[key].bucket;
      this[key].scope = data[key].scope;
    } else {
      this[key] = data[key];
    }
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
    return this.status === 'deployed' || this.status === 'paused' ?
      'Undeploy' : 'Deploy';
  }

  return this.status === 'deployed' || this.status === 'paused' ? 'deployed' :
    'undeployed';
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

  // Alpha sort the UI
  this.sortApplications = function() {
    // sort the object by appname by rebuilding
    function objectWithKeySorted(object) {
      var result = {};
      _.forEach(Object.keys(object).sort(), function(key) {
        result[key] = object[key];
      });
      return result;
    }
    var tempList = objectWithKeySorted(applications);
    for (var appname of Object.keys(tempList)) {
      delete applications[appname];
      applications[appname] = tempList[appname];
    }
  }
}

// Creates a new app in the front-end.
ApplicationManager.prototype.createApp = function(appModel) {
  if (!(appModel instanceof ApplicationModel)) {
    throw 'parameter must be an instance of ApplicationModel';
  }

  var appList = this.getApplications();
  var app = new Application(appModel);

  app.id = appList.length;
  app.enforceSchema();

  var appLocation = getAppLocation(app.appname, app.function_scope);
  // applocation is the key for the app
  appList[appLocation] = app;

  // Alpha sort the UI
  this.sortApplications();
};

ApplicationManager.prototype.pushApp = function(app) {
  if (!(app instanceof Application)) {
    throw 'Parameter must be an instance of Application';
  }

  var appLocation = getAppLocation(app.appname, app.function_scope);
  app.enforceSchema();
  this.getApplications()[appLocation] = app;
};

ApplicationManager.prototype.getAppByName = function(appName, function_scope) {
  var appList = this.getApplications();
  var appLocation = getAppLocation(appName, function_scope);

  if (appList[appLocation]) {
    return appList[appLocation];
  } else {
    throw appName + " in " + function_scope + ' does not exist';
  }
};

ApplicationManager.prototype.deleteApp = function(appName, function_scope) {
  var appList = this.getApplications();
  var appLocation = getAppLocation(appName, function_scope);

  if (appList[appLocation]) {
    delete appList[appLocation];
  } else {
    throw appName + " in " + function_scope + ' does not exist';
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
  var code = [
    'function OnUpdate(doc, meta, xattrs) {',
    '    log("Doc created/updated", meta.id);',
    '}',
    '',
    'function OnDelete(meta, options) {',
    '    log("Doc deleted/expired", meta.id);',
    '}',
    '',
    '// function OnDeploy(action) {',
    '//     log("OnDeploy function run", action);',
    '// }'
  ].join('\n');
  return {
    appname: 'Application name',
    appcode: code,
    depcfg: {
      buckets: [],
      metadata_bucket: 'eventing',
      metadata_scope: '_default',
      metadata_collection: '_default',
      source_bucket: 'default',
      source_scope: '_default',
      source_collection: '_default'
    },
    version: getVersion(),
    settings: {
      log_level: 'INFO',
      dcp_stream_boundary: 'everything',
      processing_status: false,
      deployment_status: false,
      description: '',
      worker_count: 1,
      execution_timeout: 60,
      on_deploy_timeout: 60,
      user_prefix: 'eventing',
      n1ql_consistency: 'none',
      language_compatibility: '6.5.0',
      cursor_aware: false,
      timer_context_size: 1024
    },
    function_scope: {
      bucket: "",
      scope: ""
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
  this.settings.worker_count = 1;
  this.settings.execution_timeout = 60;
  this.settings.on_deploy_timeout = 60;
  this.settings.user_prefix = 'eventing';
  this.settings.n1ql_consistency = 'none';
  this.settings.cursor_aware = false;
  this.version = getVersion();
  this.settings.timer_context_size = 1024;
  this.settings.dcp_stream_boundary = 'everything';
};

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
    return [
      'Running in compatibility mode. Please make the necessary changes and update the language compatibility to latest.'
    ]
  }
  return [];
}

function getAppLocation(appname, function_scope) {
  var appLocation = appname;
  if (function_scope && function_scope.bucket != "" && function_scope.bucket !=
    "*" && function_scope.bucket != undefined) {
    return function_scope.bucket + "/" + function_scope.scope + "/" +
      appname
  }

   if (function_scope && function_scope.bucket_name != "" && function_scope.bucket_name !=
    "*" && function_scope.bucket_name != undefined) {
     return function_scope.bucket_name + "/" + function_scope.scope_name + "/" +
      appname
  }
  return appLocation
}

function getReverseAppLocation(app_location) {
  var values = app_location.split("/")
  if (values.length == 3) {
    return {
      name: values[2],
      function_scope: {
        bucket: values[1],
        scope: values[0]
      }
    }
  }
  return { name: app_location, function_scope: { bucket: "*", scope: "*" } }
}
