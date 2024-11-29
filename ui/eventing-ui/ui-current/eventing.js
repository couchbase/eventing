import {
  format as d3Format
} from "d3-format";
import angular from "angular";
import _ from "lodash";
import saveAs from "file-saver"
import ace from 'ace/ace';

import uiRouter from "@uirouter/angularjs";
import uiAce from "ui-ace";
import mnPoolDefault from "components/mn_pool_default";
import mnPermissions from "components/mn_permissions";
import mnSelect from "components/directives/mn_select/mn_select";
import mnDetailStats from "components/directives/mn_detail_stats_controller";
import mnStatisticsNewService from "mn_admin/mn_statistics_service";
import mnStatisticsDescription from "mn_admin/mn_statistics_description";
import mnFilters from "components/mn_filters";

import Adapter from "./adapter.js";

const PASSWORD_MASK = "*****";

import appLogTemplate from './dialogs/app-log.html';
import appWarningsTemplate from './dialogs/app-warnings.html';
import appSettingsTemplate from './fragments/app-settings.html';
import appActionsTemplate from './dialogs/app-actions.html';
import eventingSettingsTemplate from './fragments/eventing-settings.html';
import appDebugTemplate from './dialogs/app-debug.html';
import eventingTemplate from '../eventing.html';
import summaryTemplate from './fragments/summary.html';
import handlerEditorTemplate from './fragments/handler-editor.html';

import {
  Application,
  ApplicationManager,
  ApplicationModel,
  determineUIStatus,
  getWarnings,
  getAppLocation,
  getReverseAppLocation
} from "../app-model.js";

import {
  ErrorMessage,
  ErrorHandler
} from "../err-model.js";

export default 'eventing';

angular
  .module('eventing', [
    uiAce,
    uiRouter,
    mnPoolDefault,
    mnPermissions,
    mnSelect,
    mnDetailStats,
    mnStatisticsNewService,
    mnFilters
  ])
  .controller('SummaryDeployedStatsItemCtrl', [
    '$scope', 'mnPoolDefault', 'mnStatisticsNewService',
    function($scope, mnPoolDefault, mnStatisticsNewService) {
      let vm = this;
      let isAtLeast70 = mnPoolDefault.export.compat.atLeast70;

      vm.isFinite = Number.isFinite;

      let row = $scope.app;

      let perItemStats = [
        "@eventing-.@items.eventing_processed_count",
        "@eventing-.@items.eventing_failed_count",
        "@eventing-.@items.eventing_timeout_count",
        "@eventing-.@items.eventing_dcp_backlog",
        "@eventing-.@items.eventing_on_deploy_failure"
      ];
      let getStatSamples = isAtLeast70 ? getStatSamples70 :
        getStatSamplesPre70;
      let uiStatNames = perItemStats.map(stat => mnStatisticsDescription
        .mapping70(stat).split(".").pop());

      var app_location = getAppLocation(row.appname, row.function_scope);
      $scope.summaryCtrl.mnEventingStatsPoller.subscribeUIStatsPoller({
        bucket: row.depcfg.metadata_bucket,
        node: "all",
        zoom: 3000,
        step: 1,
        stats: isAtLeast70 ? perItemStats : perItemStats.map(
          mnStatisticsDescription.mapping70),
        items: {
          eventing: isAtLeast70 ? app_location : ("eventing/" +
            app_location + "/")
        }
      }, $scope);

      $scope.$watch("mnUIStats", updateValues);
      $scope.$watch("app", updateValues);

      function getEventingStatName(statName) {
        return 'eventing/' + app_location + '/' + statName;
      }

      function getStats(statName) {
        let stats = $scope.mnUIStats && $scope.mnUIStats && $scope.mnUIStats
          .stats;
        return stats && stats[statName] && stats[statName]["aggregate"];
      }

      function getStatSamples70(statName) {
        statName = mnStatisticsDescription.mapping65("@eventing-.@items." +
          statName);
        let stats = getStats(statName);
        let last = stats && stats.values[stats.values.length - 1];
        let val = last && last[1];
        val = val ? Number(val) : !!val;
        return val;
      }

      function getStatSamplesPre70(statName) {
        let stats = getStats(getEventingStatName(statName));
        return stats && stats.slice().reverse().find(stat => stat != null);
      }

      function updateValues() {
        uiStatNames.forEach(statName => row[statName] = getStatSamples(
          statName));
      }
    }
  ])
  // Controller for the summary page.
  .controller('SummaryCtrl', [
    '$q', '$scope', '$rootScope', '$state',
    '$uibModal', '$timeout', '$location', 'ApplicationService', 'serverNodes',
    'isEventingRunning', 'mnPoller', 'mnStatisticsNewService',
    function($q, $scope, $rootScope, $state, $uibModal, $timeout, $location,
      ApplicationService, serverNodes, isEventingRunning, mnPoller,
      mnStatisticsNewService) {
      var self = this;

      self.errorState = !ApplicationService.status.isErrorCodesLoaded();
      self.errorCode = 200;
      self.showErrorAlert = self.showSuccessAlert = self.showWarningAlert =
        false;
      self.serverNodes = serverNodes;
      self.isEventingRunning = isEventingRunning;
      self.workerCount = 0;
      self.cpuCount = 0;
      self.appList = ApplicationService.local.getAllApps();
      self.needAppList = new Set();
      self.disableEditButton = false;
      self.appListStaleCount = 0;
      self.statusPollMillis = 2000;
      self.annotationList = []
      self.appstorefresh = []
      self.showWarningTooltip = new Map();

      // Broadcast on channel 'isEventingRunning'
      $rootScope.$broadcast('isEventingRunning', self.isEventingRunning);

      for (var app of Object.keys(self.appList)) {
        self.appList[app].uiState = 'warmup';
        self.appList[app].warnings = getWarnings(self.appList[app]);
      }

      checkAndAddDefaults();

      // run deployedAppsTicker() every 2 seconds, but only while Eventing view is active
      new mnPoller($scope, deployedAppsTicker).setInterval(self
        .statusPollMillis).cycle();

      self.mnEventingStatsPoller = mnStatisticsNewService.createStatsPoller(
        $scope);
      self.mnEventingStatsPoller.heartbeat.setInterval(4000);

      // Poll to get the App status and reflect the same in the UI
      function deployedAppsTicker() {
        if (!self.isEventingRunning) {
          return Promise.resolve(); // need for mnPoller
        }

        return ApplicationService.public.status()
          .then(function(response) {
            response = response.data;
            var rspAppList = new Set(); // appname in UI
            var updAppList = new Set(); // appname not in UI
            var rspAppStat =
              new Map(); // composite_status by appname (in UI and not in UI)
            var uiIsStale =
              false; // if we need to reload somehting new App or state change

            ApplicationService.public.getAnnotations().then(function(
              response) { self.annotationList = response.data });
            var deprecatedMap = new Map();
            var overloadedMap = new Map();
            for (var app of self.annotationList) {
              var appLocation = getAppLocation(app.name, app
                .function_scope);
              app.function_scope.bucket = app.function_scope.bucket_name;
              app.function_scope.scope = app.function_scope.scope_name;
              deprecatedMap[appLocation] = (app.deprecatedNames ? app
                .deprecatedNames : [""]).join(", ");
              overloadedMap[appLocation] = (app.overloadedNames ? app
                .overloadedNames : [""]).join(", ");
            }

            let refreshapplist = []
            for (var rspApp of response.apps ? response.apps : []) {
              var app_location = getAppLocation(rspApp.name, rspApp
                .function_scope);
              if (rspApp.hasOwnProperty("redeploy_required") && rspApp
                .redeploy_required == true) {
                refreshapplist.push(app_location);
              }
              rspAppStat.set(app_location, rspApp.composite_status);

              if (!(app_location in self.appList)) {
                // An App from the recuring status does not exist in the UI's current list
                updAppList.add(app_location);
                uiIsStale = true;

                // add to update list to process later e.g. a remote add
                self.needAppList.add(app_location);
              } else {
                self.appList[app_location].deprecatedNames = deprecatedMap[
                  app_location] ? deprecatedMap[app_location] : "";
                self.appList[app_location].overloadedNames = overloadedMap[
                  app_location] ? overloadedMap[app_location] : "";
                rspAppList.add(app_location);
                self.appList[app_location].status = rspApp.composite_status;

                var uiApp = self.appList[app_location];
                if (rspApp.deployment_status != uiApp.settings
                  .deployment_status ||
                  rspApp.processing_status != uiApp.settings
                  .processing_status
                ) {
                  // add to update list to process later e.g. local or remote status change
                  self.needAppList.add(app_location);
                  uiIsStale = true;
                }
              }
            }
            self.appstorefresh = refreshapplist;
            for (var app of Object.keys(self.appList)) {
              if (!rspAppList.has(app)) {
                // An App from the UI's current list doesn't exisit in the recurring status
                uiIsStale = true;
              } else {
                self.appList[app].uiState = determineUIStatus(self.appList[
                  app].status);
                self.appList[app].warnings = getWarnings(self.appList[app]);
              }
            }
            if (!uiIsStale) {
              // 1:1 match between all Apps in the UI's and all Apps in the recuring status
              self.appListStaleCount = 0;
            } else {
              if (self.appListStaleCount == 0) {
                // since were stale we want to update self.workerCount at start
                fetchWorkerCount();
              }
              self.appListStaleCount++;
              // we refreash or resync the UI if needed once about every 20 to 25 seconds
              if (self.appListStaleCount >= (20 / (self.statusPollMillis /
                  1000))) {
                self.appListStaleCount = 0;

                // since were stale we want to update self.workerCount when done
                fetchWorkerCount();

                // remove stale Apps if any
                removeStaleApps(rspAppList);

                // add missing Apps if any
                updateStaleApps(rspAppList, updAppList, rspAppStat);
              }
            }

            // Only load this if non-zero or if uiIsStale
            if (self.workerCount == 0) {
              fetchWorkerCount();
            }

            // Only load this if non-zero the cpuCount or core count doesn't change
            if (self.cpuCount == 0) {
              fetchCpuCount();
            }

            // Fetch DeployesdStats once every scrapeInterval/2 seconds
            if ($scope.rbac.cluster.settings.metrics.read) {
              ApplicationService.server.getScrapeInterval().then(function(
                value) {
                self.mnEventingStatsPoller.heartbeat.setInterval(value *
                  1000 / 2);
              });
            }

          }).catch(function(errResponse) {
            self.errorCode = errResponse && errResponse.status || 500;
            // Do not log the occasional HTTP abort when we leave the Eventing view
            if (!(errResponse.status === -1 && errResponse.xhrStatus ===
                'abort')) {
              console.error('Unable to list apps', errResponse);
            }
          });
      }

      function fetchWorkerCount() {
        ApplicationService.server.getWorkerCount()
          .then(function(response) {
            if (response && response.data) {
              self.workerCount = response.data;
            }
          })
          .catch(function(errResponse) {
            console.error('Unable to get worker count', errResponse);
            self.workerCount = 0;
          });
      }

      function fetchCpuCount() {
        ApplicationService.server.getCpuCount()
          .then(function(response) {
            if (response && response.data) {
              self.cpuCount = response.data;
            }
          })
          .catch(function(errResponse) {
            console.error('Unable to get cpu count', errResponse);
            self.cpuCount = 0;
          });
      }

      function removeStaleApps(rspAppList) {
        // Remove, e.g. delete, all stale functions from the UI
        var delAppCnt = 0;
        for (var app_location of Object.keys(self.appList)) {
          if (rspAppList.size == 0 || !rspAppList.has(app_location)) {
            delAppCnt++;
            var function_scope = self.appList[app_location].function_scope;
            var tstApp = ApplicationService.local.getAppByName(app_location,
              function_scope)
            if (tstApp) {
              ApplicationService.local.deleteApp(app_location,
                function_scope);
            }
            delete self.appList[app_location];
          }
        }
        if (delAppCnt > 0) {
          // display to the user (and/or log) how many Apps were deleted
          var msg = 'Eventing: resync removing ' + delAppCnt +
            ' full ' + ((delAppCnt == 1) ? 'definition' : 'definitions');
          // console.info(msg);
          ApplicationService.server.showWarningAlert(msg);
        }
      }

      function updateStaleApps(rspAppList, updAppList, rspAppStat) {

        // Update via a full function definition fetch, to resync on state changes and new items
        //
        //     rspAppList is the list of apps in both the UI and current remote status response
        //     updAppList is the list of apps in only the remote status response
        //     rspAppStat is a Map by appname to get composite_status for the current response
        //
        //     self.appList has the UI current set of Apps
        //     self.needAppList has added Apps and all Apps that had a state change

        if (self.needAppList.size == 0) return;

        var missingAppCnt = 0;
        var stateUpdCnt = 0;
        var appsToFetch = new Set()
        for (var appname of self.needAppList) {
          if (updAppList.has(appname)) {
            missingAppCnt++;
            appsToFetch.add(appname);
          } else
          if (rspAppList.has(appname)) {
            stateUpdCnt++
            appsToFetch.add(appname);
          }
        }


        if (appsToFetch.size == 0) return;

        // display to the user (and/or log) how many Apps were reloaded and why
        var msg = 'Eventing: resync fetching ' + appsToFetch.size +
          ' full ' + ((appsToFetch.size == 1) ? 'definition' :
            'definitions') +
          ' (missing ' + missingAppCnt + ', state-update ' + stateUpdCnt +
          ')';
        ApplicationService.server.showWarningAlert(msg);
        // console.info(msg);

        // For any new apps created remotely we need more information than just the status
        var responses = new Map();
        var thePromises = [];
        for (var name of appsToFetch ? appsToFetch : []) {
          var id = getReverseAppLocation(name);
          var curPromise = ApplicationService.public.getFunction(id.name, id
            .function_scope);
          thePromises.push(curPromise);
        }

        // Sometimes we will have an error getting status for something just deleted, that's okay
        return $q.all(thePromises).then(function(result) {
          for (var i = 0; i < result.length; i++) {
            var appname = result[i].data.appname;
            var function_scope = result[i].data.function_scope;
            var appLocation = getAppLocation(appname, function_scope);
            ApplicationService.local.createApp(new ApplicationModel(
              result[i].data));
            self.appList[appLocation] = ApplicationService.local
              .getAppByName(
                appname, function_scope);
            self.appList[appLocation].status = rspAppStat.get(
              appLocation);
            self.appList[appLocation].uiState = determineUIStatus(self
              .appList[appLocation].status);
            self.appList[appLocation].warnings = getWarnings(self.appList[
              appLocation]);
          }
          self.needAppList.clear();
          checkAndAddDefaults();
        });
      }

      function checkAndAddDefaults() {
        for (var app of Object.keys(self.appList)) {
          if (!self.appList[app].depcfg.source_scope) {
            self.appList[app].depcfg.source_scope = "_default";
          }

          if (!self.appList[app].depcfg.source_collection) {
            self.appList[app].depcfg.source_collection = "_default";
          }

          if (!self.appList[app].depcfg.metadata_scope) {
            self.appList[app].depcfg.metadata_scope = "_default";
          }

          if (!self.appList[app].depcfg.metadata_collection) {
            self.appList[app].depcfg.metadata_collection = "_default";
          }
        }
      }

      self.isAppListEmpty = function() {
        return Object.keys(self.appList).length === 0;
      };

      self.openAppLog = function(appName, function_scope) {
        ApplicationService.server.getAppLog(appName, function_scope).then(
          function(log) {
            let logScope = $scope.$new(true);
            logScope.appName = appName;
            logScope.logMessages = [];
            if (log && log.length > 0) {
              logScope.logMessages = log.split(/\r?\n/);
            }
            $uibModal.open({
              template: appLogTemplate,
              scope: logScope
            });
          });
      };

      self.openWarnings = function(appName, function_scope) {
        let scope = $scope.$new(true);
        var appLocation = getAppLocation(appName, function_scope);
        scope.appName = appName;
        scope.warnings = self.appList[appLocation].warnings;
        $uibModal.open({
          template: appWarningsTemplate,
          scope: scope
        });
      };

      self.toggleWarningTooltip = function(appName) {
        self.showWarningTooltip.set(appName, !self.showWarningTooltip.get(appName));
      };

      self.openSettings = function(appName, function_scope) {
        $uibModal.open({
            template: appSettingsTemplate,
            controller: 'SettingsCtrl',
            controllerAs: 'formCtrl',
            resolve: {
              appName: [function() {
                return getAppLocation(appName, function_scope);
              }],
              savedApps: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.tempStore.getAllApps();
                }
              ],
              isAppDeployed: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.tempStore.isAppDeployed(
                      appName, function_scope)
                    .then(function(isDeployed) {
                      return isDeployed;
                    })
                    .catch(function(errResponse) {
                      console.error(
                        'Unable to get deployed apps list',
                        errResponse);
                    });
                }
              ],
              isAppPaused: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.tempStore.isAppPaused(
                      appName, function_scope)
                    .then(function(isPaused) {
                      return isPaused;
                    })
                    .catch(function(errResponse) {
                      console.error('Unable to get function status',
                        errResponse);
                    });
                }
              ],
              logFileLocation: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.server.getLogFileLocation();
                }
              ],
              snapshot: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.server.getBucketSnapshot();
                }
              ]
            }
          }).result
          .catch(function(errResponse) {

            if (errResponse !== 'cancel' && errResponse !== 'X') {
              console.log(errResponse);
            }
          });
      };

      self.toggleDeployment = function(app) {
        var deploymentScope = $scope.$new(true);
        deploymentScope.appName = app.appname;
        deploymentScope.actionTitle = app.status === 'deployed' || app
          .status === 'paused' ? 'Undeploy' : 'Deploy';
        deploymentScope.action = app.status === 'deployed' || app.status ===
          'paused' ? 'undeploy' : 'deploy';

        if (app.status === 'deployed' || app.status === 'paused') {
          undeployApp(app, deploymentScope);
        } else {
          changeState('deploy', app, deploymentScope);
        }
      };

      self.toggleProcessing = function(app) {
        var processingScope = $scope.$new(true);
        processingScope.appName = app.appname;
        processingScope.actionTitle = app.status === 'paused' ? 'Resume' :
          'Pause';
        processingScope.action = app.status === 'paused' ? 'resume' :
          'pause';

        if (app.status === 'paused') {
          changeState('resume', app, processingScope);
        } else {
          changeState('pause', app, processingScope);
        }
      };

      function changeState(operation, app, scope) {
        var appClone = app.clone();
        scope.settings = {};
        scope.settings.changeFeedBoundary = app.settings
          .dcp_stream_boundary;

        $uibModal.open({
            template: appActionsTemplate,
            scope: scope
          }).result
          .then(function(response) {
            switch (operation) {
              case 'deploy':
                appClone.settings.deployment_status = true;
                appClone.settings.processing_status = true;
                return ApplicationService.public.deployApp(appClone);
              case 'resume':
                appClone.settings.deployment_status = true;
                appClone.settings.processing_status = true;
                return ApplicationService.public.resumeApp(appClone);
              case 'pause':
                appClone.settings.deployment_status = true;
                appClone.settings.processing_status = false;
                return ApplicationService.public.pauseApp(appClone);
            }
          })
          .then(function(response) {
            var responseCode = ApplicationService.status.getResponseCode(
              response);
            if (responseCode) {
              return $q.reject(ApplicationService.status.getErrorMsg(
                responseCode, response.data));
            }

            app.settings.deployment_status = appClone.settings
              .deployment_status;
            app.settings.processing_status = appClone.settings
              .processing_status;

            self.disableEditButton = false;

            var warnings = null;
            if (response.data && response.data.info) {
              var info = response.data.info;
              if (info.warnings && info.warnings.length > 0) {
                warnings = info.warnings.join(". ");
                ApplicationService.server.showWarningAlert(warnings);
              }
            }

            ApplicationService.server.showSuccessAlert(
              `${app.appname} under ${app.function_scope.bucket}:${app.function_scope.scope} will ${operation} ${warnings ? 'with warnings' : ''}`
            );

            // since the UI is changing state update the count
            fetchWorkerCount();
          })
          .catch(function(errResponse) {
            if (errResponse === 'cancel' || errResponse === 'X') {
              return
            }

            if (errResponse.data && (errResponse.data.name ===
                'ERR_HANDLER_COMPILATION')) {
              let info = errResponse.data.runtime_info.info;
              app.compilationInfo = info;
              ApplicationService.server.showErrorAlert(
                `${operation} failed: Syntax error (${info.line_number}, ${info.column_number}) - ${info.description}`
              );
            } else if (errResponse.data && (errResponse.data.name ===
                'ERR_CLUSTER_VERSION')) {
              var data = errResponse.data;
              ApplicationService.server.showErrorAlert(
                `${operation} failed: ${data.description} - ${data.runtime_info.info}`
              );
            } else {
              let info = errResponse.data;
              ApplicationService.server.showErrorAlert(
                `${operation} failed: ` + JSON.stringify(info));
            }

            self.disableEditButton = false;
            console.error(errResponse);
          });
      }

      function undeployApp(app, scope) {
        $uibModal.open({
            template: appActionsTemplate,
            scope: scope
          }).result
          .then(function(response) {
            return ApplicationService.public.undeployApp(app.appname, app
              .function_scope);
          })
          .then(function(response) {
            var responseCode = ApplicationService.status.getResponseCode(
              response);
            if (responseCode) {
              return $q.reject(ApplicationService.status.getErrorMsg(
                responseCode, response.data));
            }

            app.settings.deployment_status = false;
            app.settings.processing_status = false;
            ApplicationService.server.showSuccessAlert(
              `${app.appname} under ${app.function_scope.bucket}:${app.function_scope.scope} will be undeployed`
            );

            // since the UI is changing state via undeploy update the count
            fetchWorkerCount();

          })
          .catch(function(errResponse) {
            let info = errResponse.data.runtime_info;
            ApplicationService.server.showErrorAlert(
              `Undeploy failed due to ` + JSON.stringify(info));
            console.error(errResponse);
          });
      }

      // Callback to export the app.
      self.exportApp = function(appName, function_scope) {
        ApplicationService.public.export()
          .then(function(apps) {
            var app = apps.data.find(function(app) {
              var app_function_scope = app.function_scope;
              return app.appname === appName && function_scope
                .bucket === app_function_scope.bucket &&
                function_scope.scope === app_function_scope.scope;
            });
            if (!app) {
              return $q.reject('app not found');
            }

            var fileName = appName + '.json';
            // Create a new blob of the app.
            var fileToSave = new Blob([JSON.stringify([app])], {
              type: 'application/json',
              name: fileName
            });

            // Save the file.
            saveAs(fileToSave, fileName);
          })
          .catch(function(errResponse) {
            ApplicationService.server.showErrorAlert(
              `Failed to export the function due to "${errResponse.data.description}"`
            );
            console.error(errResponse);
          });
      };

      // Callback for deleting application.
      self.deleteApp = function(appName, function_scope) {
        $scope.appName = appName;
        $scope.actionTitle = 'Delete';
        $scope.action = 'delete';

        // Open dialog to confirm delete.
        $uibModal.open({
            template: appActionsTemplate,
            scope: $scope
          }).result
          .then(function(response) {
            return ApplicationService.public.deleteApp(appName,
              function_scope);
          })
          .then(function(response) {
            // Delete the local copy of the app in the browser
            ApplicationService.local.deleteApp(appName, function_scope);
            ApplicationService.server.showSuccessAlert(
              `${appName} under ${function_scope.bucket}:${function_scope.scope} deleted successfully!`
            );
          })
          .catch(function(errResponse) {
            ApplicationService.server.showErrorAlert(
              `Delete failed due to "${errResponse.data.description}"`);
            console.error(errResponse);
          });
      };
    }
  ])
  // Controller for the buttons in header.
  .controller('HeaderCtrl', ['$q', '$scope', '$uibModal', '$state',
    'mnPoolDefault', 'ApplicationService',
    function($q, $scope, $uibModal, $state, mnPoolDefault,
      ApplicationService) {
      var self = this;
      self.isEventingRunning = true;

      // Subscribe to channel 'isEventingRunning' from SummaryCtrl
      $scope.$on('isEventingRunning', function(event, args) {
        self.isEventingRunning = args;
      });

      function createApp(creationScope) {
        // Open the settings fragment as create dialog.
        $uibModal.open({
            template: appSettingsTemplate,
            scope: creationScope,
            controller: 'CreateCtrl',
            controllerAs: 'formCtrl',
            resolve: {
              savedApps: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.tempStore.getAllApps();
                }
              ],
              snapshot: ['ApplicationService',
                function(ApplicationService) {
                  console.log('Snapshot');
                  return ApplicationService.server.getBucketSnapshot();
                }
              ],
              logFileLocation: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.server.getLogFileLocation();
                }
              ],
            }
          }).result
          .then(function(response) { // Upon continue.

            Object.assign(creationScope.appModel.depcfg, ApplicationService
              .convertBindingToConfig(creationScope.bindings));
            creationScope.appModel.fillWithMissingDefaults();

            // When we import the application, we want it to be in disabled and
            // undeployed state with feed bondary "everything" ("from_prior" is not legal)
            creationScope.appModel.settings.processing_status = false;
            creationScope.appModel.settings.deployment_status = false;
          })
          .then(function(response) {
            return ApplicationService.public.import(creationScope.appModel);
          })
          .then(function(response) {
            var responseCode = ApplicationService.status.getResponseCode(
              response);
            if (responseCode) {
              return $q.reject(ApplicationService.status.getErrorMsg(
                responseCode, response.data));
            } else {

              ApplicationService.local.createApp(creationScope.appModel);

              ApplicationService.server.showSuccessAlert(
                'Operation successful. Update the code and save, or return back to Eventing summary page.'
              );

              return $state.transitionTo('app.admin.eventing.handler', {
                appName: creationScope.appModel.appname,
                function_scope: creationScope.appModel.function_scope,
              }, {
                // Explained in detail - https://github.com/angular-ui/ui-router/issues/3196
                reload: true
              });
            }
          })
          .catch(function(errResponse) { // Upon cancel.
            if (errResponse === 'cancel' || errResponse === 'X') {
              return
            }
            if (errResponse.data && errResponse.data[0] && errResponse.data[
                0].info) {
              ApplicationService.server.showErrorAlert(
                "Changes cannot be saved. Reason: " + JSON.stringify(
                  errResponse.data[0].info));
              return;
            }
            ApplicationService.server.showErrorAlert(
              "Changes cannot be saved. Reason: " + JSON.stringify(
                errResponse.data));
          });
      }

      // Callback for create.
      self.showCreateDialog = function() {
        var scope = $scope.$new(true);
        scope.appModel = new ApplicationModel();
        scope.appModel.initializeDefaults();
        scope.bindings = [];
        scope.bindings.push({
          type: '',
          name: '',
          scope: '',
          collection: '',
          value: '',
          access: 'r',
          auth_type: 'no-auth',
          allow_cookies: true,
          validate_ssl_certificate: false
        });
        createApp(scope);
      };

      self.showEventingSettings = function() {
        $uibModal.open({
            template: eventingSettingsTemplate,
            controller: 'EventingSettingsCtrl',
            controllerAs: 'ctrl',
            resolve: {
              config: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.public.getConfig();
                }
              ],
              encryptionLevel: ['ApplicationService',
                function(ApplicationService) {
                  return ApplicationService.server.getEncryptionLevel();
                }
              ]
            }
          }).result
          .catch(function(errResponse) {
            if (errResponse !== 'cancel' && errResponse !== 'X') {
              console.log(errResponse);
            }
          });
      };

      // Callback for importing application.
      self.importConfig = function() {
        function handleFileSelect(evt) {
          var file = evt.target.files;
          if (file[0].type != 'application/json') {
            ApplicationService.server.showErrorAlert(
              'Imported file format is not JSON, please verify the file being imported'
            );
            return;
          }
          var reader = new FileReader();
          reader.onloadend = function() {
            try {
              var app = JSON.parse(reader.result);
              if (app instanceof Array) {
                app = app[0];
              }

              var scope = $scope.$new(true);
              scope.appModel = new ApplicationModel(app);
              scope.bindings = ApplicationService.getBindingFromConfig(app
                .depcfg);

              if (!scope.bindings.length) {
                // Add a sample row of bindings.
                scope.bindings.push({
                  type: '',
                  name: '',
                  scope: '',
                  collection: '',
                  value: '',
                  access: 'r'
                });
              }
              if (!scope.appModel.settings.language_compatibility) {
                scope.appModel.settings.language_compatibility = '6.0.0';
              }
              createApp(scope);
            } catch (error) {
              ApplicationService.server.showErrorAlert(
                'The imported JSON file is not supported. Please check the format.'
              );
              console.error('Failed to load config:', error);
            }
          };

          reader.readAsText(this.files[0]);
        }

        var loadConfigElement = document.getElementById("loadConfig");
        loadConfigElement.value = null;
        loadConfigElement.addEventListener('change', handleFileSelect,
          false);
        loadConfigElement.click();
      };
    }
  ])
  .controller('EventingSettingsCtrl', ['$scope', '$rootScope', '$stateParams',
    'ApplicationService', 'config', 'encryptionLevel',
    function($scope, $rootScope, $stateParams, ApplicationService, config,
      encryptionLevel) {
      var self = this;
      config = config.data;
      self.enableDebugger = config.enable_debugger;
      self.enableCurl = !config.disable_curl;
      self.encryptionLevel = encryptionLevel

      if (self.encryptionLevel == 'strict') {
        $rootScope.debugDisable = true;
        self.enableDebugger = false;
        ApplicationService.public.updateConfig({
          enable_debugger: self.enableDebugger
        });
      }
      self.saveSettings = function(closeDialog) {
        ApplicationService.public.updateConfig({
          enable_debugger: self.enableDebugger,
          disable_curl: !self.enableCurl
        }).then(function(response) {
          if ($stateParams.appName) {
            let app = ApplicationService.local.getAppByName($stateParams
              .appName, $stateParams.function_scope);
            $rootScope.debugDisable = !(app.settings
              .deployment_status && app
              .settings.processing_status) || !self.enableDebugger;
            closeDialog('ok');
          } else {
            closeDialog('ok');
          }
        }).catch(function(errResponse) {
          ApplicationService.server.showErrorAlert(
            "Error in storing config changes: " +
            JSON.stringify(errResponse.data));
          closeDialog('error');
        });
      };
    }
  ])
  // Controller for creating an application.
  .controller('CreateCtrl', ['$scope', 'FormValidationService',
    'savedApps', 'snapshot', 'logFileLocation',
    function($scope, FormValidationService, savedApps, snapshot,
      logFileLocation) {
      var self = this;
      self.isDisable = false;
      self.isDialog = true;
      self.functionBuckets = [];
      self.functionScopes = [];

      self.Scopes = [];
      self.Collections = [];

      self.sourceCollections = [];
      self.metadataCollections = [];
      self.sourceScopes = [];
      self.metadataScopes = [];
      self.sourceBuckets = [];
      self.metadataBuckets = [];
      self.savedApps = savedApps.getApplications();
      self.logFileLocation = logFileLocation

      self.bindings = [];
      self.scopes = [];
      self.collections = [];
      self.buckets = [];
      // Checks whether source and metadata buckets are the same.
      self.srcMetaSameBucket = function(appModel) {
        return appModel.depcfg.source_bucket === appModel.depcfg
          .metadata_bucket &&
          ((appModel.depcfg.source_scope === appModel.depcfg
            .metadata_scope) || (appModel.depcfg.source_scope == "*")) &&
          ((appModel.depcfg.source_collection === appModel.depcfg
            .metadata_collection) || (appModel.depcfg.source_collection ==
            "*"));
      };

      self.srcBindingSameBucket = function(appModel, binding) {
        return appModel.depcfg.source_bucket === binding.name;
      };

      self.executionTimeoutCheck = function(appModel) {
        return appModel.settings.execution_timeout > 60;
      };

      self.ondeployTimeoutCheck = function(appModel) {
        return appModel.settings.on_deploy_timeout > 60;
      };

      self.getScopes = function(bucketName, bucketList, wildcard_allowed) {
        var scope = new Set();
        for (var index in bucketList) {
          if (bucketList[index].bucket_name == bucketName) {
            if (wildcard_allowed || (bucketList[index].scope_name != "*" &&
                bucketList[index].collection_name != "*")) {
              scope.add(bucketList[index].scope_name);
            }
          }
        }
        return Array.from(scope);
      };

      self.getCollection = function(bucketName, scopeName, bucketList,
        wildcard_allowed) {
        var collection = [];
        for (var index in bucketList) {
          if (bucketList[index].bucket_name == bucketName && bucketList[
              index].scope_name == scopeName) {
            if (wildcard_allowed || bucketList[index].collection_name !=
              "*") {
              collection.push(bucketList[index].collection_name);
            }
          }
        }
        return collection;
      };

      self.getLatestBuckets = function(bucketList) {
        var bucketsSet = new Set();
        for (var index in bucketList) {
          bucketsSet.add(bucketList[index].bucket_name);
        }
        return Array.from(bucketsSet);
      };

      self.isBucketValid = function(bucketName, bucketList) {
        for (var index in bucketList) {
          if (bucketList[index].bucket_name == bucketName) {
            return true;
          }
        }
        return false;
      };
      self.isScopeValid = function(bucketName, scopeName, bucketList) {
        for (var index in bucketList) {
          if (bucketList[index].bucket_name == bucketName && bucketList[
              index].scope_name == scopeName) {
            return true;
          }
        }
        return false;
      };
      self.isCollectionValid = function(bucketName, scopeName, collectionName,
        bucketList) {
        for (var index in bucketList) {
          if (bucketList[index].bucket_name == bucketName && bucketList[
              index].scope_name == scopeName && bucketList[index]
            .collection_name == collectionName) {
            return true;
          }
        }
        return false;
      };

      self.populatefunctionScopes = function(bucketName) {
        var scopes = self.getScopes(bucketName, snapshot.data.func_scope,
          true);
        self.functionScopes = scopes;
      };

      self.populateBuckets = function(access, index) {
        var list = snapshot.data.read_write_permission;
        if (access === "r") {
          list = snapshot.data.read_permission;
        }
        self.buckets[index] = self.getLatestBuckets(list);
        self.populateScope($scope.bindings[index].name, index);
      }

      self.populateScope = function(bucketName, index) {
        var list = snapshot.data.read_write_permission;
        if ($scope.bindings[index].access === "r") {
          list = snapshot.data.read_permission;
        }
        var scopes = self.getScopes(bucketName, list, true);
        self.scopes[index] = scopes;
        self.populateCollections(bucketName, $scope.bindings[index].scope,
          index);
      };

      self.populateCollections = function(bucketName, scopeName, index) {
        var list = snapshot.data.read_write_permission;
        if ($scope.bindings[index].access === "r") {
          list = snapshot.data.read_permission;
        }
        var collections = self.getCollection(bucketName, scopeName, list,
          true);
        self.collections[index] = collections;
      };

      self.populateSourceScopes = function(bucketName) {
        self.sourceScopes = self.getScopes(bucketName, snapshot.data
          .dcp_stream_permission, true);
        self.populateSourceCollections(bucketName, $scope.appModel.depcfg
          .source_scope);
      };

      self.populateSourceCollections = function(bucketName, scopeName) {
        self.sourceCollections = self.getCollection(bucketName, scopeName,
          snapshot.data.dcp_stream_permission, true);;
      };

      self.Initialize = function() {
        $scope.bindings.push({
          type: '',
          name: '',
          scope: '',
          collection: '',
          value: '',
          auth_type: 'no-auth',
          allow_cookies: true,
          access: 'r'
        });
        var index = $scope.bindings.length - 1;
        self.buckets.push([]);
        self.scopes.push([]);
        self.collections.push([]);
        self.populateBuckets('r', index);
        if (self.buckets[index].length > 0) {
          $scope.bindings[index].name = self.buckets[index][0];
        }
      };

      self.Remove = function(index) {
        $scope.bindings.splice(index, 1);
        self.buckets.splice(index, 1);
        self.scopes.splice(index, 1);
        self.collections.splice(index, 1);
      }

      self.populateMetadataScopes = function(bucketName) {
        self.metadataScopes = self.getScopes(bucketName, snapshot.data
          .read_write_permission, false);
        self.populateMetadataCollections(bucketName, $scope.appModel.depcfg
          .metadata_scope);
      };

      self.populateMetadataCollections = function(bucketName, scopeName) {
        self.metadataCollections = self.getCollection(bucketName, scopeName,
          snapshot.data.read_write_permission, false);
      };

      self.isFormInvalid = function() {
        return $scope.formCtrl.createAppForm.source_bucket &&
          FormValidationService.isFormInvalid(
            self, $scope.bindings, $scope.formCtrl.createAppForm
            .source_bucket.$viewValue);
      };

      self.isFuncNameUndefined = function() {
        return !$scope.appModel.appname;
      };

      self.validateVariableRegex = function(binding) {
        if (binding && binding.value) {
          return FormValidationService.isValidVariableRegex(binding.value);
        }

        return true;
      };

      self.validateVariable = function(binding) {
        if (binding && binding.value) {
          return FormValidationService.isValidVariable(binding.value);
        }

        return true;
      }

      for (var binding in $scope.bindings) {
        self.buckets.push([]);
        self.scopes.push([]);
        self.collections.push([]);

        var collectionList = snapshot.data.read_write_permission;
        if ($scope.bindings[binding].access === "r") {
          collectionList = snapshot.data.read_permission;
        }

        // Is the bucketname present in the binding valid?
        if (!self.isBucketValid($scope.bindings[binding].name,
            collectionList)) {
          $scope.bindings[binding].name = "";
          $scope.bindings[binding].scope = "";
          $scope.bindings[binding].collection = "";
        } else {
          if (!self.isScopeValid($scope.bindings[binding].name, $scope
              .bindings[binding].scope, collectionList)) {
            $scope.bindings[binding].scope = "";
            $scope.bindings[binding].collection = "";
          } else {
            if (!self.isCollectionValid($scope.bindings[binding].name, $scope
                .bindings[binding].scope, $scope.bindings[binding].collection,
                collectionList)) {
              $scope.bindings[binding].collection = "";
            }
          }
        }

        if ($scope.bindings[binding].name != "") {
          self.populateScope($scope.bindings[binding].name, binding);
        }

        if ($scope.bindings[binding].scope != "") {
          self.populateCollections($scope.bindings[binding].name, $scope
            .bindings[binding].scope, binding);
        }
      }

      self.functionBuckets = self.getLatestBuckets(snapshot.data.func_scope);
      self.sourceBuckets = self.getLatestBuckets(snapshot.data
        .dcp_stream_permission);
      self.metadataBuckets = self.getLatestBuckets(snapshot.data
        .read_write_permission);

      self.populateSourceScopes($scope.appModel.depcfg.source_bucket);
      self.populateMetadataScopes($scope.appModel.depcfg.metadata_bucket);
    }
  ])
  // Controller for settings.
  .controller('SettingsCtrl', ['$q', '$timeout', '$scope', 'ApplicationService',
    'FormValidationService',
    'appName', 'savedApps', 'isAppDeployed', 'isAppPaused', 'snapshot',
    'logFileLocation',
    function($q, $timeout, $scope, ApplicationService, FormValidationService,
      appName, savedApps, isAppDeployed, isAppPaused,
      snapshot, logFileLocation) {
      var self = this;
      var appModel = ApplicationService.local.getAppByName(appName);

      self.isDisable = true;
      self.isDialog = false;
      self.showSuccessAlert = false;
      self.showWarningAlert = false;
      self.isAppDeployed = isAppDeployed;
      self.isAppPaused = isAppPaused;
      self.logFileLocation = logFileLocation;
      self.sourceAndBindingSame = false;

      // Need to initialize buckets if they are empty,
      // otherwise self.saveSettings() would compare 'null' with '[]'.
      appModel.depcfg.buckets = appModel.depcfg.buckets ? appModel.depcfg
        .buckets : [];
      self.bindings = ApplicationService.getBindingFromConfig(appModel
        .depcfg);
      for (var idx = 0; idx < self.bindings.length; idx++) {
        if (self.bindings[idx].type == "url") {
          self.bindings[idx].password = PASSWORD_MASK;
          self.bindings[idx].bearer_key = PASSWORD_MASK;
        }
      }

      self.getScopes = function(bucketName, bucketList, wildcard_allowed) {
        var scope = new Set();
        for (var index in bucketList) {
          if (bucketList[index].bucket_name == bucketName) {
            if (wildcard_allowed || (bucketList[index].scope_name != "*" &&
                bucketList[index].collection_name != "*")) {
              scope.add(bucketList[index].scope_name);
            }
          }
        }
        return Array.from(scope);
      };

      self.getCollection = function(bucketName, scopeName, bucketList,
        wildcard_allowed) {
        var collection = [];
        for (var index in bucketList) {
          if (bucketList[index].bucket_name == bucketName && bucketList[
              index].scope_name == scopeName) {
            if (wildcard_allowed || bucketList[index].collection_name !=
              "*") {
              collection.push(bucketList[index].collection_name);
            }
          }
        }
        return collection;
      };

      self.getLatestBuckets = function(bucketList) {
        var bucketsSet = new Set();
        for (var index in bucketList) {
          bucketsSet.add(bucketList[index].bucket_name);
        }
        return Array.from(bucketsSet);
      };

      self.scopes = [];
      self.collections = [];
      self.buckets = [];
      // TODO : The following two lines may not be needed as we don't allow the user to edit
      //        the source and metadata buckets in the settings page.

      if (appModel.function_scope.bucket == undefined || appModel
        .function_scope.bucket == "") {
        appModel.function_scope.bucket = "*";
      }
      if (appModel.function_scope.scope == undefined || appModel
        .function_scope.scope == "") {
        appModel.function_scope.scope = "*";
      }

      if (appModel.depcfg.source_scope == "") {
        appModel.depcfg.source_scope = "_default";
      }
      if (appModel.depcfg.metadata_scope == "") {
        appModel.depcfg.metadata_scope = "_default";
      }

      if (appModel.depcfg.source_collection == "") {
        appModel.depcfg.source_collection = "_default";
      }
      if (appModel.depcfg.metadata_collection == "") {
        appModel.depcfg.metadata_collection = "_default";
      }

      self.functionBuckets = self.getLatestBuckets(snapshot.data.func_scope);
      self.functionScopes = self.getScopes(appModel.function_scope
        .bucket,
        snapshot.data.func_scope, true);
      self.sourceBuckets = self.getLatestBuckets(snapshot.data
        .dcp_stream_permission);
      self.metadataBuckets = self.getLatestBuckets(snapshot.data
        .read_write_permission);
      self.sourceCollections = [appModel.depcfg.source_collection];
      self.metadataCollections = [appModel.depcfg.metadata_collection];
      self.sourceScopes = [appModel.depcfg.source_scope];
      self.metadataScopes = [appModel.depcfg.metadata_scope];
      self.sourceBucket = appModel.depcfg.source_bucket;
      self.metadataBucket = appModel.depcfg.metadata_bucket;
      self.sourceScope = appModel.depcfg.source_scope;
      self.sourceCollection = appModel.depcfg.source_collection;
      self.metadataScope = appModel.depcfg.metadata_scope;
      self.metadataCollection = appModel.depcfg.metadata_collection;
      if (!self.sourceBuckets.includes(self.sourceBucket)) self.sourceBuckets
        .push(self.sourceBucket);
      if (!self.metadataBuckets.includes(self.metadataBucket)) self
        .metadataBuckets.push(self.metadataBucket);
      self.savedApps = savedApps;

      // Need to pass a deep copy or the changes will be stored locally till refresh.
      $scope.appModel = JSON.parse(JSON.stringify(appModel));

      self.isFormInvalid = function() {
        return FormValidationService.isFormInvalid(self, self.bindings,
          appModel.depcfg.source_bucket);
      };

      self.srcBindingSameBucket = function(binding) {
        return appModel.depcfg.source_bucket === binding.name;
      };

      self.validateVariableRegex = function(binding) {
        if (binding && binding.value) {
          return FormValidationService.isValidVariable(binding.value);
        }

        return true;
      };

      self.populateBuckets = function(access, index) {
        var list = snapshot.data.read_write_permission;
        if (access === "r") {
          list = snapshot.data.read_permission;
        }
        self.buckets[index] = self.getLatestBuckets(list);
        self.populateScope(self.bindings[index].name, index);
      }

      self.populateScope = function(bucketName, index) {
        var list = snapshot.data.read_write_permission;
        if (self.bindings[index].access === "r") {
          list = snapshot.data.read_permission;
        }
        var scopes = self.getScopes(bucketName, list, true);
        console.log(scopes, index);
        self.scopes[index] = scopes;
        self.populateCollections(bucketName, self.bindings[index].scope,
          index);
      };

      self.populateCollections = function(bucketName, scopeName, index) {
        var list = snapshot.data.read_write_permission;
        if (self.bindings[index].access === "r") {
          list = snapshot.data.read_permission;
        }
        var collections = self.getCollection(bucketName, scopeName, list,
          true);
        self.collections[index] = collections;
      };

      for (var binding in self.bindings) {
        self.buckets.push([]);
        self.scopes.push([]);
        self.collections.push([]);
        self.populateBuckets(self.bindings[binding].access, binding);
      }

      self.validateVariable = function(binding) {
        if (binding && binding.value) {
          return FormValidationService.isValidVariable(binding.value);
        }

        return true;
      };

      self.Initialize = function() {
        self.bindings.push({
          type: '',
          name: '',
          scope: '',
          collection: '',
          value: '',
          auth_type: 'no-auth',
          allow_cookies: true,
          access: 'r'
        });
        var index = self.bindings.length - 1;
        self.buckets.push([]);
        self.scopes.push([]);
        self.collections.push([]);
        self.populateBuckets('r', index);
        self.bindings[index].name = self.buckets[index][0];
      };

      self.Remove = function(index) {
        self.bindings.splice(index, 1);
        self.buckets.splice(index, 1);
        self.scopes.splice(index, 1);
        self.collections.splice(index, 1);
      }

      self.saveSettings = function(dismissDialog, closeDialog) {
        var config = $scope.appModel.depcfg;

        Object.assign(config, ApplicationService.convertBindingToConfig(self
          .bindings));
        self.copyNamespace(config, $scope.appModel.depcfg);

        if (JSON.stringify(appModel.depcfg) !== JSON.stringify(config)) {
          $scope.appModel.depcfg = config;

          ApplicationService.tempStore.saveAppDepcfg($scope.appModel).
          then(function(response) {
            ApplicationService.server.showWarningAlert(
              'Bindings changed. Deploy or Resume function for changes to take effect.'
            );
            appModel.depcfg = config;
          }).
          catch(function(errResponse) {
            console.error(errResponse.data);
            ApplicationService.server.showErrorAlert(
              'Changes cannot be saved. ' + errResponse.data
              .description);
          });
        }

        var settings = $scope.appModel.settings;
        if (JSON.stringify(appModel.settings) !== JSON.stringify(
            settings)) {
          ApplicationService.public.updateSettings($scope.appModel).
          then(function(response) {
              appModel.settings = settings;
              ApplicationService.server.showWarningAlert(
                'Settings changed successfully'
              );
            })
            .catch(function(errResponse) {
              ApplicationService.server.showErrorAlert(
                'Changes cannot be saved. ' + errResponse.data
                .description);
            })
        }

        closeDialog('ok');
      };

      self.cancelEdit = function(dismissDialog) {
        // TODO : Consider using appModel.clone()
        $scope.appModel = JSON.parse(JSON.stringify(appModel));
        dismissDialog('cancel');
      };

      self.copyNamespace = function(destinationConfig, sourceConfig) {
        destinationConfig["source_bucket"] = sourceConfig["source_bucket"];
        destinationConfig["source_scope"] = sourceConfig["source_scope"];
        destinationConfig["source_collection"] = sourceConfig[
          "source_collection"];
        destinationConfig["metadata_bucket"] = sourceConfig[
          "metadata_bucket"];
        destinationConfig["metadata_scope"] = sourceConfig[
          "metadata_scope"];
        destinationConfig["metadata_collection"] = sourceConfig[
          "metadata_collection"];
      };

      self.srcMetaSameBucket = function(appModel) {
        return appModel.depcfg.source_bucket === appModel.depcfg
          .metadata_bucket &&
          ((appModel.depcfg.source_scope === appModel.depcfg
            .metadata_scope) || (appModel.depcfg.source_scope == "*")) &&
          ((appModel.depcfg.source_collection === appModel.depcfg
            .metadata_collection) || (appModel.depcfg.source_scope ==
            "*"));
      };

      self.populateSourceScopes = function(bucketName) {
        self.sourceScopes = self.getScopes(bucketName, snapshot.data
          .dcp_stream_permission, true);
        if (!self.sourceScopes.includes(self.sourceScope)) self.sourceScopes
          .push(self.sourceScope);
        self.populateSourceCollections(bucketName, $scope.appModel.depcfg
          .source_scope);
      };

      self.populateSourceCollections = function(bucketName, scopeName) {
        self.sourceCollections = self.getCollection(bucketName, scopeName,
          snapshot.data.dcp_stream_permission, true);
        if (!self.sourceCollections.includes(self.sourceCollection)) self
          .sourceCollections.push(self.sourceCollection);
      };

      self.populateMetadataScopes = function(bucketName) {
        self.metadataScopes = self.getScopes(bucketName, snapshot.data
          .read_write_permission, false);
        if (!self.metadataScopes.includes(self.metadataScope)) self
          .metadataScopes.push(self.metadataScope);
        self.populateMetadataCollections(bucketName, $scope.appModel.depcfg
          .metadata_scope);
      };

      self.populateMetadataCollections = function(bucketName, scopeName) {
        self.metadataCollections = self.getCollection(bucketName, scopeName,
          snapshot.data.read_write_permission, false);
        if (!self.metadataCollections.includes(self.metadataCollection))
          self.metadataCollections.push(self.metadataCollection);
      };

      Object.assign(appModel.depcfg, ApplicationService
        .convertBindingToConfig(self
          .bindings));
    }
  ])
  // Controller for editing handler code.
  .controller('HandlerCtrl', ['$q', '$uibModal', '$timeout', '$state', '$scope',
    '$rootScope', '$stateParams', '$transitions', 'ApplicationService',
    function($q, $uibModal, $timeout, $state, $scope, $rootScope,
      $stateParams, $transitions, ApplicationService) {
      var self = this,
        isDebugOn = false,
        debugScope = $scope.$new(true),
        app = ApplicationService.local.getAppByName($stateParams.appName,
          $stateParams.function_scope);
      var startTime = 0;

      debugScope.appName = app.appname;

      ApplicationService.public.getConfig()
        .then(function(result) {
          if (!result.data.enable_debugger) {
            $rootScope.debugDisable = true;
          } else {
            $rootScope.debugDisable = !(app.settings.deployment_status &&
              app.settings.processing_status);
          }
        })
        .catch(function(err) {
          console.log(err);
        });

      var config = ace.require("ace/config");
      $scope.searchInCode = function() {
        config.loadModule("ace/ext/cb-searchbox",
          function(e) {
            if ($scope.editor) e.Search($scope.editor, !self
              .editorDisabled, true)
          });
      }

      self.handler = app.appcode;
      self.pristineHandler = app.appcode;
      self.debugToolTip =
        "Displays a URL that connects the Chrome Dev-Tools with the application handler. Code must be deployed and debugger must be enabled in the settings in order to debug";
      self.disableCancelButton = true;
      self.disableSaveButton = true;
      self.editorDisabled = app.status === "pausing" || app.status ===
        "deployed" ||
        app.status === "deploying" || app.status === "undeploying";

      $state.current.data.title = app.appname;

      $scope.aceLoaded = function(editor) {
        // Current line highlight would overlap on the nav bar in compressed mode.
        // Hence, we need to disable it.
        editor.setOption("highlightActiveLine", false);

        // Need to disable the syntax checking.
        // TODO : Figure out how to add N1QL grammar to ace editor.
        editor.getSession().setUseWorker(false);

        $scope.editor = editor;
        $scope.editor.commands.addCommand({
          name: "Search Pop Up.",
          exec: $scope.searchInCode,
          bindKey: { mac: "cmd-f", win: "ctrl-f" },
          readOnly: true
        });

        // Allow editor to load fully and add annotations
        var showAnnotations = function() {
          // compilation errors
          if (app.compilationInfo && !app.compilationInfo
            .compile_success) {
            var line = app.compilationInfo.line_number - 1,
              col = app.compilationInfo.column_number - 1;
            editor.getSession().setAnnotations([{
              row: line,
              column: 0,
              text: app.compilationInfo.description,
              type: "error"
            }]);
            return;
          }

          // insights
          if (self.editorDisabled) {
            ApplicationService.server.getInsight(app.appname, app
              .function_scope).then(
              function(insight) {
                console.log(insight);
                var annotations = [];
                self.codeInsight = {};
                Object.keys(insight.lines).forEach(function(pos) {
                  var info = insight.lines[pos];
                  var srcline = parseInt(pos);
                  var msg, type;
                  if (info.error_count > 0) {
                    msg = info.error_msg;
                    msg += "\n(errors: " + info.error_count + ")";
                    type = "warning";
                  } else if (info.last_log.length > 0) {
                    msg = info.last_log;
                    type = "info";
                  } else {
                    return;
                  }
                  self.codeInsight[srcline] = info;
                  annotations.push({
                    row: srcline - 1,
                    column: 0,
                    text: msg,
                    type: type
                  });
                });
                editor.getSession().setAnnotations(annotations);
              }).catch(function(err) {
              console.error("Error iterating insight", err);
            });
            return;
          }
        };

        setTimeout(showAnnotations, 500);

        if (self.editorDisabled) {
          var keyboardDisable = function(data, hash, keyString, keyCode,
            event) {
            var nowTime = new Date().getTime();
            if ((nowTime - startTime) > 5 *
              1e3) { // Refresh Rate of 5 seconds
              startTime = nowTime;
              ApplicationService.server.showWarningAlert(
                'The function is deployed. Please undeploy or pause the function in order to edit'
              );
            }
          };
          editor.keyBinding.addKeyboardHandler(keyboardDisable);
        }

        // Make the ace editor responsive to changes in browser dimensions.
        function resizeEditor() {
          var handlerEditor = document.getElementById('handler-editor');
          //handlerEditor.width($(window).width() * 0.85);
          if (!handlerEditor) return;
          handlerEditor.style.height = (window.innerHeight * 0.7) + "px";
        }

        window.addEventListener('resize', resizeEditor);
        resizeEditor();
      };

      $scope.aceChanged = function(e) {
        self.disableCancelButton = self.disableSaveButton = false;
        self.disableDeployButton = true;
        if (self.handler !== app.appcode) {
          self.warning = true;
        } else {
          self.warning = false;
        }
      };

      self.saveEdit = function(action) {
        app.appcode = self.handler;
        ApplicationService.public.status()
          .then(function(response) {
            response = response.data;
            var pos = response.apps.map(function(e) {
              return e.name;
            }).indexOf(app.appname);
            if (response.apps[pos].composite_status !== 'undeployed' &&
              response.apps[pos].composite_status !== 'paused') {
              app.appcode = self.pristineHandler;
              self.disableDeployButton = self.disableCancelButton = self
                .disableSaveButton = (self.handler === app.appcode);
              if (self.handler !== app.appcode) {
                self.warning = true;
              } else {
                self.warning = false;
              }

              ApplicationService.server.showErrorAlert(
                'Changes cannot be saved. Function can be edited only when it is undeployed or paused'
              );
            } else {
              var appSaved = true

              ApplicationService.tempStore.saveAppCode(app)
                .then(function(response) {
                  ApplicationService.server.showSuccessAlert(
                    'Code saved successfully!');
                  ApplicationService.server.showWarningAlert(
                    'Deploy Or Resume for changes to take effect!');

                  app.deprecatedNames = "";
                  app.overloadedNames = "";
                  response.data.info.split(";").filter(msg => msg
                    .includes("Deprecated:")).forEach(function(msg) {
                    var fnNames = JSON.parse(msg.split(":")[1]
                      .trim());
                    ApplicationService.server.showWarningAlert(
                      'Warning: The Function uses following deprecated API(s) - ' +
                      fnNames.join(", "));
                    app.deprecatedNames = fnNames;
                  });
                  response.data.info.split(";").filter(msg => msg
                    .includes("Overloaded:")).forEach(function(msg) {
                    var fnNames = JSON.parse(msg.split(":")[1]
                      .trim());
                    ApplicationService.server.showWarningAlert(
                      'Warning: The Function tries to overload following builtin API(s) - ' +
                      fnNames.join(", "));
                    app.overloadedNames = fnNames;
                  });

                  self.disableCancelButton = self.disableSaveButton =
                    true;
                  self.disableDeployButton = false;
                  self.warning = false;

                  delete app.compilationInfo;
                })
                .catch(function(errResponse) {
                  appSaved = false
                  ApplicationService.server.showErrorAlert(
                    "Changes cannot be saved. Reason: " + JSON
                    .stringify(errResponse.data.description));
                  console.error(errResponse);
                })
                .finally(function(response) {
                  if (appSaved && action ==
                    'SaveAndReturnToEventingSummary') {
                    self.warning = false;

                    $state.go('app.admin.eventing.summary');
                  }
                });
            }
          })
          .catch(function(errResponse) {
            ApplicationService.server.showErrorAlert(
              "Changes cannot be saved. Reason: " + JSON
              .stringify(errResponse.data.description));
          });
      };

      self.cancelEdit = function() {
        self.handler = app.appcode = self.pristineHandler;
        self.disableDeployButton = self.disableCancelButton = self
          .disableSaveButton = true;
        self.warning = false;

        $state.go('app.admin.eventing.summary');
      };


      self.debugApp = function() {
        ApplicationService.server.getEncryptionLevel()
          .then(function(level) {
            if (level == 'strict') {
              $rootScope.debugDisable = true;
              ApplicationService.public.updateConfig({
                enable_debugger: false
              });
              ApplicationService.server.showWarningAlert(
                `Debugger is disabled as cluster encryption level is strict.`
              );
              return;
            }
            return ApplicationService.primaryStore.getDeployedApps()
          })
          .then(function(response) {
            var appLocation = getAppLocation(app.appname, app
              .function_scope);
            if (!(appLocation in response.data)) {
              ApplicationService.server.showErrorAlert(
                `Function ${app.appname} under ${app.function_scope.bucket}:${app.function_scope.scope} may be undergoing bootstrap. Please try later.`
              );
              return;
            }
            return ApplicationService.public.getConfig();
          })
          .then(function(response) {
            if (!response.data.enable_debugger) {
              ApplicationService.server.showErrorAlert(
                'Unable to start debugger as it is disabled. Please enable it under Eventing Settings'
              );
              return;
            }
            return ApplicationService.server.getDefaultPool();
          })
          .then(function(response) {
            if (!isDebugOn) {
              debugScope.url = 'Waiting for mutation';
              debugScope.urlReceived = false;
              isDebugOn = true;

              // Starts the debugger agent.
              ApplicationService.debug.start(app.appname, app
                  .function_scope, response.data)
                .then(function(response) {
                  var responseCode = ApplicationService.status
                    .getResponseCode(response);
                  if (responseCode) {
                    var errMsg = ApplicationService.status.getErrorMsg(
                      responseCode, response.data);
                    return $q.reject(errMsg);
                  }

                  // Open the dialog to show the URL for debugging.
                  $uibModal.open({
                      template: appDebugTemplate,
                      scope: debugScope
                    }).result
                    .then(function(response) {
                      // Stop debugger agent.
                      return stopDebugger();
                    })
                    .catch(function(errResponse) {
                      return stopDebugger();
                    });

                  // Poll till we get the URL for debugging.
                  function getDebugUrl() {
                    console.error('Fetching debug url for ' + app
                      .appname);
                    ApplicationService.debug.getUrl(app.appname, app
                        .function_scope)
                      .then(function(response) {
                        var responseCode = ApplicationService.status
                          .getResponseCode(response);
                        if (responseCode) {
                          var errMsg = ApplicationService.status
                            .getErrorMsg(responseCode, response.data);
                          return $q.reject(errMsg);
                        }

                        if (isDebugOn && response.data === '') {
                          setTimeout(getDebugUrl, 1000);
                        } else {
                          debugScope.urlReceived = true;
                          let responseContent = response.data;
                          var raw = navigator.userAgent.match(
                            /Chrom(e|ium)\/([0-9]+)\./);
                          var chromeVersion = (raw ? parseInt(raw[2],
                            10) : false);
                          if (!isNaN(chromeVersion)) {
                            if (chromeVersion < 66) {
                              debugScope.url =
                                "chrome-devtools://devtools/bundled/inspector.html?experiments=true&v8only=true&ws=" +
                                responseContent["websocket"];
                            } else if (chromeVersion < 82) {
                              debugScope.url =
                                "chrome-devtools://devtools/bundled/js_app.html?experiments=true&v8only=true&ws=" +
                                responseContent["websocket"];
                            } else if (chromeVersion < 84) {
                              debugScope.url =
                                "devtools://devtools/bundled/js_app.html?ws=" +
                                responseContent["websocket"]
                            } else {
                              debugScope.url =
                                "devtools://devtools/bundled/inspector.html?ws=" +
                                responseContent["websocket"];
                            }
                          } else {
                            debugScope.url =
                              "devtools://devtools/bundled/inspector.html?ws=" +
                              responseContent["websocket"];
                          }
                        }
                      })
                      .catch(function(errResponse) {
                        console.error(
                          'Unable to get debugger URL for ' + app
                          .appname, errResponse.data);
                      });
                  }

                  getDebugUrl();
                })
                .catch(function(errResponse) {
                  ApplicationService.server.showErrorAlert(
                    'Unexpected error occurred. Please try again.');
                  console.error('Failed to start debugger',
                    errResponse);
                });
            }

            function stopDebugger() {
              return ApplicationService.debug.stop(app.appname, app
                  .function_scope)
                .then(function(response) {
                  var responseCode = ApplicationService.status
                    .getResponseCode(response);
                  if (responseCode) {
                    var errMsg = ApplicationService.status.getErrorMsg(
                      responseCode, response.data);
                    return $q.reject(errMsg);
                  }

                  console.log('debugger stopped');
                  isDebugOn = false;
                  debugScope.url = 'Waiting for mutation';
                  debugScope.urlReceived = false;
                });
            }
          })
          .catch(function(errResponse) {
            ApplicationService.server.showErrorAlert(
              'Unexpected error occurred. Please try again.');
            console.error('Unable to start debugger', errResponse);
          });
      };

      $transitions.onBefore({}, function(transition) {
        if (self.warning) {
          if (confirm(
              "Unsaved changes exist, and will be discarded if you leave this page. Are you sure?"
            )) {
            self.warning = false;
            self.handler = app.appcode = self.pristineHandler;
            return true;
          } else {
            self.warning = true;
            return false;
          }
        }
        return true;
      });

      window.onbeforeunload = function() {
        if (self.warning) {
          return "Unsaved changes exist, and will be discarded if you leave this page. Are you sure?";
        }
      };
    }
  ])
  // Controller to copy the debug URL.
  .controller('DebugCtrl', [function() {
    var self = this;

    self.copyUrl = function() {
      var element = document.getElementById("debug-url");
      element.select();
      document.execCommand('copy');
    };

    self.isChrome = function() {
      return !!window.chrome;
    }
  }])
  // Service to manage the applications.
  .factory('ApplicationService', ['$q', '$http', '$state', 'mnPoolDefault',
    'mnAlertsService',
    function($q, $http, $state, mnPoolDefault, mnAlertsService) {

      var self = this;

      var appManager = new ApplicationManager();
      var adapter = new Adapter();
      var errHandler;

      // Note : There's no trailing '/' after getErrorCodes in the URL,
      // It results in 404 page not found for 'getErrorCodes/'
      var loaderPromise = $http.get('/_p/event/getErrorCodes')
        .then(function(response) {

          errHandler = new ErrorHandler(response.data);
          return $http.get('/_p/event/getAppTempStore/');
        })
        .then(function getAppTempStore(response) {
          var responseCode = Number(response.headers(ErrorHandler
            .headerKey));
          if (responseCode) {
            var errMsg = errHandler.createErrorMsg(responseCode, response
              .data);
            // Try to resolve according to the attributes of the error.
            errHandler.tryResolve(function() {
              return $http.get('/_p/event/getAppTempStore/');
            }, getAppTempStore, errMsg, responseCode);
            return $q.reject(errMsg);
          }

          // Add apps to appManager.
          for (var app of response.data) {
            appManager.pushApp(new Application(app));
          }

          // Alpha sort the UI on the initial load and any browser refreshes
          appManager.sortApplications();
        })
        .catch(function(errResponse) {
          console.error('Failed to get the data:', errResponse);
          $state.go('app.admin.eventing.summary');
        });

      // APIs provided by the ApplicationService.
      self.funcs = {
        local: {
          deleteApp: function(appName, function_scope) {
            appManager.deleteApp(appName, function_scope);
          },
          loadApps: function() {
            return loaderPromise;
          },
          getAllApps: function() {
            return appManager.getApplications();
          },
          createApp: function(appModel) {
            appManager.createApp(appModel);
          },
          getAppByName: function(appName, function_scope) {
            return appManager.getAppByName(appName, function_scope);
          },
          saveApp: function(appModel) {
            appManager.pushApp(appModel);
          }
        },
        public: {
          status: function() {
            return $http.get('/_p/event/api/v1/status');
          },
          export: function() {
            return $http.get('/_p/event/api/v1/export');
          },
          import: function(app) {
            return $http({
              url: '/_p/event/api/v1/import',
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              },
              data: [app]
            });
          },
          getAnnotations: function() {
            return $http.get('/_p/event/getAnnotations')
          },
          getFunction: function(fname, function_scope) {
            return $http.get(encodeURI('/_p/event/api/v1/functions/' +
              fname + '?bucket=' + function_scope.bucket + '&scope=' +
              function_scope.scope));
          },
          updateSettings: function(appModel) {
            return $http({
              url: encodeURI(
                `/_p/event/api/v1/functions/${appModel.appname}/settings?bucket=${appModel.function_scope.bucket}&scope=${appModel.function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              },
              data: appModel.settings
            });
          },
          deployApp: function(appModel) {
            return $http({
              url: encodeURI(
                `/_p/event/api/v1/functions/${appModel.appname}/deploy?bucket=${appModel.function_scope.bucket}&scope=${appModel.function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              }
            });
          },
          resumeApp: function(appModel) {
            return $http({
              url: encodeURI(
                `/_p/event/api/v1/functions/${appModel.appname}/resume?bucket=${appModel.function_scope.bucket}&scope=${appModel.function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              }
            });
          },
          pauseApp: function(appModel) {
            return $http({
              url: encodeURI(
                `/_p/event/api/v1/functions/${appModel.appname}/pause?bucket=${appModel.function_scope.bucket}&scope=${appModel.function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              }
            });
          },
          undeployApp: function(appName, function_scope) {
            return $http({
              url: encodeURI(
                `/_p/event/api/v1/functions/${appName}/undeploy?bucket=${function_scope.bucket}&scope=${function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              }
            });
          },
          deleteApp: function(appName, function_scope) {
            return $http({
              url: encodeURI(
                `/_p/event/api/v1/functions/${appName}/?bucket=${function_scope.bucket}&scope=${function_scope.scope}`
              ),
              method: 'DELETE',
              mnHttp: {
                isNotForm: true
              }
            });
          },
          getConfig: function() {
            return $http.get('/_p/event/api/v1/config');
          },
          updateConfig: function(data) {
            return $http({
              url: '/_p/event/api/v1/config',
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              },
              data: data
            });
          }
        },
        tempStore: {
          getAllApps: function() {
            return $http.get('/_p/event/getAppTempStore/')
              .then(function(response) {
                // Create and return an ApplicationManager instance.
                var deployedAppsMgr = new ApplicationManager();

                for (var deployedApp of response.data) {
                  deployedAppsMgr.pushApp(new Application(deployedApp));
                }

                return deployedAppsMgr;
              });
          },
          saveApp: function(app) {
            return $http({
              url: encodeURI(
                `/_p/event/saveAppTempStore/?name=${app.appname}&bucket=${app.function_scope.bucket}&scope=${app.function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              },
              data: app
            });
          },
          saveAppCode: function(app) {
            return $http({
              url: encodeURI(
                `/_p/event/api/v1/functions/${app.appname}/appcode?bucket=${app.function_scope.bucket}&scope=${app.function_scope.scope}`
              ),
              method: 'POST',
              headers: {
                'Content-Type': 'application/javascript'
              },
              data: app.appcode
            });
          },
          saveAppDepcfg: function(app) {
            return $http({
              url: encodeURI(
                `/_p/event/api/v1/functions/${app.appname}/config?bucket=${app.function_scope.bucket}&scope=${app.function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              },
              data: app.depcfg
            });
          },
          isAppDeployed: function(appName, function_scope) {
            return $http.get('/_p/event/getAppTempStore/')
              .then(function(response) {
                // Create and return an ApplicationManager instance.
                var deployedAppsMgr = new ApplicationManager();

                for (var deployedApp of response.data) {
                  deployedAppsMgr.pushApp(new Application(deployedApp));
                }

                return deployedAppsMgr;
              })
              .then(function(deployedAppsMgr) {
                return deployedAppsMgr.getAppByName(appName,
                    function_scope).settings
                  .deployment_status;
              });
          },
          isAppPaused: function(appName, function_scope) {
            return $http.get('/_p/event/getAppTempStore/')
              .then(function(response) {
                var pausedAppsMgr = new ApplicationManager();

                for (var pausedApp of response.data) {
                  pausedAppsMgr.pushApp(new Application(pausedApp))
                }

                return pausedAppsMgr;
              })
              .then(function(pausedAppsMgr) {
                return pausedAppsMgr.getAppByName(appName,
                    function_scope).settings
                  .deployment_status &&
                  !pausedAppsMgr.getAppByName(appName, function_scope)
                  .settings
                  .processing_status;
              })
          },
          redactPWDApp: function(app) {
            if (app.depcfg && app.depcfg.curl) {
              for (var idx = 0; idx < app.depcfg.curl.length; idx++) {
                app.depcfg.curl[idx].password = PASSWORD_MASK;
                app.depcfg.curl[idx].bearer_key = PASSWORD_MASK;
              }
            }
            return app;
          }
        },
        primaryStore: {
          getDeployedApps: function() {
            return $http.get('/_p/event/getDeployedApps');
          }
        },
        debug: {
          start: function(appName, function_scope, nodesInfo) {
            return $http({
              url: encodeURI(
                `/_p/event/startDebugger/?name=${appName}&bucket=${function_scope.bucket}&scope=${function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              },
              data: nodesInfo
            });
          },
          getUrl: function(appName, function_scope) {
            return $http({
              url: encodeURI(
                `/_p/event/getDebuggerUrl/?name=${appName}&bucket=${function_scope.bucket}&scope=${function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              },
              data: {}
            });
          },
          stop: function(appName, function_scope) {
            return $http({
              url: encodeURI(
                `/_p/event/stopDebugger/?name=${appName}&bucket=${function_scope.bucket}&scope=${function_scope.scope}`
              ),
              method: 'POST',
              mnHttp: {
                isNotForm: true
              },
              headers: {
                'Content-Type': 'application/json'
              },
              data: {}
            });
          }
        },
        status: {
          isErrorCodesLoaded: function() {
            return errHandler;
          },
          getErrorMsg: function(errCode, details) {
            return errHandler.createErrorMsg(errCode, details);
          },
          getResponseCode: function(response) {
            return Number(response.headers(ErrorHandler.headerKey));
          }
        },
        server: {
          getDefaultPool: function() {
            return $http.get('/pools/default');
          },
          getEncryptionLevel: function() {
            return $http.get('/settings/security')
              .then(function(result) {
                if (result.data.clusterEncryptionLevel == undefined) {
                  return 'disabled';
                } else {
                  return result.data.clusterEncryptionLevel;
                }
              })
              .catch(function(err) {
                console.error(err);
                return 'disabled';
              });
          },
          getBucketSnapshot: function() {
            return $http.get('/_p/event/getUserInfo')
              .then(function(result) {
                console.log(result);
                return result;
              })
              .catch(function(err) {
                console.error("Unable to get snapshot: ", err);
              });
          },
          getLogFileLocation: function() {
            return $http.get('/_p/event/logFileLocation')
              .then(function(response) {
                return response.data.log_dir;
              })
              .catch(function(errResponse) {
                console.error('Unable to get logFileLocation',
                  errResponse
                  .data);
              });
          },
          getWorkerCount: function() {
            return $http.get('/_p/event/getWorkerCount');
          },
          getCpuCount: function() {
            return $http.get('/_p/event/getCpuCount');
          },
          getLatestBuckets: function() {
            // Getting the list of buckets.
            var poolDefault = mnPoolDefault.latestValue().value;
            return $http.get(poolDefault.buckets.uri)
              .then(function(response) {
                var buckets = [];
                for (var bucket of response.data) {
                  if (bucket.bucketType !== 'memcached') {
                    buckets.push(bucket.name);
                  }
                }

                return buckets;
              })
              .catch(function(errResponse) {
                console.error('Unable to load buckets from server',
                  errResponse);
              });
          },
          getBucketScopes: function(bucket) {
            var poolDefault = mnPoolDefault.latestValue().value;
            var uri = "/pools/default/buckets/" + bucket + "/scopes/"
            return $http.get(uri)
          },
          getKeyspaces: function() {

            return self.funcs.server.getLatestBuckets().then(function(
              buckets) {

              var promises = [];
              for (var bucket of buckets ? buckets : []) {
                var curPromise = self.funcs.server.getBucketScopes(
                  bucket);
                promises.push(curPromise);
              }

              var keyspaces = new Map();

              return $q.all(promises).then(function(result) {

                  for (var b = 0; b < result.length; b++) {
                    var bucketInfo = new Map()
                    for (var s = 0; s < result[b].data.scopes
                      .length; s++) {
                      var scope = result[b].data.scopes[s]
                      var collections = []
                      for (var c = 0; c < scope.collections
                        .length; c++) {
                        collections.push(scope.collections[c].name)
                      }

                      bucketInfo.set(scope.name, collections)
                    }

                    keyspaces.set(buckets[b], bucketInfo)
                  }

                  return keyspaces;
                })
                .catch(function(error) {
                  return keyspaces;
                });
            });
          },
          isEventingRunning: function() {
            return mnPoolDefault.get()
              .then(function(response) {
                // in 6.5 and later, sticky proxy allows eventing service on any node
                var nlist = mnPoolDefault.export.compat.atLeast65 ?
                  response.nodes : [response.thisNode];
                for (var ni = 0; ni < nlist.length; ni++) {
                  if (_.indexOf(nlist[ni].services, 'eventing') > -1) {
                    return true;
                  }
                }
                return false;
              }).catch(function(errResponse) {
                console.error('Unable to get server nodes',
                  errResponse);
              });
          },
          getAllEventingNodes: function() {
            return mnPoolDefault.get()
              .then(function(response) {
                return mnPoolDefault.getUrlsRunningService(response
                  .nodes,
                  'eventing');
              })
              .catch(function(errResponse) {
                console.error('Unable to get server nodes',
                  errResponse);
              });
          },
          showSuccessAlert: function(message) {
            mnAlertsService.formatAndSetAlerts(message, 'success', 4000);
          },
          showWarningAlert: function(message, timeout) {
            // Warnings stay on screen for 10s, can manually close using 'x'
            timeout = timeout == undefined ? 1000 * 10 : (timeout == -1 ?
              undefined : timeout)
            mnAlertsService.formatAndSetAlerts(message, 'warning',
              timeout);
          },
          showErrorAlert: function(message) {
            // Errors stay on screen forever, should manually close using 'x'
            // Not passing a timeout value leaves the popup forever
            //  Refer: ns_server/priv/public/ui/app/components/mn_alerts.js
            mnAlertsService.formatAndSetAlerts(message, 'error');
          },
          getAppLog: function(appname, function_scope) {
            return $http.get(encodeURI(
              `/_p/event/getAppLog?aggregate=true&name=${appname}&bucket=${function_scope.bucket}&scope=${function_scope.scope}`
            )).then(
              function(response) {
                return response.data;
              }).catch(function(response) {
              console.error("error getting app logs", response);
              return {};
            });
          },
          getInsight: function(appname, function_scope) {
            return $http.get(encodeURI(
              `/_p/event/getInsight?aggregate=true&name=${appname}&bucket=${function_scope.bucket}&scope=${function_scope.scope}`
            )).then(
              function(response) {
                var applocation = getAppLocation(appname,
                  function_scope);
                return response.data[applocation];
              }).catch(function(response) {
              console.log("error getting insight", response);
              return {};
            });
          },
          getScrapeInterval: function() {
            return $http.get("/internal/settings/metrics/").then(
              function(response) {
                return response.data.scrapeInterval ? response.data
                  .scrapeInterval : 10;
              }
            )
          }
        },
        convertBindingToConfig: function(bindings) {
          return adapter.convertBindingsToConfig(bindings);
        },
        getBindingFromConfig: function(config) {
          return adapter.convertConfigToBindings(config);
        }
      };

      return self.funcs;
    }
  ])
  // Service to validate the form in settings and create app.
  .factory('FormValidationService', [
    function() {
      var jsReservedWords = [
        'abstract',
        'await',
        'boolean',
        'break',
        'byte',
        'case',
        'catch',
        'char',
        'class',
        'const',
        'continue',
        'debugger',
        'default',
        'delete',
        'do',
        'double',
        'enum',
        'else',
        'export',
        'extends',
        'final',
        'finally',
        'float',
        'for',
        'function',
        'goto',
        'if',
        'implements',
        'import',
        'interface',
        'in',
        'instanceof',
        'int',
        'let',
        'long',
        'native',
        'new',
        'package',
        'private',
        'protected',
        'public',
        'return',
        'short',
        'static',
        'super',
        'switch',
        'synchronized',
        'this',
        'throw',
        'throws',
        'transient',
        'try',
        'typeof',
        'var',
        'void',
        'volatile',
        'while',
        'with',
        'yield'
      ];

      var n1qlReservedWords = [
        'alter',
        'build',
        'create',
        'delete',
        'drop',
        'execute',
        'explain',
        'from',
        'grant',
        'infer',
        'insert',
        'merge',
        'prepare',
        'rename',
        'select',
        'revoke',
        'update',
        'upsert'
      ];

      function isValidVariableRegex(value) {
        var re = /^[a-zA-Z_$][a-zA-Z0-9_$]*$/g;
        return value && value.match(re);
      }

      function isValidVariable(value) {
        if (value.length > 64 || value.length < 1) {
          return false;
        }

        if (!isValidVariableRegex(value)) {
          return false;
        }

        if (_.indexOf(jsReservedWords, value) !== -1) {
          return false;
        }

        if (_.indexOf(n1qlReservedWords, value.toLowerCase()) !== -1) {
          return false;
        }

        return true;
      }

      function isValidApplicationNameRegex(value) {
        var re = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/g;
        return value && value.match(re);
      }

      function isValidApplicationName(value) {
        if (value && (value.length > 100 || value.length < 1)) {
          return false;
        }

        if (!isValidApplicationNameRegex(value)) {
          return false;
        }

        return true;
      }

      function isValidHostname(str) {
        return str.indexOf("http://") == 0 || str.indexOf("https://") == 0
      }


      return {
        isValidVariable: function(value) {
          return isValidVariable(value);
        },
        isValidVariableRegex: function(value) {
          return isValidVariableRegex(value);
        },
        isValidHostname: function(value) {
          return isValidHostname(value);
        },
        isFormInvalid: function(formCtrl, bindings, sourceBucket) {
          var bindingsValid,
            bindingError,
            hostnameValid,
            hostnameError,
            constantLiteralError,
            bindingsValidList = [],
            bucketsValidList = [],
            hostnameValidList = [],
            constantLiteralInvalidList = [],
            form = formCtrl.createAppForm;

          for (var binding of bindings) {
            // binding.value == alias-name
            if (binding.value.length) {
              bindingsValid = isValidVariable(binding.value);
              bindingsValidList.push(!bindingsValid);
            } else {
              bindingsValidList.push(false);
            }
            if (bindingsValid === false) {
              bindingError = true;
            }

            if (binding.type === 'alias' && bindingsValid === true) {
              if (!binding.name || !binding.scope || !binding.collection) {
                bindingError = true;
                bucketsValidList.push(false);
              } else {
                bucketsValidList.push(true);
              }
            } else {
              bucketsValidList.push(true);
            }

            if (binding.type === 'url' && binding.hostname !== undefined &&
              binding.hostname.length) {
              hostnameValid = isValidHostname(binding.hostname);
              hostnameValidList.push(!hostnameValid);
            } else {
              hostnameValid = true;
              hostnameValidList.push(!hostnameValid);
            }
            if (hostnameValid === false) {
              hostnameError = true;
            }
            if (binding.type === "constant" && binding.literal === "") {
              constantLiteralError = true;
              constantLiteralInvalidList.push(constantLiteralError)
            } else {
              constantLiteralInvalidList.push(false)
            }
          }

          // Check whether the appname exists in the list of apps.
          if (form.appname.$viewValue && form.appname.$viewValue !== '') {
            var function_scope = {
              "bucket": form.function_bucket
                .$viewValue,
              "scope": form.function_scope.$viewValue
            };
            var app_location = getAppLocation(form.appname.$viewValue,
              function_scope);
            form.appname.$error.appExists = app_location in formCtrl
              .savedApps;
            form.appname.$error.appnameInvalid = !isValidApplicationName(
              form.appname.$viewValue);
            form.appname.$error.bindingsValidList = bindingsValidList;
            form.appname.$error.bucketsValidList = bucketsValidList;
            form.appname.$error.hostnameValidList = hostnameValidList;
            form.appname.$error.bindingsValid = bindingError;
            form.appname.$error.hostnameValid = hostnameError;
          }

          form.appname.$error.required = form.appname.$viewValue === '' ||
            form.appname.$viewValue === undefined;

          form.timer_context_size.$error.isnan = isNaN(form
            .timer_context_size.$viewValue) || (form.timer_context_size
            .$viewValue == null);
          form.dcp_stream_boundary.$error = form.dcp_stream_boundary
            .$viewValue === '';

          return form.appname.$error.required ||
            form.appname.$error.appExists ||
            form.worker_count.$error.required ||
            form.worker_count.$error.min ||
            form.worker_count.$error.max ||
            form.execution_timeout.$error.required ||
            form.execution_timeout.$error.min ||
            form.on_deploy_timeout.$error.required ||
            form.on_deploy_timeout.$error.min ||
            form.timer_context_size.$error.required ||
            form.timer_context_size.$error.min ||
            form.timer_context_size.$error.max ||
            form.timer_context_size.$error.isnan ||
            !formCtrl.functionBuckets.includes(form.function_bucket && form
              .function_bucket.$viewValue) ||
            !formCtrl.functionScopes.includes(form.function_scope && form
              .function_scope.$viewValue) ||
            !formCtrl.metadataBuckets.includes(form.metadata_bucket && form
              .metadata_bucket.$viewValue) ||
            !formCtrl.metadataScopes.includes(form.metadata_scope && form
              .metadata_scope.$viewValue) ||
            !formCtrl.metadataCollections.includes(form
              .metadata_collection && form.metadata_collection.$viewValue
            ) ||
            !formCtrl.sourceBuckets.includes(form.source_bucket && form
              .source_bucket.$viewValue) ||
            !formCtrl.sourceScopes.includes(form.source_scope && form
              .source_scope.$viewValue) ||
            !formCtrl.sourceCollections.includes(form.source_collection &&
              form.source_collection.$viewValue) ||
            form.appname.$error.appnameInvalid || bindingError ||
            hostnameError ||
            form.dcp_stream_boundary.$error || constantLiteralError;
        }
      }
    }
  ])
  // Routes for the application.
  .config(['$stateProvider',
    function($stateProvider) {
      $stateProvider
        .state('app.admin.eventing', {
          url: '/eventing',
          views: {
            'main@app.admin': {
              template: eventingTemplate
            }
          },
          data: {
            title: 'Eventing'
          }
        })
        .state('app.admin.eventing.summary', {
          url: '/summary',
          template: summaryTemplate,
          controller: 'SummaryCtrl',
          controllerAs: 'summaryCtrl',
          resolve: {
            loadApps: ['ApplicationService',
              function(ApplicationService) {
                return ApplicationService.local.loadApps();
              }
            ],
            serverNodes: ['ApplicationService',
              function(ApplicationService) {
                return ApplicationService.server.getAllEventingNodes();
              }
            ],
            isEventingRunning: ['ApplicationService',
              function(ApplicationService) {
                return ApplicationService.server.isEventingRunning();
              }
            ]
          }
        })
        .state('app.admin.eventing.handler', {
          url: '/handler/:appName/:{function_scope:json}',
          template: handlerEditorTemplate,
          resolve: {
            loadApps: ['ApplicationService',
              function(ApplicationService) {
                return ApplicationService.local.loadApps();
              }
            ]
          },
          data: {
            parent: { name: 'Eventing', link: 'app.admin.eventing.summary' }
          }
        });
    }
  ]);
