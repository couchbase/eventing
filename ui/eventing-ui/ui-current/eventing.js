angular.module('eventing', ['mnPluggableUiRegistry', 'ui.router', 'mnPoolDefault'])
    // Controller for the summary page.
    .controller('SummaryCtrl', ['$q', '$scope', '$rootScope', '$state', '$uibModal', '$timeout', '$location', 'ApplicationService', 'serverNodes', 'isEventingRunning',
        function($q, $scope, $rootScope, $state, $uibModal, $timeout, $location, ApplicationService, serverNodes, isEventingRunning) {
            var self = this;

            self.errorState = !ApplicationService.status.isErrorCodesLoaded();
            self.errorCode = 200;
            self.showErrorAlert = self.showSuccessAlert = self.showWarningAlert = false;
            self.serverNodes = serverNodes;
            self.isEventingRunning = isEventingRunning;
            self.workerCount = 0;
            self.cpuCount = 0;
            self.appList = ApplicationService.local.getAllApps();
            self.disableEditButton = false;

            // Broadcast on channel 'isEventingRunning'
            $rootScope.$broadcast('isEventingRunning', self.isEventingRunning);

            for (var app of Object.keys(self.appList)) {
                self.appList[app].uiState = 'warmup';
                self.appList[app].warnings = getWarnings(self.appList[app]);
            }

            // Poll to get the App status and reflect the same in the UI
            function deployedAppsTicker() {
                if (!self.isEventingRunning) {
                    return;
                }

                ApplicationService.public.status()
                    .then(function(response) {
                        response = response.data;
                        var appList = new Set();
                        for (var app of response.apps ? response.apps : []) {
                            if (!(app.name in self.appList)) {
                                console.error('Abnormal case : UI app list is stale');
                                continue;
                            }

                            appList.add(app.name);
                            self.appList[app.name].status = app.composite_status;
                        }
                        for (var app of Object.keys(self.appList)) {
                            if (!appList.has(app)) {
                                self.appList[app].status = 'undeployed';
                            }
                            self.appList[app].uiState = determineUIStatus(self.appList[app].status);
                            self.appList[app].warnings = getWarnings(self.appList[app]);
                        }

                    }).catch(function(errResponse) {
                        self.errorCode = errResponse && errResponse.status || 500;
                        console.error('Unable to list apps');
                    });

                setTimeout(deployedAppsTicker, 2000);

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

            deployedAppsTicker();

            self.isAppListEmpty = function() {
                return Object.keys(self.appList).length === 0;
            };

            self.openAppLog = function(appName) {
                ApplicationService.server.getAppLog(appName).then(function(log) {
                    logScope = $scope.$new(true);
                    logScope.appName = appName;
                    logScope.logMessages = [];
                    if (log && log.length > 0) {
                        logScope.logMessages = log.split(/\r?\n/);
                    }
                    $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/dialogs/app-log.html',
                        scope: logScope
                    });
                });
            };

            self.openWarnings = function(appName) {
                let scope = $scope.$new(true);
                scope.appName = appName;
                scope.warnings = self.appList[appName].warnings;
                $uibModal.open({
                    templateUrl: '../_p/ui/event/ui-current/dialogs/app-warnings.html',
                    scope: scope
                });
            };

            self.openSettings = function(appName) {
                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/fragments/app-settings.html',
                        controller: 'SettingsCtrl',
                        controllerAs: 'formCtrl',
                        resolve: {
                            appName: [function() {
                                return appName;
                            }],
                            bucketsResolve: ['ApplicationService',
                                function(ApplicationService) {
                                    // Getting the list of buckets from server.
                                    return ApplicationService.server.getLatestBuckets();
                                }
                            ],
                            savedApps: ['ApplicationService',
                                function(ApplicationService) {
                                    return ApplicationService.tempStore.getAllApps();
                                }
                            ],
                            isAppDeployed: ['ApplicationService',
                                function(ApplicationService) {
                                    return ApplicationService.tempStore.isAppDeployed(appName)
                                        .then(function(isDeployed) {
                                            return isDeployed;
                                        })
                                        .catch(function(errResponse) {
                                            console.error('Unable to get deployed apps list', errResponse);
                                        });
                                }
                            ],
                            isAppPaused: ['ApplicationService',
                                function(ApplicationService) {
                                    return ApplicationService.tempStore.isAppPaused(appName)
                                        .then(function(isPaused) {
                                            return isPaused;
                                        })
                                        .catch(function(errResponse) {
                                            console.error('Unable to get function status', errResponse);
                                        });
                                }
                            ],
                            logFileLocation: ['ApplicationService',
                                function(ApplicationService) {
                                    return ApplicationService.server.getLogFileLocation();
                                }
                            ]
                        }
                    }).result
                    .catch(function(errResponse) {
                        console.log(errResponse);
                    });
            };

            self.toggleDeployment = function(app) {
                var deploymentScope = $scope.$new(true);
                deploymentScope.appName = app.appname;
                deploymentScope.actionTitle = app.status === 'deployed' || app.status === 'paused' ? 'Undeploy' : 'Deploy';
                deploymentScope.action = app.status === 'deployed' || app.status === 'paused' ? 'undeploy' : 'deploy';
                deploymentScope.cleanupTimers = false;

                if (app.status === 'deployed' || app.status === 'paused') {
                    undeployApp(app, deploymentScope);
                } else {
                    changeState('deploy', app, deploymentScope);
                }
            };

            self.toggleProcessing = function(app) {
                var processingScope = $scope.$new(true);
                processingScope.appName = app.appname;
                processingScope.actionTitle = app.status === 'paused' ? 'Resume' : 'Pause';
                processingScope.action = app.status === 'paused' ? 'resume' : 'pause';
                processingScope.cleanupTimers = false;

                if (app.status === 'paused') {
                    changeState('resume', app, processingScope);
                } else {
                    changeState('pause', app, processingScope);
                }
            };

            function changeState(operation, app, scope) {
                var appClone = app.clone();
                scope.settings = {};
                scope.settings.cleanupTimers = false;
                scope.settings.changeFeedBoundary = 'everything';

                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/dialogs/app-actions.html',
                        scope: scope
                    }).result
                    .then(function(response) {
                        return ApplicationService.primaryStore.getDeployedApps();
                    })
                    .then(function(response) {
                        switch (operation) {
                            case 'deploy':
                                if (appClone.appname in response.data) {
                                    return $q.reject({
                                        data: {
                                            runtime_info: `${appClone.appname} is being undeployed. Please try later.`
                                        }
                                    });
                                }
                                appClone.settings.dcp_stream_boundary = scope.settings.changeFeedBoundary;
                                break;
                            case 'pause':
                                if (!(appClone.appname in response.data)) {
                                    return $q.reject({
                                        data: {
                                            runtime_info: `${appClone.appname} isn't currently deployed. Only deployed function can be paused.`
                                        }
                                    });
                                }
                                appClone.settings.dcp_stream_boundary = scope.settings.changeFeedBoundary;
                                break;
                            case 'resume':
                                if (!(appClone.appname in response.data)) {
                                    return $q.reject({
                                        data: {
                                            runtime_info: `${appClone.appname} isn't currently deployed. Only deployed function can be resumed.`
                                        }
                                    });
                                }
                                appClone.settings.dcp_stream_boundary = "from_prior";
                                break;
                        }

                        switch (operation) {
                            case 'deploy':
                            case 'resume':
                                appClone.settings.deployment_status = true;
                                appClone.settings.processing_status = true;
                                return ApplicationService.public.deployApp(appClone);
                            case 'pause':
                                appClone.settings.deployment_status = true;
                                appClone.settings.processing_status = false;
                                return ApplicationService.public.updateSettings(appClone);
                        }
                    })
                    .then(function(response) {
                        var responseCode = ApplicationService.status.getResponseCode(response);
                        if (responseCode) {
                            return $q.reject(ApplicationService.status.getErrorMsg(responseCode, response.data));
                        }

                        app.settings.cleanup_timers = appClone.settings.cleanup_timers;
                        app.settings.deployment_status = appClone.settings.deployment_status;
                        app.settings.processing_status = appClone.settings.processing_status;

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
                            `${app.appname} will ${operation} ${warnings?'with warnings':''}`);
                    })
                    .catch(function(errResponse) {
                        if (errResponse.data && (errResponse.data.name === 'ERR_HANDLER_COMPILATION')) {
                            var info = errResponse.data.runtime_info.info;
                            app.compilationInfo = info;
                            ApplicationService.server.showErrorAlert(`${operation} failed: Syntax error (${info.line_number}, ${info.column_number}) - ${info.description}`);
                        } else if (errResponse.data && (errResponse.data.name === 'ERR_CLUSTER_VERSION')) {
                            var data = errResponse.data;
                            ApplicationService.server.showErrorAlert(`${operation} failed: ${data.description} - ${data.runtime_info.info}`);
                        } else {
                            var info = errResponse.data.runtime_info;
                            ApplicationService.server.showErrorAlert(`${operation} failed: ` + JSON.stringify(info));
                        }

                        self.disableEditButton = false;
                        console.error(errResponse);
                    });
            }

            function undeployApp(app, scope) {
                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/dialogs/app-actions.html',
                        scope: scope
                    }).result
                    .then(function(response) {
                        return ApplicationService.primaryStore.getDeployedApps();
                    })
                    .then(function(response) {
                        // Check if the app is deployed completely before trying to undeploy.
                        if (!(app.appname in response.data)) {
                            ApplicationService.server.showErrorAlert(`Function "${app.appname}" may be undergoing bootstrap. Please try later.`);
                            return $q.reject(`Unable to undeploy "${app.appname}". Possibly, bootstrap in progress`);
                        }

                        return ApplicationService.public.undeployApp(app.appname);
                    })
                    .then(function(response) {
                        var responseCode = ApplicationService.status.getResponseCode(response);
                        if (responseCode) {
                            return $q.reject(ApplicationService.status.getErrorMsg(responseCode, response.data));
                        }

                        console.log(response.data);
                        app.settings.deployment_status = false;
                        app.settings.processing_status = false;
                        ApplicationService.server.showSuccessAlert(`${app.appname} will be undeployed`);
                    })
                    .catch(function(errResponse) {
                        ApplicationService.server.showErrorAlert(`Undeploy failed due to "${errResponse.data.description}"`);
                        console.error(errResponse);
                    });
            }

            // Callback to export the app.
            self.exportApp = function(appName) {
                ApplicationService.public.export()
                    .then(function(apps) {
                        var app = apps.data.find(function(app) {
                            return app.appname === appName;
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
                        ApplicationService.server.showErrorAlert(`Failed to export the function due to "${errResponse.data.description}"`);
                        console.error(errResponse);
                    });
            };

            // Callback for deleting application.
            self.deleteApp = function(appName) {
                $scope.appName = appName;
                $scope.actionTitle = 'Delete';
                $scope.action = 'delete';

                // Open dialog to confirm delete.
                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/dialogs/app-actions.html',
                        scope: $scope
                    }).result
                    .then(function(response) {
                        return ApplicationService.tempStore.deleteApp(appName);
                    })
                    .then(function(response) {
                        var responseCode = ApplicationService.status.getResponseCode(response);
                        if (responseCode) {
                            return $q.reject(ApplicationService.status.getErrorMsg(responseCode, response.data));
                        }

                        return ApplicationService.primaryStore.deleteApp(appName);
                    })
                    .then(function(response) {
                        // Delete the local copy of the app in the browser
                        ApplicationService.local.deleteApp(appName);
                        ApplicationService.server.showSuccessAlert(`${appName} deleted successfully!`);
                    })
                    .catch(function(errResponse) {
                        ApplicationService.server.showErrorAlert(`Delete failed due to "${errResponse.data.description}"`);
                        console.error(errResponse);
                    });
            };
        }
    ])
    // Controller for the buttons in header.
    .controller('HeaderCtrl', ['$q', '$scope', '$uibModal', '$state', 'mnPoolDefault', 'ApplicationService',
        function($q, $scope, $uibModal, $state, mnPoolDefault, ApplicationService) {
            var self = this;
            self.isEventingRunning = true;

            // Subscribe to channel 'isEventingRunning' from SummaryCtrl
            $scope.$on('isEventingRunning', function(event, args) {
                self.isEventingRunning = args;
            });

            function createApp(creationScope) {
                // Open the settings fragment as create dialog.
                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/fragments/app-settings.html',
                        scope: creationScope,
                        controller: 'CreateCtrl',
                        controllerAs: 'formCtrl',
                        resolve: {
                            bucketsResolve: ['ApplicationService',
                                function(ApplicationService) {
                                    // Getting the list of buckets from server.
                                    return ApplicationService.server.getLatestBuckets();
                                }
                            ],
                            savedApps: ['ApplicationService',
                                function(ApplicationService) {
                                    return ApplicationService.tempStore.getAllApps();
                                }
                            ],
                            logFileLocation: ['ApplicationService',
                                function(ApplicationService) {
                                    return ApplicationService.server.getLogFileLocation();
                                }
                            ]
                        }
                    }).result
                    .then(function(repsonse) { // Upon continue.
                        Object.assign(creationScope.appModel.depcfg, ApplicationService.convertBindingToConfig(creationScope.bindings));
                        creationScope.appModel.fillWithMissingDefaults();

                        // When we import the application, we want it to be in
                        // disabled and undeployed state.
                        creationScope.appModel.settings.processing_status = false;
                        creationScope.appModel.settings.deployment_status = false;

                        // Deadline timeout must be greater and execution timeout.
                        if (creationScope.appModel.settings.hasOwnProperty('execution_timeout')) {
                            creationScope.appModel.settings.deadline_timeout = creationScope.appModel.settings.execution_timeout + 2;
                        }

                        ApplicationService.local.createApp(creationScope.appModel);
                        return $state.transitionTo('app.admin.eventing.handler', {
                            appName: creationScope.appModel.appname,
                        }, {
                            // Explained in detail - https://github.com/angular-ui/ui-router/issues/3196
                            reload: true
                        });
                    })
                    .then(function(response) {
                        return ApplicationService.public.import(creationScope.appModel);
                    })
                    .then(function(response) {
                        var responseCode = ApplicationService.status.getResponseCode(response);
                        if (responseCode) {
                            return $q.reject(ApplicationService.status.getErrorMsg(responseCode, response.data));
                        }
                    })
                    .catch(function(errResponse) { // Upon cancel.
                        console.error(errResponse);
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
                        templateUrl: '../_p/ui/event/ui-current/fragments/eventing-settings.html',
                        controller: 'EventingSettingsCtrl',
                        controllerAs: 'ctrl',
                        resolve: {
                            config: ['ApplicationService',
                                function(ApplicationService) {
                                    return ApplicationService.public.getConfig();
                                }
                            ]
                        }
                    }).result
                    .catch(function(errResponse) {
                        console.error(errResponse);
                    });
            };

            // Callback for importing application.
            self.importConfig = function() {
                function handleFileSelect(evt) {
                    var file = evt.target.files;
                    if (file[0].type != 'application/json') {
                        ApplicationService.server.showErrorAlert('Imported file format is not JSON, please verify the file being imported');
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
                            scope.bindings = ApplicationService.getBindingFromConfig(app.depcfg);
                            if (!scope.bindings.length) {
                                // Add a sample row of bindings.
                                scope.bindings.push({
                                    type: '',
                                    name: '',
                                    value: '',
                                    access: 'r'
                                });
                            }
                            if (!scope.appModel.settings.language_compatibility) {
                                scope.appModel.settings.language_compatibility = '6.0.0';
                            }
                            createApp(scope);
                        } catch (error) {
                            ApplicationService.server.showErrorAlert('The imported JSON file is not supported. Please check the format.');
                            console.error('Failed to load config:', error);
                        }
                    };

                    reader.readAsText(this.files[0]);
                }

                var loadConfigElement = $("#loadConfig")[0];
                loadConfigElement.value = null;
                loadConfigElement.addEventListener('change', handleFileSelect, false);
                loadConfigElement.click();
            };
        }
    ])
    .controller('EventingSettingsCtrl', ['$scope', '$rootScope', '$stateParams', 'ApplicationService', 'config',
        function($scope, $rootScope, $stateParams, ApplicationService, config) {
            var self = this;
            config = config.data;
            self.enableDebugger = config.enable_debugger;

            self.saveSettings = function(closeDialog) {
                ApplicationService.public.updateConfig({
                    enable_debugger: self.enableDebugger
                });
                if ($stateParams.appName) {
                    app = ApplicationService.local.getAppByName($stateParams.appName);
                    $rootScope.debugDisable = !(app.settings.deployment_status && app.settings.processing_status) || !self.enableDebugger;
                    closeDialog('ok');
                } else {
                    closeDialog('ok');
                }
            };
        }
    ])
    // Controller for creating an application.
    .controller('CreateCtrl', ['$scope', 'FormValidationService', 'bucketsResolve', 'savedApps', 'logFileLocation',
        function($scope, FormValidationService, bucketsResolve, savedApps, logFileLocation) {
            var self = this;
            self.isDialog = true;

            self.sourceBuckets = bucketsResolve;
            self.logFileLocation = logFileLocation;
            self.metadataBuckets = bucketsResolve.reverse();
            self.savedApps = savedApps.getApplications();

            self.bindings = [];
            if (($scope.bindings.length > 0) && ($scope.bindings[0].name === '')) {
                $scope.bindings[0].name = bucketsResolve[0];
            }

            // Checks whether source and metadata buckets are the same.
            self.srcMetaSameBucket = function(appModel) {
                return appModel.depcfg.source_bucket === appModel.depcfg.metadata_bucket;
            };

            self.srcBindingSameBucket = function(appModel, binding) {
                return appModel.depcfg.source_bucket === binding.name;
            };

            self.executionTimeoutCheck = function(appModel) {
                return appModel.settings.execution_timeout > 60;
            };

            self.isFormInvalid = function() {
                return FormValidationService.isFormInvalid(self, $scope.bindings, $scope.formCtrl.createAppForm.source_bucket.$viewValue);
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
        }
    ])
    // Controller for settings.
    .controller('SettingsCtrl', ['$q', '$timeout', '$scope', 'ApplicationService', 'FormValidationService',
        'appName', 'bucketsResolve', 'savedApps', 'isAppDeployed', 'isAppPaused', 'logFileLocation',
        function($q, $timeout, $scope, ApplicationService, FormValidationService,
            appName, bucketsResolve, savedApps, isAppDeployed, isAppPaused, logFileLocation) {
            var self = this,
                appModel = ApplicationService.local.getAppByName(appName);

            self.isDialog = false;
            self.showSuccessAlert = false;
            self.showWarningAlert = false;
            self.isAppDeployed = isAppDeployed;
            self.isAppPaused = isAppPaused;
            self.logFileLocation = logFileLocation;
            self.sourceAndBindingSame = false;

            // Need to initialize buckets if they are empty,
            // otherwise self.saveSettings() would compare 'null' with '[]'.
            appModel.depcfg.buckets = appModel.depcfg.buckets ? appModel.depcfg.buckets : [];
            self.bindings = ApplicationService.getBindingFromConfig(appModel.depcfg);

            // TODO : The following two lines may not be needed as we don't allow the user to edit
            //			the source and metadata buckets in the settings page.

            self.sourceBuckets = bucketsResolve;
            self.metadataBuckets = bucketsResolve.reverse();
            self.sourceBucket = appModel.depcfg.source_bucket;
            self.metadataBucket = appModel.depcfg.metadata_bucket;
            self.savedApps = savedApps;

            // Need to pass a deep copy or the changes will be stored locally till refresh.
            $scope.appModel = JSON.parse(JSON.stringify(appModel));

            self.isFormInvalid = function() {
                return FormValidationService.isFormInvalid(self, self.bindings, appModel.depcfg.source_bucket);
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

            self.validateVariable = function(binding) {
                if (binding && binding.value) {
                    return FormValidationService.isValidVariable(binding.value);
                }

                return true;
            };

            self.saveSettings = function(dismissDialog, closeDialog) {
                var config = JSON.parse(JSON.stringify(appModel.depcfg));
                Object.assign(config, ApplicationService.convertBindingToConfig(self.bindings));

                if (JSON.stringify(appModel.depcfg) !== JSON.stringify(config)) {
                    $scope.appModel.depcfg = config;
                    ApplicationService.server.showWarningAlert('Bindings changed. Deploy for changes to take effect.');
                }

                // Deadline timeout must be greater than execution timeout.
                if ($scope.appModel.settings.hasOwnProperty('execution_timeout')) {
                    $scope.appModel.settings.deadline_timeout = $scope.appModel.settings.execution_timeout + 2;
                }

                // Update local changes.
                appModel.settings = $scope.appModel.settings;
                appModel.depcfg = config;

                ApplicationService.tempStore.isAppDeployed(appName)
                    .then(function(isDeployed) {
                        ApplicationService.tempStore.isAppPaused(appName)
                            .then(function(isPaused) {
                                if (isDeployed && isPaused) {
                                    return ApplicationService.tempStore.saveApp($scope.appModel);
                                } else if (isDeployed) {
                                    // deleting the dcp_stream_boundary as it is not allowed to change for a deployed app
                                    delete $scope.appModel.settings.dcp_stream_boundary;
                                    return ApplicationService.public.updateSettings($scope.appModel);
                                } else {
                                    return ApplicationService.tempStore.saveApp($scope.appModel);
                                }
                            })
                            .catch(function(errResponse) {
                                console.error('Failed to get function status', errResponse);
                            })
                    })
                    .catch(function(errResponse) {
                        console.error('Unable to get deployed apps list', errResponse);
                    });

                closeDialog('ok');
            };

            self.cancelEdit = function(dismissDialog) {
                // TODO : Consider using appModel.clone()
                $scope.appModel = JSON.parse(JSON.stringify(appModel));
                $scope.appModel.settings.cleanup_timers = String(appModel.settings.cleanup_timers);
                dismissDialog('cancel');
            };
        }
    ])
    // Controller for editing handler code.
    .controller('HandlerCtrl', ['$q', '$uibModal', '$timeout', '$state', '$scope', '$rootScope', '$stateParams', '$transitions', 'ApplicationService',
        function($q, $uibModal, $timeout, $state, $scope, $rootScope, $stateParams, $transitions, ApplicationService) {
            var self = this,
                isDebugOn = false,
                debugScope = $scope.$new(true),
                app = ApplicationService.local.getAppByName($stateParams.appName);

            debugScope.appName = app.appname;

            ApplicationService.public.getConfig()
                .then(function(result) {
                    if (!result.data.enable_debugger) {
                        $rootScope.debugDisable = true;
                    } else {
                        $rootScope.debugDisable = !(app.settings.deployment_status && app.settings.processing_status);
                    }
                })
                .catch(function(err) {
                    console.log(err);
                });

            self.handler = app.appcode;
            self.pristineHandler = app.appcode;
            self.debugToolTip = "Displays a URL that connects the Chrome Dev-Tools with the application handler. Code must be deployed and debugger must be enabled in the settings in order to debug";
            self.disableCancelButton = true;
            self.disableSaveButton = true;
            self.editorDisabled = app.settings.deployment_status && app.settings.processing_status;

            $state.current.data.title = app.appname;

            $scope.aceLoaded = function(editor) {
                var markers = [],
                    Range = ace.require('ace/range').Range;
                // Current line highlight would overlap on the nav bar in compressed mode.
                // Hence, we need to disable it.
                editor.setOption("highlightActiveLine", false);

                // Need to disable the syntax checking.
                // TODO : Figure out how to add N1QL grammar to ace editor.
                editor.getSession().setUseWorker(false);

                // Allow editor to load fully and add annotations
                var showAnnotations = function() {
                    // compilation errors
                    if (app.compilationInfo && !app.compilationInfo.compile_success) {
                        var line = app.compilationInfo.line_number - 1,
                            col = app.compilationInfo.column_number - 1;
                        var markerId = editor.session.addMarker(new Range(line, 0, line, Infinity), "", "text");
                        markers.push(markerId);
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
                        ApplicationService.server.getInsight(app.appname).then(function(insight) {
                            console.log(insight);
                            var annotations = [];
                            self.codeInsight = {};
                            Object.keys(insight.lines).forEach(function(pos) {
                                var info = insight.lines[pos];
                                var srcline = parseInt(pos) - 1; // ace is 0 indexed, v8 is 1 indexed
                                var msg, type;
                                if (info.error_count > 0) {
                                    msg = info.error_msg;
                                    msg += "\n(errors: " + info.error_count + ")";
                                    type = "error";
                                } else if (info.last_log.length > 0) {
                                    msg = info.last_log;
                                    type = "info";
                                } else {
                                    return;
                                }
                                self.codeInsight[srcline] = info;
                                var id = editor.session.addMarker(new Range(srcline - 1, 0, srcline - 1, Infinity), "", "text");
                                markers.push(id);
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
                    var keyboardDisable = function(data, hash, keyString, keyCode, event) {
                        ApplicationService.server.showWarningAlert('The function is deployed. Please undeploy or pause the function in order to edit');
                    };
                    editor.keyBinding.addKeyboardHandler(keyboardDisable);
                }

                // Make the ace editor responsive to changes in browser dimensions.
                function resizeEditor() {
                    var handlerEditor = $('#handler-editor');
                    //handlerEditor.width($(window).width() * 0.85);
                    handlerEditor.height($(window).height() * 0.7);
                }

                $(window).resize(resizeEditor);
                resizeEditor();

                self.aceEditor = {
                    clearMarkersAndAnnotations: function() {
                        var session = editor.getSession();
                        for (var m of markers) {
                            session.removeMarker(m);
                        }

                        markers = [];
                    }
                };
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

            self.saveEdit = function() {
                app.appcode = self.handler;
                ApplicationService.tempStore.saveApp(app)
                    .then(function(response) {
                        ApplicationService.server.showSuccessAlert('Code saved successfully!');
                        ApplicationService.server.showWarningAlert('Deploy for changes to take effect!');

                        self.disableCancelButton = self.disableSaveButton = true;
                        self.disableDeployButton = false;
                        self.warning = false;

                        // Optimistic that the user has fixed the errors
                        // If not the errors will anyway show up when he deploys again
                        self.aceEditor.clearMarkersAndAnnotations();
                        delete app.compilationInfo;
                        console.log(response.data);
                    })
                    .catch(function(errResponse) {
                        console.error(errResponse);
                    });
            };

            self.cancelEdit = function() {
                self.handler = app.appcode = self.pristineHandler;
                self.disableDeployButton = self.disableCancelButton = self.disableSaveButton = true;
                self.warning = false;

                $state.go('app.admin.eventing.summary');
            };


            self.debugApp = function() {
                ApplicationService.primaryStore.getDeployedApps()
                    .then(function(response) {
                        if (!(app.appname in response.data)) {
                            ApplicationService.server.showErrorAlert(`Function ${app.appname} may be undergoing bootstrap. Please try later.`);
                            return;
                        }
                        return ApplicationService.public.getConfig();
                    })
                    .then(function(response) {
                        if (!response.data.enable_debugger) {
                            ApplicationService.server.showErrorAlert('Unable to start debugger as it is disabled. Please enable it under Eventing Settings');
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
                            ApplicationService.debug.start(app.appname, response.data)
                                .then(function(response) {
                                    var responseCode = ApplicationService.status.getResponseCode(response);
                                    if (responseCode) {
                                        var errMsg = ApplicationService.status.getErrorMsg(responseCode, response.data);
                                        return $q.reject(errMsg);
                                    }

                                    // Open the dialog to show the URL for debugging.
                                    $uibModal.open({
                                            templateUrl: '../_p/ui/event/ui-current/dialogs/app-debug.html',
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
                                        console.log('Fetching debug url for ' + app.appname);
                                        ApplicationService.debug.getUrl(app.appname)
                                            .then(function(response) {
                                                var responseCode = ApplicationService.status.getResponseCode(response);
                                                if (responseCode) {
                                                    var errMsg = ApplicationService.status.getErrorMsg(responseCode, response.data);
                                                    return $q.reject(errMsg);
                                                }

                                                if (isDebugOn && response.data === '') {
                                                    setTimeout(getDebugUrl, 1000);
                                                } else {
                                                    debugScope.urlReceived = true;
                                                    debugScope.url = response.data;
                                                }
                                            })
                                            .catch(function(errResponse) {
                                                console.error('Unable to get debugger URL for ' + app.appname, errResponse.data);
                                            });
                                    }

                                    getDebugUrl();
                                })
                                .catch(function(errResponse) {
                                    ApplicationService.server.showErrorAlert('Unexpected error occurred. Please try again.');
                                    console.error('Failed to start debugger', errResponse);
                                });
                        }

                        function stopDebugger() {
                            return ApplicationService.debug.stop(app.appname)
                                .then(function(response) {
                                    var responseCode = ApplicationService.status.getResponseCode(response);
                                    if (responseCode) {
                                        var errMsg = ApplicationService.status.getErrorMsg(responseCode, response.data);
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
                        ApplicationService.server.showErrorAlert('Unexpected error occurred. Please try again.');
                        console.error('Unable to start debugger', errResponse);
                    });
            };

            $transitions.onBefore({}, function(transition) {
                if (self.warning) {
                    if (confirm("Unsaved changes exist, and will be discarded if you leave this page. Are you sure?")) {
                        self.warning = false;
                        return true;
                    } else {
                        self.warning = true;
                        return false;
                    }
                }
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
            $('#debug-url').select();
            document.execCommand('copy');
        };

        self.isChrome = function() {
            return !!window.chrome;
        }
    }])
    // Service to manage the applications.
    .factory('ApplicationService', ['$q', '$http', '$state', 'mnPoolDefault', 'mnAlertsService',
        function($q, $http, $state, mnPoolDefault, mnAlertsService) {
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
                    var responseCode = Number(response.headers(ErrorHandler.headerKey));
                    if (responseCode) {
                        var errMsg = errHandler.createErrorMsg(responseCode, response.data);
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
                })
                .catch(function(errResponse) {
                    console.error('Failed to get the data:', errResponse);
                    $state.go('app.admin.eventing.summary');
                });

            // APIs provided by the ApplicationService.
            return {
                local: {
                    deleteApp: function(appName) {
                        appManager.deleteApp(appName);
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
                    getAppByName: function(appName) {
                        return appManager.getAppByName(appName);
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
                    updateSettings: function(appModel) {
                        return $http({
                            url: `/_p/event/api/v1/functions/${appModel.appname}/settings`,
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
                            url: `/_p/event/api/v1/functions/${appModel.appname}`,
                            method: 'POST',
                            mnHttp: {
                                isNotForm: true
                            },
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            data: appModel
                        });
                    },
                    undeployApp: function(appName) {
                        var settings = {
                            deployment_status: false,
                            processing_status: false
                        };
                        return $http({
                            url: `/_p/event/api/v1/functions/${appName}/settings`,
                            method: 'POST',
                            mnHttp: {
                                isNotForm: true
                            },
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            data: settings
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
                            url: '/_p/event/saveAppTempStore/?name=' + app.appname,
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
                    isAppDeployed: function(appName) {
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
                                return deployedAppsMgr.getAppByName(appName).settings.deployment_status;
                            });
                    },
                    isAppPaused: function(appName) {
                        return $http.get('/_p/event/getAppTempStore/')
                            .then(function(response) {
                                var pausedAppsMgr = new ApplicationManager();

                                for (var pausedApp of response.data) {
                                    pausedAppsMgr.pushApp(new Application(pausedApp))
                                }

                                return pausedAppsMgr;
                            })
                            .then(function(pausedAppsMgr) {
                                return pausedAppsMgr.getAppByName(appName).settings.deployment_status &&
                                    !pausedAppsMgr.getAppByName(appName).settings.processing_status;
                            })
                    },
                    deleteApp: function(appName) {
                        return $http.get('/_p/event/deleteAppTempStore/?name=' + appName);
                    }
                },
                primaryStore: {
                    deleteApp: function(appName) {
                        return $http.get('/_p/event/deleteApplication/?name=' + appName);
                    },
                    getDeployedApps: function() {
                        return $http.get('/_p/event/getDeployedApps');
                    }
                },
                debug: {
                    start: function(appName, nodesInfo) {
                        return $http({
                            url: '/_p/event/startDebugger/?name=' + appName,
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
                    getUrl: function(appName) {
                        return $http({
                            url: '/_p/event/getDebuggerUrl/?name=' + appName,
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
                    stop: function(appName) {
                        return $http({
                            url: '/_p/event/stopDebugger/?name=' + appName,
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
                    getLogFileLocation: function() {
                        return $http.get('/_p/event/logFileLocation')
                            .then(function(response) {
                                return response.data.log_dir;
                            })
                            .catch(function(errResponse) {
                                console.error('Unable to get logFileLocation', errResponse.data);
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
                                console.error('Unable to load buckets from server', errResponse);
                            });
                    },
                    isEventingRunning: function() {
                        return mnPoolDefault.get()
                            .then(function(response) {
                                // in 6.5 and later, sticky proxy allows eventing service on any node
                                var nlist = mnPoolDefault.export.compat.atLeast65 ? response.nodes : [response.thisNode];
                                for (var ni = 0; ni < nlist.length; ni++) {
                                    if (_.indexOf(nlist[ni].services, 'eventing') > -1) {
                                        return true;
                                    }
                                }
                                return false;
                            }).catch(function(errResponse) {
                                console.error('Unable to get server nodes', errResponse);
                            });
                    },
                    getAllEventingNodes: function() {
                        return mnPoolDefault.get()
                            .then(function(response) {
                                return mnPoolDefault.getUrlsRunningService(response.nodes, 'eventing');
                            })
                            .catch(function(errResponse) {
                                console.error('Unable to get server nodes', errResponse);
                            });
                    },
                    showSuccessAlert: function(message) {
                        mnAlertsService.formatAndSetAlerts(message, 'success', 4000);
                    },
                    showWarningAlert: function(message) {
                        mnAlertsService.formatAndSetAlerts(message, 'warning', 4000);
                    },
                    showErrorAlert: function(message) {
                        mnAlertsService.formatAndSetAlerts(message, 'error', 4000);
                    },
                    getAppLog: function(appname) {
                        return $http.get('/_p/event/getAppLog?aggregate=true&name=' + appname).then(
                            function(response) {
                                return response.data;
                            }).catch(function(response) {
                            console.error("error getting app logs", response);
                            return {};
                        });
                    },
                    getInsight: function(appname) {
                        return $http.get('/_p/event/getInsight?aggregate=true&name=' + appname).then(
                            function(response) {
                                return response.data[appname];
                            }).catch(function(response) {
                            console.log("error getting insight", response);
                            return {};
                        });
                    }
                },
                convertBindingToConfig: function(bindings) {
                    return adapter.convertBindingsToConfig(bindings);
                },
                getBindingFromConfig: function(config) {
                    return adapter.convertConfigToBindings(config);
                }
            };
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

            function isValidApplicationName(value) {
                var re = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/g;
                return value && value.match(re);
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
                        bindingsValidList = []
                    hostnameValidList = []
                    form = formCtrl.createAppForm;

                    for (var binding of bindings) {
                        if (binding.value.length) {
                            bindingsValid = isValidVariable(binding.value);
                            bindingsValidList.push(!bindingsValid);
                        }
                        if (bindingsValid === false) {
                            bindingError = true;
                        }
                        if (binding.type === 'url' && binding.hostname !== undefined && binding.hostname.length) {
                            hostnameValid = isValidHostname(binding.hostname);
                            hostnameValidList.push(!hostnameValid);
                        } else {
                            hostnameValid = true;
                            hostnameValidList.push(!hostnameValid);
                        }
                        if (hostnameValid === false) {
                            hostnameError = true;
                        }
                    }

                    // Check whether the appname exists in the list of apps.
                    if (form.appname.$viewValue && form.appname.$viewValue !== '') {
                        form.appname.$error.appExists = form.appname.$viewValue in formCtrl.savedApps;
                        form.appname.$error.appnameInvalid = !isValidApplicationName(form.appname.$viewValue);
                        form.appname.$error.bindingsValidList = bindingsValidList;
                        form.appname.$error.hostnameValidList = hostnameValidList;
                        form.appname.$error.bindingsValid = bindingError;
                        form.appname.$error.hostnameValid = hostnameError;
                    }

                    form.appname.$error.required = form.appname.$viewValue === '' ||
                        form.appname.$viewValue === undefined;

                    return form.appname.$error.required ||
                        form.appname.$error.appExists ||
                        form.worker_count.$error.required ||
                        form.worker_count.$error.min ||
                        form.worker_count.$error.max ||
                        form.execution_timeout.$error.required ||
                        form.execution_timeout.$error.min ||
                        formCtrl.sourceBuckets.indexOf(form.source_bucket.$viewValue) === -1 ||
                        formCtrl.metadataBuckets.indexOf(form.metadata_bucket.$viewValue) === -1 ||
                        form.appname.$error.appnameInvalid || bindingError || hostnameError;
                }
            }
        }
    ])
    // Routes for the application.
    .config(['$stateProvider', '$urlRouterProvider', 'mnPluggableUiRegistryProvider', 'mnPermissionsProvider',
        function($stateProvider, $urlRouterProvider, mnPluggableUiRegistryProvider, mnPermissionsProvider) {
            $urlRouterProvider.otherwise('/eventing');
            $stateProvider
                .state('app.admin.eventing', {
                    url: '/eventing',
                    views: {
                        'main@app.admin': {
                            templateUrl: '../_p/ui/event/eventing.html'
                        }
                    },
                    data: {
                        title: 'Eventing'
                    }
                })
                .state('app.admin.eventing.summary', {
                    url: '/summary',
                    templateUrl: '../_p/ui/event/ui-current/fragments/summary.html',
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
                    url: '/handler/:appName',
                    templateUrl: '../_p/ui/event/ui-current/fragments/handler-editor.html',
                    resolve: {
                        loadApps: ['ApplicationService',
                            function(ApplicationService) {
                                return ApplicationService.local.loadApps();
                            }
                        ]
                    },
                    data: {
                        child: 'app.admin.eventing.summary'
                    }
                });

            mnPermissionsProvider.set('cluster.eventing.functions!manage');
            mnPluggableUiRegistryProvider.registerConfig({
                name: 'Eventing',
                state: 'app.admin.eventing.summary',
                plugIn: 'workbenchTab',
                ngShow: "rbac.cluster.eventing.functions.manage",
                index: 4,
                responsiveHide: true
            });
        }
    ]);
angular.module('mnAdmin').requires.push('eventing');
