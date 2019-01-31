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
                        }

                        setTimeout(deployedAppsTicker, 2000);
                    }).catch(function(errResponse) {
                        self.errorCode = errResponse && errResponse.status || 500;
                        console.error('Unable to list apps');
                    });

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
                deploymentScope.actionTitle = app.settings.deployment_status ? 'Undeploy' : 'Deploy';
                deploymentScope.action = app.settings.deployment_status ? 'undeploy' : 'deploy';
                deploymentScope.cleanupTimers = false;

                if (app.settings.deployment_status) {
                    undeployApp(app, deploymentScope);
                } else {
                    changeState('deploy', app, deploymentScope);
                }
            };

            self.toggleProcessing = function(app) {
                var processingScope = $scope.$new(true);
                processingScope.appName = app.appname;
                processingScope.actionTitle = app.settings.processing_status ? 'Pause' : 'Resume';
                processingScope.action = app.settings.processing_status ? 'pause' : 'resume';
                processingScope.cleanupTimers = false;

                if (app.settings.processing_status) {
                    changeState('pause', app, processingScope);
                } else {
                    changeState('resume', app, processingScope);
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
                                warnings = info.warnings.join(". <br>");
                                ApplicationService.server.showWarningAlert(warnings);
                            }
                        }

                        if (warnings == null) {
                            ApplicationService.server.showSuccessAlert(`${app.appname} will ${operation}`);
                        } else {
                            ApplicationService.server.showSuccessAlert(`${app.appname} will ${operation} with warnings`);
                        }
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
                    .catch(function() {
                        console.error('Failed to export the Function');
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
                        return ApplicationService.tempStore.saveApp(creationScope.appModel);
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
                    type: 'alias',
                    name: '',
                    value: '',
                    access: 'r',
                    auth_type: 'no-auth',
                    cookies: 'allow'
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
                function handleFileSelect() {
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
                                    type: 'alias',
                                    name: '',
                                    value: '',
                                    access: 'r'
                                });
                            }

                            createApp(scope);
                        } catch (error) {
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
    .controller('EventingSettingsCtrl', ['$scope', 'ApplicationService', 'config',
        function($scope, ApplicationService, config) {
            var self = this;
            config = config.data;
            self.enableDebugger = config.enable_debugger;

            self.saveSettings = function(closeDialog) {
                ApplicationService.public.updateConfig({
                    enable_debugger: self.enableDebugger
                });
                closeDialog('ok');
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
    .controller('HandlerCtrl', ['$q', '$uibModal', '$timeout', '$state', '$scope', '$stateParams', 'ApplicationService',
        function($q, $uibModal, $timeout, $state, $scope, $stateParams, ApplicationService) {
            var self = this,
                isDebugOn = false,
                debugScope = $scope.$new(true),
                app = ApplicationService.local.getAppByName($stateParams.appName);

            debugScope.appName = app.appname;

            self.handler = app.appcode;
            self.pristineHandler = app.appcode;
            self.debugToolTip = 'Displays a URL that connects the Chrome Dev-Tools with the application handler. Code must be deployed in order to debug.';
            self.disableCancelButton = true;
            self.disableSaveButton = true;
            self.editorDisabled = app.settings.deployment_status && app.settings.processing_status;
            self.debugDisabled = !(app.settings.deployment_status && app.settings.processing_status);

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

                // If the compilation wasn't successful, show error info in UI.
                if (app.compilationInfo && !app.compilationInfo.compile_success) {
                    var line = app.compilationInfo.line_number - 1,
                        col = app.compilationInfo.column_number - 1;

                    var markerId = editor.session.addMarker(new Range(line, col, line, col + 1), "functions-editor-info", "text");
                    markers.push(markerId);
                    editor.getSession().setAnnotations([{
                        row: line,
                        column: col,
                        text: app.compilationInfo.description,
                        type: "error"
                    }]);
                }

                editor.on('focus', function(event) {
                    if (self.editorDisabled) {
                        ApplicationService.server.showWarningAlert('Undeploy/Pause the function to edit!');
                    }
                });

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
            };

            self.saveEdit = function() {
                app.appcode = self.handler;
                ApplicationService.tempStore.saveApp(app)
                    .then(function(response) {
                        ApplicationService.server.showSuccessAlert('Code saved successfully!');
                        ApplicationService.server.showWarningAlert('Deploy for changes to take effect!');

                        self.disableCancelButton = self.disableSaveButton = true;
                        self.disableDeployButton = false;

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
                        mnAlertsService.formatAndSetAlerts(message, 'success', 2500);
                    },

                    showWarningAlert: function(message) {
                        mnAlertsService.formatAndSetAlerts(message, 'warning', 2500);
                    },
                    showErrorAlert: function(message) {
                        mnAlertsService.formatAndSetAlerts(message, 'error', 2500);
                    }
                },
                convertBindingToConfig: function(bindings) {
                    // A binding is of the form -
                    // [{type:'', name:'', value:'', auth_type:'no-auth', cookies:'allow', access:'r'}]
                    var config = {
                        buckets: [],
                        curl: []
                    };
                    for (var binding of bindings) {
                        if (binding.type === 'alias' && binding.name && binding.value) {
                            var element = {};
                            element[binding.type] = binding.value;
                            element.bucket_name = binding.name;
                            element.access = binding.access;
                            config.buckets.push(element);
                        }
                        if (binding.type === 'url' && binding.hostname && binding.value) {
                            config.curl.push({
                                hostname: binding.hostname,
                                value: binding.value
                            });

                            switch (binding.auth_type) {
                                case 'digest':
                                    Object.assign(config.curl[config.curl.length - 1], {
                                        auth_type: 'digest',
                                        username: binding.username,
                                        password: binding.password
                                    });
                                    break;

                                case 'basic':
                                    Object.assign(config.curl[config.curl.length - 1], {
                                        auth_type: 'basic',
                                        username: binding.username,
                                        password: binding.password
                                    });
                                    break;

                                case 'bearer':
                                    Object.assign(config.curl[config.curl.length - 1], {
                                        auth_type: 'bearer',
                                        bearer_key: binding.bearer_key
                                    });
                                    break;

                                case 'no-auth':
                                default:
                                    Object.assign(config.curl[config.curl.length - 1], {
                                        auth_type: 'no-auth',
                                    });
                            }

                            switch (binding.cookies) {
                                case 'allow':
                                    Object.assign(config.curl[config.curl.length - 1], {
                                        cookies: 'allow'
                                    });
                                    break;

                                case 'disallow':
                                default:
                                    Object.assign(config.curl[config.curl.length - 1], {
                                        cookies: 'disallow'
                                    });
                            }
                        }
                    }
                    return config;
                },
                getBindingFromConfig: function(config) {
                    var bindings = [];

                    function addBucketBindings(bucketConfigs) {
                        for (var config of bucketConfigs) {
                            bindings.push({
                                type: 'alias',
                                name: config.bucket_name,
                                value: config.alias,
                                access: config.access ? config.access : (config.source_bucket === config.bucket_name ? 'r' : 'rw')
                            });
                        }
                    }

                    function addCurlBindings(curlConfigs) {
                        for (var config of curlConfigs) {
                            bindings.push({
                                type: 'url',
                                hostname: config.hostname,
                                value: config.value
                            });

                            switch (config.auth_type) {
                                case 'basic':
                                    Object.assign(bindings[bindings.length - 1], {
                                        auth_type: 'basic',
                                        username: config.username,
                                        password: config.password
                                    });
                                    break;

                                case 'digest':
                                    Object.assign(bindings[bindings.length - 1], {
                                        auth_type: 'digest',
                                        username: config.username,
                                        password: config.password
                                    });
                                    break;

                                case 'bearer':
                                    Object.assign(bindings[bindings.length - 1], {
                                        auth_type: 'bearer',
                                        bearer_key: config.bearer_key
                                    });
                                    break;

                                case 'no-auth':
                                default:
                                    Object.assign(bindings[bindings.length - 1], {
                                        auth_type: 'no-auth',
                                    });
                            }

                            switch (config.cookies) {
                                case 'allow':
                                    Object.assign(bindings[bindings.length - 1], {
                                        cookies: 'allow'
                                    });
                                    break;

                                case 'disallow':
                                default:
                                    Object.assign(bindings[bindings.length - 1], {
                                        cookies: 'disallow'
                                    });
                                    break;
                            }
                        }
                    }

                    if (config && config.buckets) {
                        addBucketBindings(config.buckets);
                    }
                    if (config && config.curl) {
                        addCurlBindings(config.curl);
                    }
                    return bindings;
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
                var re = /^[a-zA-Z_$][a-zA-Z_$0-9]*$/g;
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

            return {
                isValidVariable: function(value) {
                    return isValidVariable(value);
                },
                isValidVariableRegex: function(value) {
                    return isValidVariableRegex(value);
                },
                isFormInvalid: function(formCtrl, bindings, sourceBucket) {
                    var bindingsValid = true,
                        form = formCtrl.createAppForm;

                    for (var binding of bindings) {
                        if (!bindingsValid) {
                            break;
                        }

                        if (binding.value.length) {
                            bindingsValid = isValidVariable(binding.value);
                        }
                    }

                    // Check whether the appname exists in the list of apps.
                    if (form.appname.$viewValue && form.appname.$viewValue !== '') {
                        form.appname.$error.appExists = form.appname.$viewValue in formCtrl.savedApps;
                        form.appname.$error.appnameInvalid = !isValidApplicationName(form.appname.$viewValue);
                    }

                    form.appname.$error.required = form.appname.$viewValue === '';

                    return form.appname.$error.required ||
                        form.appname.$error.appExists ||
                        form.worker_count.$error.required ||
                        form.worker_count.$error.min ||
                        form.worker_count.$error.max ||
                        form.execution_timeout.$error.required ||
                        form.execution_timeout.$error.min ||
                        form.execution_timeout.$error.max ||
                        formCtrl.sourceBuckets.indexOf(form.source_bucket.$viewValue) === -1 ||
                        formCtrl.metadataBuckets.indexOf(form.metadata_bucket.$viewValue) === -1 ||
                        form.appname.$error.appnameInvalid ||
                        !bindingsValid;
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