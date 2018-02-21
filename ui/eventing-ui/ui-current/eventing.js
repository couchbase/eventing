angular.module('eventing', ['mnPluggableUiRegistry', 'ui.router', 'mnPoolDefault'])
    // Controller for the summary page.
    .controller('SummaryCtrl', ['$q', '$scope', '$rootScope', '$state', '$uibModal', '$timeout', '$location', 'ApplicationService', 'serverNodes', 'isEventingRunning',
        function($q, $scope, $rootScope, $state, $uibModal, $timeout, $location, ApplicationService, serverNodes, isEventingRunning) {
            var self = this;

            self.errorState = !ApplicationService.status.isErrorCodesLoaded();
            self.errorCode = 200;
            self.showErrorAlert = self.showSuccessAlert = false;
            self.serverNodes = serverNodes;
            self.isEventingRunning = isEventingRunning;
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

                ApplicationService.primaryStore.getDeployedApps()
                    .then(function(response) {
                        for (var app of Object.keys(self.appList)) {
                            if (app in response.data) {
                                self.appList[app].uiState = 'healthy';
                            } else {
                                self.appList[app].uiState = 'warmup';
                            }
                        }

                        setTimeout(deployedAppsTicker, 2000);
                    })
                    .catch(function(errResponse) {
                        self.errorCode = errResponse && errResponse.status || 500;
                        console.error('Unable to get deployed apps', errResponse);
                    });
            }

            deployedAppsTicker();

            self.getAppUiProcessingState = function(app) {
                if (app.getDeploymentStatus() === 'deployed') {
                    if (self.appList[app.appname].uiState === 'healthy') {
                        return 'running';
                    } else {
                        return 'bootstrapping';
                    }
                } else {
                    return 'paused';
                }
            };

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
                        ]
                    }
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
                    deployApp(app, deploymentScope);
                }
            };

            function deployApp(app, scope) {
                var appClone = app.clone();
                scope.settings = {};
                scope.settings.cleanupTimers = false;
                scope.settings.changeFeedBoundary = 'everything';

                // Open dialog to confirm application deployment.
                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/dialogs/app-actions.html',
                        scope: scope
                    }).result
                    .then(function(response) {
                        appClone.settings.cleanup_timers = scope.settings.cleanupTimers;
                        appClone.settings.dcp_stream_boundary = scope.settings.changeFeedBoundary;

                        // Set app to 'deployed & enabled' state and save.
                        appClone.settings.deployment_status = true;
                        appClone.settings.processing_status = true;
                        // Disable edit button till we get the compilation info
                        self.disableEditButton = true;
                        return ApplicationService.public.deployApp(appClone);
                    })
                    .then(function(response) {
                        var responseCode = ApplicationService.status.getResponseCode(response);
                        if (responseCode) {
                            return $q.reject(ApplicationService.status.getErrorMsg(responseCode, response.data));
                        }

                        console.log(response.data);

                        // Update settings in the UI.
                        app.settings.cleanup_timers = appClone.settings.cleanup_timers;
                        app.settings.deployment_status = appClone.settings.deployment_status;
                        app.settings.processing_status = appClone.settings.processing_status;
                        // Enable edit button as we got compilation info
                        self.disableEditButton = false;

                        // Show an alert upon successful deployment.
                        showSuccessAlert(`${app.appname} deployed successfully!`);
                    })
                    .catch(function(errResponse) {
                        if (errResponse.data && (errResponse.data.name === 'ERR_HANDLER_COMPILATION')) {
                            var info = JSON.parse(errResponse.data.runtime_info);
                            app.compilationInfo = info;
                            showErrorAlert(`Deployment failed: Syntax error (${info.line_number}, ${info.column_number}) - ${info.description}`);
                        } else {
                            showErrorAlert(`Deployment failed: ${errResponse.data.runtime_info}`);
                        }

                        // Enable edit button as we got compilation info
                        self.disableEditButton = false;
                        console.error(errResponse);
                    });
            }

            function undeployApp(app, scope) {
                var appClone = app.clone();

                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/dialogs/app-actions.html',
                        scope: scope
                    }).result
                    .then(function(response) {
                        return ApplicationService.primaryStore.getDeployedApps();
                    })
                    .then(function(response) {
                        // Check if the app is deployed completely before trying to undeploy.
                        if (!(appClone.appname in response.data)) {
                            showErrorAlert(`Function "${appClone.appname}" may be undergoing bootstrap. Please try later.`);
                            return $q.reject(`Unable to undeploy "${appClone.appname}". Possibly, bootstrap in progress`);
                        }

                        // Set app to 'undeployed & disabled' state and save.
                        appClone.settings.deployment_status = false;
                        appClone.settings.processing_status = false;
                        return ApplicationService.primaryStore.saveSettings(appClone);
                    })
                    .then(function(response) {
                        var responseCode = ApplicationService.status.getResponseCode(response);
                        if (responseCode) {
                            return $q.reject(ApplicationService.status.getErrorMsg(responseCode, response.data));
                        }

                        console.log(response.data);
                        return ApplicationService.tempStore.saveApp(appClone);
                    })
                    .then(function(response) {
                        var responseCode = ApplicationService.status.getResponseCode(response);
                        if (responseCode) {
                            return $q.reject(ApplicationService.status.getErrorMsg(responseCode, response.data));
                        }

                        console.log(response.data);
                        app.settings.deployment_status = appClone.settings.deployment_status;
                        app.settings.processing_status = appClone.settings.processing_status;
                        showSuccessAlert(`${app.appname} undeployed successfully!`);
                    })
                    .catch(function(errResponse) {
                        console.error(errResponse);
                    });
            }

            // Callback to export the app.
            self.exportApp = function(appName) {
                ApplicationService.tempStore.getAllApps()
                    .then(function(deployedAppsMgr) {
                        var app = deployedAppsMgr.getAppByName(appName);
                        var fileName = appName + '.json';

                        // Create a new blob of the app.
                        var fileToSave = new Blob([JSON.stringify(app)], {
                            type: 'application/json',
                            name: fileName
                        });

                        // Save the file.
                        saveAs(fileToSave, fileName);
                    })
                    .catch(function(errResponse) {
                        console.error('Failed to get apps from server', errResponse);
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
                        showSuccessAlert(`${appName} deleted successfully!`);
                    })
                    .catch(function(errResponse) {
                        console.error(errResponse);
                    });
            };

            // Utility function to show success alert.
            function showSuccessAlert(message) {
                self.successMessage = message;
                self.showSuccessAlert = true;
                $timeout(function() {
                    self.showSuccessAlert = false;
                }, 4000);
            }

            // Utility function to show error alert.
            function showErrorAlert(message) {
                self.errorMessage = message;
                self.showErrorAlert = true;
                $timeout(function() {
                    self.showErrorAlert = false;
                }, 4000);
            }
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
                            ]
                        }
                    }).result
                    .then(function(repsonse) { // Upon continue.
                        creationScope.appModel.depcfg.buckets = ApplicationService.convertBindingToConfig(creationScope.bindings);
                        creationScope.appModel.fillWithMissingDefaults();

                        // When we import the application, we want it to be in
                        // disabled and undeployed state.
                        creationScope.appModel.settings.processing_status = false;
                        creationScope.appModel.settings.deployment_status = false;

                        // Deadline timeout must be greater and execution timeout.
                        creationScope.appModel.settings.deadline_timeout = creationScope.appModel.settings.execution_timeout + 2;

                        ApplicationService.local.createApp(creationScope.appModel);
                        return $state.go('app.admin.eventing.handler', {
                            appName: creationScope.appModel.appname
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
                    value: ''
                });
                createApp(scope);
            };

            // Callback for importing application.
            // BUG : Sometimes the continue button must be pressed twice.
            self.importConfig = function() {
                function handleFileSelect() {
                    var reader = new FileReader();
                    reader.onloadend = function() {
                        try {
                            var app = JSON.parse(reader.result);
                            var scope = $scope.$new(true);
                            scope.appModel = new ApplicationModel(app);
                            scope.bindings = ApplicationService.getBindingFromConfig(app.depcfg.buckets);
                            if (!scope.bindings.length) {
                                // Add a sample row of bindings.
                                scope.bindings.push({
                                    type: 'alias',
                                    name: '',
                                    value: ''
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
    // Controller for creating an application.
    .controller('CreateCtrl', ['$scope', 'FormValidationService', 'bucketsResolve', 'savedApps',
        function($scope, FormValidationService, bucketsResolve, savedApps) {
            var self = this;
            self.isDialog = true;

            self.sourceBuckets = bucketsResolve;
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

            self.validateBinding = function(binding) {
                if (binding && binding.value) {
                    return FormValidationService.isValidIdentifier(binding.value);
                }

                return true;
            };
        }
    ])
    // Controller for settings.
    .controller('SettingsCtrl', ['$q', '$timeout', '$scope', 'ApplicationService', 'FormValidationService', 'appName', 'bucketsResolve', 'savedApps', 'isAppDeployed',
        function($q, $timeout, $scope, ApplicationService, FormValidationService, appName, bucketsResolve, savedApps, isAppDeployed) {
            var self = this,
                appModel = ApplicationService.local.getAppByName(appName);

            self.isDialog = false;
            self.showSuccessAlert = false;
            self.showWarningAlert = false;
            self.isAppDeployed = isAppDeployed;
            self.sourceAndBindingSame = false;

            // Need to initialize buckets if they are empty,
            // otherwise self.saveSettings() would compare 'null' with '[]'.
            appModel.depcfg.buckets = appModel.depcfg.buckets ? appModel.depcfg.buckets : [];
            self.bindings = ApplicationService.getBindingFromConfig(appModel.depcfg.buckets);

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

            self.validateBinding = function(binding) {
                if (binding && binding.value) {
                    return FormValidationService.isValidIdentifier(binding.value);
                }

                return true;
            };

            self.saveSettings = function(dismissDialog, closeDialog) {
                var bindings = ApplicationService.convertBindingToConfig(self.bindings);
                if (JSON.stringify(appModel.depcfg.buckets) !== JSON.stringify(bindings)) {
                    $scope.appModel.depcfg.buckets = bindings;
                    showWarningAlert('Bindings changed. Deploy for changes to take effect.');
                }

                // Deadline timeout must be greater than execution timeout.
                $scope.appModel.settings.deadline_timeout = $scope.appModel.settings.execution_timeout + 2;

                // Update local changes.
                appModel.settings = $scope.appModel.settings;
                appModel.depcfg.buckets = bindings;

                ApplicationService.tempStore.isAppDeployed(appName)
                    .then(function(isDeployed) {
                        if (isDeployed) {
                            return ApplicationService.public.updateSettings($scope.appModel);
                        }
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

            function showSuccessAlert(message) {
                self.successMessage = message;
                self.showSuccessAlert = true;
                $timeout(function() {
                    self.showSuccessAlert = false;
                }, 4000);
            }

            function showWarningAlert(message) {
                self.warningMessage = message;
                self.showWarningAlert = true;
                $timeout(function() {
                    self.showWarningAlert = false;
                }, 4000);
            }
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
            self.editorDisabled = app.settings.deployment_status || app.settings.processing_status;
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
                        showWarningAlert('Undeploy the application to edit!');
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
                        showSuccessAlert('Code saved successfully!');
                        showWarningAlert('Deploy for changes to take effect!');

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
                            showErrorAlert(`Function ${app.appname} may be undergoing bootstrap. Please try later.`);
                            return;
                        }

                        if (!isDebugOn) {
                            debugScope.url = 'Waiting for mutation';
                            debugScope.urlReceived = false;
                            isDebugOn = true;

                            // Starts the debugger agent.
                            ApplicationService.debug.start(app.appname)
                                .then(function(response) {
                                    var responseCode = ApplicationService.status.getResponseCode(response);
                                    if (responseCode) {
                                        var errMsg = ApplicationService.status.getErrorMsg(responseCode, response.data);
                                        return $q.reject(errMsg);
                                    }

                                    console.log('Start debug:', response.data);

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
                                    console.error('Failed to start debugger', errResponse);
                                })
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
                    })
                    .catch(function(errResponse) {
                        console.error('Unable to start debugger', errResponse);
                    });
            };

            function showSuccessAlert(message) {
                self.successMessage = message;
                self.showSuccessAlert = true;
                $timeout(function() {
                    self.showSuccessAlert = false;
                }, 4000);
            }

            function showWarningAlert(message) {
                self.warningMessage = message;
                self.showWarningAlert = true;
                $timeout(function() {
                    self.showWarningAlert = false;
                }, 4000);
            }

            function showErrorAlert(message) {
                self.errorMessage = message;
                self.showErrorAlert = true;
                $timeout(function() {
                    self.showErrorAlert = false;
                }, 4000);
            }
        }
    ])
    // Controller to copy the debug URL.
    .controller('DebugCtrl', [function() {
        var self = this;

        self.copyUrl = function() {
            $('#debug-url').select();
            document.execCommand('copy');
        };
    }])
    // Service to manage the applications.
    .factory('ApplicationService', ['$q', '$http', '$state', 'mnPoolDefault',
        function($q, $http, $state, mnPoolDefault) {
            var appManager = new ApplicationManager();
            var errHandler;

            // Note : There's no trailing '/' after getErrorCodes in the URL,
            // It results in 404 page not found for 'getErrorCodes/'
            var loaderPromise = $http.get('/_p/event/getErrorCodes')
                .then(function(response) {
                    console.log('Obtained error codes');

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

                    console.log('Got applications from TempStore');

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
                    deleteApp: function(appName) {
                        return $http.get('/_p/event/deleteAppTempStore/?name=' + appName);
                    }
                },
                primaryStore: {
                    deployApp: function(app) {
                        return $http({
                            url: '/_p/event/setApplication/?name=' + app.appname,
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
                    saveSettings: function(app) {
                        return $http({
                            url: '/_p/event/setSettings/?name=' + app.appname,
                            method: 'POST',
                            mnHttp: {
                                isNotForm: true
                            },
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            data: app.settings
                        });
                    },
                    deleteApp: function(appName) {
                        return $http.get('/_p/event/deleteApplication/?name=' + appName);
                    },
                    getDeployedApps: function() {
                        return $http.get('/_p/event/getDeployedApps');
                    }
                },
                debug: {
                    start: function(appName) {
                        return $http({
                            url: '/_p/event/startDebugger/?name=' + appName,
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
                                var isEventingRunning = _.indexOf(response.thisNode.services, 'eventing') > -1;
                                console.log('isEventingRunning', isEventingRunning);
                                return isEventingRunning;
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
                    }
                },
                convertBindingToConfig: function(bindings) {
                    // A binding is of the form -
                    // [{type:'alias', name:'', value:''}]
                    var config = [];
                    for (var binding of bindings) {
                        if (binding.type === 'alias' && binding.name && binding.value) {
                            var element = {};
                            element[binding.type] = binding.value;
                            element.bucket_name = binding.name;
                            config.push(element);
                        }
                    }
                    return config;
                },
                getBindingFromConfig: function(config) {
                    var bindings = [];
                    if (config) {
                        for (var c of config) {
                            var element = {};
                            element.type = 'alias';
                            element.name = c.bucket_name;
                            element.value = c.alias;
                            bindings.push(element);
                        }
                    }
                    return bindings;
                }
            };
        }
    ])
    // Service to validate the form in settings and create app.
    .factory('FormValidationService', [
        function() {
            function isValidIdentifier(value) {
                var re = /^[a-zA-Z_$][a-zA-Z_$0-9]*$/g;
                return value && value.trim().match(re);
            }

            return {
                isValidIdentifier: function(value) {
                    return isValidIdentifier(value);
                },
                isFormInvalid: function(formCtrl, bindings, sourceBucket) {
                    var bindingsValid = true,
                        form = formCtrl.createAppForm;

                    for (var binding of bindings) {
                        if (!bindingsValid) {
                            break;
                        }

                        if (binding.value.length) {
                            bindingsValid = isValidIdentifier(binding.value);
                        }
                    }

                    // Check whether the appname exists in the list of apps.
                    if (form.appname.$viewValue && form.appname.$viewValue !== '') {
                        form.appname.$error.appExists = form.appname.$viewValue in formCtrl.savedApps;
                        form.appname.$error.appnameInvalid = !isValidIdentifier(form.appname.$viewValue);
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
    .config(['$stateProvider', '$urlRouterProvider', 'mnPluggableUiRegistryProvider',
        function($stateProvider, $urlRouterProvider, mnPluggableUiRegistryProvider) {
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

            mnPluggableUiRegistryProvider.registerConfig({
                name: 'Eventing',
                state: 'app.admin.eventing.summary',
                plugIn: 'adminTab',
                after: 'indexes',
            });
        }
    ]);
angular.module('mnAdmin').requires.push('eventing');
