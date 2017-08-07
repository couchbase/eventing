angular.module('eventing', ['mnPluggableUiRegistry', 'ui.router', 'mnPoolDefault'])
    // Controller for the summary page.
    .controller('SummaryCtrl', ['$scope', '$state', '$uibModal', '$timeout', 'ApplicationService', 'serverNodes',
        function($scope, $state, $uibModal, $timeout, ApplicationService, serverNodes) {
            var self = this;

            self.showSuccessAlert = false;
            self.serverNodes = serverNodes;
            self.appList = ApplicationService.getAllApps();

            self.isAppListEmpty = function() {
                return Object.keys(self.appList).length === 0;
            };

            // Callback for deployment.
            self.deployApp = function(appName) {
                $scope.appName = appName;
                $scope.actionTitle = 'Deployment';
                $scope.action = 'deploy';
                $scope.cleanupTimers = false;

                // Open dialog to confirm application deployment.
                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/dialogs/app-actions.html',
                        scope: $scope
                    }).result
                    .then(function(cleanupTimers) {
                        // checkbox for cleanupTimers.
                        if (cleanupTimers) {
                            // If cleanupTimers is set, then first modify its settings.
                            var app = ApplicationService.getAppByName(appName);
                            app.settings.cleanup_timers = cleanupTimers;
                        }

                        // Deploy the application.
                        ApplicationService.deployApp(appName)
                            .then(function(response) {
                                // Show an alert upon successful deployment.
                                showSuccessAlert(appName + ' deployed successfully!');

                                // TODO : This is a small work-around. Need to model this properly.
                                var app = ApplicationService.getAppByName(appName);
                                app.appState = {
                                    isDeployed: 'Deployed',
                                    isEnabled: 'Enabled'
                                };
                            }, function(errResponse) {
                                console.error('Unable to deploy:', errResponse.data);
                            });
                    }, function(errResponse) {
                        console.error('Deployment cancelled');
                    });
            };

            // Callback to export the app.
            self.exportApp = function(appName) {
                ApplicationService.getDeployedApps()
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
                    });
            };

            self.attachAppState = function(app) {
                var state = ApplicationService.getAppState(app.appname);
                // Workaround - must not check for the appState to set it.
                if (!app.appState) {
                    app.appState = {
                        isDeployed: state.deployed ? 'Deployed' : 'Undeployed',
                        isEnabled: state.enabled ? 'Enabled' : 'Disabled'
                    };
                }
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
                        ApplicationService.deleteApp(appName)
                            .then(function(response) {
                                showSuccessAlert(appName + ' deleted successfully!');
                            }, function(errResponse) {
                                console.error('error in deleting application', errResponse.data);
                            });
                    }, function(errResponse) {
                        console.error('Delete cancelled');
                    });
            };

            // Utility function to show success alert.
            function showSuccessAlert(message) {
                self.alertMessage = message;
                self.showSuccessAlert = true;
                $timeout(function() {
                    self.showSuccessAlert = false;
                }, 4000);
            }
        }
    ])
    // Controller for the buttons in header.
    .controller('HeaderCtrl', ['$scope', '$uibModal', '$state', 'mnPoolDefault', 'ApplicationService',
        function($scope, $uibModal, $state, mnPoolDefault, ApplicationService) {
            var self = this;

            function createApp(creationScope) {
                // Open the settings fragment as create dialog.
                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/fragments/app-settings.html',
                        scope: creationScope,
                        controller: 'CreateCtrl',
                        controllerAs: 'formCtrl',
                        resolve: {
                            bucketsResolve: ['$http', 'mnPoolDefault',
                                function($http, mnPoolDefault) {
                                    // Getting the list of buckets from server.
                                    var poolDefault = mnPoolDefault.latestValue().value;
                                    return $http.get(poolDefault.buckets.uri)
                                        .then(function(response) {
                                            var buckets = [];
                                            for (var bucket of response.data) {
                                                buckets.push(bucket.name);
                                            }

                                            return buckets;
                                        }, function(errResponse) {
                                            console.error(errResponse);
                                        });
                                }
                            ]
                        }
                    }).result
                    .then(function(response) { // Upon continue.
                        creationScope.appModel.depcfg.buckets = ApplicationService.convertBindingToConfig(creationScope.bindings);
                        creationScope.appModel.fillWithMissingDefaults();
                        ApplicationService.createApp(creationScope.appModel);

                        $state.go('app.admin.eventing.handler', {
                            appName: creationScope.appModel.appname
                        });
                    }, function(errResponse) { // Upon cancel.
                        console.error('failure');
                    });
            }

            // Callback for create.
            self.showCreateDialog = function() {
                var scope = $scope.$new(true);
                scope.appModel = new ApplicationModel();
                scope.appModel.initializeDefaults();
                scope.bindings = [];
                // Add a sample row of bindings.
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
                            scope.bindings = ApplicationService.getBindingFromConfig(app.buckets);
                            if (scope.bindings.length) {
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

                $("#loadConfig")[0].value = null;
                $("#loadConfig")[0].addEventListener('change', handleFileSelect, false);
                $("#loadConfig")[0].click();
            };
        }
    ])
    // Controller for creating an application.
    .controller('CreateCtrl', ['FormValidationService', 'bucketsResolve',
        function(FormValidationService, bucketsResolve) {
            var self = this;
            self.isDialog = true;

            // TODO : Need to make sure that source and metadata buckets are not the same.
            self.sourceBuckets = bucketsResolve;
            self.metadataBuckets = bucketsResolve.reverse();

            self.isFromInvalid = function() {
                return FormValidationService.isFromInvalid(self.createAppForm);
            };
        }
    ])
    // Controller for settings.
    .controller('SettingsCtrl', ['$uibModal', '$state', '$timeout', '$scope', '$stateParams', 'ApplicationService', 'FormValidationService', 'bucketsResolve',
        function($uibModal, $state, $timeout, $scope, $stateParams, ApplicationService, FormValidationService, bucketsResolve) {
            var self = this,
                appModel = ApplicationService.getAppByName($stateParams.appName);

            self.isDialog = false;
            self.showSuccessAlert = false;
            self.showWarningAlert = false;

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

            // Need to pass a deep copy or the changes will be stored locally till refresh.
            $scope.appModel = JSON.parse(JSON.stringify(appModel));

            self.isFromInvalid = function() {
                return FormValidationService.isFromInvalid(self.createAppForm);
            };

            self.saveSettings = function() {
                var bindings = ApplicationService.convertBindingToConfig(self.bindings);
                if (JSON.stringify(appModel.depcfg.buckets) !== JSON.stringify(bindings)) {
                    showWarningAlert('Bindings changed. Re-deploy for changes to take effect!')
                }

                appModel.depcfg.buckets = bindings;
                appModel.depcfg.source_bucket = self.sourceBucket;
                appModel.depcfg.metadata_bucket = self.metadataBucket;

                ApplicationService.saveAppSettings($scope.appModel);
                showSuccessAlert('Settings saved successfully!');
            };

            self.cancelEdit = function() {
                $scope.appModel = JSON.parse(JSON.stringify(appModel));
                $scope.appModel.settings.cleanup_timers = String(appModel.settings.cleanup_timers);

                // Upon pressing cancel in the settings page, go back to the summary page.
                $state.go('app.admin.eventing.summary');
            };

            function showSuccessAlert(message) {
                self.alertMessage = message;
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
    .controller('HandlerCtrl', ['$uibModal', '$timeout', '$state', '$scope', '$stateParams', 'ApplicationService',
        function($uibModal, $timeout, $state, $scope, $stateParams, ApplicationService) {
            var self = this,
                isDebugOn = false,
                debugScope = $scope.$new(true),
                app = ApplicationService.getAppByName($stateParams.appName);

            debugScope.appName = app.appname;

            self.handler = app.appcode;
            self.pristineHandler = app.appcode;
            self.debugToolTip = 'Displays a URL that connects the Chrome Dev-Tools with the application handler. Code must be deployed in order to debug.';
            self.disableCancelButton = true;
            self.disableSaveButton = true;

            $scope.aceLoaded = function(editor) {
                // Current line highlight would overlap on the nav bar in compressed mode.
                // Hence, we need to disable it.
                editor.setOption("highlightActiveLine", false);

                // Need to disable the syntax checking.
                // TODO : Figure out how to add N1QL grammar to ace editor.
                editor.getSession().setUseWorker(false);

                // Make the ace editor responsive to changes in browser dimensions.
                function resizeEditor() {
                    $('#handler-editor').width($(window).width() * 0.9);
                    $('#handler-editor').height($(window).height() * 0.7);
                }

                $(window).resize(resizeEditor);
                resizeEditor();
            };

            $scope.aceChanged = function(e) {
                self.disableCancelButton = self.disableSaveButton = false;
                self.disableDeployButton = true;
            };

            self.saveEdit = function() {
                app.appcode = self.handler;
                showSuccessAlert('Code saved successfully!');
                showWarningAlert('Re-deploy for changes to take effect!');
                // TODO: Make a server call here to save the handler code.
                console.log(ApplicationService.getAllApps());

                self.disableCancelButton = self.disableSaveButton = true;
                self.disableDeployButton = false;
            };

            self.cancelEdit = function() {
                self.handler = app.appcode = self.pristineHandler;
                self.disableDeployButton = self.disableCancelButton = self.disableSaveButton = true;

                $state.go('app.admin.eventing.summary');
            };

            self.debugApp = function() {
                if (!isDebugOn) {
                    debugScope.url = 'Waiting for mutation';
                    debugScope.urlReceived = false;
                    isDebugOn = true;

                    // Starts the debugger agent.
                    ApplicationService.startAppDebug(app.appname)
                        .then(function(response) {
                            console.log('Start debug:', response.data);

                            // Poll till we get the URL for debugging.
                            function getDebugUrl() {
                                console.log('Fetching debug url for ' + app.appname);
                                ApplicationService.getDebugUrl(app.appname)
                                    .then(function(response) {
                                        if (response.data === '') {
                                            setTimeout(getDebugUrl, 1000);
                                        } else {
                                            debugScope.urlReceived = true;
                                            debugScope.url = response.data;
                                        }
                                    }, function(errResponse) {
                                        console.error('Unable to get debugger URL for ' + app.appname, errResponse.data);
                                    });
                            }

                            getDebugUrl();
                        }, function(errResponse) {
                            console.error('Failed to start debugger', errResponse.data);
                        });
                }

                // Open the dialog to show the URL for debugging.
                $uibModal.open({
                        templateUrl: '../_p/ui/event/ui-current/dialogs/app-debug.html',
                        scope: debugScope
                    }).result
                    .then(function(response) {
                        // Stop debugger agent.
                        ApplicationService.stopAppDebug(app.appname)
                            .then(function(response) {
                                console.log('debugger stopped');
                                isDebugOn = false;
                                debugScope.url = 'Waiting for mutation';
                                debugScope.urlReceived = false;
                            }, function(errResponse) {
                                console.error('Unable to stop debugger for ' + app.appname, errResponse.data);
                            });
                    }, function(errResponse) {});
            };

            function showSuccessAlert(message) {
                self.alertMessage = message;
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
    // Controller to copy the debug URL.
    .controller('DebugCtrl', ['ApplicationService', function(ApplicationService) {
        var self = this;

        self.copyUrl = function() {
            $('#debug-url').select();
            document.execCommand('copy');
        };
    }])
    // Service to manage the applications.
    .factory('ApplicationService', ['$http',
        function($http) {
            var appManager = new ApplicationManager();

            // Retrieve all the applications in the eventing server.
            var loaderPromise = $http.get('/_p/event/getApplication/')
                .then(function(response) {
                    // Add apps to appManager.
                    for (var app of response.data) {
                        appManager.pushApp(new Application(app), {
                            deployed: true,
                            enabled: true
                        });
                    }
                }, function(errResponse) {
                    console.error('Failed to get the data:', errResponse);
                });

            // APIs provided by the ApplicationService.
            return {
                // loadApps just returns the Promise for the init call to load apps.
                loadApps: function() {
                    return loaderPromise;
                },
                getDeployedApps: function() {
                    // Make a new call to get the apps from server.
                    return $http.get('/_p/event/getApplication/')
                        .then(function(response) {
                            // Create and return an ApplicationManager instance.
                            var deployedAppsMgr = new ApplicationManager();

                            for (var deployedApp of response.data) {
                                deployedAppsMgr.pushApp(new Application(deployedApp), {
                                    deployed: true,
                                    enabled: true
                                });
                            }
                            return deployedAppsMgr;
                        }, function(errResponse) {
                            console.error('Failed to get apps from server', errResponse.data);
                        });
                },
                getAllApps: function() {
                    return appManager.getApplications();
                },
                createApp: function(appModel) {
                    appManager.createApp(appModel);
                    console.log(appManager.getApplications());
                },
                getAppByName: function(appName) {
                    return appManager.getAppByName(appName);
                },
                getAppState: function(appName) {
                    return appManager.appState[appName];
                },
                deployApp: function(appName) {
                    var app = appManager.getAppByName(appName);
                    return $http({
                        url: '/_p/event/setApplication/?name=' + appName,
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
                saveAppSettings: function(appModel) {
                    var app = appManager.getAppByName(appModel.appname);
                    app.settings = appModel.settings;

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
                    appManager.deleteApp(appName);
                    return $http({
                        url: '/_p/event/deleteApplication/?name=' + appName,
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
                startAppDebug: function(appName) {
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
                getDebugUrl: function(appName) {
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
                stopAppDebug: function(appName) {
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
            return {
                isFromInvalid: function(form) {
                    // Need to check where rbacpass is empty manually and update state.
                    // Don't know why AngularJS is giving wrong state for this field.
                    form.rbacpass.$error.required = form.rbacpass.$modelValue === '';

                    return form.appname.$error.required ||
                        form.rbacuser.$error.required ||
                        form.rbacpass.$error.required;
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
                        title: 'Functions'
                    }
                })
                .state('app.admin.eventing.summary', {
                    url: '/summary',
                    templateUrl: '../_p/ui/event/ui-current/fragments/summary.html',
                    controller: 'SummaryCtrl',
                    controllerAs: 'summaryCtrl',
                    resolve: {
                        serverNodes: ['mnPoolDefault', function(mnPoolDefault) {
                            return mnPoolDefault.get()
                                .then(function(response) {
                                    return mnPoolDefault.getUrlsRunningService(response.nodes, 'eventing');
                                }, function(errResponse) {
                                    console.error('Unable to get server nodes', errResponse.data);
                                });
                        }]
                    }
                })
                .state('app.admin.eventing.settings', {
                    url: '/settings/:appName',
                    templateUrl: '../_p/ui/event/ui-current/fragments/app-settings.html',
                    controller: 'SettingsCtrl',
                    controllerAs: 'formCtrl',
                    resolve: {
                        loadApps: ['ApplicationService', function(ApplicationService) {
                            return ApplicationService.loadApps();
                        }],
                        bucketsResolve: ['$http', 'mnPoolDefault', function($http, mnPoolDefault) {
                            // Getting the list of buckets.
                            var poolDefault = mnPoolDefault.latestValue().value;
                            return $http.get(poolDefault.buckets.uri).then(function(response) {
                                var buckets = [];
                                for (var bucket of response.data) {
                                    buckets.push(bucket.name);
                                }
                                return buckets;
                            }, function(errResponse) {
                                console.error(errResponse);
                            });
                        }]
                    }
                })
                .state('app.admin.eventing.handler', {
                    url: '/handler/:appName',
                    templateUrl: '../_p/ui/event/ui-current/fragments/handler-editor.html',
                    resolve: {
                        loadApps: ['ApplicationService', function(ApplicationService) {
                            return ApplicationService.loadApps();
                        }]
                    }
                });

            mnPluggableUiRegistryProvider.registerConfig({
                name: 'Functions',
                state: 'app.admin.eventing.summary',
                plugIn: 'adminTab',
                after: 'indexes',
            });
        }
    ]);
angular.module('mnAdmin').requires.push('eventing');