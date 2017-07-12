(function() {
    var ev = angular.module('event', ['ui.ace', 'ui.router', 'mnPluggableUiRegistry']);
    var applications = [];
    var appLoaded = false;
    var resources = [
        {id:0, name:'Deployment Plan'},
        {id:1, name:'Settings'},
        {id:2, name:'Handlers'},
    ];

    ev.config(['$stateProvider', '$urlRouterProvider', 'mnPluggableUiRegistryProvider', function($stateProvider, $urlRouterProvider, mnPluggableUiRegistryProvider) {
        $urlRouterProvider.otherwise('/event');
        $stateProvider
            .state('app.admin.event.applications', {
                url: '/applications',
                templateUrl: '/_p/ui/event/createApp-frag.html',
                controller: 'CreateController',
                controllerAs: 'createCtrl'
            })
            .state('app.admin.event.resName', {
                url: '/:appName/:resName',
                templateUrl: '/_p/ui/event/editor-frag.html',
                controller: 'ResEditorController',
                controllerAs: 'resEditCtrl',
            })

            .state('app.admin.event.appName', {
                url: '/:appName',
                templateUrl: '/_p/ui/event/applications-frag.html',
                controller: 'PerAppController',
                controllerAs: 'perAppCtrl',
            })
            .state('app.admin.event', {
                url: '/event',
                controller: 'EventController',
                controllerAs: 'eventCtrl',
                templateUrl: '/_p/ui/event/event.html'
            })

        mnPluggableUiRegistryProvider.registerConfig({
            name: 'Eventing',
            state: 'app.admin.event.applications',
            plugIn: 'adminTab',
            after: 'indexes',
        });

    }]);

    ev.directive('appListsLeftPanel', function(){
        return {
            restrict: 'E',
            templateUrl: '/_p/ui/event/ui-classic/applist-frag.html',
            controller: 'AppListController',
            controllerAs: 'appListCtrl',
        };
    });

    ev.run(['$http', 'mnPoolDefault', function($http, mnPoolDefault){
        $http.get('/_p/event/getApplication/')
            .then(function(response) {
                for(var i = 0; i < response.data.length; i++) {
                    response.data[i].depcfg = JSON.stringify(response.data[i].depcfg, null, ' ');
                    if(!appLoaded) {
                        applications.push(response.data[i]);
                    }
                }
                appLoaded = true;
            }, function(response){})
    }]);
    ev.controller('EventController',['$http', 'mnPoolDefault', function($http, mnPoolDefault) {
        this.eventingNodes = [];
        this.showCreation = false;
        this.errorMsg = '';
        var parent = this;
        $http.get('/_p/event/getApplication/')
            .then(function(response) {
                for(var i = 0; i < response.data.length; i++) {
                    response.data[i].depcfg = JSON.stringify(response.data[i].depcfg, null, ' ');
                    if(!appLoaded) {
                        applications.push(response.data[i]);
                    }
                }
                appLoaded = true;
                parent.showCreation = true;
            }, function(response) {
                parent.showCreation = false;
                // if we got a 404, there is no eventing service on this node.
                // let's go through the list of nodes
                // and see which ones have a eventing service
                if (response.status == 404) {
                    mnPoolDefault.get().then(function(value){
                        parent.eventingNodes = mnPoolDefault.getUrlsRunningService(value.nodes, "eventing");
                        if (parent.eventingNodes.length === 0) {
                            parent.errorMsg = "No node in the cluster runs Eventing service"
                        }
                        else {
                            parent.errorMsg = "The eventing interface is only available on Couchbase nodes running eventing service.<br>\
                                                You may access the interface here:<br>"
                        }                   });
                } else {
                    parent.errorMsg = response.data;
                }
            });
    }]);

    ev.controller('CreateController',[function() {
        this.applications = applications;
        this.createApplication = function(application) {
            if (application.appname.length > 0) {
                application.id = this.applications.length;
                application.deploy = false;
                application.expand = false;
                application.depcfg = '{"_comment": "Enter deployment configuration"}';
                application.appcode = "/* Enter handlers code here */";
                application.assets = [];
                application.debug = false;
                application.settings={"log_level" : "INFO",
                    "dcp_stream_boundary" : "everything",
                    "sock_batch_size" : 1,
                    "tick_duration" : 5000,
                    "checkpoint_interval" : 10000,
                    "worker_count" : 1,
                    "cleanup_timers" : false,
                    "timer_worker_pool_size" : 3,
                    "skip_timer_threshold" : 86400,
                    "timer_processing_tick_interval" : 500,
                    "rbacuser" : "",
                    "rbacpass" : "",
                    "rbacrole" : "admin",
                }
                this.applications.push(application);
            }
            this.newApplication={};
        }
    }]);


    ev.controller('AppListController', [function() {
        this.resources = resources;
        this.applications = applications;
        this.currentApp = null;
        this.setCurrentApp = function (application) {
            application.expand = !application.expand;
            this.currentApp = application;
        }
        this.isCurrentApp = function(application) {
            var flag = this.currentApp !== null && application.appname === this.currentApp.appname;
            if (!flag) application.expand = false;
            return flag;
        }
    }]);

    ev.controller('PerAppController', ['$location', '$http', function($location, $http) {
        this.currentApp = null;
        var isOpera = !!window.opera;
        this.isChromeBrowser = !!window.chrome && !isOpera;
        this.debugUrl = null;

        var appName = $location.path().slice(7);
        for(var i = 0; i < applications.length; i++) {
            if(applications[i].appname === appName) {
                this.currentApp = applications[i];
                break;
            }
        }

        this.deployApplication = function() {
            this.currentApp.deploy = true;
            var x = angular.copy(this.currentApp);
            x.depcfg = JSON.parse(x.depcfg);
            var uri = '/_p/event/setApplication/?name=' + this.currentApp.appname;
            var res = $http({url: uri,
                method: "POST",
                mnHttp: {
                    isNotForm: true
                },
                headers: {'Content-Type': 'application/json'},
                data: x
            });
            res.then(function(response) {
                this.setApplication = response.data;
            }, function(response) {
                alert( "failure message: " + JSON.stringify({data: response.data}));
            });
        }

        this.undeployApplication = function() {
            this.currentApp.deploy = false;
        }

        function getDebugUrl(appName) {
            var uri = '/_p/event/getDebuggerUrl/?name=' + appName;
            var res = $http.post(uri, null);
            res.then(function(response) {
                if (response.data == '') {
                    setTimeout(getDebugUrl, 1000, appName);
                }
            }, function(response) {
                alert( "failure message: " + JSON.stringify({data: response.data}));
            });
        }
        var self = this;
        function getDebugUrl(appName) {
            var uri = '/_p/event/getDebuggerUrl/?name=' + appName;
            var res = $http.post(uri, null);
            res.then(function(response) {
                if (response.data == '') {
                  setTimeout(getDebugUrl, 1000, appName);
                }
                else {
                    self.debugUrl = response.data;
                }
            }, function(response) {
                alert( "failure message: " + JSON.stringify({data: response.data}));
            });
        }
        this.stopDbg = function() {
            this.currentApp.debug = false;
            var uri = '/_p/event/stopDebugger/?name=' + this.currentApp.appname;
            var res = $http.post(uri, null);
            res.then(function(response) {
                this.setApplication = response.data;
                self.debugUrl = null;
            }, function(response) {
                alert( "failure message: " + JSON.stringify({data: response.data}));
            });
        }
    }]);

    ev.controller('ResEditorController', ['$location', '$http', function($location, $http){
        this.currentApp = null;
        var values = $location.path().split('/');
        appName = values[2];
        for(var i = 0; i < applications.length; i++) {
            if(applications[i].appname === appName) {
                this.currentApp = applications[i];
                break;
            }
        }
        if(values[3] == 'Deployment Plan') {
            this.showJsonEditor = true;
            this.showJSEditor = false;
            this.showLoading = false;
            this.showSettings = false;
        }
        else if(values[3] == 'Handlers') {
            this.showJsonEditor = false;
            this.showJSEditor = true;
            this.showLoading = false;
            this.showSettings = false;
        }
        else if(values[3] == 'Static Resources') {
            this.showJsonEditor = false;
            this.showJSEditor = false;
            this.showLoading = true;
            this.showSettings = false;
        }
        else if(values[3] == 'Settings') {
            this.showJsonEditor = false;
            this.showJSEditor = false;
            this.showLoading = false;
            this.showSettings = true;
        }
        else {
            this.showJSEditor = false;
            this.showJsonEditor = false;
            this.showLoading = false;
            this.showSettings = false;
        }
        this.saveAsset = function(asset, content) {
            this.currentApp.assets.push({name:asset.appname, content:content, operation:"add", id:this.currentApp.assets.length});
        }
        this.deleteAsset = function(asset) {
            asset.operation = "delete";
            asset.content = null;
        }
        this.saveSettings = function(settings) {
            var uri = '/_p/event/setSettings/?name=' + this.currentApp.appname;
            var res = $http({url: uri,
                method: "POST",
                mnHttp: {
                    isNotForm: true
                },
                headers: {'Content-Type': 'application/json'},
                data: settings
            });
            res.then( function(response) {}, function(response) {
                alert( "failure message: " + JSON.stringify({data: response.data}));
            });
        }

    }]);

    ev.directive('onReadFile', ['$parse', function ($parse) {
        return {
            restrict: 'A',
            scope: false,
            link: function(scope, element, attrs) {
                var fn = $parse(attrs.onReadFile);
                element.on('change', function(onChangeEvent) {
                    var reader = new FileReader();
                    reader.onload = function(onLoadEvent) {
                        scope.$apply(function() {
                            fn(scope, {asset : (onChangeEvent.srcElement || onChangeEvent.target).files[0], content : onLoadEvent.target.result});
                        });
                    };
                    reader.readAsDataURL((onChangeEvent.srcElement || onChangeEvent.target).files[0]);
                });
            }
        };
    }]);
    angular.module('mnAdmin').requires.push('event');
})();
