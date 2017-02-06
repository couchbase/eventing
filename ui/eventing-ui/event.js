(function() {
    var ev = angular.module('event', ['ui.ace', 'ui.router', 'mnPluggableUiRegistry']);
    var applications = [];
    var appLoaded = false;
    var resources = [
        {id:0, name:'Deployment Plan'},
        {id:1, name:'Static Resources'},
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
        .state('app.admin.event.appName', {
            url: '/:appName',
            templateUrl: '/_p/ui/event/applications-frag.html',
            controller: 'PerAppController',
            controllerAs: 'perAppCtrl',
        })
        .state('app.admin.event.resName', {
            url: '/:appName/:resName',
            templateUrl: '/_p/ui/event/editor-frag.html',
            controller: 'ResEditorController',
            controllerAs: 'resEditCtrl',
        })
      .state('app.admin.event', {
        url: '/event',
        templateUrl: '/_p/ui/event/event.html'
      })

mnPluggableUiRegistryProvider.registerConfig({
        name: 'Eventing',
        state: 'app.admin.event.applications',
        plugIn: 'adminTab',
        after: 'indexes',
      });

    }]);

    ev.run(['$http', '$window', function($http, $window) {
        $http.get('/_p/event/get_application/')
        .then(function(response) {
            for(var i = 0; i < response.data.length; i++) {
                response.data[i].depcfg = JSON.stringify(response.data[i].depcfg, null, ' ');
                applications.push(response.data[i]);
            }
            appLoaded = true;
        });
    }]);

    ev.directive('appListsLeftPanel', function(){
        return {
            restrict: 'E',
            templateUrl: '/_p/ui/event/applist-frag.html',
            controller: 'AppListController',
            controllerAs: 'appListCtrl',
        };
    });


    ev.controller('CreateController',['$http', function($http) {
    if(!appLoaded) {
    $http.get('/_p/event/get_application/')
        .then(function(response) {
            for(var i = 0; i < response.data.length; i++) {
                response.data[i].depcfg = JSON.stringify(response.data[i].depcfg, null, ' ');
                applications.push(response.data[i]);
            }
        });
     appLoaded = true;
    }

        this.showCreation = true;
        this.applications = applications;
        this.createApplication = function(application) {
            if (application.name.length > 0) {
                application.id = this.applications.length;
                application.deploy = false;
                application.expand = false;
                application.depcfg = '{"_comment": "Enter deployment configuration"}';
                application.handlers = "/* Enter handlers code here */";
                application.assets = [];
                application.debug = false;
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
            var flag = this.currentApp !== null && application.name === this.currentApp.name;
            if (!flag) application.expand = false;
            return flag;
        }
    }]);

    ev.controller('PerAppController', ['$location', '$http', function($location, $http) {
        this.currentApp = null;
        var appName = $location.path().slice(7);
        for(var i = 0; i < applications.length; i++) {
            if(applications[i].name === appName) {
                this.currentApp = applications[i];
                break;
            }
        }

        this.deployApplication = function() {
            this.currentApp.deploy = true;
            var x = angular.copy(this.currentApp);
            x.depcfg = JSON.parse(x.depcfg);
            var uri = '/_p/event/set_application/?name=' + this.currentApp.name;
            var res = $http({url: uri,
                method: "POST",
                mnHttp: {
                        isNotForm: true
                  },
                headers: {'Content-Type': 'application/json'},
                data: x
            });
            res.success(function(data, status, headers, config) {
                this.set_application = data;
            });
            res.error(function(data, status, headers, config) {
                alert( "failure message: " + JSON.stringify({data: data}));
            });
        }

        this.undeployApplication = function() {
            this.currentApp.deploy = false;
        }

        this.startDbg = function() {
            this.currentApp.debug = true;
            var uri = '/_p/event/start_dbg/?name=' + this.currentApp.name;
            var res = $http.post(uri, null);
            res.success(function(data, status, headers, config) {
                this.set_application = data;
            });
            res.error(function(data, status, headers, config) {
                alert( "failure message: " + JSON.stringify({data: data}));
            });
        }
        this.stopDbg = function() {
            this.currentApp.debug = false;
            var uri = '/_p/event/stop_dbg/?name=' + this.currentApp.name;
            var res = $http.post(uri, null);
            res.success(function(data, status, headers, config) {
                this.set_application = data;
            });
            res.error(function(data, status, headers, config) {
                alert( "failure message: " + JSON.stringify({data: data}));
            });
        }

    }]);

    ev.controller('ResEditorController', ['$location', '$http', function($location, $http){
        this.currentApp = null;
        var values = $location.path().split('/');
        appName = values[2];
        for(var i = 0; i < applications.length; i++) {
            if(applications[i].name === appName) {
                this.currentApp = applications[i];
                break;
            }
        }
        if(values[3] == 'Deployment Plan') {
            this.showJsonEditor = true;
            this.showJSEditor = false;
            this.showLoading = false;
        }
        else if(values[3] == 'Handlers') {
            this.showJsonEditor = false;
            this.showJSEditor = true;
            this.showLoading = false;
        }
        else if(values[3] == 'Static Resources') {
            this.showJsonEditor = false;
            this.showJSEditor = false;
            this.showLoading = true;
        }
        else {
            this.showJSEditor = false;
            this.showJsonEditor = false;
            this.showLoading = false;
        }
        this.saveAsset = function(asset, content) {
            this.currentApp.assets.push({name:asset.name, content:content, operation:"add", id:this.currentApp.assets.length});
        }
        this.deleteAsset = function(asset) {
            asset.operation = "delete";
            asset.content = null;
        }
        this.lineNum = null;
        this.response = null;
        this.editor = null;
        this.breakpoints = [];
        this.watchVar = null;
        this.showHistory = false;
        this.dbgHistory = [];
        parent = this;

        function sendPostCommand(uri, command) {
            var res = $http({url: uri,
                    method: "POST",
                    mnHttp: {
                        isNotForm: true
                    },
                    headers: {'Content-Type': 'application/json'},
                    data: command
                });
                res.success(function(data, status, headers, config) {
                    parent.response = data;
                    parent.dbgHistory.push({request:command, response:data});
                });
                res.error(function(data, status, headers, config) {
                    alert( "failure message: " + JSON.stringify({data: data}));
                });
        }


        this.aceLoaded = function(editor) {
            parent.editor = editor;
            editor.on("click", function(e){
                parent.lineNum = e.getDocumentPosition().row;
                    console.log("Set/clear breakpoint at :", parent.lineNum);
            });
        }
        this.setBreakpoint = function() {
            if (parent.lineNum == null) {
                alert("Select line number to set breakpoint");
            }
            else {
                var command = {
                            'seq' : 1,
                            'type': "request",
                            'command': "setbreakpoint",
                            'arguments' : {
                                            'type' : 'function',
                                            'line' : parent.lineNum + 1,
                                            'target' : 'OnUpdate',
                                          }
                };
                var uri ='/_p/event/debug?appname=' + this.currentApp.name + '&command=setbreakpoint';
                sendPostCommand(uri, command);
                parent.editor.session.setBreakpoint(parent.lineNum, 'setMarker');
                parent.breakpoints.push(parent.lineNum);
                parent.lineNum = null;
            }
        }
        this.clearBreakpoints = function() {
                var command = {
                            'seq' : 1,
                            'type': "request",
                            'command': "clearbreakpoint",
                            'arguments' : {
                                            'type' : 'script',
                                            'breakpoint' : parent.breakpoints.length,
                                          }
                };
                var uri ='/_p/event/debug?appname=' + this.currentApp.name + '&command=clearbreakpoint';
                sendPostCommand(uri, command);
                for (var i = 0; i < parent.breakpoints.length; i++) {
                    parent.editor.session.clearBreakpoint(parent.breakpoints[i]);
                }
                parent.breakpoints = [];
        }
        this.listBreakpoints = function() {
            var command = {
                'seq' : 1,
                'type' : "request",
                'command' : "listbreakpoints"
            };
            var uri ='/_p/event/debug?appname=' + this.currentApp.name + '&command=listbreakpoints';
            sendPostCommand(uri, command);
        }
        this.setMutation =  function() {
            $http.get('/_p/event/store_blob/?appname=' + this.currentApp.name);
        }
        this.continue = function() {
            var command = {
                'seq' : 1,
                'type' : "request",
                'command' : "continue"
            };
            var uri ='/_p/event/debug?appname=' + this.currentApp.name + '&command=continue';
            sendPostCommand(uri, command);
        }
        this.singleStep = function() {
            var command = {
                'seq' : 1,
                'type' : "request",
                'command' : "continue",
                'arguments' : {"stepaction" : "next",
                                "stepcount": 1}
            };
            var uri ='/_p/event/debug?appname=' + this.currentApp.name + '&command=continue';
            sendPostCommand(uri, command);
        }
	    this.stepInto = function() {
            var command = {
                'seq' : 1,
                'type' : "request",
                'command' : "continue",
                'arguments' : {"stepaction" : "in",
                                "stepcount": 1}
            };
            var uri ='/_p/event/debug?appname=' + this.currentApp.name + '&command=continue';
            sendPostCommand(uri, command);
        }
	    this.stepOut = function() {
            var command = {
                'seq' : 1,
                'type' : "request",
                'command' : "continue",
                'arguments' : {"stepaction" : "out",
                                "stepcount": 1}
            };
            var uri ='/_p/event/debug?appname=' + this.currentApp.name + '&command=continue';
            sendPostCommand(uri, command);
        }
        this.evalExpr = function() {
            if(parent.watchVar == null) {
                alert("Enter expression to evaluate");
            }
            else {
                var command = {
                    'seq' : 1,
                    'type' : "request",
                    'command' : "evaluate",
                    'arguments' : {'expression' : parent.watchVar,
                                    'global': false,
				    'disable_break' : true}
                };
                var uri ='/_p/event/debug?appname=' + this.currentApp.name + '&command=evaluate';
                sendPostCommand(uri, command);
            }
            parent.watchVar = null;
        }

        this.showDbgHistory = function() {
            this.showJSEditor = false;
            this.showHistory = true;
        }
        this.closeDbgHistory = function() {
            this.showJSEditor = true;
            this.showHistory = false;
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
