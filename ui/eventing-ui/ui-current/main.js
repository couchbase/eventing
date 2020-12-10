import angular from "/ui/web_modules/angular.js";
import app from "/ui/app/app.js";

import { NgModule } from '/ui/web_modules/@angular/core.js';
import { UIRouterUpgradeModule } from '/ui/web_modules/@uirouter/angular-hybrid.js';

angular
  .module(app)
  .config(function(mnPluggableUiRegistryProvider, mnPermissionsProvider) {
    mnPermissionsProvider.set('cluster.eventing.functions!manage');
    mnPluggableUiRegistryProvider.registerConfig({
      name: 'Eventing',
      state: 'app.admin.eventing.summary',
      plugIn: 'workbenchTab',
      ngShow: "rbac.cluster.eventing.functions.manage",
      index: 4,
      responsiveHide: true
    });
  });

class EventingUI {
  static get annotations() {
    return [
      new NgModule({
        imports: [
          UIRouterUpgradeModule.forRoot({
            states: [{
              name: "app.admin.eventing.**",
              url: "/eventing",
              lazyLoad: ($transition$) => {
                return import(
                  '/_p/ui/event/ui-current/eventing.js').then(
                  m => {
                    $transition$.injector().get('$ocLazyLoad')
                      .load({ name: 'eventing' });
                  });
              }
            }]
          })
        ]
      })
    ]
  }
}

export default EventingUI;
