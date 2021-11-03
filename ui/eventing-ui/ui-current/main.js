import angular from "angular";
import app from "app";
import { mnLazyload } from "mn.app.imports";

import { NgModule } from '@angular/core';
import { UIRouterUpgradeModule } from '@uirouter/angular-hybrid';

angular
  .module(app)
  .config(function(mnPluggableUiRegistryProvider, mnPermissionsProvider) {
    mnPermissionsProvider.set("cluster.collection[.:.:.].eventing.function!manage");
    mnPluggableUiRegistryProvider.registerConfig({
      name: 'Eventing',
      state: 'app.admin.eventing.summary',
      plugIn: 'workbenchTab',
      ngShow: "rbac.cluster.bucket['.'].settings.read && rbac.cluster.collection['.:.:.'].eventing.function.manage",
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
              lazyLoad: mnLazyload(() => import('./eventing.js'), 'eventing')
            }]
          })
        ]
      })
    ]
  }
}

export default EventingUI;
