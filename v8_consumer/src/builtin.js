// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS"
// BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing
// permissions and limitations under the License.

function N1qlQuery(query, options) {
    this.query = query;
    this.options = options;
    this.metadata = null;
    this.isInstance = true;
    this.iter = iter;
    this.execQuery = execQuery;
    this.stopIter = stopIter;
    this.getReturnValue = getReturnValue;

    for (var i in this.options.posParams) {
        var param = this.options.posParams[i];
        if (typeof param === 'object') {
            param = JSON.stringify(param);
        }

        var quotesEscaped = '';
        for (var c of param) {
            if (c == '"') {
                quotesEscaped += '\\';
            }

            quotesEscaped += c;
        }

        this.options.posParams[i] = '"' + quotesEscaped + '"';
    }
}