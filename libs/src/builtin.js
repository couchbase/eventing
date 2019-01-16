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

    // Stringify all the named parameters. This is necessary for libcouchbase C SDK.
    for (var i in this.options.namedParams) {
        var param = this.options.namedParams[i];
        switch (typeof param) {
            case 'boolean':
            case 'number':
                continue;

            case 'undefined':
                // Mapping 'undefined' type of JavaScript to 'null' type.
                this.options.namedParams[i] = null;
                break;

            case 'object':
                // In JavaScript, the expression 'typeof null' yields 'object' as the type.
                // Since 'null' type is supported by N1QL, no need to do anything.
                if (!param) {
                    continue;
                }

                // JSON must be stringified and quotes must be escaped subsequently.
                param = JSON.stringify(param);
            case 'string':
                // Enclose the positional parameter within double-quotes (required by libcouchbase C SDK)
                // if it's a string.
                var quotesEscaped = '';
                for (var c of param) {
                    if (c === '"') {
                        quotesEscaped += '\\';
                    }

                    quotesEscaped += c;
                }

                this.options.namedParams[i] = '"' + quotesEscaped + '"';
                break;

            default:
                throw `Data type "${typeof param}" is not yet supported by N1LQJs`;
        }
    }
}