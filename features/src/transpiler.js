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

function compile(code, headers, footers) {
    let parsingProperties = {
        range: true,
        tokens: true,
        comment: true,
        sourceType: 'script',
        loc: true
    };

    function ErrorInfo(error) {
        this.language = 'JavaScript';
        this.compileSuccess = false;
        this.index = error.index;
        this.lineNumber = error.lineNumber;
        this.columnNumber = error.column;
        this.description = error.description;
        this.area = error.area;
    }

    try {
        let headerStatements = headers.join('\n');
        esprima.parse(headerStatements, parsingProperties);
    } catch (e) {
        e.area = 'handlerHeaders';
        return new ErrorInfo(e);
    }

    try {
        let footerStatements = footers.join('\n');
        esprima.parse(footerStatements, parsingProperties);
    } catch (e) {
        e.area = 'handlerFooters';
        return new ErrorInfo(e);
    }

    try {
        esprima.parse(AddHeadersAndFooters(code, headers, footers), parsingProperties);
    } catch (e) {
        // Need to offset the errors in the handlerCode area
        // in order to correspond to the user's code
        let indexOffset = 0;
        for (let header of headers) {
            indexOffset += header.length;
        }
        e.index -= indexOffset;
        e.area = 'handlerCode';
        e.lineNumber -= headers.length;
        return new ErrorInfo(e);
    }

    try {
        let ast = esprima.parse(code, parsingProperties);
        let nodeUtils = new NodeUtils();
        nodeUtils.checkGlobals(ast);
    } catch (e) {
        e.area = 'handlerCode';
        return new ErrorInfo(e);
    }

    return {
        language: 'JavaScript',
        compileSuccess: true
    };
}

// Gets compatability level of code. Returns [<release>, <ga/beta/dp>, <using_timer>]
function getCodeVersion(code) {
    var versions = ["vulcan", "alice", "mad-hatter"],
        vp = 0;
    var levels = ["ga", "beta", "dp"],
        lp = 0;
    var using_timer = ["false", "true"],
        tp = 0;

    var ast = esprima.parse(code, {
        attachComment: true,
        sourceType: 'script'
    });

    estraverse.traverse(ast, {
        // todo: handle aliased functions, ex: var cn = cronTimer
        enter: function (node) {
            if (/CallExpression/.test(node.type)) {
                if (node.callee.name === 'createTimer') {
                    tp = 1;
                    if (vp < 1) vp = 1;
                }
                if (node.callee.name === 'crc64' || node.callee.name === 'curl') {
                    if (vp < 2) vp = 2;
                }
            }
        }
    });

    return [versions[vp], levels[lp], using_timer[tp]];
}

function transpileQuery(query, namedParams, isSelectQuery) {
    let exprAst = new N1QLExprAst(query, namedParams);

    // N1QL expression need not have a semi-colon at it's end.
    // But it's essential to turn the expression into a statement in order to
    // append the semi colon.
    // Appending the semi colon is essential because it causes a syntax error if
    // the JavaScript is uni-lined.
    let stmtAst = new N1QLStmtAst(exprAst);
    return escodegen.generate(stmtAst);
}

/**
 * @return {string}
 */
function AddHeadersAndFooters(code, headers, footers) {
    let headersCombined = headers.join('\n') + '\n';
    let footersCombined = footers.join('\n') + '\n';
    return headersCombined + code + footersCombined;
}

// A utility class for handling nodes of an AST.
function NodeUtils() {
}

// Checks if the global scope contains only function declarations.
NodeUtils.prototype.checkGlobals = function (ast) {
    var check = false;
    for (var node of ast.body) {
        if (!/FunctionDeclaration/.test(node.type)) {
            if (typeof node.loc === 'undefined' || typeof node.range === 'undefined') {
                throw 'The AST is missing loc and range nodes';
            }

            throw {
                index: node.range[0],
                lineNumber: node.loc.start.line,
                column: node.loc.start.column,
                description: 'Only function declaration are allowed in global scope'
            };
        }

        if (node.id.name === "OnUpdate" || node.id.name === "OnDelete") {
            check = true;
        }
    }
    if (check === false) {
        throw {
            index: 1,
            lineNumber: 1,
            column: 1,
            description: 'Handler code is missing OnUpdate() and OnDelete() functions. At least one of them is needed to deploy the handler'
        };
    }
};

// Data types JavaScript AST nodes - http://esprima.readthedocs.io/en/latest/syntax-tree-format.html
function Ast(type) {
    this.type = type;
}

function N1QLStmtAst(exprAst) {
    Ast.call(this, 'ExpressionStatement');
    this.expression = exprAst;
}

function N1QLExprAst(query, namedParams) {
    Ast.call(this, 'CallExpression');
    this.callee = {
        "type": "Identifier",
        "name": "N1QL"
    };

    this.arguments = [
        {
            "type": "Literal",
            "value": query
        },
        {
            "type": "ObjectExpression",
            "properties": []
        }
    ];

    for (let param of namedParams) {
        this.arguments[1].properties.push({
            "type": "Property",
            "key": {
                "type": "Identifier",
                "name": '$' + param
            },
            "computed": false,
            "value": {
                "type": "Identifier",
                "name": param
            },
            "kind": "init",
            "method": false,
            "shorthand": false
        });
    }
}
