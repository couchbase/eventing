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

var console = {};
console.log = log;

// LoopModifier types.
LoopModifier.CONST = {
    BREAK: 'break',
    CONTINUE: 'continue',
    LABELED_BREAK: 'labeled_break',
    RETURN: 'return',
    LABELED_CONTINUE: 'labeled_continue'
};

var Context = {
    IterTypeCheck: 'iter_type_check',
    BreakStatement: 'break_statement',
    BreakAltInterrupt: 'break_alt_interrupt',
    ContinueStatement: 'continue_statement',
    ContinueAltInterrupt: 'continue_alt_interrupt',
    ReturnStatement: 'return_statement',
    ReturnAltFound: 'return_alt_found',
    ReturnAltInterrupt: 'return_alt_interrupt',
    IterConsequent: 'iter_consequent'
};

function compile(code, headers, footers) {
    var parsingProperties = {
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
        var ast = esprima.parse(code, parsingProperties),
            nodeUtils = new NodeUtils();

        nodeUtils.checkGlobals(ast);
        // TODO : Remove this check once UUID is used to create variables
        nodeUtils.checkForOfNodeRight(ast);
    } catch (e) {
        e.area = 'handlerCode';
        return new ErrorInfo(e);
    }

    try {
        var headerStatements = headers.join('\n');
        esprima.parse(headerStatements, parsingProperties);
    } catch (e) {
        e.area = 'handlerHeaders';
        return new ErrorInfo(e);
    }

    try {
        var footerStatements = footers.join('\n');
        esprima.parse(footerStatements, parsingProperties);
    } catch (e) {
        e.area = 'handlerFooters';
        return new ErrorInfo(e);
    }

    return {
        language: 'JavaScript',
        compileSuccess: true
    };
}

function jsFormat(code) {
    var ast = esprima.parse(code);
    return escodegen.generate(ast);
}

function transpile(code, sourceFileName, headers, footers) {
    code = AddHeadersAndFooters(code, headers, footers);
    var ast = getAst(code, sourceFileName);
    return escodegen.generate(ast, {
        sourceMap: true,
        sourceMapWithCode: true,
        comment: true
    });
}

function isTimerCalled(code) {
    return isFuncCalled('docTimer', code) || isFuncCalled('cronTimer', code);
}

// Checks if a function is called.
function isFuncCalled(methodName, code) {
    // Get the Abstract Syntax Tree (ast) of the input code.
    var ast = esprima.parse(code, {
        attachComment: true,
        sourceType: 'script'
    });

    var methodExists = false;
    estraverse.traverse(ast, {
        enter: function(node) {
            // todo: handle aliased functions, ex: var dt = docTimer
            if (!methodExists && /CallExpression/.test(node.type)) {
                methodExists = node.callee.name === methodName;
            }
        }
    });

    return methodExists;
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
        enter: function(node) {
            if (/CallExpression/.test(node.type)) {
                if (node.callee.name === 'createTimer') {
                    tp = 1;
                    if (vp < 1) vp = 1;
                }
                // TODO : Change this to mad-hatter when we move CI to mad-hatter
                if (node.callee.name === 'crc64' || node.callee.name === 'curl') {
                    if (vp < 1) vp = 1;
                }
            } else if (/NewExpression/.test(node.type)) {
                if (node.callee.name === 'N1qlQuery' && lp < 1) lp = 1;
            }
        }
    });

    return [versions[vp], levels[lp], using_timer[tp]];
}
// Checks if the given statement is a valid JavaScript expression.
function isJsExpression(stmt) {
    try {
        esprima.parse(stmt);
        return true;
    } catch (e) {
        return false;
    }
}

function transpileQuery(query, namedParams, isSelectQuery) {
    var exprAst = new N1QLQueryExprAst(query, namedParams);
    if (!isSelectQuery) {
        // Call execQuery() for non select queries
        exprAst = new CallExecQueryAst(exprAst);
    }

    // N1QL expression need not have a semi-colon at it's end.
    // But it's essential to turn the expression into a statement in order to
    // append the semi colon.
    // Appending the semi colon is essential because it causes a syntax error if
    // the JavaScript is uni-lined.
    var stmtAst = new N1QLQueryStmtAst(exprAst);
    return escodegen.generate(stmtAst);
}

function AddHeadersAndFooters(code, headers, footers) {
    var headersCombined = headers.join('\n') + '\n';
    var footersCombined = footers.join('\n') + '\n';
    return headersCombined + code + footersCombined;
}

// A utility class for handling nodes of an AST.
function NodeUtils() {
    var self = this;
    // Performs deep copy of the given node.
    this.deepCopy = function(node) {
        return JSON.parse(JSON.stringify(node));
    };

    // Deletes a node from the body.
    this.deleteNode = function(parentBody, nodeToDel) {
        var deleteIndex = parentBody.indexOf(nodeToDel);
        parentBody.splice(deleteIndex, 1);
    };

    // Replaces source node with the target node and returns a reference to the new node.
    this.replaceNode = function(source, target, context) {
        var sourceCopy = self.deepCopy(source);

        Object.keys(source).forEach(function(key) {
            delete source[key];
        });
        Object.keys(target).forEach(function(key) {
            source[key] = target[key];
        });

        // Using this check temporarily.
        // if (!self.hasLocNode(sourceCopy)) {
        // 	return source;
        // }

        // Attach the loc nodes based on the context.
        switch (context) {
            // Mapping of if-else block to for-of loop.
            /*
            	Before:
            	for (var r of res1){...}
            	After:
            	if (res1.isInstance) {
            		res1.iter(function (r) {...}
            	} else {...}
            */
            case Context.IterTypeCheck:
                source.loc = self.deepCopy(sourceCopy.loc);
                source.consequent.loc = self.deepCopy(sourceCopy.body.loc);
                source.test.loc = self.deepCopy(sourceCopy.right.loc);
                self.setLocForAllNodes(sourceCopy.right.loc, source.test.left);
                source.test.right.loc = self.deepCopy(sourceCopy.right.loc);

                // TODO: Currently, after breaking out from labeled break statement, it goes to the beginning of the for-of loop.
                //		Ideally, it should go to the end of the labeled block. This looks quite ideal to show the iteration behaviour -
                //		It stops at the enclosing for-of loops (iterators) before coming out and thus, demonstrating the stopping
                //		of iteration. Need to ask whether this is ok or if the default behaviour is needed.
                if (source.consequent.body.length > 1 && /SwitchStatement/.test(source.consequent.body[1].type)) {
                    self.forceSetLocForAllNodes(sourceCopy.loc, source.consequent.body[1]);
                }
                break;

                // The following case handles two-way mapping of loc nodes between continue and return statements.
            case Context.ContinueStatement:
                source.loc = self.deepCopy(sourceCopy.loc);
                switch (source.type) {
                    // Return to continue statement mapping - source: return, target: continue
                    case 'ContinueStatement':
                        if (source.label) {
                            source.label.loc = self.deepCopy(sourceCopy.loc);
                        }
                        break;

                        // Continue to return statement mapping - source: continue, target: return
                    case 'ReturnStatement':
                        if (source.argument && sourceCopy.label.loc) {
                            source.argument = self.setLocForAllNodes(sourceCopy.label.loc, source.argument);
                        }
                        break;

                    default:
                        throw 'Not yet handled for ' + source.type;
                }
                break;

                // The following case handles two-way mapping of loc nodes between break and return statements.
            case Context.BreakStatement:
                source.loc = self.deepCopy(sourceCopy.loc);
                switch (source.type) {
                    // Return to break statement mapping - source: return, target: break
                    case 'BreakStatement':
                        source.label.loc = self.deepCopy(sourceCopy.argument.loc);
                        break;

                        // Break to return statement mapping - source: break, target: return
                    case 'ReturnStatement':
                        source.argument = self.setLocForAllNodes(sourceCopy.loc, source.argument);
                        break;

                    default:
                        throw 'Not yet handled for ' + source.type;
                }
                break;

                // The following case handles mapping of loc nodes between two different 'stopIter' calls.
                /*
                    Before:
                    return res2.stopIter({
                        'code': 'labeled_break',
                        'args': 'x'
                    });
                    After:
                    return res1.stopIter({
                        'code': 'labeled_break',
                        'args': 'x'
                    });
                */
            case Context.BreakAltInterrupt:
                self.setLocMatchingNodes(sourceCopy, source);
                break;

                // The following case handles the mapping of loc nodes between stopIter and
                // return statement or between two stopIter statements as the above case.
                /*
                    Before:
                    return res2.stopIter({
                            'code': 'labeled_continue',
                            'args': 'x'
                        });
                    After:
                    return;
                 */
            case Context.ContinueAltInterrupt:
                if (source.argument) {
                    self.setLocMatchingNodes(sourceCopy, source);
                } else {
                    source.loc = sourceCopy.loc;
                }
                break;
        }

        return source;
    };

    // Checks if atleast one loc node is present in the AST.
    this.hasLocNode = function(ast) {
        var hasLoc = false;
        estraverse.traverse(ast, {
            enter: function(node) {
                if (hasLoc) {
                    return;
                }

                hasLoc = node.loc;
            }
        });

        return hasLoc;
    };

    // Adds loc node for all the nodes in the AST.
    // Thus, all the nodes of AST might end up having unnecessary loc nodes.
    // Though this method wouldn't modify the parsing behaviour, it must be used as a last resort.
    this.forceSetLocForAllNodes = function(loc, ast) {
        estraverse.traverse(ast, {
            enter: function(node) {
                if (!node.loc) {
                    node.loc = self.deepCopy(loc);
                }
            }
        });
    };

    // This is a safe method for adding loc nodes for a given AST.
    // The disadvantage is that it can not be used with all the AST.
    this.setLocForAllNodes = function(loc, ast) {
        // Generate the code snippet for the given AST.
        var codeSnippet = escodegen.generate(ast);
        // Parse with loc enabled to determine the nodes to which we can attach loc node.
        var astWithLoc = esprima.parse(codeSnippet, {
            loc: true
        });

        // We now traverse astWithLoc and replace all the loc nodes.
        estraverse.traverse(astWithLoc, {
            enter: function(node) {
                node.loc = node.loc ? self.deepCopy(loc) : null;
            }
        });

        return astWithLoc.body[0].expression;
    };

    // This is a safe method for performing a one-to-one copy of the loc nodes from AST1 to AST2.
    // The two ASTs must have the same structure.
    this.setLocMatchingNodes = function(source, target) {
        var sourceNodeStack = new Stack(),
            targetNodeStack = new Stack();

        // Linearizes the given AST into a stack.
        function convertTreeToStack(ast, stack) {
            estraverse.traverse(ast, {
                enter: function(node) {
                    stack.push(node);
                }
            });
        }

        convertTreeToStack(source, sourceNodeStack);
        convertTreeToStack(target, targetNodeStack);

        // Pop all nodes from the sourceNodeStack and if an element contains loc node,
        // copy it to the corresponding element in the targetNodeStack.
        while (!sourceNodeStack.isEmpty()) {
            var sourceNode = sourceNodeStack.pop();
            var targetNode = targetNodeStack.pop();
            if (sourceNode.loc) {
                targetNode.loc = self.deepCopy(sourceNode.loc);
            }
        }
    };

    // Inserts the given node to the given parentBody at the specified index.
    this.insertNode = function(parentBody, refNode, nodeToInsert, insertAfter) {
        var insertIndex = insertAfter ? parentBody.indexOf(refNode) + 1 : parentBody.indexOf(refNode);
        parentBody.splice(insertIndex, 0, nodeToInsert);
    };

    this.convertToBlockStmt = function(node) {
        switch (node.type) {
            case 'ForOfStatement':
                // Transform the previous single-line statement into a block.
                node.body.body = [self.deepCopy(node.body)];
                node.body.type = 'BlockStatement';
                break;
            case 'IfStatement':
                node.consequent.body = [self.deepCopy(node.consequent)];
                node.consequent.type = 'BlockStatement';
                // If the 'else' part exists, convert it to a block statement.
                if (node.alternate) {
                    node.alternate.body = [self.deepCopy(node.alternate)];
                    node.alternate.type = 'BlockStatement';
                }
                break;
            default:
                throw 'unhandled case for: ' + node.type;
        }
    };

    // Inserts an array of AST nodes into parentBody at the specified index.
    this.insertNodeArray = function(parentBody, insAfterNode, arrayToInsert) {
        var insertIndex = parentBody.indexOf(insAfterNode) + 1;
        parentBody.splice.apply(parentBody, [insertIndex, 0].concat(arrayToInsert));
    };

    // Checks if the global scope contains only function declarations.
    this.checkGlobals = function(ast) {
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
        }
    };

    this.checkForOfNodeRight = function(ast) {
        estraverse.traverse(ast, {
            leave: function(node) {
                if (/ForOfStatement/.test(node.type)) {
                    if (!/MemberExpression/.test(node.right.type) && !/Identifier/.test(node.right.type)) {
                        throw {
                            index: node.range[0],
                            lineNumber: node.loc.start.line,
                            column: node.loc.start.column,
                            description: `Only identifier or member expression is allowed for for-of loop. ${node.right.type} is not allowed`
                        };
                    }
                }
            }
        });
    };
}

// A general purpose stack.
function Stack() {
    var stack = [],
        nodeUtils = new NodeUtils();

    this.stackCopy = stack;

    this.push = function(item) {
        stack.push(item);
    };

    this.pop = function() {
        if (stack.length === 0) {
            throw 'Stack underflow exception';
        }
        return stack.pop();
    };

    this.peek = function() {
        var size = this.getSize();
        if (size >= 0) {
            return stack[size - 1];
        }
    };

    this.getSize = function() {
        return stack.length;
    };

    this.cloneStack = function() {
        var clone = new Stack();

        for (var item of stack) {
            clone.push(nodeUtils.deepCopy(item));
        }

        return clone;
    };

    this.reverseElements = function() {
        stack.reverse();
    };

    this.isEmpty = function() {
        return stack.length === 0;
    };

    // debug.
    this.printAll = function() {
        for (var item of stack) {}
    }
}

// A sub-class of Stack. Its purpose it to maintain the ancestral information
// of nodes : node-parent relationship.
function AncestorStack() {
    Stack.call(this);
    var nodeUtils = new NodeUtils();

    // Returns or pops a node that satisfies the comparator.
    function getOrPopAncestor(_this, comparator, pop) {
        var tempStack = new Stack(),
            found = false;

        // Run through all the elements in the stack against the comparator and break out once the element is found.
        while (_this.getSize() > 0 && !found) {
            var node = _this.pop();
            tempStack.push(node);
            found = comparator(nodeUtils.deepCopy(node));
        }

        if (pop) {
            tempStack.pop();
        }

        // Restore everything back to the stack.
        while (tempStack.getSize() > 0) {
            _this.push(tempStack.pop());
        }

        return found ? node : null;
    }

    this.getAncestor = function(comparator) {
        return getOrPopAncestor(this, comparator, false);
    };

    this.popNode = function(comparator) {
        return getOrPopAncestor(this, comparator, true);
    };

    // Returns the topmost node of the given type. Need not necessarily be the top of stack.
    this.getTopNodeOfType = function(nodeType) {
        return this.getAncestor(function(node) {
            return nodeType === node.type;
        });
    };

    this.popTopNodeOfType = function(nodeType) {
        return this.popNode(function(node) {
            return nodeType === node.type;
        });
    };

    this.cloneAncestorStack = function() {
        var clone = new AncestorStack();
        // Clone the stack.
        var stackClone = this.cloneStack();

        while (stackClone.getSize() > 0) {
            clone.push(stackClone.pop());
        }

        // Reverse the stack elements.
        clone.reverseElements();

        return clone;
    };
}

// Class to maintain the association of loop modifiers - break, continue, return etc. with JavaScript loops.
function LoopModifier(modifier) {
    var ancestorStack = new AncestorStack();
    this.modType = modifier;
    this.stackIndex = -1;

    // debug.
    this.stackCopy = ancestorStack.stackCopy;

    // Initializing association information.
    // Obtained from JavaScript reference - https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference
    var associations = new Set();
    switch (this.modType) {
        case LoopModifier.CONST.BREAK:
            associations.add('DoWhileStatement');
            associations.add('ForStatement');
            associations.add('ForInStatement');
            associations.add('ForOfStatement');
            associations.add('SwitchStatement');
            associations.add('WhileStatement');
            associations.add('LabeledStatement');
            break;

        case LoopModifier.CONST.CONTINUE:
            associations.add('DoWhileStatement');
            associations.add('ForStatement');
            associations.add('ForInStatement');
            associations.add('ForOfStatement');
            associations.add('SwitchStatement');
            associations.add('WhileStatement');
            break;

        case LoopModifier.CONST.LABELED_CONTINUE:
        case LoopModifier.CONST.LABELED_BREAK:
            associations.add('LabeledStatement');
            break;

        case LoopModifier.CONST.RETURN:
            associations.add('FunctionDeclaration');
            associations.add('FunctionExpression');
            break;

        default:
            throw 'Invalid modifier';
    }

    this.checkAssoc = function(nodeType) {
        return associations.has(nodeType);
    };

    // Push the node if it associates with the loop modifier instance.
    this.pushIfAssoc = function(node) {
        if (this.checkAssoc(node.type)) {
            switch (this.modType) {
                case LoopModifier.CONST.BREAK:
                    node.breakStackIndex = this.stackIndex;
                    break;

                case LoopModifier.CONST.CONTINUE:
                    node.continueStackIndex = this.stackIndex;
                    break;

                case LoopModifier.CONST.LABELED_BREAK:
                    node.lblBreakStackIndex = this.stackIndex;
                    break;

                case LoopModifier.CONST.RETURN:
                    node.returnStackIndex = this.stackIndex;
                    break;

                case LoopModifier.CONST.LABELED_CONTINUE:
                    node.lblContinueStackIndex = this.stackIndex;
                    break;

                default:
                    throw 'Invalid modifier type';
            }

            ancestorStack.push(node);
        }
    };

    this.popIfAssoc = function() {
        if (ancestorStack.getSize() > 0) {
            switch (this.modType) {
                case LoopModifier.CONST.BREAK:
                    if (this.stackIndex === ancestorStack.peek().breakStackIndex) {
                        return ancestorStack.pop();
                    }
                    break;

                case LoopModifier.CONST.CONTINUE:
                    if (this.stackIndex === ancestorStack.peek().continueStackIndex) {
                        return ancestorStack.pop();
                    }
                    break;

                case LoopModifier.CONST.LABELED_BREAK:
                    if (this.stackIndex === ancestorStack.peek().lblBreakStackIndex) {
                        return ancestorStack.pop();
                    }
                    break;

                case LoopModifier.CONST.RETURN:
                    if (this.stackIndex === ancestorStack.peek().returnStackIndex) {
                        return ancestorStack.pop();
                    }
                    break;

                case LoopModifier.CONST.LABELED_CONTINUE:
                    if (this.stackIndex === ancestorStack.peek().lblContinueStackIndex) {
                        return ancestorStack.pop();
                    }
                    break;

                default:
                    throw 'Invalid modifier type';
            }
        }
    };


    this.getSize = function() {
        return ancestorStack.getSize();
    };

    // Returns a boolean suggesting whether the loop modifier needs to be replaced.
    this.isReplaceReq = function(args) {
        switch (this.modType) {
            // For break and continue, the replacement criteria is the for-of node being the parent on TOS.
            case LoopModifier.CONST.CONTINUE:
            case LoopModifier.CONST.BREAK:
                return ancestorStack.getSize() > 0 && /ForOfStatement/.test(ancestorStack.peek().type);

            case LoopModifier.CONST.LABELED_CONTINUE:
                // For labelled break, the replacement criteria is the absence of the label which the break is
                // associated with.
            case LoopModifier.CONST.LABELED_BREAK:
                return !ancestorStack.getAncestor(function(node) {
                    if (/LabeledStatement/.test(node.type)) {
                        return args === node.label.name;
                    }
                });

                // For return statement, the replacement criteria is the absence of a function on TOS.
            case LoopModifier.CONST.RETURN:
                if (ancestorStack.getSize() === 0) {
                    return true;
                }
                return !(/FunctionDeclaration/.test(ancestorStack.peek().type) ||
                    /FunctionExpression/.test(ancestorStack.peek().type));

            default:
                throw 'Invalid modifier type';
        }
    };

    // debug.
    this.printAll = function() {
        ancestorStack.printAll();
    }
}

// Utilities for AncestorStack
function StackHelper(ancestorStack) {
    var nodeUtils = new NodeUtils();
    this.ancestorStack = ancestorStack.cloneAncestorStack();

    this.getTopForOfNode = function() {
        return this.ancestorStack.getTopNodeOfType('ForOfStatement');
    };

    this.popTopForOfNode = function() {
        return this.ancestorStack.popTopNodeOfType('ForOfStatement');
    };

    // Targeted search - search the stack for the target, but stop and return if the stop-condition is met.
    // Comparator must define targetComparator and stopComparator and each of them should return a boolean.
    // Setting searchAll to true will return all the stopNodes till the targetNode is encountered.
    function search(_this, comparator, searchAll) {
        var tempStack = new Stack(),
            stopNodes = [],
            returnArgs = {
                targetFound: false
            };

        while (_this.ancestorStack.getSize() > 0) {
            var node = _this.ancestorStack.pop();
            tempStack.push(node);

            if (comparator.targetComparator(nodeUtils.deepCopy(node))) {
                returnArgs = {
                    targetFound: true,
                    stopNode: nodeUtils.deepCopy(node),
                    searchInterrupted: false
                };
                break;
            } else if (comparator.stopComparator(nodeUtils.deepCopy(node))) {
                returnArgs = {
                    targetFound: false,
                    stopNode: nodeUtils.deepCopy(node),
                    searchInterrupted: true
                };
                if (searchAll) {
                    stopNodes.push(returnArgs.stopNode);
                } else {
                    break;
                }
            }
        }

        // Restore the elements back to the stack.
        while (tempStack.getSize() > 0) {
            _this.ancestorStack.push(tempStack.pop());
        }

        // A check to validate that targetFound and searchInterrupted are mutually exclusive.
        if (returnArgs.targetFound && returnArgs.searchInterrupted) {
            throw 'Invalid case: targetFound=' + returnArgs.targetFound +
                '\tsearchInterrupted=' + returnArgs.searchInterrupted;
        }

        if (searchAll && stopNodes.length > 0) {
            returnArgs = {
                targetFound: false,
                stopNodes: stopNodes,
                searchInterrupted: true
            };
        }

        return returnArgs;
    }

    this.searchStack = function(comparator) {
        return search(this, comparator);
    };

    this.searchAllStopNodes = function(comparator) {
        return search(this, comparator, true);
    }
}

// Data types JavaScript AST nodes - http://esprima.readthedocs.io/en/latest/syntax-tree-format.html
function Ast(type) {
    this.type = type;
}

function SwitchAst(discriminantAst) {
    Ast.call(this, 'SwitchStatement');
    this.discriminant = discriminantAst;
    this.cases = [];
}

function CaseAst(testStr) {
    Ast.call(this, 'SwitchCase');
    this.test = {
        "type": "Literal",
        "value": testStr,
        "raw": String.raw `${testStr}`
    };
    this.consequent = [];
}

function BreakAst() {
    Ast.call(this, 'BreakStatement');
    this.label = null;
}

function LabeledStmtAst(label, body) {
    Ast.call(this, 'LabeledStatement');
    this.label = {
        "type": "Identifier",
        "name": label
    };
    this.body = body;
}

function LabeledBreakAst(label) {
    BreakAst.call(this);
    this.label = {
        "type": "Identifier",
        "name": label
    };
}

function ContinueAst() {
    Ast.call(this, 'ContinueStatement');
    this.label = null;
}

function LabeledContinueAst(label) {
    ContinueAst.call(this);
    this.label = {
        "type": "Identifier",
        "name": label
    };
}

function ReturnAst(argument) {
    Ast.call(this, 'ReturnStatement');
    this.argument = argument;
}

function StopIterAst(inst) {
    Ast.call(this, 'CallExpression');
    this.callee = {
        "type": "MemberExpression",
        "computed": false,
        "object": {
            "type": "Identifier",
            "name": inst
        },
        "property": {
            "type": "Identifier",
            "name": "stopIter"
        }
    };
    this.arguments = [];
}

function IterTypeCheckAst(objAst) {
    Ast.call(this, 'BinaryExpression');
    this.operator = 'instanceof';
    this.right = {
        "type": "Identifier",
        "name": "N1qlQuery"
    };
    this.left = objAst;
}

function IfElseAst(IterTypeCheckAst) {
    Ast.call(this, 'IfStatement');
    this.test = IterTypeCheckAst;
    this.consequent = {
        "type": "BlockStatement",
        "body": []
    };
    this.alternate = {
        "type": "BlockStatement",
        "body": []
    };
}

// Returns AST of the form 'iterVar'.'prop()'.data
function ReturnDataAst(iterVar, prop) {
    Ast.call(this, 'ExpressionStatement');
    this.expression = {
        "type": "MemberExpression",
        "computed": false,
        "object": {
            "type": "CallExpression",
            "callee": {
                "type": "MemberExpression",
                "computed": false,
                "object": {
                    "type": "Identifier",
                    "name": iterVar
                },
                "property": {
                    "type": "Identifier",
                    "name": prop
                }
            },
            "arguments": []
        },
        "property": {
            "type": "Identifier",
            "name": "data"
        }
    };
}

// Returns AST of the form -
/*
	res.iter(function(row){...});
 */
function IteratorSkeletonAst(iterVar, arg) {
    Ast.call(this, 'ExpressionStatement');
    this.expression = {
        "type": "CallExpression",
        "callee": {
            "type": "MemberExpression",
            "computed": false,
            "object": {
                "type": "Identifier",
                "name": iterVar
            },
            "property": {
                "type": "Identifier",
                "name": "iter"
            }
        },
        "arguments": [{
            "type": "FunctionExpression",
            "id": null,
            "params": [{
                "type": "Identifier",
                "name": arg
            }],
            "body": {
                "type": "BlockStatement",
                "body": []
            },
            "generator": false,
            "expression": false,
            "async": false
        }]
    }
}

function BlockStatementAst(body) {
    Ast.call(this, 'BlockStatement');
    this.body = [body];
}

function N1QLQueryStmtAst(exprAst) {
    Ast.call(this, 'ExpressionStatement');
    this.expression = exprAst;
}

function N1QLQueryExprAst(query, namedParams) {
    Ast.call(this, 'NewExpression');
    this.callee = {
        "type": "Identifier",
        "name": "N1qlQuery"
    };
    this.arguments = [{
        "type": "Literal",
        "value": query
    }, {
        "type": "ObjectExpression",
        "properties": [{
            "type": "Property",
            "key": {
                "type": "Identifier",
                "name": "namedParams"
            },
            "computed": false,
            "value": {
                "type": "ObjectExpression",
                "properties": []
            },
            "kind": "init",
            "method": false,
            "shorthand": false
        }]
    }];

    for (var param of namedParams) {
        this.arguments[1].properties[0].value.properties.push({
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

function CallExecQueryAst(object) {
    Ast.call(this, 'CallExpression');
    this.callee = {
        "type": "MemberExpression",
        "computed": false,
        "object": object,
        "property": {
            "type": "Identifier",
            "name": "execQuery"
        }
    };

    this.arguments = [];
}

// Class for maintaining the object that will be passed to 'stopIter'.
function Arg(arg) {
    this.code = arg.code;
    this.args = arg.args;

    this.getAst = function() {
        // Need to wrap 'arg' inside '()' to turn it into a statement - it becomes a JSON object otherwise.
        var argsAst = esprima.parse('(' + this.toString() + ')').body[0].expression;

        // Setting appendData to 'true' will generate the AST for 'args' and append it to 'argsAst'.
        if (arg.appendData) {
            var dataAst = {
                "type": "Property",
                "key": {
                    "type": "Literal",
                    "value": "data",
                    "raw": "'data'"
                },
                "computed": false,
                "value": this.getDataAst(),
                "kind": "init",
                "method": false,
                "shorthand": false
            };

            argsAst.properties.push(dataAst);
        }

        return argsAst;
    };

    // Returns the AST for 'args'.
    this.getDataAst = function() {
        if (!this.args) {
            throw '"args" field is needed to add "data" field';
        }

        return esprima.parse(this.args).body[0].expression;
    };

    // Stringify only the attributes of this class.
    this.toString = function() {
        var obj = {};
        for (var key of Object.keys(this)) {
            if (this.hasOwnProperty(key)) {
                obj[key] = this[key];
            }
        }

        return JSON.stringify(obj);
    }
}

// Class to generate post iteration steps - switch-case block.
function PostIter(iterProp, returnBubbleFunc) {
    var stmts = [];

    this.iterProp = iterProp;
    this.returnBubbleFunc = returnBubbleFunc;

    this.push = function(arg) {
        // Avoid duplicates while insertion.
        // TODO :   replace the list with a set in the final version - for faster lookup.
        if (stmts.indexOf(arg) === -1) {
            stmts.push(arg);
        }
    };

    // Returns a switch-case block to perform post-iteration steps.
    this.getAst = function(iterVar, stackHelper) {
        var discriminantAst = esprima.parse(iterVar + '.' + this.iterProp).body[0].expression,
            switchAst = new SwitchAst(discriminantAst),
            postIter, caseAst, lookup, stopIterAst, arg, returnStmtAst, pushCase;

        // Loop over all the stmts and generate the corresponding 'case' block.
        for (var postIterStmt of stmts) {
            pushCase = true;
            // TODO :   Changing 'var postIter' to 'const postIter' causes a unit test to fail. Investigate this issue.
            postIter = JSON.parse(postIterStmt);
            caseAst = new CaseAst(postIter.code + postIter.args);

            switch (postIter.code) {
                case LoopModifier.CONST.BREAK:
                case LoopModifier.CONST.CONTINUE:
                    break;
                case LoopModifier.CONST.LABELED_BREAK:
                    // Search the ancestor stack for the label. Interrupt the search if a for-of node is found.
                    lookup = stackHelper.searchStack({
                        targetComparator: function(node) {
                            return /LabeledStatement/.test(node.type) && node.label.name === postIter.args;
                        },
                        stopComparator: function(node) {
                            return /ForOfStatement/.test(node.type);
                        }
                    });
                    // If the label is found and doesn't point to the for-of node, then add a break <label>.
                    if (lookup.targetFound) {
                        if (/ForOfStatement/.test(lookup.stopNode.body.type)) {
                            pushCase = false;
                        } else {
                            caseAst.consequent.push(new LabeledBreakAst(postIter.args));
                        }
                    }
                    // If the search was interrupted, then it means that it encountered a for-of node. So, add a
                    // 'return stopIter' node.
                    if (lookup.searchInterrupted) {
                        stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                        arg = new Arg({
                            code: LoopModifier.CONST.LABELED_BREAK,
                            args: postIter.args
                        });
                        returnStmtAst = new ReturnAst(stopIterAst);
                        stopIterAst.arguments.push(arg.getAst());

                        // Annotate the node as it is subjected to change in the next iteration.
                        returnStmtAst.isAnnotated = true;
                        returnStmtAst.metaData = postIter;

                        caseAst.consequent.push(returnStmtAst);
                    }
                    break;
                case LoopModifier.CONST.LABELED_CONTINUE:
                    // This is very similar to Labeled break case.
                    lookup = stackHelper.searchStack({
                        targetComparator: function(node) {
                            return /LabeledStatement/.test(node.type) && node.label.name === postIter.args;
                        },
                        stopComparator: function(node) {
                            return /ForOfStatement/.test(node.type);
                        }
                    });
                    if (lookup.targetFound) {
                        if (/ForOfStatement/.test(lookup.stopNode.body.type)) {
                            pushCase = false;
                        } else {
                            caseAst.consequent.push(new LabeledContinueAst(postIter.args));
                        }
                    }
                    if (lookup.searchInterrupted) {
                        if (lookup.stopNode.parentLabel === postIter.args) {
                            returnStmtAst = new ReturnAst(null);
                        } else {
                            stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                            arg = new Arg({
                                code: LoopModifier.CONST.LABELED_CONTINUE,
                                args: postIter.args
                            });
                            returnStmtAst = new ReturnAst(stopIterAst);
                            stopIterAst.arguments.push(arg.getAst());
                        }

                        returnStmtAst.isAnnotated = true;
                        returnStmtAst.metaData = postIter;

                        caseAst.consequent.push(returnStmtAst);
                    }
                    break;
                case LoopModifier.CONST.RETURN:
                    // Target is to find the function name that binds to the 'return' node. Search is interrupted by
                    // a for-of node.
                    lookup = stackHelper.searchStack({
                        targetComparator: function(item) {
                            return (/FunctionDeclaration/.test(item.type) || /FunctionExpression/.test(item.type)) &&
                                (item.id ? item.id.name : null) === postIter.targetFunction;
                        },
                        stopComparator: function(item) {
                            return /ForOfStatement/.test(item.type);
                        }
                    });
                    if (lookup.targetFound) {
                        returnStmtAst = new ReturnAst(new ReturnDataAst(postIter.iterVar, this.returnBubbleFunc));
                    }
                    if (lookup.searchInterrupted) {
                        stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                        arg = new Arg({
                            code: LoopModifier.CONST.RETURN,
                            args: postIter.iterVar + '.' + this.returnBubbleFunc + '().data',
                            appendData: true
                        });
                        stopIterAst.arguments.push(arg.getAst());
                        returnStmtAst = new ReturnAst(stopIterAst);

                        returnStmtAst.isAnnotated = true;
                        postIter.args = arg.args;
                        returnStmtAst.metaData = postIter;
                    }
                    caseAst.consequent.push(returnStmtAst);
                    break;
            }

            if (pushCase) {
                switchAst.cases.push(caseAst);
            }
        }

        return switchAst.cases.length > 0 ? switchAst : null;
    }
}

function Iter(forOfNode) {
    var _this = this,
        breakMod = new LoopModifier(LoopModifier.CONST.BREAK),
        continueMod = new LoopModifier(LoopModifier.CONST.CONTINUE),
        lblBreakMod = new LoopModifier(LoopModifier.CONST.LABELED_BREAK),
        returnMod = new LoopModifier(LoopModifier.CONST.RETURN),
        lblContinueMod = new LoopModifier(LoopModifier.CONST.LABELED_CONTINUE),
        nodeUtils = new NodeUtils();

    this.nodeCopy = nodeUtils.deepCopy(forOfNode);

    this.incrAndPush = function(node) {
        ++breakMod.stackIndex;
        ++continueMod.stackIndex;
        ++lblBreakMod.stackIndex;
        ++returnMod.stackIndex;
        ++lblContinueMod.stackIndex;

        breakMod.pushIfAssoc(node);
        continueMod.pushIfAssoc(node);
        lblBreakMod.pushIfAssoc(node);
        returnMod.pushIfAssoc(node);
        lblContinueMod.pushIfAssoc(node);
    };

    this.decrAndPop = function() {
        breakMod.popIfAssoc();
        continueMod.popIfAssoc();
        lblBreakMod.popIfAssoc();
        returnMod.popIfAssoc();
        lblContinueMod.popIfAssoc();

        --breakMod.stackIndex;
        --continueMod.stackIndex;
        --lblBreakMod.stackIndex;
        --returnMod.stackIndex;
        --lblContinueMod.stackIndex;
    };

    this.traverse = function(traversal) {
        estraverse.traverse(_this.nodeCopy, {
            enter: function(node) {
                traversal(node, _this.nodeCopy, breakMod, continueMod, lblBreakMod, lblContinueMod, returnMod);
            },
            leave: function(node) {
                _this.decrAndPop();
            }
        });
    };

    // debug.
    this.assertEmpty = function() {}
}

// Returns if-else AST having iterator in consequent and for-of in alternate (dynamic type checking).
function IterCompatible(forOfNode, globalAncestorStack) {
    var self = this,
        stackHelper = new StackHelper(globalAncestorStack),
        nodeUtils = new NodeUtils();

    // Returns an iterator construct for a given for-of loop ast.
    this.getIterConsequentAst = function() {
        var iterator = new Iter(forOfNode);

        // This is the property that will be set on the N1qlQuery instance - contains return value of iterator.
        var iterProp = 'getReturnValue(true)',
            returnBubbleFunc = 'getReturnValue';

        // List to store post iteration exit statements.
        var postIter = new PostIter(iterProp, returnBubbleFunc);

        iterator.traverse(function(node, nodeCopy, breakMod, continueMod, lblBreakMod, lblContinueMod, returnMod) {
            iterator.incrAndPush(node);

            var arg,
                stopIterAst,
                returnStmtAst;

            // Annotated nodes are those nodes that have been marked to be changed by the previous iteration.
            if (node.isAnnotated) {
                switch (node.metaData.code) {
                    case LoopModifier.CONST.RETURN:
                        // For 'return', the 'iterVar' must be set to the current for-of loop's source.
                        node.metaData.iterVar = nodeCopy.right.name;
                        arg = JSON.stringify(node.metaData);
                        break;

                    case LoopModifier.CONST.LABELED_BREAK:
                    case LoopModifier.CONST.LABELED_CONTINUE:
                        arg = new Arg({
                            code: node.metaData.code,
                            args: node.metaData.args
                        });
                        break;

                    default:
                        throw 'Unhandled case: ' + node.metaData.code;
                }

                postIter.push(arg.toString());

                // Remove the annotation if it's already present.
                // This is needed to prevent the else-block from replacing the already annotated node.
                delete node.isAnnotated;
                delete node.metaData;
            }

            if (node.isGen) {
                return;
            }

            // If any of the exit criteria is encountered, then that statement may be replaced.
            switch (node.type) {
                case 'BreakStatement':
                    stopIterAst = arg = null;
                    // Labeled break statement.
                    /*
                    	Before:
                    	break x;
                    	After:
                    	return res.stopIter({
                    		'code': 'labeled_break',
                    		'args': 'x'
                    	});
                    */
                    if (node.label && lblBreakMod.isReplaceReq(node.label.name)) {
                        stopIterAst = new StopIterAst(nodeCopy.right.name);
                        arg = new Arg({
                            code: LoopModifier.CONST.LABELED_BREAK,
                            args: node.label.name
                        });
                        postIter.push(arg.toString());
                    } else if (!node.label && breakMod.isReplaceReq()) {
                        // Unlabeled break statement.
                        /*
                        	Before:
                        	break;
                        	After:
                        	return res.stopIter({ 'code': 'break' });
                         */
                        stopIterAst = new StopIterAst(nodeCopy.right.name);
                        arg = new Arg({
                            code: LoopModifier.CONST.BREAK
                        });
                    }

                    if (stopIterAst && arg) {
                        returnStmtAst = new ReturnAst(stopIterAst);
                        // Add 'arg' as the argument to 'stopIter()'.
                        stopIterAst.arguments.push(arg.getAst());
                        nodeUtils.replaceNode(node, returnStmtAst, Context.BreakStatement);
                    }
                    break;

                case 'ContinueStatement':
                    // Labeled continue statement.
                    /*
                    	Before:
                    	continue x;
                    	After:
                    	return res.stopIter({
                    		'code': 'labeled_continue',
                    		'args': 'x'
                    	});
                     */
                    if (node.label && lblContinueMod.isReplaceReq(node.label.name)) {
                        if (nodeCopy.parentLabel === node.label.name) {
                            // If the target of labeled continue is its immediate parent, then just 'return'.
                            returnStmtAst = new ReturnAst(null);
                        } else {
                            arg = new Arg({
                                code: LoopModifier.CONST.LABELED_CONTINUE,
                                args: node.label.name
                            });
                            stopIterAst = new StopIterAst(nodeCopy.right.name);
                            returnStmtAst = new ReturnAst(stopIterAst);
                            stopIterAst.arguments.push(arg.getAst());

                            postIter.push(arg.toString());
                        }

                        nodeUtils.replaceNode(node, returnStmtAst, Context.ContinueStatement);
                    } else if (continueMod.isReplaceReq()) {
                        // Unlabeled continue statement.
                        /*
                        	Before:
                        	continue;
                        	After:
                        	return;
                         */
                        nodeUtils.replaceNode(node, new ReturnAst(null), Context.ContinueStatement);
                    }
                    break;

                case 'ReturnStatement':
                    /*
                    	Before:
                    	return a + b;
                    	After:
                    	return res.stopIter({
                    		'code': 'return',
                    		'args': '(a + b)',
                    		'data': a + b
                    	});
                     */
                    if (returnMod.isReplaceReq(node)) {
                        // Return statement may or may not have arguments.
                        // In case there's no argument, we populate it with null.
                        var argStr = node.argument ? escodegen.generate(node.argument) : null;
                        // Must enclose the return statement's argument within an expression '()'.
                        // Otherwise, it causes an error when returning anonymous function.
                        arg = new Arg({
                            code: LoopModifier.CONST.RETURN,
                            args: '(' + argStr + ')',
                            appendData: true
                        });
                        stopIterAst = new StopIterAst(nodeCopy.right.name);
                        stopIterAst.arguments.push(arg.getAst());
                        returnStmtAst = new ReturnAst(stopIterAst);
                        self.mapSourceNode(node, returnStmtAst, Context.ReturnStatement);

                        var postIterArgs = JSON.stringify({
                            code: LoopModifier.CONST.RETURN,
                            args: arg.args,
                            iterVar: nodeCopy.right.name,
                            targetFunction: node.targetFunction
                        });

                        postIter.push(postIterArgs);
                        nodeUtils.replaceNode(node, returnStmtAst);
                    }
                    break;

                case 'IfStatement':
                    if (!/BlockStatement/.test(node.consequent.type)) {
                        nodeUtils.convertToBlockStmt(node);
                    }
                    break;
            }
        });

        var iter = new IteratorSkeletonAst(forOfNode.right.name, (forOfNode.left.name ? forOfNode.left.name : forOfNode.left.declarations[0].id.name));
        iter.expression.arguments[0].body = iterator.nodeCopy.body;
        self.mapSourceNode(forOfNode, iter, Context.IterConsequent);

        var iterBlockAst = new BlockStatementAst(iter);

        // Pop the top for-of node.
        stackHelper.popTopForOfNode();

        var postIterAst = postIter.getAst(forOfNode.right.name, stackHelper);
        if (postIterAst) {
            iterBlockAst.body.push(postIterAst);
        }

        iterator.assertEmpty();

        return iterBlockAst;
    };

    // Maps loc nodes of source to target according to the context.
    this.mapSourceNode = function(source, target, context) {
        switch (context) {
            // Maps the source to target loc during the following kind of transformation -
            /*
            	Before:
            	for (var r of res3){...}
            	After:
            	res.iter(function (r) {...}
             */
            case Context.IterConsequent:
                target.loc = nodeUtils.deepCopy(source.loc);
                target.expression.loc = nodeUtils.deepCopy(source.loc);
                target.expression.callee.loc = nodeUtils.deepCopy(source.loc);
                target.expression.callee.object.loc = nodeUtils.deepCopy(source.right.loc);
                target.expression.callee.property.loc = nodeUtils.deepCopy(source.right.loc);
                target.expression.arguments[0].loc = nodeUtils.deepCopy(source.right.loc);
                target.expression.arguments[0].params[0].loc = nodeUtils.deepCopy(source.left.declarations[0].id.loc);
                break;

                // Maps the source to target loc during the following kind of transformation -
                /*
                    source: return function () {
                        return inner;
                    };
                    target: return res1.stopIter({
                        'code': 'return',
                        'args': '(function () {\n    return inner;\n})',
                        'data': function () {
                            return inner;
                        }
                    });
                 */
            case Context.ReturnStatement:
                target.loc = source.loc;
                nodeUtils.forceSetLocForAllNodes(source.loc, target.argument);
                if (source.argument) {
                    for (var prop of target.argument.arguments[0].properties) {
                        if (prop.key.value === 'data') {
                            prop.value = nodeUtils.deepCopy(source.argument);
                            break;
                        }
                    }
                }
                break;

                // Maps the source to target loc during the following kind of transformation -
                /*
                    source: return res.stopIter({
                        'code': 'return',
                        'args': 'res.getReturnValue().data',
                        'data': res.getReturnValue().data
                    });
                    target: return res.getReturnValue().data;
                 */
            case Context.ReturnAltFound:
                target.loc = source.loc;
                for (var prop of source.argument.arguments[0].properties) {
                    if (prop.key.value === 'data') {
                        target.argument = nodeUtils.deepCopy(prop.value);
                        break;
                    }
                }
                break;

                // Maps the source to target loc during the following kind of transformation -
                /*
                    source: return res1.stopIter({
                        'code': 'return',
                        'args': 'res.getReturnValue().data',
                        'data': res.getReturnValue().data
                    });
                    target: return res2.stopIter({
                        'code': 'return',
                        'args': 'res.getReturnValue().data',
                        'data': res.getReturnValue().data
                    });
                 */
            case Context.ReturnAltInterrupt:
                nodeUtils.setLocMatchingNodes(source, target);
                break;
            default:
                throw 'Unhandled case: ' + context;
        }
    };

    // Returns AST for 'else' block.
    this.getIterAlternateAst = function() {
        var iterator = new Iter(forOfNode);
        iterator.traverse(function(node, nodeCopy, breakMod, continueMod, lblBreakMod, lblContinueMod, returnMod) {
            var lookup, stopIterAst, arg, returnStmtAst, stopNode = null,
                context;

            if (node.isAnnotated) {
                // Targeted lookup for annotated nodes.
                lookup = stackHelper.searchStack({
                    targetComparator: function(item) {
                        switch (node.metaData.code) {
                            case LoopModifier.CONST.RETURN:
                                // For a 'return' statement, the target is to find the function that the 'return'
                                // statement was associated with, before transpilation.
                                return (/FunctionDeclaration/.test(item.type) || /FunctionExpression/.test(item.type)) &&
                                    item.id.name === node.metaData.targetFunction;
                            case LoopModifier.CONST.LABELED_CONTINUE:
                            case LoopModifier.CONST.LABELED_BREAK:
                                return /LabeledStatement/.test(item.type) && item.label.name === node.metaData.args;
                            default:
                                throw 'Unhandled case: ' + node.metaData.code;
                        }
                    },
                    stopComparator: function(item) {
                        return /ForOfStatement/.test(item.type);
                    }
                });
                if (lookup.targetFound) {
                    switch (node.metaData.code) {
                        case LoopModifier.CONST.LABELED_BREAK:
                            nodeUtils.replaceNode(node, new LabeledBreakAst(node.metaData.args), Context.BreakStatement);
                            break;
                        case LoopModifier.CONST.LABELED_CONTINUE:
                            nodeUtils.replaceNode(node, new LabeledContinueAst(node.metaData.args), Context.ContinueStatement);
                            break;
                        case LoopModifier.CONST.RETURN:
                            arg = new Arg({
                                code: LoopModifier.CONST.RETURN,
                                args: node.metaData.args,
                                appendData: true
                            });
                            returnStmtAst = new ReturnAst(arg.getDataAst());
                            self.mapSourceNode(node, returnStmtAst, Context.ReturnAltFound);
                            nodeUtils.replaceNode(node, returnStmtAst);
                            break;
                    }
                }
                if (lookup.searchInterrupted) {
                    switch (node.metaData.code) {
                        case LoopModifier.CONST.LABELED_BREAK:
                            stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                            arg = new Arg({
                                code: node.metaData.code,
                                args: node.metaData.args
                            });
                            stopIterAst.arguments.push(arg.getAst());
                            returnStmtAst = new ReturnAst(stopIterAst);
                            context = Context.BreakAltInterrupt;
                            break;

                        case LoopModifier.CONST.LABELED_CONTINUE:
                            if (lookup.stopNode.parentLabel === node.metaData.args) {
                                returnStmtAst = new ReturnAst(null);
                            } else {
                                stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                                arg = new Arg({
                                    code: node.metaData.code,
                                    args: node.metaData.args
                                });
                                stopIterAst.arguments.push(arg.getAst());
                                returnStmtAst = new ReturnAst(stopIterAst);
                            }
                            context = Context.ContinueAltInterrupt;
                            break;

                        case LoopModifier.CONST.RETURN:
                            arg = new Arg({
                                code: LoopModifier.CONST.RETURN,
                                args: node.metaData.args,
                                appendData: true
                            });
                            stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                            stopIterAst.arguments.push(arg.getAst());
                            returnStmtAst = new ReturnAst(stopIterAst);
                            self.mapSourceNode(node, returnStmtAst, Context.ReturnAltInterrupt);
                            break;
                    }

                    returnStmtAst.isAnnotated = true;
                    returnStmtAst.metaData = node.metaData;

                    nodeUtils.replaceNode(node, returnStmtAst, context);

                }

                return;
            }

            iterator.incrAndPush(node);

            switch (node.type) {
                case 'BreakStatement':
                    if (node.label && lblBreakMod.isReplaceReq(node.label.name)) {
                        lookup = stackHelper.searchStack({
                            targetComparator: function(item) {
                                return /LabeledStatement/.test(item.type) && item.label.name === node.label.name;
                            },
                            stopComparator: function(item) {
                                return /ForOfStatement/.test(item.type);
                            }
                        });
                        if (lookup.searchInterrupted) {
                            stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                            arg = new Arg({
                                code: LoopModifier.CONST.LABELED_BREAK,
                                args: node.label.name
                            });
                            returnStmtAst = new ReturnAst(stopIterAst);
                            stopIterAst.arguments.push(arg.getAst());

                            returnStmtAst.isAnnotated = true;
                            returnStmtAst.metaData = {
                                code: LoopModifier.CONST.LABELED_BREAK,
                                args: node.label.name
                            };
                            nodeUtils.replaceNode(node, returnStmtAst, Context.BreakStatement);
                        }
                    }
                    break;
                case 'ContinueStatement':
                    if (node.label && lblContinueMod.isReplaceReq(node.label.name)) {
                        lookup = stackHelper.searchStack({
                            targetComparator: function(item) {
                                return /LabeledStatement/.test(item.type) && item.label.name === node.label.name;
                            },
                            stopComparator: function(item) {
                                return /ForOfStatement/.test(item.type);
                            }
                        });
                        if (lookup.searchInterrupted) {
                            if (lookup.stopNode.parentLabel === node.label.name) {
                                returnStmtAst = new ReturnAst(null);
                            } else {
                                stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                                arg = new Arg({
                                    code: LoopModifier.CONST.LABELED_CONTINUE,
                                    args: node.label.name
                                });
                                returnStmtAst = new ReturnAst(stopIterAst);
                                stopIterAst.arguments.push(arg.getAst());
                            }

                            returnStmtAst.isAnnotated = true;
                            returnStmtAst.metaData = {
                                code: LoopModifier.CONST.LABELED_CONTINUE,
                                args: node.label.name
                            };
                            nodeUtils.replaceNode(node, returnStmtAst, Context.ContinueStatement);
                        }
                    }
                    break;
                case 'ReturnStatement':
                    if (node.targetFunction) {
                        lookup = stackHelper.searchStack({
                            targetComparator: function(item) {
                                return (/FunctionDeclaration/.test(item.type) || /FunctionExpression/.test(item.type)) &&
                                    item.id.name === node.targetFunction;
                            },
                            stopComparator: function(item) {
                                return /ForOfStatement/.test(item.type);
                            }
                        });
                        if (lookup.searchInterrupted) {
                            var argStr = node.argument ? escodegen.generate(node.argument) : null;

                            arg = new Arg({
                                code: LoopModifier.CONST.RETURN,
                                args: '(' + argStr + ')',
                                appendData: true
                            });
                            stopIterAst = new StopIterAst(lookup.stopNode.right.name);
                            stopIterAst.arguments.push(arg.getAst());
                            returnStmtAst = new ReturnAst(stopIterAst);
                            self.mapSourceNode(node, returnStmtAst, Context.ReturnStatement);

                            returnStmtAst.isAnnotated = true;
                            returnStmtAst.metaData = {
                                code: LoopModifier.CONST.RETURN,
                                args: arg.args,
                                iterVar: nodeCopy.right.name,
                                targetFunction: node.targetFunction
                            };

                            nodeUtils.replaceNode(node, returnStmtAst);
                        }
                    }
                    break;
            }
        });

        iterator.assertEmpty();

        return iterator.nodeCopy.parentLabel ? new LabeledStmtAst(iterator.nodeCopy.parentLabel, iterator.nodeCopy) : iterator.nodeCopy;
    };

    this.getAst = function() {
        // if-else block which perform dynamic type checking.
        // TODO : Must not parse the forOfNode's right's name. Need to revert it back to just forOfNode.right.name once
        //        UUID is used for creating variables.
        var ifElseAst = new IfElseAst(new IterTypeCheckAst(esprima.parse(forOfNode.right.name).body[0].expression));

        // Iterator AST.
        var iterConsequentAst = this.getIterConsequentAst();
        // Push the iterator AST into 'if' block.
        ifElseAst.consequent.body = iterConsequentAst.body;
        // Push the user-written 'for ... of ...' loop into 'else' block.
        ifElseAst.alternate.body.push(this.getIterAlternateAst());

        estraverse.traverse(ifElseAst, {
            enter: function(node) {
                // Mark all the nodes of 'ifElseAst' to avoid repeated operations.
                node.isGen = true;

                // Traverse all the for-of loops in the 'else' block and mark them as visited - so that we don't
                // recursively convert these into iterator constructs.
                if (/ForOfStatement/.test(node.type))
                    node.isVisited = true;
            }
        });

        return ifElseAst;
    };
}

// TODO : Handle the case when comment appears inside a string - /* this is 'a comm*/'ent */ - must be
// handled in the lex.
// TODO : Variables created in the iterator must be made available outside its scope.
// TODO : Need to call execQuery() if the query isn't a select query.
function getAst(code, sourceFileName) {
    var nodeUtils = new NodeUtils(),
        globalAncestorStack = new AncestorStack();

    // Get the Abstract Syntax Tree (ast) of the input code.
    var ast = esprima.parse(code, {
        range: true,
        tokens: true,
        comment: true,
        sourceType: 'script',
        loc: true,
        source: sourceFileName
    });

    // Attaching comments is a separate step.
    ast = escodegen.attachComments(ast, ast.comments, ast.tokens);

    estraverse.traverse(ast, {
        enter: function(node, parent) {
            globalAncestorStack.push(node);

            // Grab the for-of statement's label and mark the label for deletion.
            if (/ForOfStatement/.test(node.type) && !node.isVisited && /LabeledStatement/.test(parent.type)) {
                node.parentLabel = parent.label.name;
                parent.remLabel = true;
            }

            // Find the function that the 'return' statement associates with.
            if (/ReturnStatement/.test(node.type)) {
                var stackHelper = new StackHelper(globalAncestorStack);
                var lookup = stackHelper.searchStack({
                    targetComparator: function(item) {
                        return /FunctionDeclaration/.test(item.type) || /FunctionExpression/.test(item.type);
                    },
                    stopComparator: function(item) {
                        return false;
                    }
                });
                if (lookup.targetFound) {
                    // TODO :   Anonymous function might require some attention because comparing null doesn't make sense.
                    node.targetFunction = lookup.stopNode.id ? lookup.stopNode.id.name : null;
                }
            }
        },
        leave: function(node) {
            // Modifies all the for-of statements to support iteration.
            // Takes care to see to it that it visits the node only once.
            if (/ForOfStatement/.test(node.type) && !node.isVisited) {
                if (!/BlockStatement/.test(node.body.type)) {
                    nodeUtils.convertToBlockStmt(node);
                }

                // for-of node's right.name will be null when the right is anything other than IDENTIFIER
                // TODO : Must skip this once UUID is used to create variables
                if (!node.right.name) {
                    node.right.name = escodegen.generate(node.right);
                }

                var iterator = new IterCompatible(node, globalAncestorStack);
                var iterAst = iterator.getAst();
                nodeUtils.replaceNode(node, nodeUtils.deepCopy(iterAst), Context.IterTypeCheck);
            } else if (/LabeledStatement/.test(node.type) && node.remLabel) {
                // Delete the label.
                nodeUtils.replaceNode(node, node.body);
            }

            globalAncestorStack.pop();
        }
    });

    return ast;
}