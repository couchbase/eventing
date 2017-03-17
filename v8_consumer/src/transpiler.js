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

function get_ast(code, esprima, estraverse, escodegen) {
    function Stack() {
        var stack = [];

        this.stackCopy = stack;
        this.push = function(item) {
            stack.push(item);
        }

        this.pop = function() {
            if (stack.length == 0)
                throw 'Stack underflow exception';
            return stack.pop();
        }

        this.peek = function() {
            var size = this.getSize();
            if (size >= 0) {
                return stack[size - 1];
            }
            return;
        }

        this.getSize = function() {
            return stack.length;
        }

        this.contains = function(item) {
            for (var _item of stack) {
                if (_item === item) {
                    return true;
                }
            }
            return false;
        }

        // debug.
        this.printAll = function() {
            for (var item of stack) {
                console.log(item);
                console.log();
            }
        }
    }

    function LoopModifier(modifier) {
        this.modType = modifier;
        var ancestorStack = new Stack();
        this.stackIndex = -1;

        // debug.
        this.stackCopy = ancestorStack.stackCopy;

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
            case LoopModifier.CONST.LABELED_BREAK:
                associations.add('LabeledStatement');
                break;
            default:
                throw 'Invalid modifier';
        }

        this.checkAssoc = function(nodeType) {
            return associations.has(nodeType);
        }

        this.pushIfAssoc = function(node) {
            if (this.checkAssoc(node.type)) {
                switch (this.modType) {
                    case LoopModifier.CONST.BREAK:
                        node.breakStackIndex = this.stackIndex;
                        ancestorStack.push(node);
                        break;
                    case LoopModifier.CONST.CONTINUE:
                        node.continueStackIndex = this.stackIndex;
                        ancestorStack.push(node);
                        break;
                    case LoopModifier.CONST.LABELED_BREAK:
                        node.lblBreakStackIndex = this.stackIndex;
                        ancestorStack.push(node.label.name);
                        break;
                    default:
                        throw 'Invalid modifier type';
                }
            }
        }

        this.popIfAssoc = function() {
            if (ancestorStack.getSize() > 0) {
                switch (this.modType) {
                    case LoopModifier.CONST.BREAK:
                        if (this.stackIndex == ancestorStack.peek().breakStackIndex) {
                            return ancestorStack.pop();
                        }
                        break;
                    case LoopModifier.CONST.CONTINUE:
                        if (this.stackIndex == ancestorStack.peek().continueStackIndex) {
                            return ancestorStack.pop();
                        }
                        break;
                    case LoopModifier.CONST.LABELED_BREAK:
                        if (this.stackIndex == ancestorStack.peek().lblBreakStackIndex) {
                            return ancestorStack.pop();
                        }
                    default:
                        throw 'Invalid modifier type';
                }
            }
        }

        this.getSize = function() {
            return ancestorStack.getSize();
        }

        this.isReplaceReq = function(args) {
            switch (this.modType) {
                case LoopModifier.CONST.CONTINUE:
                case LoopModifier.CONST.BREAK:
                    return ancestorStack.getSize() > 0 && /ForOfStatement/.test(ancestorStack.peek().type);
                    break;
                case LoopModifier.CONST.LABELED_BREAK:
                    return !ancestorStack.contains(args);
                    break;
                default:
                    throw 'Invalid modifier type';
            }
        }

        // debug.
        this.printAll = function() {
            ancestorStack.printAll();
        }
    }

    LoopModifier.CONST = {
        BREAK: 'break',
        CONTINUE: 'continue',
        LABELED_BREAK: 'labelled_break'
    };

    function Ast(_type) {
        this.type = _type;
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

    function LabeledBreakAst(label) {
        BreakAst.call(this);
        this.label = {
            "type": "Identifier",
            "name": label
        }
    }

    // Replaces source node with the target node and returns a reference to the new node.
    function replace_node(source, target) {
        Object.keys(source).forEach(function(key) {
            delete source[key];
        });
        Object.keys(target).forEach(function(key) {
            source[key] = target[key];
        });

        return source;
    }

    function insert_node(parentBody, insAfterNode, nodeToInsert) {
        var insertIndex = parentBody.indexOf(insAfterNode) + 1;
        parentBody.splice(insertIndex, 0, nodeToInsert);
    }

    function insert_array(parentBody, insAfterNode, arrayToInsert) {
        var insertIndex = parentBody.indexOf(insAfterNode) + 1;
        parentBody.splice.apply(parentBody, [insertIndex, 0].concat(arrayToInsert));
    }

    function convert_to_block_stmt(node) {
        switch (node.type) {
            case 'ForOfStatement':
                var forBodyAst = {};
                // Make a deep copy of the body.
                Object.keys(node.body).forEach(function(key) {
                    forBodyAst[key] = node.body[key];
                });
                // Transform the previous single-line statement into a block.
                node.body.body = [forBodyAst];
                node.body.type = 'BlockStatement';
                break;
            case 'IfStatement':
                var ifBodyAst = {};
                // Make a deep copy of the 'if' body.
                Object.keys(node.consequent).forEach(function(key) {
                    ifBodyAst[key] = node.consequent[key];
                });
                node.consequent.body = [ifBodyAst];
                node.consequent.type = 'BlockStatement';
                // If the 'else' part exists, convert it to a block statement.
                if (node.alternate) {
                    var elseBodyAst = {};
                    // Make a deep copy of the 'else' body.
                    Object.keys(node.alternate).forEach(function(key) {
                        elseBodyAst[key] = node.alternate[key];
                    });
                    node.alternate.body = [elseBodyAst];
                    node.alternate.type = 'BlockStatement';
                }
                break;
        }
    }

    function deep_copy(node) {
        return JSON.parse(JSON.stringify(node));
    }

    function get_iter_ast(forOfNode, mode) {
        var postIter = [];
        var iterProp = 'x';
        var nodeCopy = deep_copy(forOfNode);

        if (mode === 'for_of') {
            estraverse.traverse(nodeCopy, {
                enter: function(node, parent) {
                    ++breakMod.stackIndex;
                    ++continueMod.stackIndex;
                    ++lblBreakMod.stackIndex;

                    if (node.isGen) {
                        return;
                    }

                    breakMod.pushIfAssoc(node);
                    continueMod.pushIfAssoc(node);
                    lblBreakMod.pushIfAssoc(node);

                    switch (node.type) {
                        case 'BreakStatement':
                            if (node.label && lblBreakMod.isReplaceReq(node.label.name)) {
                                var stopIterAst = esprima.parse(nodeCopy.right.name + ".stopIter();");
                                var arg = '{"code":"' + LoopModifier.CONST.LABELED_BREAK + '", "args":"' + node.label.name + '"}';
                                var argsAst = esprima.parse('(' + arg + ')');

                                if (!(arg in postIter)) {
                                    postIter.push(arg);
                                }
                            } else if (breakMod.isReplaceReq()) {
                                var stopIterAst = esprima.parse(nodeCopy.right.name + ".stopIter();");
                                var argsAst = esprima.parse("({code:'" + LoopModifier.CONST.BREAK + "'})");
                            }
                            if (stopIterAst && argsAst) {
                                var returnStmtAst = {
                                    "type": "ReturnStatement",
                                    "argument": stopIterAst.body[0].expression
                                };
                                stopIterAst.body[0].expression.arguments.push(argsAst.body[0].expression);
                                replace_node(node, returnStmtAst);
                            }
                            break;
                        case 'ContinueStatement':
                            if (continueMod.isReplaceReq()) {
                                var returnStmtAst = {
                                    "type": "ReturnStatement",
                                    "argument": null
                                };
                                replace_node(node, returnStmtAst);
                            }
                            break;
                        case 'IfStatement':
                            if (!/BlockStatement/.test(node.type)) {
                                convert_to_block_stmt(node);
                            }
                            break;
                    }
                },
                leave: function(node) {
                    breakMod.popIfAssoc();
                    --breakMod.stackIndex;
                }
            });
            var iter = esprima.parse(
                forOfNode.right.name + '.' + iterProp + '=' + forOfNode.right.name +
                '.iter(function (' + (forOfNode.left.name ? forOfNode.left.name : forOfNode.left.declarations[0].id.name) + '){});'
            ).body[0];
            iter.expression.right.arguments[0].body = nodeCopy.body;

            var iterBlockAst = esprima.parse('{}').body[0];
            iterBlockAst.body.push(iter);

            var postIterAst = get_post_iter_ast(forOfNode.right.name, iterProp, postIter);
            if (postIterAst) {
                iterBlockAst.body.push(get_post_iter_ast(forOfNode.right.name, iterProp, postIter));
            }

            return iterBlockAst;
        }

        throw 'Invalid arg ' + mode + ' for get_iter_ast';
    }

    function get_post_iter_ast(iterVar, prop, postIterStmts) {
        if (postIterStmts.length <= 0) {
            return null;
        }
        var discriminantAst = esprima.parse(iterVar + '.' + prop + '.code' + '+' + iterVar + '.' + prop + '.args').body[0].expression;
        var switchAst = new SwitchAst(discriminantAst);
        for (var postIterStmt of postIterStmts) {
            var postIter = JSON.parse(postIterStmt);
            var caseAst = new CaseAst(postIter.code + postIter.args);
            switch (postIter.code) {
                case LoopModifier.CONST.BREAK:
                case LoopModifier.CONST.CONTINUE:
                    break;
                case LoopModifier.CONST.LABELED_BREAK:
                    caseAst.consequent.push(new LabeledBreakAst(postIter.args));
                    break;
            }
            switchAst.cases.push(caseAst);
        }

        return switchAst;
    }

    // Returns iterator consturct with dynamic type checking.
    function get_iter_compatible_ast(forOfNode) {
        // Make a copy of the 'for ... of ...' loop.
        var nodeCopy = deep_copy(forOfNode);

        // Iterator AST.
        var iterAst = get_iter_ast(nodeCopy, 'for_of');

        // 'if ... else ...' which perform dynamic type checking.
        var ifElseAst = esprima.parse('if(' + forOfNode.right.name + '.isInstance){}else{}').body[0];
        // Push the iterator AST into 'if' block.
        ifElseAst.consequent.body = iterAst.body;
        // Push the user-written 'for ... of ...' loop into 'else' block.
        ifElseAst.alternate.body.push(forOfNode);

        estraverse.traverse(ifElseAst, {
            enter: function(node) {
                node.isGen = true;
            }
        });

        // Traverse all the 'for ... of ...' loops in the 'else' block and mark them as visited - so that we don't recursively convert these into iterator constructs.
        estraverse.traverse(nodeCopy, {
            enter: function(node) {
                if (/ForOfStatement/.test(node.type))
                    node.isVisited = true;
            }
        });

        return ifElseAst;
    }

    // Build an ast node for N1QL function call from the query.
    function get_query_ast(query) {
        // Identifier regex.
        var re = /:([a-zA-Z_$][a-zA-Z_$0-9]*)/g;

        // Match the regex against the query to find all the variables that are used.
        var matches = query.match(re);

        // Replace the :<var> with proper substitution.
        query = query.replace(re, '" + $1 + "');
        query = 'new N1qlQuery("' + query + '");';

        // Return the ast.
        return esprima.parse(query).body[0].expression;
    }

    function is_n1ql_node(node) {
        return /NewExpression/.test(node.type) &&
            /N1qlQuery/.test(node.callee.name);
    }

    var breakMod = new LoopModifier(LoopModifier.CONST.BREAK);
    var continueMod = new LoopModifier(LoopModifier.CONST.CONTINUE);
    var lblBreakMod = new LoopModifier(LoopModifier.CONST.LABELED_BREAK);

    // Get the Abstract Syntax Tree (ast) of the input code.
    var ast = esprima.parse(code, {
        attachComment: true,
        sourceType: 'script'
    });

    estraverse.traverse(ast, {
        leave: function(node) {
            // Perform variable substitution in query constructor.
            if (is_n1ql_node(node) && node.arguments.length > 0) {
                var queryAst = get_query_ast(node.arguments[0].quasis[0].value.raw);
                replace_node(node, deep_copy(queryAst));
            }

            // TODO : Handle the case when the source of 'for ... of ...' is of type x.y
            // Modifies all the 'for ... of ...' constructs to work with iteration. Takes care to see to it that it visits the node only once.
            if (/ForOfStatement/.test(node.type) && !node.isVisited) {
                if (!/BlockStatement/.test(node.body.type)) {
                    convert_to_block_stmt(node);
                }

                var iterAst = get_iter_compatible_ast(node);
                replace_node(node, deep_copy(iterAst));
            }
        }
    });

    return ast;
}
