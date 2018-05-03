%{
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

    #include <list>
    #include <algorithm>
    #include "n1ql.h"
    #include "utils.h"

    lex_op_code lex_op;
    int pos_type_len[2];
    std::list<InsertedCharsInfo> *insertions;
    ParseInfo parse_info;
    // Contains the output plain JavaScript code.
    std::string js_code, n1ql_query;
    // Storing the state for resuming on switch.
    int previous_state;
%}
%option nounput
%x N1QL MLCMT SLCMT DSTR SSTR TSTR
%%
	previous_state=YYSTATE;
"/*"	{
        /* Start of a multi-line comment */
        previous_state = YYSTATE;
        BEGIN MLCMT;
        js_code += "/*";
    }
<MLCMT>"*/"	{
        /* Stop of a multi-line comment */
        BEGIN previous_state;
        js_code += "*/";
    }
<MLCMT>\n   |
<MLCMT>\r   |
<MLCMT>\r\n {js_code += std::string(yytext);}
"//"	{
        /* Single-line comment */
        previous_state = YYSTATE;
        BEGIN SLCMT;
        js_code += "//";
    }
<SLCMT>\n   |
<SLCMT>\r   |
<SLCMT>\r\n {
        BEGIN previous_state;
        js_code += std::string(yytext);
    }
["]	{HandleStrStart(DSTR); /* Handling double-quoted string */}
<DSTR>["]	{HandleStrStop(DSTR);}
[']	{HandleStrStart(SSTR); /* Handling single-quoted string */}
<SSTR>[']	{HandleStrStop(SSTR);}
[`]	{HandleStrStart(TSTR); /* Handling templated string */}
<TSTR>[`]	{HandleStrStop(TSTR);}
(var|function)[ \t\r\n]+[aA][lL][tT][eE][rR][ \t\r\n;=(]|[aA][lL][tT][eE][rR][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordAlter; /* Checking the constraints in this section */}
(var|function)[ \t\r\n]+[bB][uU][iI][lL][dD][ \t\r\n;=(]|[bB][uU][iI][lL][dD][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordBuild;}
(var|function)[ \t\r\n]+[cC][rR][eE][aA][tT][eE][ \t\r\n;=(]|[cC][rR][eE][aA][tT][eE][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordCreate;}
(var|function)[ \t\r\n]+[dD][eE][lL][eE][tT][eE][ \t\r\n;=(]|[dD][eE][lL][eE][tT][eE][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordDelete;}
(var|function)[ \t\r\n]+[dD][rR][oO][pP][ \t\r\n;=(]|[dD][rR][oO][pP][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordDrop;}
(var|function)[ \t\r\n]+[eE][xX][eE][cC][uU][tT][eE][ \t\r\n;=(]|[eE][xX][eE][cC][uU][tT][eE][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordExecute;}
(var|function)[ \t\r\n]+[eE][xX][pP][lL][aA][iI][nN][ \t\r\n;=(]|[eE][xX][pP][lL][aA][iI][nN][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordExplain;}
(var|function)[ \t\r\n]+[fF][rR][oO][mM][ \t\r\n;=(]|[fF][rR][oO][mM][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordFrom;}
(var|function)[ \t\r\n]+[gG][rR][aA][nN][tT][ \t\r\n;=(]|[gG][rR][aA][nN][tT][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordGrant;}
(var|function)[ \t\r\n]+[iI][nN][fF][eE][rR][ \t\r\n;=(]|[iI][nN][fF][eE][rR][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordInfer;}
(var|function)[ \t\r\n]+[iI][nN][sS][eE][rR][tT][ \t\r\n;=(]|[iI][nN][sS][eE][rR][tT][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordInsert;}
(var|function)[ \t\r\n]+[mM][eE][rR][gG][eE][ \t\r\n;=(]|[mM][eE][rR][gG][eE][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordMerge;}
(var|function)[ \t\r\n]+[pP][rR][eE][pP][aA][rR][eE][ \t\r\n;=(]|[pP][rR][eE][pP][aA][rR][eE][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordPrepare;}
(var|function)[ \t\r\n]+[rR][eE][nN][aA][mM][eE][ \t\r\n;=(]|[rR][eE][nN][aA][mM][eE][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordRename;}
(var|function)[ \t\r\n]+[sS][eE][lL][eE][cC][tT][ \t\r\n;=(]|[sS][eE][lL][eE][cC][tT][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordSelect;}
(var|function)[ \t\r\n]+[rR][eE][vV][oO][kK][eE][ \t\r\n;=(]|[rR][eE][vV][oO][kK][eE][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordRevoke;}
(var|function)[ \t\r\n]+[uU][pP][dD][aA][tT][eE][ \t\r\n;=(]|[uU][pP][dD][aA][tT][eE][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordUpdate;}
(var|function)[ \t\r\n]+[uU][pP][sS][eE][rR][tT][ \t\r\n;=(]|[uU][pP][sS][eE][rR][tT][ \t\r\n]*:[ \t\r\n]*\{	{return kKeywordUpsert;}
[aA][lL][tT][eE][rR][ \t\r\n][ \t\r\n]?	|
[bB][uU][iI][lL][dD][ \t\r\n][ \t\r\n]?	|
[cC][rR][eE][aA][tT][eE][ \t\r\n][ \t\r\n]? |
[dD][eE][lL][eE][tT][eE][ \t\r\n][ \t\r\n]?	|
[dD][rR][oO][pP][ \t\r\n][ \t\r\n]?	|
[eE][xX][eE][cC][uU][tT][eE][ \t\r\n][ \t\r\n]?	|
[eE][xX][pP][lL][aA][iI][nN][ \t\r\n][ \t\r\n]?	|
[fF][rR][oO][mM][ \t\r\n][ \t\r\n]?	|
[gG][rR][aA][nN][tT][ \t\r\n][ \t\r\n]?	|
[iI][nN][fF][eE][rR][ \t\r\n][ \t\r\n]?	|
[iI][nN][sS][eE][rR][tT][ \t\r\n][ \t\r\n]?	|
[mM][eE][rR][gG][eE][ \t\r\n][ \t\r\n]?	|
[pP][rR][eE][pP][aA][rR][eE][ \t\r\n][ \t\r\n]?	|
[rR][eE][nN][aA][mM][eE][ \t\r\n][ \t\r\n]?	|
[sS][eE][lL][eE][cC][tT][ \t\r\n][ \t\r\n]?	|
[rR][eE][vV][oO][kK][eE][ \t\r\n][ \t\r\n]?	|
[uU][pP][dD][aA][tT][eE][ \t\r\n][ \t\r\n]?	|
[uU][pP][sS][eE][rR][tT][ \t\r\n][ \t\r\n]?	{
        BEGIN N1QL;

        n1ql_query = std::string(yytext);

        if(lex_op == kCommentN1QL) {
            UpdatePos(insert_type::kN1QLBegin);
        } else {
            // The '\n' might be consumed by the regex above
            // It's essential to replace it with a space as multi-line string with single-quotes isn't possible in JavaScript
            ReplaceRecentChar(n1ql_query, '\n', ' ');
            ReplaceRecentChar(n1ql_query, '\r', ' ');
        }
    }
<N1QL>";"	{
        BEGIN INITIAL;

        n1ql_query += ";";
        switch(lex_op) {
            case kUniLineN1QL:
                js_code += n1ql_query;
                break;

            case kJsify:
            case kCommentN1QL: {
                    auto isolate = v8::Isolate::GetCurrent();
                    parse_info = ParseQuery(n1ql_query);
                    if(parse_info.is_valid) {
                        // If the query is DML, it should not execute on the source bucket
                        if(parse_info.is_dml_query) {
                            auto source = UnwrapData(isolate)->v8worker->cb_source_bucket;
                            if(source == parse_info.keyspace_name) {
                                parse_info.is_valid = false;
                                parse_info.info = R"(Can not execute DML query on bucket ")" + source + R"(")";
                                return kN1QLParserError;
                            }
                        }

                        // It's a valid N1QL query, transpile and add to code
                        js_code += TranspileQuery(n1ql_query);
                    } else {
                        // It's not a N1QL query, maybe it's a JS expression
                        auto transpiler = UnwrapData(isolate)->transpiler;
                        if(!transpiler->IsJsExpression(n1ql_query)) {
                            // Neither a N1QL query nor a JS expression
                            return kN1QLParserError;
                        }

                        // It's a JS expression, no need to transpile
                        js_code += n1ql_query;
                    }

                    if(lex_op == kCommentN1QL) {
                        UpdatePos(insert_type::kN1QLEnd);
                    }
                }
                break;
        }
    }
<N1QL>. |
<N1QL>\n    |
<N1QL>\r    |
<N1QL>\r\n  {
        std::string str(yytext);
        if(lex_op == kCommentN1QL) {
            n1ql_query += str;
        } else {
            ReplaceRecentChar(str, '\n', ' ');
            ReplaceRecentChar(str, '\r', ' ');
            n1ql_query += str;
        }
    }
<MLCMT,SLCMT,DSTR,SSTR,TSTR>.	{js_code += std::string(yytext);}
.   |
\n  |
\r  |
\r\n    {js_code += std::string(yytext);}
%%
// Parses the given input string.
int TransformSource(const char* input, std::string *output, Pos *last_pos) {
    // Set the input stream.
    yy_scan_string(input);

    // pos_type_len represents the length that each insert_type will take
    pos_type_len[static_cast<std::size_t>(insert_type::kN1QLBegin)] = 2;
    pos_type_len[static_cast<std::size_t>(insert_type::kN1QLEnd)] = 3;

    // Reset flex state for the subsequent calls.
    BEGIN INITIAL;
    int code = yylex();

    // Clear the buffer allocation after the lex.
    yy_delete_buffer(YY_CURRENT_BUFFER);

    *output = js_code;
    if(last_pos != nullptr) {
        UpdatePos(last_pos);
    }

    // Clear the global variable for the next input.
    n1ql_query = js_code = "";
    return code;
}

// Converts N1QL embedded JS to native JS.
JsifyInfo Jsify(const std::string &input) {
    lex_op = kJsify;
    JsifyInfo info;

    info.code = TransformSource(input.c_str(), &info.handler_code, &info.last_pos);
    return info;
}

// Unilines Multiline N1QL embeddings.
UniLineN1QLInfo UniLineN1QL(const std::string &input) {
    lex_op = kUniLineN1QL;
    UniLineN1QLInfo info;

    info.code = TransformSource(input.c_str(), &info.handler_code, &info.last_pos);
    return info;
}

// Comments out N1QL statements and substitutes $ in its place
CommentN1QLInfo CommentN1QL(const std::string &input) {
    lex_op = kCommentN1QL;
    CommentN1QLInfo info;

    insertions = &info.insertions;
    info.code = TransformSource(input.c_str(), &info.handler_code, &info.last_pos);
    info.parse_info = parse_info;
    return info;
}

// Update line number, column number and index based on the current value of js_code
void UpdatePos(Pos *pos) {
    pos->line_no = CountNewLines(js_code) + 1;
    for(auto c = js_code.crbegin(); (c != js_code.crend()) && (*c != '\r') && (*c != '\n'); ++c) {
        ++pos->col_no;
    }

    // To make col_no atleast 1
    ++pos->col_no;
    pos->index = js_code.length() == 0 ? 0 : js_code.length() - 1;
}

// Adds an entry to keep track of N1QL queries in the js_code
void UpdatePos(insert_type type) {
    InsertedCharsInfo pos(type);
    if(!insertions->empty()) {
        pos = insertions->back();
    }

    // Count the number of newlines since the previously updated pos
    pos.line_no = CountNewLines(js_code, pos.line_no) + 1;
    switch(type) {
        case insert_type::kN1QLBegin:
            pos.index = js_code.length();
            pos.type = insert_type::kN1QLBegin;
            pos.type_len = pos_type_len[static_cast<std::size_t>(insert_type::kN1QLBegin)];
            break;

        case insert_type::kN1QLEnd:
            pos.index = js_code.length() - 1;
            pos.type = insert_type::kN1QLEnd;
            pos.type_len = pos_type_len[static_cast<std::size_t>(insert_type::kN1QLEnd)];
            break;
    }

    insertions->push_back(pos);
}

// Handles the concatenation of different types of strings.
// It tries to escape the quote of the same kind.
void HandleStrStart(int state) {
    previous_state=YYSTATE;

    switch (state) {
        case DSTR:
            BEGIN DSTR;
            js_code += "\"";
            break;

        case SSTR:
            BEGIN SSTR;
            js_code += "'";
            break;

        case TSTR:
            BEGIN TSTR;
            js_code += "`";
            break;
    }
}

// Restores the previous state and adds the appropriate closing quote.
void HandleStrStop(int state) {
    if(!IsEsc(js_code)) {
        BEGIN previous_state;
    }

    switch(state) {
        case DSTR:
            js_code += "\"";
            break;

        case SSTR:
            js_code += "'";
            break;

        case TSTR:
            js_code += "`";
            break;
    }
}

// Tests whether the quote character is escaped.
bool IsEsc(const std::string &str) {
    auto escaped = false;
    auto i = str.length();
    while(i-- > 0) {
        if(str[i] != '\\') {
            break;
        }

        escaped = !escaped;
    }

    return escaped;
}

// A default yywrap
extern "C" int yywrap() {
    return 1;
}

// Transpiles the given N1QL query into a JavaScript expression - "new N1qlQuery('...')"
std::string TranspileQuery(const std::string &query) {
    switch(lex_op) {
        case kJsify: {
                auto isolate = v8::Isolate::GetCurrent();
                auto comm = UnwrapData(isolate)->comm;
                auto transpiler = UnwrapData(isolate)->transpiler;

                auto info = comm->GetNamedParams(query);
                parse_info = info.p_info;
                return transpiler->TranspileQuery(query, info);
            }

        case kCommentN1QL: {
                // For kCommentN1QL, instead of appending the character read, we substitute a '*'
                // This is done because it will be ambiguous to JavaScript parser if it sees comment in N1QL query.
                std::string query_transpiled = "/*";
                for(const auto &c: query) {
                    query_transpiled += (c == '\r' || c == '\n'? c : '*');
                }

                query_transpiled += "*/$;";
                return query_transpiled;
            }

        default:
            throw "Transpile Query not handled for this lex_op";
    }
}

// Replaces the recent occurrence of char m in str with char n
void ReplaceRecentChar(std::string &str, char m, char n) {
    auto find = str.rfind(m);
    if(find != std::string::npos) {
        str[find] = n;
    }
}

// Parse the N1QL query and return the parser result
ParseInfo ParseQuery(const std::string &query) {
    auto isolate = v8::Isolate::GetCurrent();
    auto comm = UnwrapData(isolate)->comm;
    return comm->ParseQuery(query);
}

// Returns the number of the logically equivalent newlines in str
int32_t CountNewLines(const std::string &str, const int32_t from) {
    return CountStr("\r", str, from) + CountStr("\n", str, from) - CountStr("\r\n", str, from);
}

// Returns the number of needles in haystack
int32_t CountStr(const std::string &needle, const std::string &haystack, const int32_t from) {
  int32_t count = 0;
  for (auto i = haystack.find(needle, static_cast<std::size_t>(from)); i != std::string::npos;
       i = haystack.find(needle, i + 1)) {
    ++count;
  }

  return count;
}