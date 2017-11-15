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

	lex_op_code lex_op;
	int pos_type_len[2];
	std::list<InsertedCharsInfo> *insertions;
	// Contains the output plain JavaScript code.
	std::string js_code;
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
		js_code += "*/";
		BEGIN previous_state;
	}
<MLCMT>\n	{
		js_code += "\n";
	}
"//"	{
		/* Single-line comment */
		previous_state = YYSTATE;
		BEGIN SLCMT;
		js_code += "//";
	}
<SLCMT>\n	{
		BEGIN previous_state;
		js_code += "\n";
	}
["]	{HandleStrStart(DSTR); /* Handling double-quoted string */}
<DSTR>["]	{HandleStrStop(DSTR);}
[']	{HandleStrStart(SSTR); /* Handling single-quoted string */}
<SSTR>[']	{HandleStrStop(SSTR);}
[`]	{HandleStrStart(TSTR); /* Handling templated string */}
<TSTR>[`]	{HandleStrStop(TSTR);}
(var|function)[ \t\n]+[aA][lL][tT][eE][rR][ \t\n;=(]|[aA][lL][tT][eE][rR][ \t\n]*:[ \t\n]*\{	{return kKeywordAlter; /* Checking the constraints in this section */}
(var|function)[ \t\n]+[bB][uU][iI][lL][dD][ \t\n;=(]|[bB][uU][iI][lL][dD][ \t\n]*:[ \t\n]*\{	{return kKeywordBuild;}
(var|function)[ \t\n]+[cC][rR][eE][aA][tT][eE][ \t\n;=(]|[cC][rR][eE][aA][tT][eE][ \t\n]*:[ \t\n]*\{	{return kKeywordCreate;}
(var|function)[ \t\n]+[dD][eE][lL][eE][tT][eE][ \t\n;=(]|[dD][eE][lL][eE][tT][eE][ \t\n]*:[ \t\n]*\{	{return kKeywordDelete;}
(var|function)[ \t\n]+[dD][rR][oO][pP][ \t\n;=(]|[dD][rR][oO][pP][ \t\n]*:[ \t\n]*\{	{return kKeywordDrop;}
(var|function)[ \t\n]+[eE][xX][eE][cC][uU][tT][eE][ \t\n;=(]|[eE][xX][eE][cC][uU][tT][eE][ \t\n]*:[ \t\n]*\{	{return kKeywordExecute;}
(var|function)[ \t\n]+[eE][xX][pP][lL][aA][iI][nN][ \t\n;=(]|[eE][xX][pP][lL][aA][iI][nN][ \t\n]*:[ \t\n]*\{	{return kKeywordExplain;}
(var|function)[ \t\n]+[gG][rR][aA][nN][tT][ \t\n;=(]|[gG][rR][aA][nN][tT][ \t\n]*:[ \t\n]*\{	{return kKeywordGrant;}
(var|function)[ \t\n]+[iI][nN][fF][eE][rR][ \t\n;=(]|[iI][nN][fF][eE][rR][ \t\n]*:[ \t\n]*\{	{return kKeywordInfer;}
(var|function)[ \t\n]+[iI][nN][sS][eE][rR][tT][ \t\n;=(]|[iI][nN][sS][eE][rR][tT][ \t\n]*:[ \t\n]*\{	{return kKeywordInsert;}
(var|function)[ \t\n]+[mM][eE][rR][gG][eE][ \t\n;=(]|[mM][eE][rR][gG][eE][ \t\n]*:[ \t\n]*\{	{return kKeywordMerge;}
(var|function)[ \t\n]+[pP][rR][eE][pP][aA][rR][eE][ \t\n;=(]|[pP][rR][eE][pP][aA][rR][eE][ \t\n]*:[ \t\n]*\{	{return kKeywordPrepare;}
(var|function)[ \t\n]+[rR][eE][nN][aA][mM][eE][ \t\n;=(]|[rR][eE][nN][aA][mM][eE][ \t\n]*:[ \t\n]*\{	{return kKeywordRename;}
(var|function)[ \t\n]+[sS][eE][lL][eE][cC][tT][ \t\n;=(]|[sS][eE][lL][eE][cC][tT][ \t\n]*:[ \t\n]*\{	{return kKeywordSelect;}
(var|function)[ \t\n]+[rR][eE][vV][oO][kK][eE][ \t\n;=(]|[rR][eE][vV][oO][kK][eE][ \t\n]*:[ \t\n]*\{	{return kKeywordRevoke;}
(var|function)[ \t\n]+[uU][pP][dD][aA][tT][eE][ \t\n;=(]|[uU][pP][dD][aA][tT][eE][ \t\n]*:[ \t\n]*\{	{return kKeywordUpdate;}
(var|function)[ \t\n]+[uU][pP][sS][eE][rR][tT][ \t\n;=(]|[uU][pP][sS][eE][rR][tT][ \t\n]*:[ \t\n]*\{	{return kKeywordUpsert;}
[aA][lL][tT][eE][rR][ \t\n][ \t\n]?	|
[bB][uU][iI][lL][dD][ \t\n][ \t\n]?	|
[cC][rR][eE][aA][tT][eE][ \t\n][ \t\n]? |
[dD][eE][lL][eE][tT][eE][ \t\n][ \t\n]?	|
[dD][rR][oO][pP][ \t\n][ \t\n]?	|
[eE][xX][eE][cC][uU][tT][eE][ \t\n][ \t\n]?	|
[eE][xX][pP][lL][aA][iI][nN][ \t\n][ \t\n]?	|
[gG][rR][aA][nN][tT][ \t\n][ \t\n]?	|
[iI][nN][fF][eE][rR][ \t\n][ \t\n]?	|
[iI][nN][sS][eE][rR][tT][ \t\n][ \t\n]?	|
[mM][eE][rR][gG][eE][ \t\n][ \t\n]?	|
[pP][rR][eE][pP][aA][rR][eE][ \t\n][ \t\n]?	|
[rR][eE][nN][aA][mM][eE][ \t\n][ \t\n]?	|
[sS][eE][lL][eE][cC][tT][ \t\n][ \t\n]?	|
[rR][eE][vV][oO][kK][eE][ \t\n][ \t\n]?	|
[uU][pP][dD][aA][tT][eE][ \t\n][ \t\n]?	|
[uU][pP][sS][eE][rR][tT][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery('"+ std::string(yytext) + " ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		} else if(lex_op == kCommentN1QL) {
			UpdatePos(insert_type::kN1QLBegin);
			js_code += "/*" + std::string(yytext);
		}
	}
<N1QL>";"	{
		BEGIN INITIAL;
		if(lex_op == kJsify) {
			js_code += "');";
		} else if(lex_op == kUniLineN1QL) {
			js_code += ";";
		} else if(lex_op == kCommentN1QL) {
			js_code += "*/$;";
			// TODO : Before updating insertions, need to mangle the multiline comment tokens '/* and '*/' that are present in
			// the N1QL queries as comment matching in JavaScript follows greedy matching
			UpdatePos(insert_type::kN1QLEnd);
		}
	}
<N1QL>.	{
		// Need to escape the escape character to preserve the raw-ness.
		// Need to escape the single quotes as the N1QL query is going to be enclosed in a single quoted string.
		if(yytext[0] == '\\' || (yytext[0] == '\'' && !IsEsc() && lex_op == kJsify)) {
			js_code += "\\";
		}

		js_code += std::string(yytext);
	}
<N1QL>\n {
		if(lex_op == kCommentN1QL) {
			js_code += "\n";
		}
	}
<MLCMT,SLCMT,DSTR,SSTR,TSTR>.	{js_code += std::string(yytext);}
.	{js_code += std::string(yytext);}
\n	{js_code += "\n";}
%%
// Parses the given input string.
int TransformSource(const char* input, std::string *output, Pos *last_pos) {
	// Set the input stream.
	yy_scan_string(input);

	// pos_type_len represents the length that each insert_type will take
	pos_type_len[static_cast<std::size_t>(insert_type::kN1QLBegin)] = 2;
	pos_type_len[static_cast<std::size_t>(insert_type::kN1QLEnd)] = 3;

	int code = yylex();

	// Clear the buffer allocation after the lex.
	yy_delete_buffer(YY_CURRENT_BUFFER);

	*output = js_code;
	if(last_pos != nullptr) {
		UpdatePos(last_pos);
	}

	// Clear the global variable for the next input.
	js_code = "";
	return code;
}

// Converts N1QL embedded JS to native JS.
int Jsify(const char* input, std::string *output, Pos *last_pos_out) {
	lex_op = kJsify;
	return TransformSource(input, output, last_pos_out);
}

// Unilines Multiline N1QL embeddings.
int UniLineN1QL(const char *input, std::string *output, Pos *last_pos_out) {
	lex_op = kUniLineN1QL;
	return TransformSource(input, output, last_pos_out);
}

// Comments out N1QL statements and substitutes $ in its place
int CommentN1QL(const char *input, std::string *output, std::list<InsertedCharsInfo> *insertions_out, Pos *last_pos_out) {
	lex_op = kCommentN1QL;
	insertions = insertions_out;

	return TransformSource(input, output, last_pos_out);
}

void UpdatePos(Pos *pos) {
	pos->line_no = std::count(js_code.begin(), js_code.end(), '\n') + 1;
	for(auto c = js_code.crbegin(); (c != js_code.crend()) && (*c != '\n'); ++c) {
		++pos->col_no;
	}

	++pos->col_no;
	pos->index = std::max(static_cast<decltype(js_code.length())>(0), js_code.length() - 1);
}

// Adds an entry to keep track of N1QL queries in the js_code
void UpdatePos(insert_type type) {
	InsertedCharsInfo pos(type);
	if(!insertions->empty()) {
		pos = insertions->back();
	}

	// Count the number of newlines since the previously updated pos
	pos.line_no = std::count(js_code.begin() + pos.line_no, js_code.end(), '\n') + 1;
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
	if(!IsEsc()) {
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
bool IsEsc() {
	auto escaped = false;
	auto i = js_code.length();
	while(i-- > 0) {
		if(js_code[i] != '\\') {
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
