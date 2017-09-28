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
	/*
	* Input:	JavaScript code embedded with N1QL statements.

	* Output:	Syntactically and semantically valid JavaScript code.

	* TODO:		Escape the DSTR strings - necessary for esprima.
	*/
	#include <iostream>
	#include <fstream>
        #include "n1ql.h"

	std::string parse(const char*);
	lex_op_code lex_op;
	void handle_str_start(int state);
	void handle_str_stop(int state);
	bool is_esc();

	// Contains the output plain JavaScript code.
	std::string js_code;
	// Storing the state for resuming on switch.
	int previous_state;
%}
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
<MLCMT>\n	{js_code += "\n";}
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
["]	{handle_str_start(DSTR); /* Handling double-quoted string */}
<DSTR>["]	{handle_str_stop(DSTR);}
[']	{handle_str_start(SSTR); /* Handling single-quoted string */}
<SSTR>[']	{handle_str_stop(SSTR);}
[`]	{handle_str_start(TSTR); /* Handling templated string */}
<TSTR>[`]	{handle_str_stop(TSTR);}
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
[aA][lL][tT][eE][rR][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`alter ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[bB][uU][iI][lL][dD][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`build ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[cC][rR][eE][aA][tT][eE][ \t\n][ \t\n]? {
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`create ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[dD][eE][lL][eE][tT][eE][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`delete ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[dD][rR][oO][pP][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`drop ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[eE][xX][eE][cC][uU][tT][eE][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`execute ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[eE][xX][pP][lL][aA][iI][nN][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`explain ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[gG][rR][aA][nN][tT][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`grant ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[iI][nN][fF][eE][rR][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`infer ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[iI][nN][sS][eE][rR][tT][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`insert ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[mM][eE][rR][gG][eE][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`merge ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[pP][rR][eE][pP][aA][rR][eE][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`prepare ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[rR][eE][nN][aA][mM][eE][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`rename ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[sS][eE][lL][eE][cC][tT][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`select ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[rR][eE][vV][oO][kK][eE][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`revoke ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[uU][pP][dD][aA][tT][eE][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`update ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
[uU][pP][sS][eE][rR][tT][ \t\n][ \t\n]?	{
		BEGIN N1QL;
		if(lex_op == kJsify) {
			js_code += "new N1qlQuery(`upsert ";
		} else if(lex_op == kUniLineN1QL) {
			js_code += std::string(yytext);
		}
	}
<N1QL>";"	{
		BEGIN INITIAL;
		if(lex_op == kJsify) {
			js_code+="`);";
		} else if(lex_op == kUniLineN1QL) {
			js_code += ";";
		}
	}
<N1QL>.	{
		if(yytext[0] == '`' && !is_esc() && lex_op == kJsify) {
			js_code += "\\";
		}

		js_code += std::string(yytext);
	}
<MLCMT,SLCMT,DSTR,SSTR,TSTR>.	{js_code += std::string(yytext);}
.	{js_code += std::string(yytext);}
\n	{js_code += "\n";}
%%
// Parses the given input string.
int TransformSource(const char* input, std::string *output)
{
	// Set the input stream.
	yy_scan_string(input);

	// Begin lexer.
	int code=yylex();

	// Clear the buffer allocation after the lex.
	yy_delete_buffer(YY_CURRENT_BUFFER);

	// Copy the output;
	*output=js_code;

	// Clear the global variable for the next input.
	js_code="";

	return code;
}

// Converts N1QL embedded JS to native JS.
int Jsify(const char* input, std::string *output)
{
	lex_op = kJsify;
	return TransformSource(input, output);
}

// Unilines Multiline N1QL embeddings.
int UniLineN1QL(const char *input, std::string *output)
{
	lex_op = kUniLineN1QL;
	return TransformSource(input, output);
}

// Handles the concatenation of different types of strings.
// It tries to escape the quote of the same kind.
void handle_str_start(int state)
{
	previous_state=YYSTATE;

	switch (state)
	{
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
void handle_str_stop(int state)
{
	if(!is_esc())
		BEGIN previous_state;

	switch(state)
	{
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

// Tests whether the current character is an escape character.
bool is_esc()
{
	return js_code.length() > 0 ? js_code[js_code.length() - 1] == '\\' : 0;
}

// A default yywrap
extern "C" int yywrap()
{
	return 1;
}
