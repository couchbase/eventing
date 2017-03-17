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
	*/
	#include <iostream>
	#include <fstream>
	#include "../include/n1ql.h"

	std::string parse(const char*);
	void handle_str_start(int state);
	void handle_str_stop(int state);
	bool is_esc();

	using namespace std;

	// Contains the output plain JavaScript code.
	string plain_js;
	// Storing the state for resuming on switch.
	int previous_state;
%}
%x N1QL MLCMT SLCMT DSTR SSTR TSTR
%%
	previous_state=YYSTATE;
"/*"	{
			/* Start of a multi-line comment */
			previous_state=YYSTATE;
			BEGIN MLCMT;
			plain_js+="/*";
		}
<MLCMT>"*/"	{
				/* Stop of a multi-line comment */
				plain_js+="*/";
				BEGIN previous_state;
			}
<MLCMT>\n	{plain_js+="\n";}
"//"	{
			/* Single-line comment */
			previous_state=YYSTATE;
			BEGIN SLCMT;
			plain_js+="//";
		}
<SLCMT>\n	{
				BEGIN previous_state;
				plain_js+="\n";
			}
["]	{handle_str_start(DSTR); /* Handling double-quoted string */}
<DSTR>["]	{handle_str_stop(DSTR);}
[']	{handle_str_start(SSTR); /* Handling single-quoted string */}
<SSTR>[']	{handle_str_stop(SSTR);}
[`]	{handle_str_start(TSTR); /* Handling templated string */}
<TSTR>[`]	{handle_str_stop(TSTR);}
(var|function)[ \t\n]+[aA][lL][tT][eE][rR][ \t\n;=(]|[aA][lL][tT][eE][rR][ \t\n]*:[ \t\n]*\{	{return KWD_ALTER; /* Checking the constraints in this section */}
(var|function)[ \t\n]+[bB][uU][iI][lL][dD][ \t\n;=(]|[bB][uU][iI][lL][dD][ \t\n]*:[ \t\n]*\{	{return KWD_BUILD;}
(var|function)[ \t\n]+[cC][rR][eE][aA][tT][eE][ \t\n;=(]|[cC][rR][eE][aA][tT][eE][ \t\n]*:[ \t\n]*\{	{return KWD_CREATE;}
(var|function)[ \t\n]+[dD][eE][lL][eE][tT][eE][ \t\n;=(]|[dD][eE][lL][eE][tT][eE][ \t\n]*:[ \t\n]*\{	{return KWD_DELETE;}
(var|function)[ \t\n]+[dD][rR][oO][pP][ \t\n;=(]|[dD][rR][oO][pP][ \t\n]*:[ \t\n]*\{	{return KWD_DROP;}
(var|function)[ \t\n]+[eE][xX][eE][cC][uU][tT][eE][ \t\n;=(]|[eE][xX][eE][cC][uU][tT][eE][ \t\n]*:[ \t\n]*\{	{return KWD_EXECUTE;}
(var|function)[ \t\n]+[eE][xX][pP][lL][aA][iI][nN][ \t\n;=(]|[eE][xX][pP][lL][aA][iI][nN][ \t\n]*:[ \t\n]*\{	{return KWD_EXPLAIN;}
(var|function)[ \t\n]+[gG][rR][aA][nN][tT][ \t\n;=(]|[gG][rR][aA][nN][tT][ \t\n]*:[ \t\n]*\{	{return KWD_GRANT;}
(var|function)[ \t\n]+[iI][nN][fF][eE][rR][ \t\n;=(]|[iI][nN][fF][eE][rR][ \t\n]*:[ \t\n]*\{	{return KWD_INFER;}
(var|function)[ \t\n]+[iI][nN][sS][eE][rR][tT][ \t\n;=(]|[iI][nN][sS][eE][rR][tT][ \t\n]*:[ \t\n]*\{	{return KWD_INSERT;}
(var|function)[ \t\n]+[mM][eE][rR][gG][eE][ \t\n;=(]|[mM][eE][rR][gG][eE][ \t\n]*:[ \t\n]*\{	{return KWD_MERGE;}
(var|function)[ \t\n]+[pP][rR][eE][pP][aA][rR][eE][ \t\n;=(]|[pP][rR][eE][pP][aA][rR][eE][ \t\n]*:[ \t\n]*\{	{return KWD_PREPARE;}
(var|function)[ \t\n]+[rR][eE][nN][aA][mM][eE][ \t\n;=(]|[rR][eE][nN][aA][mM][eE][ \t\n]*:[ \t\n]*\{	{return KWD_RENAME;}
(var|function)[ \t\n]+[sS][eE][lL][eE][cC][tT][ \t\n;=(]|[sS][eE][lL][eE][cC][tT][ \t\n]*:[ \t\n]*\{	{return KWD_SELECT;}
(var|function)[ \t\n]+[rR][eE][vV][oO][kK][eE][ \t\n;=(]|[rR][eE][vV][oO][kK][eE][ \t\n]*:[ \t\n]*\{	{return KWD_REVOKE;}
(var|function)[ \t\n]+[uU][pP][dD][aA][tT][eE][ \t\n;=(]|[uU][pP][dD][aA][tT][eE][ \t\n]*:[ \t\n]*\{	{return KWD_UPDATE;}
(var|function)[ \t\n]+[uU][pP][sS][eE][rR][tT][ \t\n;=(]|[uU][pP][sS][eE][rR][tT][ \t\n]*:[ \t\n]*\{	{return KWD_UPSERT;}
[aA][lL][tT][eE][rR][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`alter ";}
[bB][uU][iI][lL][dD][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`build ";}
[cC][rR][eE][aA][tT][eE][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`create ";}
[dD][eE][lL][eE][tT][eE][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`delete ";}
[dD][rR][oO][pP][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`drop ";}
[eE][xX][eE][cC][uU][tT][eE][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`execute ";}
[eE][xX][pP][lL][aA][iI][nN][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`explain ";}
[gG][rR][aA][nN][tT][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`grant ";}
[iI][nN][fF][eE][rR][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`infer ";}
[iI][nN][sS][eE][rR][tT][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`insert ";}
[mM][eE][rR][gG][eE][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`merge ";}
[pP][rR][eE][pP][aA][rR][eE][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`prepare ";}
[rR][eE][nN][aA][mM][eE][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`rename ";}
[sS][eE][lL][eE][cC][tT][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`select ";}
[rR][eE][vV][oO][kK][eE][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`revoke ";}
[uU][pP][dD][aA][tT][eE][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`update ";}
[uU][pP][sS][eE][rR][tT][ \t\n][ \t\n]?	{BEGIN N1QL; plain_js+="new N1qlQuery(`upsert ";}
<N1QL>";"	{
				BEGIN INITIAL;
				plain_js+="`);";
			}
<N1QL>.	{
			if(yytext[0]=='`' && !is_esc())
				plain_js+="\\";
			plain_js+=string(yytext);
		}
<MLCMT,SLCMT,DSTR,SSTR,TSTR>.	{plain_js+=string(yytext);}
.	{plain_js+=string(yytext);}
\n	{plain_js+="\n";}
%%
// Parses the given input string.
int Jsify(const char* input, string *output)
{
	// Set the input stream.
	yy_scan_string(input);

	// Begin lexer.
	int code=yylex();

	// Clear the buffer allocation after the lex.
	yy_delete_buffer(YY_CURRENT_BUFFER);

	// Copy the output;
	*output=plain_js;

	// Clear the global variable for the next input.
	plain_js="";

	return code;
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
		plain_js+="\"";
		break;

	case SSTR:
		BEGIN SSTR;
		plain_js+="'";
		break;

	case TSTR:
		BEGIN TSTR;
		plain_js+="`";
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
		plain_js+="\"";
		break;

	case SSTR:
		plain_js+="'";
		break;

	case TSTR:
		plain_js+="`";
		break;
	}
}

// Tests whether the current character is an escape character.
bool is_esc()
{
	return plain_js.length()>0?plain_js[plain_js.length()-1]=='\\':0;
}
