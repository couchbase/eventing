// +build all handler

package eventing

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/couchbase/eventing/parser"
)

var snippet_inputs = []string{
	"select * from `beer-samples`;",
	"select 23 from beer; update a set a=5 where 2=3;",
	`var anr = select * from hello;`,
	`var foo = "select * from this_is_a_comment;";`,
	`select * from a; select * from b; select 23 ;`,
	`select 23;`,
	`// select 23;`,
	`/* select 23; */`,
	`
		var foo = "hello";
		var bar = "he'llo";
		var baz = 'hello"';
		var gaa = /* hi */ 23;
		var baz = hello\\;;
		var boo=delete from beers where a > 23 and b < 56;
		// a comment
		/*
		var bar = "he'llo";
		var baz = 'hello"';
		select * from beers where a="this is in a comment";
		*/
		var hello = 2 + 3;
		if (a > 5) b++;
		select * from beers where foo = bar;
		select *
		  from helloworld
		    where a="23" and b=26;
		var f = "hello\"world";
		var x = select(23);
		var x = select 'hello' from bar where t="23";
		var y = select 'hello' from bar where t='23';
		var z = select 'hello' from bar where t="23";
	`,
	`select * from beers; // and a comment`,
	`select * from "invalid-n1ql-syntax" where a=23`,
	`delete everything; // invalid-n1ql-syntax`,
	`select * from select * from beers;`,
	`	var foo = 23;
		var bar = select * from beerbkt where arg = $foo and bar = $bar and xx = 23;
	`,
	"var bar = select * from beerbkt where arg = $foo and bar = `$bar`;",
	"var bar = select * from beerbkt where\narg = $foo and bar = `$bar`;",
	`var bar = select /* hello *`,
	`var bar = select * /**/ from foo;`,
	`	var foo = 23;
		var bar = select * from beerbkt where arg = $foo and bar = $bar and xx = 23;
		var origin = "BLR"
		var destn = "LONDON"
		var bar = select * from travelsim where origin = $origin and destn = $destn and xx = 23;
		var val = 'Hello World'
		var bar = UPSERT INTO gamesim (KEY, VALUE) VALUES ('reskey', $val);
		var upsert_query5 = couchbase.n1qlQuery('UPSERT INTO eventing-bucket-1 (KEY, VALUE) VALUES ($docId5, \'Hello World5\');', {'$docId5':docId5}, {'consistency' : 'request'});
	`,
	`var bar = select * from beerbkt where//opening comment
		//another comment
		/* yet another comment */
		something=nothing and // or // exists // any
		nosuchthing = /* annoying comment */ "nosuchvalue"; // ending comment`,
	`DELETE from dst_bucket.scope_1.coll_4 USE KEYS "newDocID2";`,
	`var curl=SELECT CURL("http://localhost:8091/pools/default/buckets",{"header":"authorization: Basic HelloWorldAbcdefghijklmnopqrstuvwxyz==",
		  "request":"GET"});`,
	`var query = (SELECT * FROM src_bucket WHERE NOT (NUMERIC_FIELD IS NOT NULL) ORDER BY NUMERIC_FIELD_LIST, STRING_FIELD_LIST, BOOL_FIELD_LIST DESC)
		UNION
		(SELECT * FROM src_bucket WHERE
			(NUMERIC_FIELD IS NULL OR ((STRING_FIELD IS NOT NULL) OR (STRING_FIELD <= STRING_VALUES)) AND (STRING_FIELD NOT BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE))
			ORDER BY STRING_FIELD_LIST);`,
}

var snippet_outputs = []string{
	"couchbase.n1qlQuery('select * from `beer-samples`;', {});",
	`couchbase.n1qlQuery('select 23 from beer;', {}); couchbase.n1qlQuery('update a set a=5 where 2=3;', {});`,
	`var anr = couchbase.n1qlQuery('select * from hello;', {});`,
	`var foo = "select * from this_is_a_comment;";`,
	`couchbase.n1qlQuery('select * from a;', {}); couchbase.n1qlQuery('select * from b;', {}); couchbase.n1qlQuery('select 23 ;', {});`,
	`couchbase.n1qlQuery('select 23;', {});`,
	`// select 23;`,
	`/* select 23; */`,
	`
		var foo = "hello";
		var bar = "he'llo";
		var baz = 'hello"';
		var gaa = /* hi */ 23;
		var baz = hello\\;;
		var boo=couchbase.n1qlQuery('delete from beers where a > 23 and b < 56;', {});
		// a comment
		/*
		var bar = "he'llo";
		var baz = 'hello"';
		select * from beers where a="this is in a comment";
		*/
		var hello = 2 + 3;
		if (a > 5) b++;
		couchbase.n1qlQuery('select * from beers where foo = bar;', {});
		couchbase.n1qlQuery('select *\n' +
		  'from helloworld\n' +
		    'where a=\"23\" and b=26;', {});
		var f = "hello\"world";
		var x = select(23);
		var x = couchbase.n1qlQuery('select \'hello\' from bar where t=\"23\";', {});
		var y = couchbase.n1qlQuery('select \'hello\' from bar where t=\'23\';', {});
		var z = couchbase.n1qlQuery('select \'hello\' from bar where t=\"23\";', {});
	`,
	`couchbase.n1qlQuery('select * from beers;', {}); // and a comment`,
	`select * from "invalid-n1ql-syntax" where a=23`,
	`delete everything; // invalid-n1ql-syntax`,
	`select * from select * from beers;`,
	`	var foo = 23;
		var bar = couchbase.n1qlQuery('select * from beerbkt where arg = $foo and bar = $bar and xx = 23;', {'$bar':bar, '$foo':foo});
	`,
	"var bar = couchbase.n1qlQuery('select * from beerbkt where arg = $foo and bar = `$bar`;', {'$foo':foo});",
	"var bar = couchbase.n1qlQuery('select * from beerbkt where\\n' +\n'arg = $foo and bar = `$bar`;', {'$foo':foo});",
	`var bar = select /* hello *`,
	`var bar = couchbase.n1qlQuery('select *  from foo;', {});`,
	`	var foo = 23;
		var bar = couchbase.n1qlQuery('select * from beerbkt where arg = $foo and bar = $bar and xx = 23;', {'$bar':bar, '$foo':foo});
		var origin = "BLR"
		var destn = "LONDON"
		var bar = couchbase.n1qlQuery('select * from travelsim where origin = $origin and destn = $destn and xx = 23;', {'$destn':destn, '$origin':origin});
		var val = 'Hello World'
		var bar = couchbase.n1qlQuery('UPSERT INTO gamesim (KEY, VALUE) VALUES (\'reskey\', $val);', {'$val':val});
		var upsert_query5 = couchbase.n1qlQuery('UPSERT INTO eventing-bucket-1 (KEY, VALUE) VALUES ($docId5, \'Hello World5\');', {'$docId5':docId5}, {'consistency' : 'request'});
	`,
	`var bar = couchbase.n1qlQuery('select * from beerbkt where\n' +
		'\n' +
		'\n' +
		'something=nothing and \n' +
		'nosuchthing =  \"nosuchvalue\";', {}); // ending comment`,
	`couchbase.n1qlQuery('DELETE from dst_bucket.scope_1.coll_4 USE KEYS \"newDocID2\";', {});`,
	`var curl=couchbase.n1qlQuery('SELECT CURL(\"http://localhost:8091/pools/default/buckets\",{\"header\":\"authorization: Basic HelloWorldAbcdefghijklmnopqrstuvwxyz==\",\n' +
		  '\"request\":\"GET\"});', {});`,
	`var query = couchbase.n1qlQuery('(SELECT * FROM src_bucket WHERE NOT (NUMERIC_FIELD IS NOT NULL) ORDER BY NUMERIC_FIELD_LIST, STRING_FIELD_LIST, BOOL_FIELD_LIST DESC)\n' +
		'UNION\n' +
		'(SELECT * FROM src_bucket WHERE\n' +
			'(NUMERIC_FIELD IS NULL OR ((STRING_FIELD IS NOT NULL) OR (STRING_FIELD <= STRING_VALUES)) AND (STRING_FIELD NOT BETWEEN LOWER_BOUND_VALUE and UPPER_BOUND_VALUE))\n' +
			'ORDER BY STRING_FIELD_LIST);', {});`,
}

const template = `
  function OnUpdate() {
  }
`

type parserTestCase struct {
	input string
	valid bool
	timer bool
}

var script_data = []parserTestCase{
	parserTestCase{
		input: `var goo = 23` + template,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `// var foo = 23` + template,
		valid: true,
		timer: false,
	},
	parserTestCase{
		input: ` function hello() {
						var a = 23;
					}` + template,
		valid: true,
		timer: false,
	},
	parserTestCase{
		input: `function there() { var baz }` + template,
		valid: true,
		timer: false,
	},
	parserTestCase{
		input: ``,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `{var foo=createTimer\n();}` + template,
		valid: true,
		timer: true,
	},
	parserTestCase{
		input: `{createTimer();` + template,
		valid: false,
		timer: true,
	},
	parserTestCase{
		input: ` var foo = 23
					function foo() {}` + template,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: "\xbd\xb2\x3d\xbc" + template,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `function OnUpdate () { createTimer()}`,
		valid: true,
		timer: true,
	},
	parserTestCase{
		input: `function\nOnDelete() {}`,
		valid: true,
		timer: false,
	},
	parserTestCase{
		input: `function OnUpdate           (){}`,
		valid: true,
		timer: false,
	},
	parserTestCase{
		input: `// function OnDelete(`,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `function test() {} function OnUpdate() {}`,
		valid: true,
		timer: false,
	},
	parserTestCase{
		input: `"function OnUpdate () {}"`,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `/*function OnUpdate () {}*/function OnUpdate() {}`,
		valid: true,
		timer: false,
	},
	parserTestCase{
		input: `function OnUpdate () {} function OnUpdate () {}`,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `var goo = 23`,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `// var foo = 23`,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: ` function hello() {
						var a = 23;
					}`,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `function there() { var baz }`,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: ``,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: `{var foo=createTimer\n();'}`,
		valid: false,
		timer: true,
	},
	parserTestCase{
		input: `{createTimer();`,
		valid: false,
		timer: true,
	},
	parserTestCase{
		input: `var foo = 23
        function foo() {}`,
		valid: false,
		timer: false,
	},
	parserTestCase{
		input: "\xbd\xb2\x3d\xbc",
		valid: false,
		timer: false,
	},
}

var esc_lt = regexp.MustCompile(
	`\\u003C`)

var esc_gt = regexp.MustCompile(
	`\\u003E`)

var esc_eq = regexp.MustCompile(
	`\\u003D`)

func normalise(res string) string {
	res = esc_lt.ReplaceAllString(res, `$1<`)
	res = esc_gt.ReplaceAllString(res, `$1>`)
	res = esc_eq.ReplaceAllString(res, `$1=`)
	return res
}

func TestParserTransform(t *testing.T) {
	for i := 0; i < len(snippet_inputs); i++ {
		result, _ := parser.TranspileQueries(snippet_inputs[i], "")
		expectedOutput := snippet_outputs[i]
		normalisedResult := normalise(result)
		if normalisedResult != expectedOutput {
			t.Errorf("Mismatch: %s\nExpected:\n%s\nGot:\n%s\n", Diff(expectedOutput, normalisedResult), expectedOutput, normalisedResult)
		}
	}
}

func TestParserDetect(t *testing.T) {
	for i := 0; i < len(script_data); i++ {
		parsed := parser.GetStatements(script_data[i].input)
		err := parsed.ValidateStructure()
		allowed := (err == nil)
		if allowed != script_data[i].valid {
			t.Errorf("Mismatch structure check:%s\nExpected:%v\nGot:%v\nError:%v\n", script_data[i].input, script_data[i].valid, allowed, err)
		}

		timers := parser.UsingTimer(script_data[i].input)
		if timers != script_data[i].timer {
			t.Errorf("Mismatch timer check:%s\nExpected:%v\nGot:%v\n", script_data[i].input, script_data[i].timer, timers)
		}
	}
}

func Diff(e, a string) string {
	l_a := len(a)
	l_e := len(e)
	diff := -1
	for i := 0; i < l_a && i < l_e; i++ {
		if a[i] != e[i] {
			diff = i
			break
		}
	}
	switch {
	case l_a != l_e && diff == -1:
		return fmt.Sprintf("actual len %d expected %d, shorter is a prefix of longer", l_a, l_e)
	case l_a != l_e && diff != -1:
		return fmt.Sprintf("actual len %d expected %d, first diff at idx %d actual char '%c'(%d) vs expected char '%c'(%d)", l_a, l_e, diff, a[diff], a[diff], e[diff], e[diff])
	case l_a == l_e && diff != -1:
		return fmt.Sprintf("lengths are same, first diff at idx %d actual char '%c'(%d) vs expected char '%c'(%d)", diff, a[diff], a[diff], e[diff], e[diff])
	}
	return ""
}
