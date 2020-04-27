// +build all handler

package eventing

import (
	"fmt"
	"github.com/couchbase/eventing/parser"
	"testing"
)

var input = []string{
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
		var upsert_query5 = N1QL('UPSERT INTO eventing-bucket-1 (KEY, VALUE) VALUES ($docId5, \'Hello World5\');', {'$docId5':docId5}, {'consistency' : 'request'});
	`,
	`var foo = 2 ** (3 + 1);`,
}

var output = []string{
	"N1QL('select * from `beer-samples`;', {});",
	`N1QL('select 23 from beer;', {}); N1QL('update a set a=5 where 2=3;', {});`,
	`var anr = N1QL('select * from hello;', {});`,
	`var foo = "select * from this_is_a_comment;";`,
	`N1QL('select * from a;', {}); N1QL('select * from b;', {}); N1QL('select 23 ;', {});`,
	`N1QL('select 23;', {});`,
	`// select 23;`,
	`/* select 23; */`,
	`
		var foo = "hello";
		var bar = "he'llo";
		var baz = 'hello"';
		var gaa = /* hi */ 23;
		var baz = hello\\;;
		var boo=N1QL('delete from beers where a > 23 and b < 56;', {});
		// a comment
		/*
		var bar = "he'llo";
		var baz = 'hello"';
		select * from beers where a="this is in a comment";
		*/
		var hello = 2 + 3;
		if (a > 5) b++;
		N1QL('select * from beers where foo = bar;', {});
		N1QL('select *\n'+
		  'from helloworld\n'+
		    'where a=\"23\" and b=26;', {});
		var f = "hello\"world";
		var x = select(23);
		var x = N1QL('select \'hello\' from bar where t=\"23\";', {});
		var y = N1QL('select \'hello\' from bar where t=\'23\';', {});
		var z = N1QL('select \'hello\' from bar where t=\"23\";', {});
	`,
	`N1QL('select * from beers;', {}); // and a comment`,
	`select * from "invalid-n1ql-syntax" where a=23`,
	`delete everything; // invalid-n1ql-syntax`,
	`select * from select * from beers;`,
	`	var foo = 23;
		var bar = N1QL('select * from beerbkt where arg = $foo and bar = $bar and xx = 23;', {'$bar':bar, '$foo':foo});
	`,
	"var bar = N1QL('select * from beerbkt where arg = $foo and bar = `$bar`;', {'$foo':foo});",
	"var bar = N1QL('select * from beerbkt where\\n'+\n'arg = $foo and bar = `$bar`;', {'$foo':foo});",
	`var bar = select /* hello *`,
	`var bar = N1QL('select * /**/ from foo;', {});`,
	`	var foo = 23;
		var bar = N1QL('select * from beerbkt where arg = $foo and bar = $bar and xx = 23;', {'$bar':bar, '$foo':foo});
		var origin = "BLR"
		var destn = "LONDON"
		var bar = N1QL('select * from travelsim where origin = $origin and destn = $destn and xx = 23;', {'$destn':destn, '$origin':origin});
		var val = 'Hello World'
		var bar = N1QL('UPSERT INTO gamesim (KEY, VALUE) VALUES (\'reskey\', $val);', {'$val':val});
		var upsert_query5 = N1QL('UPSERT INTO eventing-bucket-1 (KEY, VALUE) VALUES ($docId5, \'Hello World5\');', {'$docId5':docId5}, {'consistency' : 'request'});
	`,
	`var foo = 2 ** (3 + 1);`,
}

func TestTranspile(t *testing.T) {
	for i := 0; i < len(input); i++ {
		result, _ := parser.TranspileQueries(input[i], "")
		if result != output[i] {
			t.Errorf("Mismatch: %s\nExpected:\n%s\nGot:\n%s\n", Diff(output[i], result), output[i], result)
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
		return fmt.Sprintf("actual len %d expected %d, first diff at idx %d actual char '%v' vs expected char '%v'", l_a, l_e, diff, a[diff], e[diff])
	case l_a == l_e && diff != -1:
		return fmt.Sprintf("lengths are same, first diff at idx %d actual char '%v' vs expected char '%v'", diff, a[diff], e[diff])
	}
	return ""
}
