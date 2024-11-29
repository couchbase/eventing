package parser

// Transpile embedded N1QL in JavaScript while preserving
// line numbers and whitespace as far as possible

import (
	"errors"
	"fmt"
	"html/template"
	"regexp"
	"sort"
	"strings"
)

var maybe_n1ql = regexp.MustCompile(
	`(?iU)` +
		`((\([[:space:]]*)*(?:alter|build|create|` +
		// 'delete' is a js keyword, so look for 'delete from'
		`delete[[:space:]]+from` +
		`|drop|execute|advise|explain|from|grant|infer|` +
		// 'with' is a js keyword, so look for 'with alias as'
		`with[[:space:]]+([[:word:]]|[[:punct:]])+(?:[[:space:]]+as\(?)` +
		`|insert|merge|prepare|rename|select|` +
		`revoke|update|upsert)[[:space:]]+` +
		`[^;]+;)`)

var spaced_line = regexp.MustCompile(
	`^([[:space:]]*)((?U).*)([[:space:]]*)$`)

var esc_lt = regexp.MustCompile(
	`([^\\])\\x3C`)

var esc_gt = regexp.MustCompile(
	`([^\\])\\x3E`)

var timer_use = regexp.MustCompile(
	`createTimer([[:space:]]*)\(`)

var printable_stmt = regexp.MustCompile(
	`^[[:print:]]*$`)

var function_name = regexp.MustCompile(
	`^function[[:space:]]+([_$a-zA-Z\xA0-\x{FFFF}][_$a-zA-Z0-9\xA0-\x{FFFF}]*)[[:space:]]*\(`)

var requiredFunctions = map[string]struct{}{"OnUpdate": struct{}{},
	"OnDelete": struct{}{}}

var n1qlQueryUse = regexp.MustCompile(
	`N1qlQuery([[:space:]]*)\(`)

var functionOverload = regexp.MustCompile(
	`(function[[:space:]]+(createTimer|cancelTimer|curl|log|crc64|N1QL|N1qlQuery|couchbase)[[:space:]]*\(` +
		`|([[:space:]|\\n|\;|\,|}]+(createTimer|cancelTimer|curl|log|crc64|N1QL|N1qlQuery|couchbase)([[:space:]]*\.[[:space:]]*[0-9a-zA-Z$_]+)?)[[:space:]]*=)`)

func stripComments(str string) string {
	return cleanse(str,
		false, // keepComment
		true,  // keepQuoted
		true,  // keepSpecial
		true,  // keepSpace
		false) // keepAlignment
}

func stripAll(str string) string {
	return cleanse(str,
		false, // keepComment
		false, // keepQuoted
		false, // keepSpecial
		false, // keepSpace
		true)  // keepAlignment
}

func cleanse(str string, keepComment, keepQuoted, keepSpecial, keepSpace, keepAlignment bool) string {
	washed := strings.Builder{}
	isComment, isQuoted, isSpecial, isSpace := false, false, false, false
	add := func(data interface{}) {
		var val string
		switch typed := data.(type) {
		case string:
			val = typed
		case byte:
			val = string([]byte{typed})
		case []byte:
			val = string(typed)
		default:
			panic("unknown type")
		}
		if isComment && !keepComment ||
			isSpecial && !keepSpecial ||
			isQuoted && !keepQuoted ||
			isSpace && !keepSpace {
			if keepAlignment {
				washed.WriteString(strings.Repeat(" ", len(val)))
			}
			return
		}
		washed.WriteString(val)
	}

	for esc, pos := "", 0; pos < len(str); pos++ {
		switch {
		case str[pos] == '\\' && pos < len(str)-1:
			// backquote
			isSpecial = true
			add(str[pos : pos+2])
			isSpecial = false
			pos++
		case len(esc) > 0:
			// currently escaping. check for end
			add(str[pos])
			end := pos + len(esc)
			if end > len(str) {
				end = len(str)
			}
			sub := str[pos:end]
			if sub == esc {
				for b := 1; b < len(esc); b++ {
					add(str[pos+b])
					pos++
				}
				esc = ""
				isComment, isQuoted = false, false
			}
		case strings.IndexByte("\"'`", str[pos]) >= 0:
			// escape quote begin
			isQuoted = true
			add(str[pos])
			esc = string(str[pos])
		case strings.HasPrefix(str[pos:], "/*"):
			// escape multi-line comment first
			isComment = true
			add(str[pos : pos+2])
			pos++
			esc = "*/"
		case strings.HasPrefix(str[pos:], "//"):
			// escape single-line comment next
			isComment = true
			add(str[pos : pos+2])
			pos++
			esc = "\n"
		case strings.IndexByte("\t\n\v\f\r", str[pos]) >= 0:
			// handle whitespace last
			isSpace = true
			add(str[pos])
			isSpace = false
		default:
			add(str[pos])
		}
	}
	return washed.String()
}

func jsEscapeString(str string) string {
	res := template.JSEscapeString(str)
	// undo escaped '<' and '>' as it is hard to read and we're not in html
	res = esc_lt.ReplaceAllString(res, `$1<`)
	res = esc_gt.ReplaceAllString(res, `$1>`)
	return res
}

func wrapQuery(query string, params string, n1ql_params string) string {
	lines := strings.Split(query, "\n")
	js_lines := []string{}
	for i, line := range lines {
		split := spaced_line.FindStringSubmatch(line)
		nl := ""
		if i < len(lines)-1 {
			nl = `\n`
		}
		body := stripComments(split[2])
		js_lines = append(js_lines,
			fmt.Sprintf(`%s'%s%s'%s`,
				split[1],
				jsEscapeString(body),
				nl,
				split[3]))
	}
	result := ""
	for i, line := range js_lines {
		if i == 0 {
			result += "couchbase.n1qlQuery("
		}
		result += line
		if i == len(js_lines)-1 {
			if n1ql_params == "" {
				result += ", " + params + ");"
			} else {
				result += ", " + params + ", " + n1ql_params + ");"
			}
		}
		if i < len(js_lines)-1 {
			result += " +\n"
		}
	}
	return result
}

type queryMatch struct {
	begin  int
	end    int
	params string
	info   NamedParamsInfo
}

func findQueries(input string) []queryMatch {
	matches := []queryMatch{}
	bare := stripAll(input)
	posns := maybe_n1ql.FindAllStringSubmatchIndex(bare, -1)
	for _, pos := range posns {
		query := input[pos[2]:pos[3]]
		query = stripComments(query)
		info := GetNamedParams(query)
		if !info.PInfo.IsValid {
			continue
		}
		sort.Strings(info.NamedParams)
		params := "{"
		for i, entry := range info.NamedParams {
			if i > 0 {
				params += `, `
			}
			safe_entry := jsEscapeString(entry)
			params += fmt.Sprintf(`'$%s':%s`, safe_entry, safe_entry)
		}
		params += "}"
		m := queryMatch{
			begin:  pos[2],
			end:    pos[3],
			params: params,
			info:   *info,
		}
		matches = append(matches, m)
	}
	return matches
}

func TranspileQueries(input string, n1ql_params string) (result string, info []NamedParamsInfo) {
	result = ""
	info = []NamedParamsInfo{}

	matches := findQueries(input)
	sort.SliceStable(matches, func(i, j int) bool {
		return matches[i].begin < matches[j].begin
	})
	pos := 0
	for _, match := range matches {
		result += input[pos:match.begin]
		result += wrapQuery(input[match.begin:match.end], match.params, n1ql_params)
		pos = match.end
		info = append(info, match.info)
	}
	result += input[pos:]
	return
}

type parsedStatements struct {
	stmts           []string
	onDeployPresent bool
}

func GetStatements(input string) *parsedStatements {
	js := stripAll(input)
	parsed := &parsedStatements{}
	start := 0
	for pos := 0; pos < len(js); pos++ {
		if js[pos] == '{' || js[pos] == '}' || js[pos] == ';' {
			part := strings.TrimSpace(js[start:pos])
			if len(part) > 0 {
				parsed.stmts = append(parsed.stmts, part)
			}
			parsed.stmts = append(parsed.stmts, string(js[pos]))
			start = pos + 1
		}
	}
	if start < len(input) {
		part := strings.TrimSpace(js[start:])
		if len(part) > 0 {
			parsed.stmts = append(parsed.stmts, part)
		}
	}
	return parsed
}

func (parsed *parsedStatements) ValidateStructure() error {
	depth, onDeletes, onUpdates, onDeploys := 0, 0, 0, 0
	for _, stmt := range parsed.stmts {
		switch stmt {
		case "{":
			depth++
		case "}":
			depth--
		default:
			if depth > 0 {
				continue
			}
			function := function_name.FindStringSubmatch(stmt)
			if len(function) > 1 {
				functionName := function[1]
				if functionName == "OnUpdate" {
					onUpdates++
				}
				if functionName == "OnDelete" {
					onDeletes++
				}
				if functionName == "OnDeploy" {
					onDeploys++
				}
			}
			if !strings.HasPrefix(stmt, "function") {
				msg := fmt.Sprintf("Only functions allowed in global space, but found: %s", stmt)
				if !printable_stmt.MatchString(stmt) {
					msg += fmt.Sprintf(" bytes: %v", []byte(stmt))
				}
				return errors.New(msg)
			}
		}
	}

	if onUpdates == 0 && onDeletes == 0 {
		err := errors.New("handler must have at least OnUpdate() or OnDelete() function")
		return err
	}

	if onUpdates > 1 {
		err := errors.New("handler cannot have multiple OnUpdate() functions")
		return err
	}

	if onDeletes > 1 {
		err := errors.New("handler code cannot have multiple OnDelete() functions")
		return err
	}

	if onDeploys > 1 {
		err := errors.New("handler code cannot have multiple OnDeploy() functions")
		return err
	} else if onDeploys == 1 {
		parsed.onDeployPresent = true
	}

	return nil
}

func (parsed *parsedStatements) UsingOnDeploy() bool {
	return parsed.onDeployPresent
}

func IsCodeUsingOnDeploy(input string) bool {
	parsedCode := GetStatements(input)
	parsedCode.ValidateStructure()
	return parsedCode.UsingOnDeploy()
}

func UsingTimer(input string) bool {
	bare := stripAll(input)
	return timer_use.MatchString(bare)
}

func ListDeprecatedFunctions(input string) []string {
	bare := stripAll(input)
	listOfFns := []string{}
	if n1qlQueryUse.MatchString(bare) {
		listOfFns = append(listOfFns, "N1qlQuery")
	}
	return listOfFns
}

func ListOverloadedFunctions(input string) []string {
	bare := stripAll(input)
	listOfFns := []string{}
	// Below list contains all the APIs that can be accessed from the JS source.
	// Any attempt to overwrite them will display a warning to the user.
	// In future, whenever we expose more APIs we need to maintain this and
	// alter the regex in "var functionOverload" in the definition section of this file
	// and add the name of the function to the below list.
	builtIns := []string{"createTimer", "cancelTimer", "curl",
		"log", "crc64", "N1QL", "N1qlQuery", "couchbase",
		"couchbase.get", "couchbase.insert", "couchbase.upsert",
		"couchbase.replace", "couchbase.delete",
		"couchbase.increment", "couchbase.decrement"}
	matches := strings.Join(functionOverload.FindAllString(bare, -1), ",")
	for _, item := range builtIns {
		if strings.Contains(matches, item) {
			listOfFns = append(listOfFns, item)
		}
	}
	return listOfFns
}
