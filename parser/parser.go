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

type ParsedStatements struct {
	stmts []string
}

var maybe_n1ql = regexp.MustCompile(
	`(?iU)` +
		`((?:alter|build|create|` +
		// 'delete' is a js keyword, so look for 'delete from'
		`delete[[:space:]]+from[[:space:]]+[[:word:]]+` +
		`|drop|execute|explain|from|grant|infer|` +
		`insert|merge|prepare|rename|select|` +
		`revoke|update|upsert) +` +
		`[^;]+;)`)

var spaced_line = regexp.MustCompile(
	`^([[:space:]]*)((?U).*)([[:space:]]*)$`)

var commented_line = regexp.MustCompile(
	`//(.*)(\n|$)`)

var esc_lt = regexp.MustCompile(
	`([^\\])\\x3C`)

var esc_gt = regexp.MustCompile(
	`([^\\])\\x3E`)

var timer_use = regexp.MustCompile(
	`createTimer([[:space:]]*)\(`)

var printable_stmt = regexp.MustCompile(
	`^[[:print:]]*$`)

var function_name = regexp.MustCompile(
	`^function[[:space:]]+([A-Za-z]+)[[:space:]]*\(`)

var requiredFunctions = map[string]struct{}{"OnUpdate": struct{}{},
	"OnDelete": struct{}{}}

var n1qlQueryUse = regexp.MustCompile(
	`N1qlQuery([[:space:]]*)\(`)

func cleanse(str string) string {
	washed := []byte(str)
	for esc, sub, pos := "", "", 0; pos < len(str); pos++ {
		switch {
		case str[pos] == '\\' && pos < len(str)-1:
			// backquote
			washed[pos], washed[pos+1] = ' ', ' '
			pos++
		case len(esc) > 0:
			// currently escaping. check for end
			washed[pos] = ' '
			end := pos + len(esc)
			if end > len(str) {
				end = len(str)
			}
			sub = str[pos:end]
			if sub == esc {
				for b := 1; b < len(esc); b++ {
					washed[pos+b] = ' '
					pos++
				}
				esc = ""
			}
		case strings.IndexByte("\"'`", str[pos]) >= 0:
			// escape quote begin
			washed[pos] = ' '
			esc = string(str[pos])
		case strings.HasPrefix(str[pos:], "/*"):
			// escape multi-line comment begin
			washed[pos], washed[pos+1] = ' ', ' '
			pos++
			esc = "*/"
		case strings.HasPrefix(str[pos:], "//"):
			// escape single-line comment begin
			washed[pos], washed[pos+1] = ' ', ' '
			pos++
			esc = "\n"
		case strings.IndexByte("\t\n\v\f\r", str[pos]) >= 0:
			// replace whitespace with plain space. do this last
			washed[pos] = ' '
		}
	}
	return string(washed)
}

func JSEscapeString(str string) string {
	res := template.JSEscapeString(str)
	// undo escaped '<' and '>' as it is hard to read and we're not in html
	res = esc_lt.ReplaceAllString(res, `$1<`)
	res = esc_gt.ReplaceAllString(res, `$1>`)
	return res
}

type Match struct {
	Begin  int
	End    int
	Params string
	Info   NamedParamsInfo
}

func WrapQuery(query string, params string, n1ql_params string) string {
	lines := strings.Split(query, "\n")
	js_lines := []string{}
	for i, line := range lines {
		split := spaced_line.FindStringSubmatch(line)
		nl := ""
		if i < len(lines)-1 {
			nl = `\n`
		}
		body := commented_line.ReplaceAllString(split[2], " -- $1$2")
		js_lines = append(js_lines,
			fmt.Sprintf(`%s'%s%s'%s`,
				split[1],
				JSEscapeString(body),
				nl,
				split[3]))
	}
	result := ""
	for i, line := range js_lines {
		if i == 0 {
			result += "N1QL("
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

func FindQueries(input string) []Match {
	matches := []Match{}
	bare := cleanse(input)
	posns := maybe_n1ql.FindAllStringSubmatchIndex(bare, -1)
	for _, pos := range posns {
		query := input[pos[2]:pos[3]]
		query = commented_line.ReplaceAllString(query, " -- $1$2")
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
			safe_entry := JSEscapeString(entry)
			params += fmt.Sprintf(`'$%s':%s`, safe_entry, safe_entry)
		}
		params += "}"
		m := Match{}
		m.Begin = pos[2]
		m.End = pos[3]
		m.Params = params
		m.Info = *info
		matches = append(matches, m)
	}
	return matches
}

func TranspileQueries(input string, n1ql_params string) (result string, info []NamedParamsInfo) {
	result = ""
	info = []NamedParamsInfo{}

	matches := FindQueries(input)
	sort.SliceStable(matches, func(i, j int) bool {
		return matches[i].Begin < matches[j].Begin
	})
	pos := 0
	for _, match := range matches {
		result += input[pos:match.Begin]
		result += WrapQuery(input[match.Begin:match.End], match.Params, n1ql_params)
		pos = match.End
		info = append(info, match.Info)
	}
	result += input[pos:]
	return
}

func GetStatements(input string) *ParsedStatements {
	js := cleanse(input)
	parsed := &ParsedStatements{}
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

func (parsed *ParsedStatements) ValidateGlobals() (bool, error) {
	depth := 0
	for _, stmt := range parsed.stmts {
		switch stmt {
		case "{":
			depth++
		case "}":
			depth--
		default:
			if depth <= 0 && !strings.HasPrefix(stmt, "function") {
				msg := fmt.Sprintf("Only functions allowed in global space, but found: %s", stmt)
				if !printable_stmt.MatchString(stmt) {
					msg += fmt.Sprintf(" bytes: %v", []byte(stmt))
				}
				return false, errors.New(msg)
			}
		}
	}
	return true, nil
}

func (parsed *ParsedStatements) ValidateExports() (bool, error) {
	depth := 0
	for _, stmt := range parsed.stmts {
		switch stmt {
		case "{":
			depth++
		case "}":
			depth--
		default:
			if depth <= 0 {
				function := function_name.FindStringSubmatch(stmt)
				if len(function) >= 2 {
					functionName := strings.TrimSpace(function[1])
					if _, ok := requiredFunctions[functionName]; ok {
						return true, nil
					}
				}
			}
		}
	}
	msg := fmt.Sprintf("Handler code is missing OnUpdate() and OnDelete() functions. At least one of them is needed to deploy the handler")
	return false, errors.New(msg)
}

func UsingTimer(input string) bool {
	bare := cleanse(input)
	return timer_use.MatchString(bare)
}

func ListDeprecatedFunctions(input string) []string {
	bare := cleanse(input)
	listOfFns := []string{}
	if n1qlQueryUse.MatchString(bare) {
		listOfFns = append(listOfFns, "N1qlQuery")
	}
	return listOfFns
}
