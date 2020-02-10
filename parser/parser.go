package main

// Transpile embedded N1QL in JavaScript while preserving
// line numbers and whitespace as far as possible

import (
	"fmt"
	"html/template"
	"regexp"
	"sort"
	"strings"
)

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

var esc_lt = regexp.MustCompile(
	`([^\\])\\x3C`)

var esc_gt = regexp.MustCompile(
	`([^\\])\\x3E`)

func whitewash(str string) string {
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
}

func WrapQuery(query string, params string) string {
	lines := strings.Split(query, "\n")
	js_lines := []string{}
	for i, line := range lines {
		split := spaced_line.FindStringSubmatch(line)
		nl := ""
		if i < len(lines)-1 {
			nl = `\n`
		}
		js_lines = append(js_lines,
			fmt.Sprintf(`%s'%s%s'%s`,
				split[1],
				JSEscapeString(split[2]),
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
			result += ", " + params + ");"
		}
		if i < len(js_lines)-1 {
			result += "+\n"
		}
	}
	return result
}

func FindQueries(input string) []Match {
	matches := []Match{}
	bare := whitewash(input)
	posns := maybe_n1ql.FindAllStringSubmatchIndex(bare, -1)
	for _, pos := range posns {
		query := input[pos[2]:pos[3]]
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
		matches = append(matches, m)
	}
	return matches
}

func TranspileQueries(input string) string {
	result := ""
	matches := FindQueries(input)
	sort.SliceStable(matches, func(i, j int) bool {
		return matches[i].Begin < matches[j].Begin
	})
	pos := 0
	for _, match := range matches {
		result += input[pos:match.Begin]
		result += WrapQuery(input[match.Begin:match.End], match.Params)
		pos = match.End
	}
	result += input[pos:]
	return result
}
