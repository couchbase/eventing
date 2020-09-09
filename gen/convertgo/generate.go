/**
 * @copyright 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

/* This code takes in list of file names in argv and embed the contents
 * into a const string.
 **/

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) != 4 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		fmt.Printf("usage: generate <source file> <variable/package name> <output file to append>\n")
		os.Exit(1)
	}

	src, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	dst, err := os.Create(os.Args[3])
	if err != nil {
		panic(err)
	}
	defer dst.Close()

	writer := bufio.NewWriter(dst)
	defer writer.Flush()

	dir := filepath.Dir(os.Args[3])
	pkg := filepath.Base(dir)
	if len(pkg) == 0 || strings.Contains(pkg, ".") {
		panic(fmt.Sprintf("unable to figure out package name converting %v", os.Args))
	}

	fmt.Fprintf(writer, "// Automatically generated from %v, do not edit\n", os.Args[1])
	fmt.Fprintf(writer, "package %v\n", pkg)
	fmt.Fprintf(writer, "const %v =\n`", os.Args[2])
	for _, ch := range src {
		switch ch {
		case '`':
			fmt.Fprintf(writer, "` + \"`\" + `")
		default:
			fmt.Fprintf(writer, "%c", ch)
		}
	}
	fmt.Fprintln(writer, "`")
}
