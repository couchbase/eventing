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
 * into a const char array.
 **/

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
)

func main() {
	if len(os.Args) != 4 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		fmt.Printf("usage: generate <source file> <variable name> <output file to append>\n")
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

	fmt.Fprintf(writer, "// Automatically generated from %v, do not edit\n", os.Args[1])
	fmt.Fprintf(writer, "const unsigned char %v[] = {", os.Args[2])
	for idx := 0; idx < len(src); idx++ {
		if idx%20 == 0 {
			fmt.Fprintf(writer, "\n   ")
		}
		fmt.Fprintf(writer, "0x%02x,", src[idx])
	}
	fmt.Fprintf(writer, "0x00\n};\n")
}
