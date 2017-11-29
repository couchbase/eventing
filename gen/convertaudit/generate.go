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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func main() {
	if len(os.Args) != 3 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		fmt.Printf("usage: generate <source file> <output file>\n")
		os.Exit(1)
	}

	src, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	var desc map[string]interface{}
	err = json.Unmarshal([]byte(src), &desc)
	if err != nil {
		panic(err)
	}

	dst, err := os.Create(os.Args[2])
	if err != nil {
		panic(err)
	}
	defer dst.Close()

	writer := bufio.NewWriter(dst)
	defer writer.Flush()

	fmt.Fprintf(writer, "// Automatically generated from %v, do not edit\n", os.Args[1])
	fmt.Fprintf(writer, "package auditevent\n")
	fmt.Fprintf(writer, "type AuditEvent uint32\n")
	fmt.Fprintf(writer, "const (\n")

	events := desc["events"].([]interface{})
	for _, raw := range events {
		event := raw.(map[string]interface{})
		id := int(event["id"].(float64))
		name := strings.Replace(event["name"].(string), " ", "", -1)
		fmt.Fprintf(writer, "  %s = AuditEvent(%d)\n", name, id)
	}

	fmt.Fprintf(writer, ")\n")
}
