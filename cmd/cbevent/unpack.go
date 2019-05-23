package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"reflect"
)

func unpack(handlerfn string, codefn string) {
	log.Printf("Starting unpack of %v into %v", handlerfn, codefn)
	hdata, err := ioutil.ReadFile(handlerfn)
	if err != nil {
		log.Fatalf("Error reading handler file %v: %v", handlerfn, err)
	}
	var handler interface{}
	err = json.Unmarshal(hdata, &handler)
	if err != nil {
		log.Fatalf("Error unmarshaling handler file %v: %v", handlerfn, err)
	}
	top, ok := handler.([]interface{})
	if !ok || len(top) < 1 {
		log.Fatalf("Invalid handler, expecting array but got %v", reflect.TypeOf(handler))
	}
	app, ok := top[0].(map[string]interface{})
	if !ok {
		log.Fatalf("Invalid handler, expecting object got %v", reflect.TypeOf(app))
	}
	appcode, ok := app["appcode"].(string)
	if !ok {
		log.Fatalf("Invalid handler, expecting string got %v", reflect.TypeOf(appcode))
	}

	err = ioutil.WriteFile(codefn, []byte(appcode), 0600)
	if err != nil {
		log.Fatalf("Error writing code file %v: %v", codefn, err)
	}
	log.Printf("Finished unpack of %v into %v", handlerfn, codefn)
}

func pack(handlerfn string, codefn string) {
	log.Printf("Starting pack of %v into %v", codefn, handlerfn)
	hdata, err := ioutil.ReadFile(handlerfn)
	if err != nil {
		log.Fatalf("Error reading handler file %v: %v", handlerfn, err)
	}
	var handler interface{}
	err = json.Unmarshal(hdata, &handler)
	if err != nil {
		log.Fatalf("Error unmarshaling handler file %v: %v", handlerfn, err)
	}
	top, ok := handler.([]interface{})
	if !ok || len(top) < 1 {
		log.Fatalf("Invalid handler, expecting array but got %v", reflect.TypeOf(handler))
	}
	app, ok := top[0].(map[string]interface{})
	if !ok {
		log.Fatalf("Invalid handler, expecting object got %v", reflect.TypeOf(app))
	}

	cdata, err := ioutil.ReadFile(codefn)
	if err != nil {
		log.Fatalf("Error reading code file %v: %v", codefn, err)
	}

	app["appcode"] = string(cdata)

	ndata, err := json.MarshalIndent(handler, "", "  ")
	if err != nil {
		log.Fatalf("Error marshaling handler: %v", err)
	}

	err = ioutil.WriteFile(handlerfn, []byte(ndata), 0600)
	if err != nil {
		log.Fatalf("Error writing handler file %v: %v", handlerfn, err)
	}
	log.Printf("Finished pack of %v into %v", codefn, handlerfn)
}
