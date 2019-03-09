package main

import (
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
)

func getQueryBool(r *http.Request, name string) bool {
	value, err := strconv.ParseBool(r.FormValue(name))
	if err != nil {
		log.Trace(err)
	}
	log.Debugf("%v: %v", name, value)
	return value
}

func getQueryString(r *http.Request, name string) string {
	value := r.FormValue(name)
	log.Debugf("%v: %v", name, value)
	return value
}

func getQueryInt(r *http.Request, name string) int {
	value, _ := strconv.Atoi(r.FormValue(name))
	log.Debugf("%v: %v", name, value)
	return value
}

func getHeaderString(r *http.Request, name string) string {
	value := r.Header.Get(name)
	log.Debugf("%v: %v", name, value)
	return value
}
