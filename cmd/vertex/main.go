/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

*/
package main

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/cmd/vertex/cmd"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()

	defer func() {
		glog.Flush()
	}()

	cmd.Execute()
}
