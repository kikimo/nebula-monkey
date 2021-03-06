/*
Copyright © 2022 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"math/rand"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/kikimo/nebula-monkey/cmd/multikv/cmd"
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
