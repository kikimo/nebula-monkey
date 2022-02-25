module github.com/kikimo/nebula-monkey

go 1.16

require (
	github.com/abrander/go-supervisord v0.0.0-20210517172913-a5469a4c50e2
	github.com/facebook/fbthrift v0.31.1-0.20211129061412-801ed7f9f295
	github.com/golang/glog v1.0.0
	github.com/kikimo/goremote v1.0.4
	github.com/kolo/xmlrpc v0.0.0-20201022064351-38db28db192b // indirect
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.9.0
	github.com/vesoft-inc/nebula-go/v2 v2.6.0
	github.com/vesoft-inc/nebula-go/v3 v3.0.0
	// github.com/kikimo/nebula-go/v2 v2.6.4
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
)

replace github.com/vesoft-inc/nebula-go/v2 v2.6.0 => /Users/wenlinwu/src/nebula-go // indirect

// replace github.com/vesoft-inc/nebula-go/v2 => github.com/kikimo/nebula-go/v2 v2.7.3 // indirect
