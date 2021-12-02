module github.com/kikimo/nebula-monkey

go 1.16

require (
	github.com/facebook/fbthrift v0.31.1-0.20210223140454-614a73a42488
	github.com/golang/glog v1.0.0
	github.com/kikimo/goremote v1.0.4
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.9.0
	github.com/vesoft-inc/nebula-go/v2 v2.6.0
	// github.com/kikimo/nebula-go/v2 v2.6.4
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
)

// replace github.com/vesoft-inc/nebula-go/v2 v2.5.1 => /Users/wenlinwu/src/nebula-go // indirect
// replace github.com/vesoft-inc/nebula-go/v2 v2.5.1 => github.com/kikimo/nebula-go/v2 v2.6.3 // indirect
replace github.com/vesoft-inc/nebula-go/v2 => github.com/kikimo/nebula-go/v2 v2.7.1 // indirect

// replace github.com/vesoft-inc/nebula-go/v2/raftex => github.com/kikimo/nebula-go/v2/raftex v1 // indirect
