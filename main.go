package main

import (
	"go.searchlight.dev/ruler/pkg/cmds"

	"github.com/golang/glog"
)

func main() {
	if err := cmds.NewRootCmd().Execute(); err != nil {
		glog.Fatal(err)
	}
}
