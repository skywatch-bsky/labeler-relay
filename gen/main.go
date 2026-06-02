package main

import (
	"fmt"
	"os"

	cbg "github.com/whyrusleeping/cbor-gen"

	community "github.com/scarndp/labeler-relay/api/community"
)

func main() {
	if err := cbg.WriteMapEncodersToFile(
		"api/community/cbor_gen.go",
		"community",
		community.LabelerSyncSubscribeLabelers_Labels{},
		community.LabelerSyncSubscribeLabelers_Service{},
		community.LabelerSyncSubscribeLabelers_Info{},
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
