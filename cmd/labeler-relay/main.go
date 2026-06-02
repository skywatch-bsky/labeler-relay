package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "labeler-relay:", err)
		os.Exit(1)
	}
}

// run is the real entry point. Wiring for store, slurper, discovery, and the
// output server is added in later phases.
func run() error {
	fmt.Println("labeler-relay: not yet wired")
	return nil
}
