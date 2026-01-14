package main

import (
	"flag"

	"github.com/hugolhafner/go-streams/examples"
)

func main() {
	flag.Parse()
	example := flag.Arg(0)
	switch example {
	case "basic_map":
		examples.BasicMap()
	case "branching":
		examples.BranchProcessor()
	case "event_sourcing":
		examples.EventSourcing()
	default:
		println("Please provide a valid example name:")
		println("  basic_map")
		println("  branching")
		println("  event_sourcing")
	}
}
