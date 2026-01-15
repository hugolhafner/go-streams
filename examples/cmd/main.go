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
	case "kgo_complete":
		examples.KgoComplete()
	default:
		println("Please provide a valid example name:")
		println("  basic_map")
		println("  branching")
		println("  event_sourcing")
		println("  kgo_complete")
	}
}
