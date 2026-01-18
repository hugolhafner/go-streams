package kstream

import (
	"fmt"
)

type nameGenerator struct {
	prefix string
	count  int
}

func newNameGenerator() *nameGenerator {
	return &nameGenerator{
		prefix: "NODE",
		count:  0,
	}
}

func (ng *nameGenerator) next(customPrefix string) string {
	ng.count++
	if customPrefix != "" {
		return fmt.Sprintf("%s_%d", customPrefix, ng.count)
	}
	return fmt.Sprintf("%s_%d", ng.prefix, ng.count)
}
