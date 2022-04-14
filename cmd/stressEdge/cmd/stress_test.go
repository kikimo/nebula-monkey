package cmd

import (
	"fmt"
	"testing"
)

func TestVertexStore(t *testing.T) {
	store := NewVertexStore(4)
	edges := store.Take(16)
	for _, e := range edges {
		fmt.Printf("from: %d, to: %d\n", e[0], e[1])
	}
}
