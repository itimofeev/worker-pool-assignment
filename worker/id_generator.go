package worker

import (
	"strconv"
	"sync/atomic"
)

// IDGenerator generates unique integers sequence and returns each as string
type IDGenerator struct {
	id int64
}

func (g *IDGenerator) Next() string {
	n := atomic.AddInt64(&g.id, 1)
	return strconv.FormatInt(n, 10)
}
