package worker

import (
	"context"
	"fmt"
)

type worker struct {
	id             string
	workerCtx      context.Context
	workerCancel   context.CancelFunc
	jobCh          chan Job
	resultCh       chan JobResult
	notifyClosedCh chan *worker
}

func (w *worker) run() {
	fmt.Println("worker", w.id, "started")
	defer fmt.Println("worker", w.id, "stopped")

	// todo add defer catching panic

labelFor:
	for {
		select {
		case job, ok := <-w.jobCh:
			if !ok {
				break labelFor
			}
			err := job.Do()
			w.resultCh <- JobResult{
				JobID: job.ID(),
				Err:   err,
			}

		case <-w.workerCtx.Done():
			fmt.Println("worker", w.id, "ctx done")
			break labelFor
		}
	}

	w.notifyClosedCh <- w
}
