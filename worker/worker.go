package worker

import (
	"context"
	"fmt"
)

// worker performs jobs from jobsCh and send results to resultsCh
// when worker finished after worker.stop called it'll notify via notifyClosedCh
type worker struct {
	id             string
	jobsCh         <-chan Job
	resultsCh      chan<- JobResult
	notifyClosedCh chan<- *worker

	// ctx is worker context, that is cancelled when worker.stop called
	ctx    context.Context
	cancel context.CancelFunc
}

func newWorker(id string, jobsCh <-chan Job, resultsCh chan<- JobResult, notifyClosedCh chan<- *worker) *worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &worker{
		id:             id,
		ctx:            ctx,
		cancel:         cancel,
		jobsCh:         jobsCh,
		resultsCh:      resultsCh,
		notifyClosedCh: notifyClosedCh,
	}
}

func (w *worker) run() {
	fmt.Println("worker", w.id, "started")
	defer fmt.Println("worker", w.id, "stopped")
	defer func() {
		w.notifyClosedCh <- w
	}()

	// todo add defer catching panic

labelFor:
	for {
		select {
		case job, ok := <-w.jobsCh:
			if !ok {
				break labelFor
			}
			err := job.Do()
			w.resultsCh <- JobResult{
				JobID: job.ID(),
				Err:   err,
			}

		case <-w.ctx.Done():
			// worker stops when context cancelled via worker.stop()
			fmt.Println("worker", w.id, "ctx done")
			break labelFor
		}
	}
}

// stop will result in stopping processing new jobs and will return after finishing current job
func (w *worker) stop() {
	w.cancel()
}
