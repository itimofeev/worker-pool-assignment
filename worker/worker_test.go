package worker

import "testing"

func TestWorker_DoesJob_ReturnsResult_AndStops(t *testing.T) {
	jobs := make(chan Job)
	results := make(chan JobResult)
	notifyClosed := make(chan *worker)
	w := newWorker("1", jobs, results, notifyClosed)

	runStoppedCh := make(chan struct{})
	go func() {
		w.run()
		close(runStoppedCh)
	}()

	jobs <- job{id: "1"}
	<-results
	w.stop()
	<-notifyClosed
	<-runStoppedCh
}
