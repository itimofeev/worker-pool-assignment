package worker

import (
	"context"
	"fmt"
	"sync"
)

type Pool struct {
	jobCh         chan Job
	ctx           context.Context
	resultCh      chan JobResult
	subscriberChs []chan JobResult

	subscribeMu sync.RWMutex
	initOnce    sync.Once
}

type Job interface {
	ID() string
	Do() error
}

type JobResult struct {
	JobID string
	Err   error
}

func NewPool() *Pool {
	return &Pool{
		jobCh:    make(chan Job),
		resultCh: make(chan JobResult),
	}
}

func (p *Pool) Start(ctx context.Context) {
	p.initOnce.Do(func() {
		p.initPool(ctx)
	})
}

func (p *Pool) initPool(ctx context.Context) {
	p.ctx = ctx
	go p.fanOutResultToSubscribers()
	go p.waitForContextClose()
}

func (p *Pool) fanOutResultToSubscribers() {
	for result := range p.resultCh {
		p.sendResultToSubscribers(result)
	}
}

func (p *Pool) sendResultToSubscribers(result JobResult) {
	p.subscribeMu.RLock()
	defer p.subscribeMu.RUnlock()

	for _, subscriberCh := range p.subscriberChs {
		subscriberCh <- result
	}
}

func (p *Pool) waitForContextClose() {
	<-p.ctx.Done()
	fmt.Println("ctx done, stopping worker pool")

	close(p.jobCh)
	close(p.resultCh)
	for _, subscriberCh := range p.subscriberChs {
		close(subscriberCh)
	}
}

func (p *Pool) AddWorkers(count int) {
	for i := 0; i < count; i++ {
		go p.worker()
	}
}

func (p *Pool) RemoveWorkers(count int) {}

func (p *Pool) AddJob(job Job) {
	p.jobCh <- job
}

func (p *Pool) Subscribe() chan JobResult {
	p.subscribeMu.Lock()
	defer p.subscribeMu.Unlock()

	subscribedCh := make(chan JobResult)
	p.subscriberChs = append(p.subscriberChs, subscribedCh)
	return subscribedCh
}

func (p *Pool) worker() {
	for job := range p.jobCh {
		err := job.Do()
		p.resultCh <- JobResult{
			JobID: job.ID(),
			Err:   err,
		}
	}
}
