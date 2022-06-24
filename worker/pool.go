package worker

import (
	"context"
	"fmt"
)

type Pool struct {
	jobCh         chan Job
	ctx           context.Context
	resultCh      chan JobResult
	subscriberChs []chan JobResult
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
	// TODO add sync.Once
	p.ctx = ctx
	go func() {
		for result := range p.resultCh {
			for _, subscriberCh := range p.subscriberChs {
				subscriberCh <- result
			}
		}
	}()
	go func() {
		<-p.ctx.Done()
		fmt.Println("ctx done, closing worker pool")
		close(p.jobCh)
		close(p.resultCh)
		for _, subscriberCh := range p.subscriberChs {
			close(subscriberCh)
		}
	}()
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
