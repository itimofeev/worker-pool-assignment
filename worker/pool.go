package worker

import (
	"context"
	"fmt"
	"sync"
)

type Pool struct {
	jobCh                chan Job
	ctx                  context.Context
	resultCh             chan JobResult
	subscriberChs        []chan JobResult
	notifyWorkerClosedCh chan *worker
	workersWg            sync.WaitGroup

	subscribeMu sync.RWMutex
	workersMu   sync.Mutex
	initOnce    sync.Once

	workers map[string]worker

	idGenerator *IDGenerator
	stoppedCh   chan struct{}
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
		jobCh:                make(chan Job, 1000),
		resultCh:             make(chan JobResult, 10),
		idGenerator:          &IDGenerator{},
		workers:              make(map[string]worker),
		stoppedCh:            make(chan struct{}),
		notifyWorkerClosedCh: make(chan *worker),
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
	go p.manageClosedWorkers()
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

	p.workersWg.Wait()
	close(p.resultCh)
	for _, subscriberCh := range p.subscriberChs {
		close(subscriberCh)
	}
	close(p.stoppedCh)
}

func (p *Pool) AddWorkers(count int) {
	for i := 0; i < count; i++ {
		workerCtx, workerCancel := context.WithCancel(p.ctx)
		w := worker{
			id:             p.idGenerator.Next(),
			workerCtx:      workerCtx,
			workerCancel:   workerCancel,
			jobCh:          p.jobCh,
			resultCh:       p.resultCh,
			notifyClosedCh: p.notifyWorkerClosedCh,
		}

		p.workersMu.Lock()
		p.workers[w.id] = w
		p.workersWg.Add(1)
		p.workersMu.Unlock()

		go w.run()
		fmt.Println("worker added", w.id, "total workers", p.WorkersCount())
	}
}

func (p *Pool) WorkersCount() int {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	return len(p.workers)
}

func (p *Pool) RemoveWorkers(count int) {
	i := 0
	for _, w := range p.workers {
		if i >= count {
			break
		}
		i++
		w.workerCancel()
	}
}

func (p *Pool) GetStoppedChan() chan struct{} {
	return p.stoppedCh
}

func (p *Pool) AddJob(job Job) {
	select {
	case p.jobCh <- job:
		fmt.Println("job added", job.ID())
	default:
		fmt.Println("!!!JOB NOT ADDED!!! Whether pool is stopping and jobCh is closed or buffer in jobCh is overflown")
	}
}

func (p *Pool) Subscribe() chan JobResult {
	p.subscribeMu.Lock()
	defer p.subscribeMu.Unlock()

	subscribedCh := make(chan JobResult)
	p.subscriberChs = append(p.subscriberChs, subscribedCh)
	return subscribedCh
}

func (p *Pool) manageClosedWorkers() {
	for closedWorker := range p.notifyWorkerClosedCh {
		p.workersMu.Lock()
		delete(p.workers, closedWorker.id)
		p.workersWg.Add(-1)
		p.workersMu.Unlock()

		fmt.Println("worker removed", closedWorker.id, "total workers", p.WorkersCount())
	}
}
