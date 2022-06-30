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

	fanoutFinishedCh chan struct{}

	stopping   bool
	stoppingMu sync.Mutex

	workers map[string]*worker

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
		jobCh:                make(chan Job, 100),
		resultCh:             make(chan JobResult, 100),
		idGenerator:          &IDGenerator{},
		workers:              make(map[string]*worker),
		stoppedCh:            make(chan struct{}),
		notifyWorkerClosedCh: make(chan *worker),
		fanoutFinishedCh:     make(chan struct{}),
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
	close(p.fanoutFinishedCh)
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

	p.stoppingMu.Lock()
	p.stopping = true
	p.stoppingMu.Unlock()

	close(p.jobCh)

	p.workersWg.Wait()
	close(p.resultCh)
	<-p.fanoutFinishedCh

	p.subscribeMu.Lock()
	for _, subscriberCh := range p.subscriberChs {
		close(subscriberCh)
	}
	p.subscriberChs = nil
	p.subscribeMu.Unlock()

	close(p.stoppedCh)
}

func (p *Pool) AddWorkers(count int) {
	for i := 0; i < count; i++ {
		workerCtx, workerCancel := context.WithCancel(context.Background()) //todo add doc
		w := &worker{
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
	p.workersMu.Lock()
	defer p.workersMu.Unlock()
	for _, w := range p.workers {
		if i >= count {
			break
		}
		i++
		w.stop()
	}
}

func (p *Pool) GetStoppedChan() chan struct{} {
	return p.stoppedCh
}

func (p *Pool) AddJob(job Job) {
	p.stoppingMu.Lock()
	defer p.stoppingMu.Unlock()

	if p.stopping {
		fmt.Println("!!!JOB NOT ADDED!!! Pool is stopping")
		return
	}
	select {
	case p.jobCh <- job:
		fmt.Println("job added", job.ID())
	default:
		fmt.Println("!!!JOB NOT ADDED!!! buffer in jobCh is overflown")
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

		fmt.Println("worker", closedWorker.id, "removed, total workers", p.WorkersCount())
	}
}
