package worker

import (
	"context"
	"fmt"
	"sync"
)

type Pool struct {
	ctx      context.Context
	initOnce sync.Once

	jobsCh               chan Job
	resultsCh            chan JobResult
	notifyWorkerClosedCh chan *worker

	subscriberChs []chan JobResult
	subscribeMu   sync.RWMutex

	workersWg sync.WaitGroup
	workersMu sync.Mutex
	workers   map[string]*worker

	fanoutFinishedCh chan struct{}

	stopping   bool
	stoppingMu sync.Mutex

	workersIDGenerator *IDGenerator
	poolStoppedCh      chan struct{}
}

type Job interface {
	ID() string
	Do() error
}

type JobResult struct {
	JobID string
	Err   error
}

func NewPoolWithBufferSizes(jobsBufferSize, resultsBufferSize int) *Pool {
	return newPool(jobsBufferSize, resultsBufferSize)
}

func NewPool() *Pool {
	return newPool(100, 100)
}

func newPool(jobsBufferSize, resultsBufferSize int) *Pool {
	return &Pool{
		jobsCh:               make(chan Job, jobsBufferSize),
		resultsCh:            make(chan JobResult, resultsBufferSize),
		workersIDGenerator:   &IDGenerator{},
		workers:              make(map[string]*worker),
		poolStoppedCh:        make(chan struct{}),
		notifyWorkerClosedCh: make(chan *worker),
		fanoutFinishedCh:     make(chan struct{}),
	}
}

// Start saves context in pool and starts internal go routines.
// Using pool without calling to Start will results in incorrect job processing.
// Consequent calls to start after first one will not have effect.
func (p *Pool) Start(ctx context.Context) {
	p.initOnce.Do(func() {
		p.initPool(ctx)
	})
}

func (p *Pool) AddWorkers(count int) {
	p.stoppingMu.Lock()
	if p.stopping {
		fmt.Println("!!!WORKERS NOT ADDED!!! Pool is stopping")
		return
	}
	p.stoppingMu.Unlock()

	for i := 0; i < count; i++ {
		w := newWorker(p.workersIDGenerator.Next(), p.jobsCh, p.resultsCh, p.notifyWorkerClosedCh)

		// adds worker to workersMap
		p.workersMu.Lock()
		p.workers[w.id] = w
		p.workersWg.Add(1)
		p.workersMu.Unlock()

		// and runs worker in goroutine
		go w.run()
		fmt.Println("worker added", w.id, "total workers", p.WorkersCount())
	}
}

// RemoveWorkers calling for worker.stop method that will result in stopping processing new jobs and returns after finishing executing current job
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

// AddJob adds job in jobsCh channel.
// Job will not be added if pool is stopping or jobsCh buffer is overflown
func (p *Pool) AddJob(job Job) {
	p.stoppingMu.Lock()
	defer p.stoppingMu.Unlock()

	if p.stopping {
		fmt.Println("!!!JOBS NOT ADDED!!! Pool is stopping")
		return
	}
	select {
	case p.jobsCh <- job:
		fmt.Println("job added", job.ID())
	default:
		fmt.Println("!!!JOBS NOT ADDED!!! buffer in jobsCh is overflown")
	}
}

// Subscribe adds subscriber to list of subscribers and returns channel to receive results from.
// You should try to process results as soon as possible. If any subscriber do it slow, it can cause in channel buffer overflow
// and stopping of executing jobs, because pool will block in sending result to channel.
func (p *Pool) Subscribe() chan JobResult {
	p.subscribeMu.Lock()
	defer p.subscribeMu.Unlock()

	subscribedCh := make(chan JobResult)
	p.subscriberChs = append(p.subscriberChs, subscribedCh)
	return subscribedCh
}

// GetStoppedChan returns chan that closed when pool gracefully shuts down
func (p *Pool) GetStoppedChan() chan struct{} {
	return p.poolStoppedCh
}

// WorkersCount returns count of currently running workers
func (p *Pool) WorkersCount() int {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()

	return len(p.workers)
}

func (p *Pool) initPool(ctx context.Context) {
	p.ctx = ctx
	go p.fanOutResultToSubscribers()
	go p.manageClosedWorkers()
	go p.waitForContextClose()
}

// fanOutResultToSubscribers sends each of results to all subscribers
func (p *Pool) fanOutResultToSubscribers() {
	for result := range p.resultsCh {
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

// manageClosedWorkers listens for stopped workers and removes them from workers map.
func (p *Pool) manageClosedWorkers() {
	for closedWorker := range p.notifyWorkerClosedCh {
		p.workersMu.Lock()
		delete(p.workers, closedWorker.id)
		p.workersWg.Add(-1)
		p.workersMu.Unlock()

		fmt.Println("worker", closedWorker.id, "removed, total workers", p.WorkersCount())
	}
}

// waitForContextClose waits for pool context closed and gracefully stops pool.
func (p *Pool) waitForContextClose() {
	<-p.ctx.Done()
	fmt.Println("ctx done, stopping worker pool")

	p.stoppingMu.Lock()
	p.stopping = true
	p.stoppingMu.Unlock()

	close(p.jobsCh)

	p.workersWg.Wait()
	close(p.resultsCh)
	<-p.fanoutFinishedCh

	p.subscribeMu.Lock()
	for _, subscriberCh := range p.subscriberChs {
		close(subscriberCh)
	}
	p.subscriberChs = nil
	p.subscribeMu.Unlock()

	close(p.poolStoppedCh)
}
