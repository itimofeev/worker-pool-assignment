package worker

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWorkerPool_RunningWorkersStopped проверяет, что все воркеры завершаются при остановке пула
func TestWorkerPool_RunningWorkersStopped(t *testing.T) {
	wp := NewPool()
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)

	tick := time.NewTicker(time.Second / 10)
	defer tick.Stop()

	go func() {
		for {
			select {
			case <-tick.C:
				wp.AddWorkers(2)
			case <-ctx.Done():
				return
			}
		}
	}()

	time.Sleep(time.Second * 5)

	cancel()
	<-wp.GetStoppedChan()
	fmt.Println("worker pool stopped")
	require.Zero(t, wp.WorkersCount())
}

// TestWorkerPool_GracefullyCompletesAllJobs проверяет, что все задачи завершаются после остановки пула
func TestWorkerPool_GracefullyCompletesAllJobs(t *testing.T) {
	wp := NewPool()
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)
	jobsIDGen := IDGenerator{}
	const jobsCount = 10

	for i := 0; i < jobsCount; i++ {
		wp.AddJob(job{jobsIDGen.Next()})
	}

	gotResults := int64(0)
	subscribeChClosed := make(chan struct{})
	go func() {
		subscribeCh := wp.Subscribe()
		for range subscribeCh {
			fmt.Println("got result")
			atomic.AddInt64(&gotResults, 1)
		}
		close(subscribeChClosed)
	}()

	wp.AddWorkers(2)
	runtime.Gosched() // ensure, that worker started and received job before we cancel context
	cancel()

	<-wp.GetStoppedChan()
	<-subscribeChClosed
	require.EqualValues(t, jobsCount, atomic.LoadInt64(&gotResults))
	require.Zero(t, wp.WorkersCount())
}

// TestWorkerPool проверяет всё - и добавление воркеров, и удалени, и добавление задач в асинхронном режиме. В конце убеждается, что пул остановился и все добавленные задачи выполнены.
func TestWorkerPool(t *testing.T) {
	wp := NewPool()
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)
	addWorkersTicker := time.NewTicker(time.Second)
	removeWorkersTicker := time.NewTicker(time.Second)
	jobTicker := time.NewTicker(time.Second / 10)
	idGen := IDGenerator{}

	addedJobsCount := int64(0)
	gotResults := int64(0)
	go func() {
		for {
			select {
			case <-addWorkersTicker.C:
				wp.AddWorkers(rand.Intn(20))
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-removeWorkersTicker.C:
				if wp.WorkersCount() <= 1 {
					continue
				}
				workersToRemove := rand.Intn(wp.WorkersCount() - 1)
				if workersToRemove <= 0 {
					continue
				}
				wp.RemoveWorkers(workersToRemove)
			case <-ctx.Done():
				return
			}
		}
	}()

	jobTickerStopCh := make(chan struct{})
	jobTickerStoppedCh := make(chan struct{})
	go func() {
		defer close(jobTickerStoppedCh)
		for {
			select {
			case <-jobTicker.C:
				wp.AddJob(job{idGen.Next()})
				atomic.AddInt64(&addedJobsCount, 1)
			case <-jobTickerStopCh:
				return
			}
		}
	}()

	subscribeChClosed := make(chan struct{})
	go func() {
		for range wp.Subscribe() {
			atomic.AddInt64(&gotResults, 1)
		}
		close(subscribeChClosed)
	}()

	time.Sleep(time.Second * 10)
	close(jobTickerStopCh)
	jobTicker.Stop()
	<-jobTickerStoppedCh
	cancel()

	<-wp.GetStoppedChan()
	<-subscribeChClosed
	fmt.Println("worker pool stopped")

	require.Equal(t, atomic.LoadInt64(&addedJobsCount), atomic.LoadInt64(&gotResults))
}

type job struct {
	id string
}

func (j job) ID() string {
	return j.id
}

func (j job) Do() error {
	fmt.Println(j.id, "started")
	time.Sleep(time.Second)
	fmt.Println(j.id, "completed")
	return nil
}
