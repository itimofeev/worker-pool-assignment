package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itimofeev/worker-pool-assignment/worker"
	"github.com/stretchr/testify/require"
)

func TestWorkerSimple(t *testing.T) {
	wp := worker.NewPool()
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)

	tick := time.NewTicker(time.Second / 10)

	go func() {
		for range tick.C {
			wp.AddWorkers(2)
		}
	}()

	time.Sleep(time.Second * 5)

	cancel()
	tick.Stop()
	<-wp.GetStoppedChan()
	fmt.Println("worker pool stopped")
}

func TestWorkerCompleteAllJobs(t *testing.T) {
	wp := worker.NewPool()
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)
	idGen := worker.IDGenerator{}
	const jobsCount = 1

	for i := 0; i < jobsCount; i++ {
		wp.AddJob(job{idGen.Next()})
	}

	gotResults := int64(0)
	go func() {
		subscribeCh := wp.Subscribe()
		for range subscribeCh {
			fmt.Println("got result")
			atomic.AddInt64(&gotResults, 1)
		}
	}()

	wp.AddWorkers(1)
	time.Sleep(time.Second)
	cancel()

	<-wp.GetStoppedChan()
	time.Sleep(time.Second)
	require.EqualValues(t, jobsCount, atomic.LoadInt64(&gotResults))
}

func TestWorkerPool(t *testing.T) {
	wp := worker.NewPool()
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)
	addWorkersTicker := time.NewTicker(time.Second)
	removeWorkersTicker := time.NewTicker(time.Second)
	jobTicker := time.NewTicker(time.Second / 10)
	testTimer := time.NewTimer(time.Second * 1)
	idGen := worker.IDGenerator{}

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

	go func() {
		for {
			select {
			case <-jobTicker.C:
				wp.AddJob(job{idGen.Next()})
				atomic.AddInt64(&addedJobsCount, 1)
			case <-testTimer.C:
				cancel()
				return
			}
		}
	}()

	go func() {
		for range wp.Subscribe() {
			atomic.AddInt64(&gotResults, 1)
		}
	}()

	<-wp.GetStoppedChan()
	fmt.Println("worker pool stopped")

	require.Equal(t, atomic.LoadInt64(&addedJobsCount), atomic.LoadInt64(&gotResults))
}
