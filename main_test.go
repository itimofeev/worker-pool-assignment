package main

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/itimofeev/worker-pool-assignment/worker"
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

func TestWorkerPool(t *testing.T) {
	wp := worker.NewPool()
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)
	addWorkersTicker := time.NewTicker(time.Second)
	removeWorkersTicker := time.NewTicker(time.Second)
	jobTicker := time.NewTicker(time.Second / 10)

	addedJobsCount := 0
	gotResults := 0
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
				wp.RemoveWorkers(rand.Intn(wp.WorkersCount() + 1))
			case <-ctx.Done():
				return
			}
		}
	}()

	idGen := worker.IDGenerator{}

	go func() {
		for {
			select {
			case <-jobTicker.C:
				wp.AddJob(job{idGen.Next()})
				addedJobsCount++
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for range wp.Subscribe() {
			gotResults++
		}
	}()

	time.Sleep(time.Second * 10)
	cancel()
	<-wp.GetStoppedChan()
	fmt.Println("worker pool stopped")

	fmt.Println(gotResults, addedJobsCount) // TODO replace with check
}
