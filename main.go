package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itimofeev/worker-pool-assignment/worker"
)

type WorkerPool interface {
	Start(ctx context.Context)
	AddWorkers(count int)
	RemoveWorkers(count int)
	AddJob(job worker.Job)
	Subscribe() chan worker.JobResult
}

func newPool() WorkerPool {
	return worker.NewPool()
}

func main() {
	ctx := contextWithSigterm(context.Background())

	wp := newPool()

	wp.Start(ctx)
	wp.AddWorkers(1)
	wp.AddWorkers(1)

	wp.RemoveWorkers(10)

	wp.AddJob(job{"1"})
	wp.AddJob(job{"2"})

	wp.AddWorkers(1)

	for result := range wp.Subscribe() {
		fmt.Println("result received", result.JobID, result.Err)
	}
}

// contextWithSigterm returns context that cancelled when user press Ctrl+C
func contextWithSigterm(ctx context.Context) context.Context {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, os.Kill)

		select {
		case <-signalCh:
			fmt.Println("got signal")
		case <-ctx.Done():
		}
		fmt.Println("contextWithSigterm quit")
	}()

	return ctxWithCancel
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
