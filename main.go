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

type Job interface {
	ID() string
	Do() error
}

type JobResult struct {
	JobID string
	Err   error
}

type WorkerPool interface {
	Start(ctx context.Context)
	AddWorkers(count int)
	RemoveWorkers(count int)
	AddJob(job Job)
	Subscribe() chan JobResult
}

func main() {
	ctx := contextWithSigterm(context.Background())

	wp := worker.NewPool()

	wp.Start(ctx)
	wp.AddWorkers(1)

	wp.AddJob(job{"1"})

	for result := range wp.Subscribe() {
		fmt.Println("result received", result.JobID, result.Err)
	}
}

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
	time.Sleep(time.Second)
	fmt.Println(j.id, "completed")
	return nil
}
