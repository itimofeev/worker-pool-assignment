# Worker Pool Assignment

Необходимо реализовать пул воркеров для обработки задач.
Добавление задачи должно быть неблокирующим.
Пул должен иметь возможностью динамически изменять количество воркеров. 
Результат обработки должен возвращаться асинхронно.
В качестве дополнительного задания добавить возможность делать grace shutdown.

```go
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
```