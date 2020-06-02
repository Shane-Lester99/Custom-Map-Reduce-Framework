package mapreduce

import "container/list"

type JobQueue struct {
	q *list.List
}

func (jq *JobQueue) InitQueue() {
	jq.q = new(list.List)
	jq.q.Init()
}

func (jq *JobQueue) Len() int {
	return jq.q.Len()
}

func (jq *JobQueue) Enqueue(jobId int) {
	jq.q.PushBack(jobId)
}

func (jq *JobQueue) Dequeue() int {
	if jq.Len() == 0 {
		return -1
	}
	oldFront := jq.q.Remove(jq.q.Front())
	return oldFront.(int)
}

func (jq *JobQueue) Front() int {
	elem := jq.q.Front()
	var jobId int
	if elem == nil {
		jobId = -1
	} else {
		jobId = elem.Value.(int)
	}
	return jobId

}

func (jq *JobQueue) Fill(ids int) {
	for i := 0; i < ids; i++ {
		jq.Enqueue(i)
	}
}
