package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	files             []string
	reduceNum         int
	mapTasks          []*mrTask
	reduceTasks       []*mrTask
	finishedMapNum    int
	finishedReduceNum int
	mapFinished       bool
	reduceFinished    bool
	guard             sync.Mutex
}

type mrTask struct {
	Type       int
	Status     int
	Index      int
	StartTime  time.Time
	MapFile    string
	ReduceFile string
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) WorkerHandler(args *MrArgs, reply *MrReply) error {
	if m.Done() {
		reply.Task.Type = OVER
		return nil
	}

	m.guard.Lock()

	switch args.Type {
	case REQUEST:
		found := false

		if m.mapFinished {
			for _, t := range m.reduceTasks {
				if t.Status == UNASSIGNED || (t.Status == RUNNING && time.Since(t.StartTime).Seconds() > float64(10)) { // 如果没有被分配，或者是已经超时
					t.StartTime = time.Now()
					t.Status = RUNNING
					reply.Task = *t
					reply.FileNum = len(m.files)
					reply.ReduceNum = m.reduceNum

					found = true
					fmt.Printf("master send a reduce task %v\n", t.Index)

					break
				}
			}

		} else {
			for _, t := range m.mapTasks {
				if t.Status == UNASSIGNED || (t.Status == RUNNING && time.Since(t.StartTime).Seconds() > float64(10)) {
					t.StartTime = time.Now()
					t.Status = RUNNING
					reply.Task = *t
					reply.ReduceNum = m.reduceNum
					reply.FileNum = len(m.files)
					found = true

					fmt.Printf("master send a map task %v\n", t.Index)
					break
				}
			}
		}

		if found == false {
			reply.Task.Type = WAIT
		}

	case DONE:
		if args.Task.Type == REDUCE {
			if !m.reduceFinished {
				curTask := args.Task
				if m.reduceTasks[curTask.Index].StartTime.Equal(curTask.StartTime) { // 因为存在超时的情况，所以要验证一下
					m.reduceTasks[curTask.Index].Status = FINISHED
					m.finishedReduceNum++

					m.reduceFinished = (m.finishedReduceNum == m.reduceNum)

					fmt.Printf("master get a finished reduce\n")
				} else {
					fmt.Printf("reduce time out")
				}
			}
		} else if args.Task.Type == MAP {
			if !m.mapFinished {
				curTask := args.Task
				if m.mapTasks[curTask.Index].StartTime.Equal(curTask.StartTime) {
					m.mapTasks[curTask.Index].Status = FINISHED
					m.finishedMapNum++

					m.mapFinished = (m.finishedMapNum == len(m.mapTasks))
					fmt.Printf("%v map tasks have finished, total %v \n", m.finishedMapNum, len(m.mapTasks))
				} else {
					fmt.Printf("map time out")
				}
			}
		} else {
			log.Fatal("unexptected situation")
		}

	default:
		log.Fatal("unexpected situation")
	}

	m.guard.Unlock()

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.mapFinished && m.reduceFinished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.reduceNum = nReduce
	for i, file := range m.files {
		mapTask := mrTask{}
		mapTask.Index = i
		mapTask.MapFile = file
		mapTask.Status = UNASSIGNED
		mapTask.Type = MAP

		m.mapTasks = append(m.mapTasks, &mapTask)
	}

	i := 0
	for i < nReduce {
		reduceTask := mrTask{}
		reduceTask.Index = i
		reduceTask.Status = UNASSIGNED
		reduceTask.Type = REDUCE

		m.reduceTasks = append(m.reduceTasks, &reduceTask)
		i++
	}

	m.server()
	return &m
}
