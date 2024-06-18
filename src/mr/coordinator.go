package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu   sync.Mutex
	cond *sync.Cond
	done bool

	mapTaskTotal    int
	mapTaskFinished []bool
	mapTaskTime     []time.Time

	reduceTaskTotal    int
	reduceTaskFinished []bool
	reduceTaskTime     []time.Time

	mapFiles []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.MapTaskTotal = c.mapTaskTotal
	reply.ReduceTaskTotal = c.reduceTaskTotal

	//fmt.Println("GetTask: MapTaskTotal", c.MapTaskTotal, "ReduceTaskTotal", c.ReduceTaskTotal)

	//分发任务
	//map
	for {
		mapFinished := true
		for i := 0; i < c.mapTaskTotal; i++ {
			if !c.mapTaskFinished[i] {

				if c.mapTaskTime[i].IsZero() || time.Now().Sub(c.mapTaskTime[i]) > 10*time.Second {

					reply.TaskType = MapTask
					reply.TaskNum = i
					reply.FileName = c.mapFiles[i]
					c.mapTaskTime[i] = time.Now()
					return nil
				} else {
					mapFinished = false
				}
			}
		}

		if mapFinished {
			break
		} else {
			c.cond.Wait()
		}

	}

	//reduce
	for {
		reduceFinished := true
		for i := 0; i < c.reduceTaskTotal; i++ {
			//map任务完成后才能开始reduce任务
			if !c.reduceTaskFinished[i] {
				if c.reduceTaskTime[i].IsZero() || time.Now().Sub(c.reduceTaskTime[i]) > 10*time.Second {

					reply.TaskType = ReduceTask
					reply.TaskNum = i
					c.reduceTaskTime[i] = time.Now()
					return nil
				} else {
					reduceFinished = false
				}
			}
		}
		if reduceFinished {
			break
		} else {
			c.cond.Wait()
		}

	}

	//完成任务
	c.done = true
	reply.TaskType = Done

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {

	case MapTask:
		c.mapTaskFinished[args.TaskNum] = true
	case ReduceTask:
		c.reduceTaskFinished[args.TaskNum] = true
	default:
		log.Fatal("FinishTask: invalid TaskType ", args.TaskType)
	}

	c.cond.Broadcast()
	return nil

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.cond = sync.NewCond(&c.mu)

	// Your code here.
	c.done = false
	c.mapFiles = files

	c.mapTaskTotal = len(files)
	c.mapTaskFinished = make([]bool, c.mapTaskTotal)
	c.mapTaskTime = make([]time.Time, c.mapTaskTotal)

	c.reduceTaskTotal = nReduce
	c.reduceTaskFinished = make([]bool, c.reduceTaskTotal)
	c.reduceTaskTime = make([]time.Time, c.reduceTaskTotal)

	go func() {
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()
	c.server()
	return &c
}
