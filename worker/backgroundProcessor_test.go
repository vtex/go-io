package worker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/vtex/go-io/redis/stubs"
)

func TestBackgroundProcessor(t *testing.T) {
	redis := stubs.NewRedis()

	Convey("Does not schedule repeated jobs", t, withTimeout(func() {
		blocker := make(chan struct{})
		resultChan := make(chan bool, 1)
		processor := NewBackgroundProcessor(5, redis, func(interface{}) time.Duration {
			defer close(resultChan)
			<-blocker
			resultChan <- true
			return 0
		})

		So(processor.Schedule("jobKey", nil), ShouldBeTrue)
		So(processor.Schedule("jobKey", nil), ShouldBeFalse)
		close(blocker)

		So(<-resultChan, ShouldBeTrue)
		_, falseIfClosed := <-resultChan
		So(falseIfClosed, ShouldBeFalse)
	}))

	Convey("Schedules repeated jobs after executed", t, withTimeout(func() {
		resultChan := make(chan bool, 2)
		processor := NewBackgroundProcessor(5, redis, func(interface{}) time.Duration {
			resultChan <- true
			return 0
		})

		So(processor.Schedule("jobKey", nil), ShouldBeTrue)
		So(<-resultChan, ShouldBeTrue)
		time.Sleep(10 * time.Millisecond)

		So(processor.Schedule("jobKey", nil), ShouldBeTrue)
		So(<-resultChan, ShouldBeTrue)
	}))

	Convey("Recovers from panics in jobs", t, withTimeout(func() {
		done := make(chan struct{})
		executed := false
		processor := NewBackgroundProcessor(5, redis, func(interface{}) time.Duration {
			defer close(done)
			executed = true
			panic("omg! 😱")
		})

		So(processor.Schedule("jobKey", nil), ShouldBeTrue)
		<-done

		So(executed, ShouldBeTrue)
	}))

	Convey("Passes arguments to jobs", t, withTimeout(func() {
		numJobs := 100
		wg := sync.WaitGroup{}
		wg.Add(numJobs)

		receivedArgs := map[int]bool{}
		processor := NewBackgroundProcessor(5, redis, func(idx interface{}) time.Duration {
			defer wg.Done()
			receivedArgs[idx.(int)] = true
			return 0
		})

		for i := 0; i < numJobs; i++ {
			So(processor.Schedule(fmt.Sprint("job", i), i), ShouldBeTrue)
		}
		wg.Wait()

		for i := 0; i < numJobs; i++ {
			So(receivedArgs[i], ShouldBeTrue)
		}
	}))

	Convey("Executes a single job at a time", t, withTimeout(func() {
		numJobs := 50
		wg := sync.WaitGroup{}
		wg.Add(numJobs)

		startTimes := make([]time.Time, numJobs)
		endTimes := make([]time.Time, numJobs)
		processor := NewBackgroundProcessor(numJobs, redis, func(idx interface{}) time.Duration {
			defer wg.Done()
			startTimes[idx.(int)] = time.Now()
			time.Sleep(5 * time.Millisecond)
			endTimes[idx.(int)] = time.Now()
			return 0
		})

		for i := 0; i < numJobs; i++ {
			So(processor.Schedule(fmt.Sprint("job", i), i), ShouldBeTrue)
		}
		wg.Wait()

		for i := 1; i < numJobs; i++ {
			timeAfterLast := startTimes[i].Sub(endTimes[i-1])
			So(timeAfterLast, ShouldBeGreaterThanOrEqualTo, 0)
		}
	}))

	Convey("Does wait interval between jobs", t, withTimeout(func() {
		numJobs := 50
		execInterval := 7 * time.Millisecond
		wg := sync.WaitGroup{}
		wg.Add(numJobs)

		execTimes := make([]time.Time, numJobs)
		processor := NewBackgroundProcessor(numJobs, redis, func(idx interface{}) time.Duration {
			defer wg.Done()
			execTimes[idx.(int)] = time.Now()
			return execInterval
		})

		for i := 0; i < numJobs; i++ {
			So(processor.Schedule(fmt.Sprint("job", i), i), ShouldBeTrue)
		}
		wg.Wait()

		for i := 1; i < numJobs; i++ {
			timeAfterLast := execTimes[i].Sub(execTimes[i-1])
			So(timeAfterLast, ShouldBeGreaterThanOrEqualTo, execInterval)
		}
	}))
}
