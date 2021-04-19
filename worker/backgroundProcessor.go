package worker

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vtex/go-io/redis"
)

const generationKey = "goio:bgProcessor:generation"

// BackgroundProcessor is a utility for running jobs in the background, avoiding
// the rescheduling of the same job by a provided key. The rescheduling prevention
// is performed both locally and in a remote Redis, for a simple best-effort global
// job deduplication across many worker instances.
//
// We avoid receiving a separate function for each job to prevent possible memory
// leaking by capturing references for example to the current request, instead
// having a constant function to which callers specify an argument on scheduling.
// There's no interface for stopping the background go-routine for now.
type BackgroundProcessor interface {
	// Schedule enqueues the job to be executed in the background, in case it's not
	// already scheduled.
	//
	// The `key` argument serves for deduplicating jobs and the `remoteBackoff` arg
	// is how long that key should be held unschedulable in the remote cache. The
	// `arg` is passed to the process function as is when it is time for processing
	// this entry.
	//
	// Returns true if job was scheduled now or false if it was already in the local
	// queue for execution or held unschedulable in the remote cache.
	Schedule(key string, remoteBackoff time.Duration, arg interface{}) bool
	// QueueLength returns the current number of scheduled jobs in the local queue.
	QueueLength() int
	// BumpRemoteDedupKey increments the base key used for dedupping jobs in the
	// remote cache (Redis), so that all jobs can be rescheduled across all worker
	// instances.
	BumpRemoteDedupKey() error
}

type JobFn func(interface{}) time.Duration

func NewBackgroundProcessor(initialCapacity int, redis redis.Cache, processFunc JobFn) BackgroundProcessor {
	processor := &bgProcessor{
		jobQueue:    NewSyncQueue(initialCapacity),
		cache:       redis,
		processFunc: processFunc,
	}
	go processor.mainLoop()
	return processor
}

type bgProcessor struct {
	jobQueue      *SyncQueue
	cache         redis.Cache
	scheduledJobs sync.Map

	processFunc    JobFn
	maxJobInterval time.Duration
}

func (p *bgProcessor) Schedule(key string, remoteBackoff time.Duration, arg interface{}) bool {
	if !p.shouldEnqueueJob(key, remoteBackoff) {
		return false
	}
	p.jobQueue.Enqueue(&scheduledJob{key, arg, time.Now(), remoteBackoff})
	return true
}

func (p *bgProcessor) QueueLength() int {
	return p.jobQueue.Len()
}

func (p *bgProcessor) BumpRemoteDedupKey() error {
	var _, err = p.cache.Incr(generationKey)
	return err
}

func (p *bgProcessor) shouldEnqueueJob(jobKey string, remoteBackoff time.Duration) bool {
	ttl := max(remoteBackoff, p.worstCaseQueueDelay())
	setOpts := redis.SetOptions{ExpireIn: ttl, IfNotExist: true}

	keyUpdated, err := p.cache.SetOpt(p.remoteDedupKey(jobKey), jobKey, setOpts)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"code":   "bg_processor_error_set_dedup_key",
			"jobKey": jobKey,
			"error":  err.Error(),
		}).Error("Error setting remote dedup key")
	}
	if err != nil || !keyUpdated {
		return false
	}

	_, alreadyScheduledLocally := p.scheduledJobs.LoadOrStore(jobKey, true)
	if alreadyScheduledLocally {
		return false
	}

	return true
}

func (p *bgProcessor) worstCaseQueueDelay() time.Duration {
	return p.maxJobInterval * time.Duration(p.QueueLength())
}

func max(d1, d2 time.Duration) time.Duration {
	if d1 > d2 {
		return d1
	}
	return d2
}

type scheduledJob struct {
	key string
	arg interface{}

	scheduledTime time.Time
	remoteBackoff time.Duration
}

func (p *bgProcessor) mainLoop() {
	defer recoverAndLog(nil)

	for {
		interval := p.processOne(p.jobQueue.Dequeue().(*scheduledJob))
		if interval > p.maxJobInterval {
			p.maxJobInterval = interval
		}
		time.Sleep(interval)
	}
}

func (p *bgProcessor) processOne(job *scheduledJob) time.Duration {
	defer recoverAndLog(job)
	defer p.scheduledJobs.Delete(job.key)
	defer func() {
		timeSinceScheduled := time.Since(job.scheduledTime)
		remainingBackoff := job.remoteBackoff - timeSinceScheduled
		dedupKey := p.remoteDedupKey(job.key)
		if remainingBackoff < 0 {
			p.cache.Del(dedupKey)
		} else {
			// This is in case we scheduled the job assuming the worst-case queue
			// delay, so that it doesn't stay indefinitely unschedulable.
			p.cache.Set(dedupKey, job.key, remainingBackoff)
		}
	}()

	return p.processFunc(job.arg)
}

func (p *bgProcessor) remoteDedupKey(jobKey string) string {
	var generation int64
	var _, err = p.cache.Get(generationKey, &generation)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"code":   "bg_processor_error_get_remote_generation",
			"jobKey": jobKey,
			"error":  err.Error(),
		}).Error("Error getting generation counter")

		// fallback to generation 0
		generation = 0
	}
	key := fmt.Sprintf("goio:bgProcessor:dedupKey:gen:%d:jobKey:%s", generation, jobKey)
	return key
}

func recoverAndLog(job *scheduledJob) {
	panicVal := recover()
	if panicVal == nil {
		return
	}

	logger := logrus.WithFields(logrus.Fields{
		"category":    "fatal_error",
		"code":        "panic",
		"source_file": "go-io/worker/backgroundProcessor",
		"panic_value": panicVal,
		"stack":       string(debug.Stack()),
	})
	if job != nil {
		logger = logger.WithFields(logrus.Fields{
			"job_key":      job.key,
			"job_argument": job.arg,
		})
	}
	logger.Errorf("Panic in background processor!")
}
