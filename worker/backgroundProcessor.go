package housekeeping

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/vtex/go-sdk/utils/metrics"

	"github.com/sirupsen/logrus"
	"github.com/vtex/apps/cache"
	"github.com/vtex/go-io/redis"
)

const generationKey = "Generation"

// BackgroundProcessor is a utility for running jobs in the background, avoiding
// the rescheduling of the same job by a provided key (used as the account name
// for now). We avoid receiving a separate function for each job to prevent
// possible memory leaking by capturing references to the current request, instead
// having a constant function to which callers specify an argument on scheduling.
// There's no interface for stopping the background go-routine for now.
type BackgroundProcessor interface {
	// Schedule enqueues the job to be executed in the background, in case it's not
	// already scheduled. Returns true if job was scheduled now or false if it was
	// already in the queue for execution.
	Schedule(account string, arg interface{}) bool

	ClearRemoteQueue() error
}

type JobFn func(interface{}) time.Duration

func StartBackgroundProcessor(initialCapacity int, processFunc JobFn) BackgroundProcessor {
	processor := &bgProcessor{
		jobQueue:    NewSyncQueue(initialCapacity),
		processFunc: processFunc,
		cache:       cache.Redis(),
	}
	go processor.mainLoop()
	return processor
}

type bgProcessor struct {
	jobQueue      *SyncQueue
	scheduledJobs sync.Map
	cache         redis.Cache

	processFunc    JobFn
	maxJobInterval time.Duration
}

func (p *bgProcessor) Schedule(account string, arg interface{}) bool {
	accountBackoff := getAccountBackoff(account)
	if !p.shouldEnqueueAccount(account, accountBackoff) {
		return false
	}
	p.jobQueue.Enqueue(&scheduledJob{
		key:            account,
		arg:            arg,
		scheduledTime:  time.Now(),
		accountBackoff: accountBackoff,
	})
	return true
}

func (p *bgProcessor) ClearRemoteQueue() error {
	var _, err = p.cache.Incr(generationKey)
	return err
}

func (p *bgProcessor) shouldEnqueueAccount(account string, accountBackoff time.Duration) (should bool) {
	ttl := max(accountBackoff, p.worstCaseQueueDelay())
	setOpts := redis.SetOptions{ExpireIn: ttl, IfNotExist: true}

	keyUpdated, err := p.cache.SetOpt(p.accountHousekeepingRedisKey(account), account, setOpts)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"code":    "housekeeping_error_add_redis",
			"account": account,
			"error":   err.Error(),
		}).Error("Error add account on housekeeper cache")
	}
	if err != nil || !keyUpdated {
		return false
	}

	_, alreadyScheduledLocally := p.scheduledJobs.LoadOrStore(account, true)
	if alreadyScheduledLocally {
		return false
	}

	return true
}

func (p *bgProcessor) worstCaseQueueDelay() time.Duration {
	return p.maxJobInterval * time.Duration(p.jobQueue.Len())
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

	scheduledTime  time.Time
	accountBackoff time.Duration
}

func (p *bgProcessor) mainLoop() {
	defer recoverAndLog(nil)

	for {
		metrics.GetTracker().Observe("housekeeping_queue_length", int64(p.jobQueue.Len()), nil)
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
		timeSinceScheduled := time.Now().Sub(job.scheduledTime)
		remainingBackoff := job.accountBackoff - timeSinceScheduled
		if remainingBackoff < 0 {
			p.cache.Del(p.accountHousekeepingRedisKey(job.key))
		} else {
			p.cache.Set(p.accountHousekeepingRedisKey(job.key), job.key, remainingBackoff)
		}
	}()

	return p.processFunc(job.arg)
}

func (p *bgProcessor) accountHousekeepingRedisKey(account string) string {
	var generation int64
	var _, err = p.cache.Get(generationKey, &generation)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"code":    "housekeeping_error_generation_incr_counter_redis",
			"account": account,
			"error":   err.Error(),
		}).Error("Error incrementing generation counter")

		// fall back to generation 0
		generation = 0
	}
	key := fmt.Sprintf("apps:housekeeping:account:%s:generation:%s", account, strconv.FormatInt(generation, 10))
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
		"source_file": "housekeeping/backgroundJob",
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
