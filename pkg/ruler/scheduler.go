package ruler

import (
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"strings"
	"sync"
	"time"

	"go.searchlight.dev/ruler/pkg/logger"

	utilerrors "github.com/appscode/go/util/errors"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/common/instrument"
)

var backoffConfig = util.BackoffConfig{
	// Backoff for loading initial configuration set.
	MinBackoff: 100 * time.Millisecond,
	MaxBackoff: 2 * time.Second,
}

const (
	timeLogFormat = "2006-01-02T15:04:05"
)

var (
	totalUserRules = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "appscode",
		Name:      "rules",
		Help:      "How many user rules the scheduler knows about.",
	})
	ruleUpdates = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "appscode",
		Name:      "scheduler_rule_updates_total",
		Help:      "How many rules updates the scheduler has made.",
	})
	rulesRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "appscode",
		Name:      "rules_request_duration_seconds",
		Help:      "Time spent requesting rules.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation", "status_code"}))
)

func init() {
	rulesRequestDuration.Register()
	prometheus.MustRegister(totalUserRules)
	prometheus.MustRegister(ruleUpdates)
}

type workItem struct {
	userID        string
	groupID       string
	ruleGroupName string
	group         *group
	scheduled     time.Time
	interval      time.Duration
}

// Key implements ScheduledItem
func (w workItem) Key() string {
	return w.userID + ":" + w.groupID + ":" + w.ruleGroupName
}

// Scheduled implements ScheduledItem
func (w workItem) Scheduled() time.Time {
	return w.scheduled
}

// Defer returns a work item with updated rules, rescheduled to a later time.
func (w workItem) Defer(interval time.Duration) workItem {
	return workItem{userID: w.userID, groupID: w.groupID, ruleGroupName: w.ruleGroupName, group: w.group, scheduled: w.scheduled.Add(interval), interval: w.interval}
}

func (w workItem) String() string {
	return fmt.Sprintf("%s:%s:%s:%d@%s", w.userID, w.groupID, w.ruleGroupName, len(w.group.Rules()), w.scheduled.Format(timeLogFormat))
}

type ruleGroup struct {
	ruleGroupName string
	interval      time.Duration
	rules         []rules.Rule
}

type groupFactory func(userID string, groupID string, ruleGroupName string, rls []rules.Rule) (*group, error)

type scheduler struct {
	ruleClient         RuleGetter
	evaluationInterval time.Duration // how often we re-evaluate each rule set
	q                  *SchedulingQueue

	pollInterval time.Duration // how often we check for new rules

	processingRules map[ruleKey]bool // keep tracks of all processing rules for all users, key should be userID:groupID

	groupFn groupFactory // function to create a new group
	sync.RWMutex

	stop chan struct{}
	done chan struct{}
}

// newScheduler makes a new scheduler.
func newScheduler(rc RuleGetter, evaluationInterval, pollInterval time.Duration, groupFn groupFactory) scheduler {
	return scheduler{
		ruleClient:         rc,
		evaluationInterval: evaluationInterval,
		pollInterval:       pollInterval,
		q:                  NewSchedulingQueue(clockwork.NewRealClock()),
		processingRules:    map[ruleKey]bool{},
		groupFn:            groupFn,

		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Run polls the source of rules for changes.
func (s *scheduler) Run() {
	utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler started"))
	defer close(s.done)
	// Load initial set of all rules before polling for new ones.
	s.addNewRules(time.Now(), s.loadAllRules())
	ticker := time.NewTicker(s.pollInterval)
	for {
		select {
		case now := <-ticker.C:
			err := s.updateRules(now)
			if err != nil {
				utilerrors.Must(level.Warn(logger.Logger).Log("msg", "scheduler: error updating rules", "err", err))
			}
		case <-s.stop:
			ticker.Stop()
			return
		}
	}
}

func (s *scheduler) Stop() {
	close(s.stop)
	s.q.Close()
	<-s.done
	utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler stopped"))
}

// Load the full set of rules from the server, retrying with backoff
// until we can get them.
func (s *scheduler) loadAllRules() []RuleGroupsWithInfo {
	backoff := util.NewBackoff(context.Background(), backoffConfig)
	for {
		cfgs, err := s.poll(true)
		if err == nil {
			utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler: initial rules load", "num_ruless", len(cfgs)))
			return cfgs
		}
		utilerrors.Must(level.Warn(logger.Logger).Log("msg", "scheduler: error fetching all rules, backing off", "err", err))
		backoff.Wait()
	}
}

func (s *scheduler) updateRules(now time.Time) error {
	rls, err := s.poll(false)
	if err != nil {
		return err
	}

	s.addNewRules(now, rls)
	return nil
}

// poll all the newly updated or deleted rules
// if 'all' is true then it will fetch all the rules
func (s *scheduler) poll(all bool) ([]RuleGroupsWithInfo, error) {
	var rules []RuleGroupsWithInfo
	err := instrument.CollectedRequest(context.Background(), "Rules.GetRules", rulesRequestDuration, instrument.ErrorCode, func(_ context.Context) error {
		var err error
		if all {
			rules, err = s.ruleClient.GetAllRuleGroups()
		} else {
			rules, err = s.ruleClient.GetAllUpdatedRuleGroups()
		}
		return err
	})
	if err != nil {
		utilerrors.Must(level.Warn(logger.Logger).Log("msg", "scheduler: rules server poll failed", "err", err))
		return nil, err
	}
	return rules, nil
}

// computeNextEvalTime Computes when a user's rules should be next evaluated, based on how far we are through an evaluation cycle
func (s *scheduler) computeNextEvalTime(hasher hash.Hash64, now time.Time, userID string) time.Time {
	intervalNanos := float64(s.evaluationInterval.Nanoseconds())
	// Compute how far we are into the current evaluation cycle
	currentEvalCyclePoint := math.Mod(float64(now.UnixNano()), intervalNanos)

	hasher.Reset()
	_, err := hasher.Write([]byte(userID))
	utilerrors.Must(err)
	offset := math.Mod(
		// We subtract our current point in the cycle to cause the entries
		// before 'now' to wrap around to the end.
		// We don't want this to come out negative, so we add the interval to it
		float64(hasher.Sum64())+intervalNanos-currentEvalCyclePoint,
		intervalNanos)
	return now.Add(time.Duration(int64(offset)))
}

func (s *scheduler) addNewRules(now time.Time, rList []RuleGroupsWithInfo) {
	// TODO: instrument how many rules we have, both valid & invalid.
	utilerrors.Must(level.Debug(logger.Logger).Log("msg", "adding rules", "num_rules", len(rList)))
	s.addOrRemoveRules(now, rList)

	ruleUpdates.Add(float64(len(rList)))
	s.Lock()
	lenRules := len(s.processingRules)
	s.Unlock()
	totalUserRules.Set(float64(lenRules))
}

// add or update or delete rules
func (s *scheduler) addOrRemoveRules(now time.Time, rList []RuleGroupsWithInfo) {

	// key should be userID:groupID
	rulesByGroup := map[ruleKey][]ruleGroup{}
	for _, r := range rList {
		key := GetRuleKey(r.UserID, r.ID)

		// check whether this rule deleted or not
		if r.DeletedAtInUnix > 0 {
			s.Lock()
			delete(s.processingRules, key)
			s.Unlock()
			continue
		} else {
			s.Lock()
			s.processingRules[key] = true
			s.Unlock()
		}

		rls, err := r.Parse()
		if err != nil {
			utilerrors.Must(level.Warn(logger.Logger).Log("msg", "scheduler: invalid rule group", "user_id", key.UserID(), "group_id", key.GroupID(), "err", err))
			return
		}
		rulesByGroup[key] = rls
	}

	utilerrors.Must(level.Info(logger.Logger).Log("msg", "scheduler: updating rules", "num_groups", len(rulesByGroup)))

	hasher := fnv.New64a()
	workItems := []workItem{}

	for key, rules := range rulesByGroup {
		userID := key.UserID()
		groupID := key.GroupID()
		evalTime := s.computeNextEvalTime(hasher, now, userID)

		utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler: updating rules for user and group", "user_id", userID, "group", groupID, "num_rules", len(rules)))
		for _, rg := range rules {
			g, err := s.groupFn(userID, groupID, rg.ruleGroupName, rg.rules)
			if err != nil {
				// XXX: similarly to above if a user has a working configuration and
				// for some reason we cannot create a group for the new one we'll use
				// the last known working configuration
				utilerrors.Must(level.Warn(logger.Logger).Log("msg", "scheduler: failed to create group for user", "user_id", userID, "group", groupID, "err", err))
				return
			}

			// TODO: set min interval limitations?
			if rg.interval == 0 {
				rg.interval = s.evaluationInterval
			}
			workItems = append(workItems, workItem{userID: userID, groupID: groupID, ruleGroupName: rg.ruleGroupName, group: g, scheduled: evalTime, interval: rg.interval})
		}
	}

	for _, i := range workItems {
		s.addWorkItem(i)
	}
}

func (s *scheduler) addWorkItem(i workItem) {
	// The queue is keyed by userID+groupID+ruleGroupName, so items for existing userID+groupID+ruleGroupName will be replaced.
	s.q.Enqueue(i)
	utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler: work item added", "item", i))
}

// Get the next scheduled work item, blocking if none.
//
// Call `workItemDone` on the returned item to indicate that it is ready to be
// rescheduled.
func (s *scheduler) nextWorkItem() *workItem {
	utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler: work item requested, pending..."))
	// TODO: We are blocking here on the second Dequeue event. Write more
	// tests for the scheduling queue.
	op := s.q.Dequeue()
	if op == nil {
		utilerrors.Must(level.Info(logger.Logger).Log("msg", "queue closed; no more work items"))
		return nil
	}
	item := op.(workItem)
	utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler: work item granted", "item", item))
	return &item
}

// workItemDone marks the given item as being ready to be rescheduled.
func (s *scheduler) workItemDone(i workItem) {
	s.Lock()
	processing, found := s.processingRules[GetRuleKey(i.userID, i.groupID)]
	s.Unlock()
	if !found || !processing {
		utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler: stopping item", "user_id", i.userID, "group_id", i.groupID, "rule_group", i.ruleGroupName, "found", found))
		return
	}

	// TODO: Add check whether this user belongs to this node

	interval := s.evaluationInterval
	if i.interval != 0 {
		interval = i.interval
	}
	next := i.Defer(interval)
	utilerrors.Must(level.Debug(logger.Logger).Log("msg", "scheduler: work item rescheduled", "item", i, "time", next.scheduled.Format(timeLogFormat)))
	s.addWorkItem(next)
}

type ruleKey string

func GetRuleKey(userID, rGroupID string) ruleKey {
	return ruleKey(fmt.Sprintf("%s:%s", userID, rGroupID))
}

func (k ruleKey) UserID() string {
	return strings.Split(string(k), ":")[0]
}

func (k ruleKey) GroupID() string {
	return strings.Split(string(k), ":")[1]
}
