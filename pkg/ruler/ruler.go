package ruler

import (
	native_ctx "context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.searchlight.dev/ruler/pkg/cluster"
	logger2 "go.searchlight.dev/ruler/pkg/logger"
	"go.searchlight.dev/ruler/pkg/m3coordinator"

	utilerrors "github.com/appscode/go/util/errors"
	gklog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	sd_config "github.com/prometheus/prometheus/discovery/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/util/strutil"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/net/context"
	"golang.org/x/net/context/ctxhttp"
)

var (
	evalDuration = instrument.NewHistogramCollectorFromOpts(prometheus.HistogramOpts{
		Namespace: "appscode",
		Name:      "group_evaluation_duration_seconds",
		Help:      "The duration for a rule group to execute.",
		Buckets:   []float64{.1, .25, .5, 1, 2.5, 5, 10, 25},
	})
	rulesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "appscode",
		Name:      "rules_processed_total",
		Help:      "How many rules have been processed.",
	})
	blockedWorkers = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "appscode",
		Name:      "blocked_workers",
		Help:      "How many workers are waiting on an item to be ready.",
	})
	workerIdleTime = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "appscode",
		Name:      "worker_idle_seconds_total",
		Help:      "How long workers have spent waiting for work.",
	})
	evalLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "appscode",
		Name:      "group_evaluation_latency_seconds",
		Help:      "How far behind the target time each rule group executed.",
		Buckets:   []float64{.1, .25, .5, 1, 2.5, 5, 10, 25},
	})
)

func init() {
	evalDuration.Register()
	prometheus.MustRegister(evalLatency)
	prometheus.MustRegister(rulesProcessed)
	prometheus.MustRegister(blockedWorkers)
	prometheus.MustRegister(workerIdleTime)
}

// Ruler evaluates rules.
type Ruler struct {
	queryFunc        rules.QueryFunc
	writer           m3coordinator.Writer
	alertExternalURL *url.URL
	notifierCfg      *config.Config
	queueCapacity    int
	groupTimeout     time.Duration
	metrics          *rules.Metrics

	// Per-user notifiers with separate queues.
	notifiersMtx sync.Mutex
	notifiers    map[string]*rulerNotifier

	// for cluster
	peer        *cluster.Peer
	distributor *Distributor
}

func (r *Ruler) Distributor() *Distributor {
	return r.distributor
}

// rulerNotifier bundles a notifier.Manager together with an associated
// Alertmanager service discovery manager and handles the lifecycle
// of both actors.
type rulerNotifier struct {
	notifier  *notifier.Manager
	logger    gklog.Logger
	sdCancel  context.CancelFunc
	sdManager *discovery.Manager
	wg        sync.WaitGroup
}

func newRulerNotifier(o *notifier.Options, l gklog.Logger) *rulerNotifier {
	sdCtx, sdCancel := context.WithCancel(context.Background())
	return &rulerNotifier{
		notifier:  notifier.NewManager(o, l),
		sdCancel:  sdCancel,
		sdManager: discovery.NewManager(sdCtx, l),
		logger:    l,
	}
}

func (rn *rulerNotifier) run() {
	rn.wg.Add(1)
	go func() {
		if err := rn.sdManager.Run(); err != nil {
			utilerrors.Must(level.Error(rn.logger).Log("msg", "error starting notifier discovery manager", "err", err))
		}
		rn.wg.Done()
	}()
	go func() {
		rn.notifier.Run(rn.sdManager.SyncCh())
		rn.wg.Done()
	}()
}

func (rn *rulerNotifier) applyConfig(cfg *config.Config) error {
	if err := rn.notifier.ApplyConfig(cfg); err != nil {
		return err
	}

	sdCfgs := make(map[string]sd_config.ServiceDiscoveryConfig)
	for _, v := range cfg.AlertingConfig.AlertmanagerConfigs {
		// AlertmanagerConfigs doesn't hold an unique identifier so we use the config hash as the identifier.
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		// This hash needs to be identical to the one computed in the notifier in
		// https://github.com/prometheus/prometheus/blob/719c579f7b917b384c3d629752dea026513317dc/notifier/notifier.go#L265
		// This kind of sucks, but it's done in Prometheus in main.go in the same way.
		sdCfgs[fmt.Sprintf("%x", md5.Sum(b))] = v.ServiceDiscoveryConfig
	}

	return rn.sdManager.ApplyConfig(sdCfgs)
}

func (rn *rulerNotifier) stop() {
	rn.sdCancel()
	rn.notifier.Stop()
	rn.wg.Wait()
}

// NewRuler creates a new ruler
func NewRuler(cfg *Config, queryFunc rules.QueryFunc, w m3coordinator.Writer) (*Ruler, error) {
	ncfg, err := buildNotifierConfig(cfg)
	if err != nil {
		return nil, err
	}

	externalUrl, err := url.Parse(cfg.ExternalURL)
	if err != nil {
		return nil, err
	}

	rulr := &Ruler{
		queryFunc:        queryFunc,
		writer:           w,
		alertExternalURL: externalUrl,
		notifierCfg:      ncfg,
		queueCapacity:    cfg.NotificationQueueCapacity,
		notifiers:        map[string]*rulerNotifier{},
		groupTimeout:     cfg.GroupTimeout,
		metrics:          rules.NewGroupMetrics(prometheus.DefaultRegisterer),
	}

	// for cluster
	if cfg.Cluster.BindAddr != "" {
		peer, err := cluster.Create(cfg.Cluster,
			gklog.With(logger2.Logger, "domain", "cluster"),
			prometheus.DefaultRegisterer,
			nil)
		if err != nil {
			return nil, err
		}

		distributor, err := NewDistributor(peer)
		if err != nil {
			return nil, err
		}

		// join in the cluster
		err = peer.Join()
		if err != nil {
			return nil, err
		}

		// settle the cluster before start serving
		peer.Settle()

		// Do initial refresh
		distributor.Refresh()

		// TODO: should use flag for this
		go distributor.HandleRefresh(time.Second * 3)

		rulr.peer = peer
		rulr.distributor = distributor
	}
	return rulr, nil
}

// Builds a Prometheus config.Config from a ruler.Config with just the required
// options to configure notifications to Alertmanager.
func buildNotifierConfig(rulerConfig *Config) (*config.Config, error) {
	if len(rulerConfig.AlertmanagerURL) == 0 {
		return &config.Config{}, nil
	}

	amConfigs := []*config.AlertmanagerConfig{}
	for _, addr := range rulerConfig.AlertmanagerURL {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse alertmanager url")
		}

		sdConfig := sd_config.ServiceDiscoveryConfig{
			StaticConfigs: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						{
							model.AddressLabel: model.LabelValue(u.Host),
						},
					},
				},
			},
		}

		amConfigs = append(amConfigs, &config.AlertmanagerConfig{
			Scheme:                 u.Scheme,
			PathPrefix:             u.Path,
			Timeout:                model.Duration(rulerConfig.NotificationTimeout),
			ServiceDiscoveryConfig: sdConfig,
		})
	}

	promConfig := &config.Config{
		AlertingConfig: config.AlertingConfig{
			AlertmanagerConfigs: amConfigs,
		},
	}
	return promConfig, nil
}

func (r *Ruler) newGroup(userID string, groupID string, ruleGroupName string, rls []rules.Rule) (*group, error) {
	appendable := &appendableAppender{writer: r.writer}
	notifier, err := r.getOrCreateNotifier(userID)
	if err != nil {
		return nil, err
	}
	opts := &rules.ManagerOptions{
		Appendable:  appendable,
		QueryFunc:   r.queryFunc,
		Context:     context.Background(),
		ExternalURL: r.alertExternalURL,
		NotifyFunc:  sendAlerts(notifier, r.alertExternalURL.String()),
		Logger:      logger2.Logger,
		Metrics:     r.metrics,
	}
	return newGroup(ruleGroupName, rls, appendable, opts), nil
}

// sendAlerts implements a rules.NotifyFunc for a Notifier.
// It filters any non-firing alerts from the input.
//
// Copied from Prometheus's main.go.
func sendAlerts(n *notifier.Manager, externalURL string) rules.NotifyFunc {
	return func(ctx native_ctx.Context, expr string, alerts ...*rules.Alert) {
		var res []*notifier.Alert
		for _, alert := range alerts {
			// Only send actually firing alerts.
			if alert.State == rules.StatePending {
				continue
			}
			a := &notifier.Alert{
				StartsAt:     alert.FiredAt,
				Labels:       alert.Labels,
				Annotations:  alert.Annotations,
				GeneratorURL: externalURL + strutil.TableLinkForExpression(expr),
			}
			if !alert.ResolvedAt.IsZero() {
				a.EndsAt = alert.ResolvedAt
			}
			res = append(res, a)
		}

		if len(alerts) > 0 {
			n.Send(res...)
		}
	}
}

func (r *Ruler) getOrCreateNotifier(userID string) (*notifier.Manager, error) {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	n, ok := r.notifiers[userID]
	if ok {
		return n.notifier, nil
	}

	n = newRulerNotifier(&notifier.Options{
		QueueCapacity: r.queueCapacity,
		Do: func(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
			// add userID in header
			if err := SetUserIDInHTTPRequest(userID, req); err != nil {
				return nil, err
			}

			return ctxhttp.Do(ctx, client, req)
		},
	}, logger2.Logger)

	go n.run()

	// This should never fail, unless there's a programming mistake.
	if err := n.applyConfig(r.notifierCfg); err != nil {
		return nil, err
	}

	// TODO: Remove notifiers for stale users. Right now this is a slow leak.
	r.notifiers[userID] = n
	return n.notifier, nil
}

// Evaluate a list of rules in the given context.
func (r *Ruler) Evaluate(userID string, item *workItem) {
	// TODO:
	ctx := context.Background()
	logger := gklog.With(logger2.Logger, "user_id", userID)
	utilerrors.Must(level.Debug(logger).Log("msg", "evaluating rules...", "num_rules", len(item.group.Rules())))
	ctx, cancelTimeout := context.WithTimeout(ctx, r.groupTimeout)
	_ = instrument.CollectedRequest(ctx, "Evaluate", evalDuration, nil, func(ctx native_ctx.Context) error {
		if span := opentracing.SpanFromContext(ctx); span != nil {
			span.SetTag("instance", userID)
			span.SetTag("groupName", item.ruleGroupName)
		}
		item.group.Eval(ctx, time.Now())
		return nil
	})
	if err := ctx.Err(); err == nil {
		cancelTimeout() // release resources
	} else {
		utilerrors.Must(level.Warn(logger).Log("msg", "context error", "error", err))
	}

	rulesProcessed.Add(float64(len(item.group.Rules())))
}

// Stop stops the Ruler.
func (r *Ruler) Stop() {
	if r.peer != nil {
		_ = r.peer.Leave(10 * time.Second)
	}

	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	for _, n := range r.notifiers {
		n.stop()
	}
}

func (r *Ruler) ClusterStatus(w http.ResponseWriter, req *http.Request) {
	status := struct {
		Status string                 `json:"status"`
		Peers  map[string]interface{} `json:"peers,omitempty"`
	}{}
	if r.peer == nil {
		status.Status = "disabled"
	} else {
		status.Status = r.peer.Status()
		status.Peers = r.peer.Info()
	}
	if err := json.NewEncoder(w).Encode(status); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, e2 := w.Write([]byte(err.Error()))
		utilerrors.Must(utilerrors.NewAggregate([]error{err, e2}))
		return
	}
}

func (r *Ruler) HashRingStatus(w http.ResponseWriter, req *http.Request) {
	status := struct {
		Status   string   `json:"status"`
		NodeList []string `json:"nodeList"`
	}{}
	if r.distributor == nil {
		status.Status = "disabled"
	} else {
		status.Status = "enabled"
		status.NodeList = r.distributor.MemberNodeList()
	}
	if err := json.NewEncoder(w).Encode(status); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, e2 := w.Write([]byte(err.Error()))
		utilerrors.Must(utilerrors.NewAggregate([]error{err, e2}))
		return
	}
}

// Server is a rules server.
type Server struct {
	scheduler *scheduler
	workers   []worker
}

// NewServer makes a new rule processing server.
func NewServer(cfg *Config, ruler *Ruler, rg RuleGetter) (*Server, error) {
	// TODO: Separate configuration for polling interval.

	s := newScheduler(rg, cfg.EvaluationInterval, cfg.PollInterval, ruler.newGroup)
	if cfg.NumWorkers <= 0 {
		return nil, errors.Errorf("must have at least 1 worker, got %d", cfg.NumWorkers)
	}

	workers := make([]worker, cfg.NumWorkers)
	for i := 0; i < cfg.NumWorkers; i++ {
		workers[i] = newWorker(&s, ruler)
	}
	srv := Server{
		scheduler: &s,
		workers:   workers,
	}
	go srv.run()
	return &srv, nil
}

// Run the server.
func (s *Server) run() {
	go s.scheduler.Run()
	for _, w := range s.workers {
		go w.Run()
	}
	utilerrors.Must(level.Info(logger2.Logger).Log("msg", "ruler up and running"))
}

// Stop the server.
func (s *Server) Stop() {
	for _, w := range s.workers {
		w.Stop()
	}
	s.scheduler.Stop()
}

// Worker does a thing until it's told to stop.
type Worker interface {
	Run()
	Stop()
}

type worker struct {
	scheduler *scheduler
	ruler     *Ruler

	done       chan struct{}
	terminated chan struct{}
}

func newWorker(scheduler *scheduler, ruler *Ruler) worker {
	return worker{
		scheduler:  scheduler,
		ruler:      ruler,
		done:       make(chan struct{}),
		terminated: make(chan struct{}),
	}
}

func (w *worker) Run() {
	defer close(w.terminated)
	for {
		select {
		case <-w.done:
			return
		default:
		}
		waitStart := time.Now()
		blockedWorkers.Inc()
		utilerrors.Must(level.Debug(logger2.Logger).Log("msg", "waiting for next work item"))
		item := w.scheduler.nextWorkItem()
		blockedWorkers.Dec()
		waitElapsed := time.Since(waitStart)
		if item == nil {
			utilerrors.Must(level.Debug(logger2.Logger).Log("msg", "queue closed and empty; terminating worker"))
			return
		}
		evalLatency.Observe(time.Since(item.scheduled).Seconds())
		workerIdleTime.Add(waitElapsed.Seconds())
		utilerrors.Must(level.Debug(logger2.Logger).Log("msg", "processing item", "item", item))

		// Add work distribution
		if w.ruler.distributor != nil {
			if ok, err := w.ruler.distributor.IsAssigned(item.userID); err != nil {
				utilerrors.Must(level.Warn(logger2.Logger).Log("msg", "check user is assigned", "err", err))
			} else if !ok {
				// this user is not assigned to this node
				continue
			}
		}

		w.ruler.Evaluate(item.userID, item)
		w.scheduler.workItemDone(*item)
		utilerrors.Must(level.Debug(logger2.Logger).Log("msg", "item handed back to queue", "item", item))
	}
}

func (w *worker) Stop() {
	close(w.done)
	<-w.terminated
}
