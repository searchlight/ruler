package ruler

import (
	"flag"
	"time"

	"go.searchlight.dev/ruler/pkg/cluster"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/spf13/pflag"
)

// Config is the configuration for the recording rules server.
type Config struct {
	// API port for ruler service.
	APIPort string

	// This is used for template expansion in alerts; must be a valid URL
	ExternalURL string

	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration

	// How frequently to fetch user ruler configs
	PollInterval time.Duration

	NumWorkers int

	// URL of the Alertmanager to send notifications to.
	AlertmanagerURL []string

	// Capacity of the queue for notifications to be sent to the Alertmanager.
	NotificationQueueCapacity int
	// HTTP timeout duration when sending notifications to the Alertmanager.
	NotificationTimeout time.Duration
	// Timeout for rule group evaluation, including sending result to ingester
	GroupTimeout time.Duration

	Cluster *cluster.Config
}

func NewRulerConfig() *Config {
	return &Config{
		Cluster: &cluster.Config{},
	}
}

// AddFlags adds the flags required to config this to the given FlagSet
func (c *Config) AddFlags(f *pflag.FlagSet) {
	f.StringVar(&c.APIPort, "ruler.api-port", "8443", "API port for ruler service.")
	f.StringVar(&c.ExternalURL, "ruler.external-url", "", "URL of alerts return path.")
	f.DurationVar(&c.EvaluationInterval, "ruler.evaluation-interval", 15*time.Second, "How frequently to evaluate rules")
	f.DurationVar(&c.PollInterval, "ruler.poll-interval", 15*time.Second, "How frequently to fetch user ruler configs")
	f.IntVar(&c.NumWorkers, "ruler.num-workers", 100, "Number of rule evaluator worker routines in this process")
	f.StringArrayVar(&c.AlertmanagerURL, "ruler.alertmanager-url", []string{}, "URL of the Alertmanager to send notifications to.")
	f.IntVar(&c.NotificationQueueCapacity, "ruler.notification-queue-capacity", 10000, "Capacity of the queue for notifications to be sent to the Alertmanager.")
	f.DurationVar(&c.NotificationTimeout, "ruler.notification-timeout", 10*time.Second, "HTTP timeout duration when sending notifications to the Alertmanager.")
	f.DurationVar(&c.GroupTimeout, "ruler.group-timeout", 10*time.Second, "Timeout for rule group evaluation, including sending result to ingester")
	if flag.Lookup("promql.lookback-delta") == nil {
		flag.DurationVar(&promql.LookbackDelta, "promql.lookback-delta", promql.LookbackDelta, "Time since the last sample after which a time series is considered stale and ignored by expression evaluations.")
	}

	c.Cluster.AddFlags(f)
}

func (c *Config) Validate() error {
	if c.APIPort == "" {
		return errors.New("ruler.api-port must be non empty")
	}
	if len(c.AlertmanagerURL) == 0 {
		return errors.New("ruler.alertmanagers-url must be non empty")
	}

	if c.Cluster.BindAddr != "" {
		if err := c.Cluster.Validate(); err != nil {
			return err
		}
	}
	return nil
}
