package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	utilerrors "github.com/appscode/go/util/errors"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Peer is a single peer in a gossip cluster.
type Peer struct {
	cfg   *Config
	mlist *memberlist.Memberlist

	mtx   sync.RWMutex
	stopc chan struct{}

	// This should only updated/handle by (p *Peer) Settle() function
	readyc chan struct{}

	failedRefreshCounter prometheus.Counter
	refreshCounter       prometheus.Counter

	logger log.Logger
}

const (
	DefaultPushPullInterval  = 60 * time.Second
	DefaultGossipInterval    = 200 * time.Millisecond
	DefaultTcpTimeout        = 10 * time.Second
	DefaultProbeTimeout      = 500 * time.Millisecond
	DefaultProbeInterval     = 1 * time.Second
	DefaultReconnectInterval = 10 * time.Second
	DefaultReconnectTimeout  = 6 * time.Hour
	DefaultRefreshInterval   = 15 * time.Second
	maxGossipPacketSize      = 1400
)

func Create(cf *Config, l log.Logger, reg prometheus.Registerer, delegate memberlist.EventDelegate) (*Peer, error) {
	bindHost, bindPortStr, err := net.SplitHostPort(cf.BindAddr)
	if err != nil {
		return nil, err
	}
	bindPort, err := strconv.Atoi(bindPortStr)
	if err != nil {
		return nil, errors.Wrap(err, "invalid listen address")
	}

	var advertiseHost string
	var advertisePort int
	if advertiseAddr, err := getAdvertiseAddr(cf); err != nil {
		return nil, errors.Wrap(err, "failed to get advertise address")
	} else {
		var advertisePortStr string
		advertiseHost, advertisePortStr, err = net.SplitHostPort(advertiseAddr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid advertise address")
		}
		advertisePort, err = strconv.Atoi(advertisePortStr)
		if err != nil {
			return nil, errors.Wrap(err, "invalid advertise address, wrong port")
		}
	}

	knownPeers, err := getPeers(cf)
	if err != nil {
		// No need to return error. handleRefresh() will try to rejoin
		// return error will cause problem in case of deployment using statefulset, dns doesn't resolve up until pod is ready.
		utilerrors.Must(level.Warn(l).Log("msg", "failed to get known peers", "err", err))
	}
	utilerrors.Must(level.Debug(l).Log("known peers", strings.Join(knownPeers, ",")))

	p := &Peer{
		stopc:  make(chan struct{}),
		readyc: make(chan struct{}),
		logger: l,
		cfg:    cf,
	}

	p.register(reg)

	retransmit := len(knownPeers) / 2
	if retransmit < 3 {
		retransmit = 3
	}

	cfg := memberlist.DefaultLANConfig()
	// by default they use hostname
	if !cf.UseHostName {
		name, err := ulid.New(ulid.Now(), rand.New(rand.NewSource(time.Now().UnixNano())))
		if err != nil {
			return nil, err
		}
		cfg.Name = name.String()
	}
	cfg.BindAddr = bindHost
	cfg.BindPort = bindPort
	cfg.AdvertiseAddr = advertiseHost
	cfg.AdvertisePort = advertisePort
	cfg.GossipInterval = cf.GossipInterval
	cfg.PushPullInterval = cf.PushPullInterval
	cfg.TCPTimeout = cf.TcpTimeout
	cfg.ProbeTimeout = cf.ProbeTimeout
	cfg.ProbeInterval = cf.ProbeInterval
	cfg.LogOutput = &logWriter{l: l}
	cfg.GossipNodes = retransmit
	cfg.UDPBufferSize = maxGossipPacketSize
	cfg.Events = delegate

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "create memberlist")
	}
	p.mlist = ml
	return p, nil
}

func (p *Peer) Join() error {
	peers, err := getPeers(p.cfg)
	if err != nil {
		// No need to return error. handleRefresh() will try to rejoin
		// return error will cause problem in case of deployment using statefulset, dns doesn't resolve up until pod is ready.
		utilerrors.Must(level.Warn(p.logger).Log("msg", "failed to get known peers", "err", err))
	} else {
		n, err := p.mlist.Join(peers)
		if err != nil {
			utilerrors.Must(level.Warn(p.logger).Log("msg", "failed to join cluster", "err", err))
		} else {
			utilerrors.Must(level.Debug(p.logger).Log("msg", "joined cluster", "peers", n))
		}
	}

	go p.handleRefresh(DefaultRefreshInterval)

	// no need to send error
	return nil
}

type logWriter struct {
	l log.Logger
}

func (l *logWriter) Write(b []byte) (int, error) {
	return len(b), level.Debug(l.l).Log("memberlist", string(b))
}

func (p *Peer) register(reg prometheus.Registerer) {
	p.failedRefreshCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ruler_cluster_refresh_join_failed_total",
		Help: "A counter of the number of failed cluster peer joined attempts via refresh.",
	})
	p.refreshCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ruler_cluster_refresh_join_total",
		Help: "A counter of the number of cluster peer joined via refresh.",
	})

	reg.MustRegister(p.refreshCounter, p.failedRefreshCounter)
}

func (p *Peer) handleRefresh(d time.Duration) {
	tick := time.NewTicker(d)
	defer tick.Stop()

	for {
		select {
		case <-p.stopc:
			return
		case <-tick.C:
			p.refresh()
		}
	}
}

func (p *Peer) refresh() {
	logger := log.With(p.logger, "msg", "refresh")
	knownPeers, err := getPeers(p.cfg)
	if err != nil {
		utilerrors.Must(level.Warn(logger).Log("msg", "failed to get known peers", "err", err.Error()))
	}

	resolvedPeers, err := resolvePeers(context.Background(), knownPeers, new(net.Resolver))
	if err != nil {
		utilerrors.Must(level.Debug(logger).Log("peers", knownPeers, "err", err))
		return
	}

	members := p.mlist.Members()
	for _, peer := range resolvedPeers {
		var isPeerFound bool
		for _, member := range members {
			if member.Address() == peer {
				isPeerFound = true
				break
			}
		}

		if !isPeerFound {
			if _, err := p.mlist.Join([]string{peer}); err != nil {
				p.failedRefreshCounter.Inc()
				utilerrors.Must(level.Warn(logger).Log("result", "failure", "addr", peer))
			} else {
				p.refreshCounter.Inc()
				utilerrors.Must(level.Debug(logger).Log("result", "success", "addr", peer))
			}
		}
	}
}

// Leave the cluster, waiting up to timeout.
func (p *Peer) Leave(timeout time.Duration) error {
	close(p.stopc)
	utilerrors.Must(level.Debug(p.logger).Log("msg", "leaving cluster"))
	return p.mlist.Leave(timeout)
}

// Name returns the unique ID of this peer in the cluster.
func (p *Peer) Name() string {
	return p.mlist.LocalNode().Name
}

// ClusterSize returns the current number of alive members in the cluster.
func (p *Peer) ClusterSize() int {
	return p.mlist.NumMembers()
}

// Return true when router has settled.
func (p *Peer) Ready() bool {
	select {
	case <-p.readyc:
		return true
	default:
	}
	return false
}

// Wait until Settle() has finished.
func (p *Peer) WaitReady() {
	<-p.readyc
}

// Return a status string representing the peer state.
func (p *Peer) Status() string {
	if p.Ready() {
		return "ready"
	} else {
		return "settling"
	}
}

// Info returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) Info() map[string]interface{} {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	info := map[string]interface{}{}

	type nodeInfo struct {
		Name string `json:"name"`
		Addr string `json:"address"`
	}
	self := p.mlist.LocalNode()
	info["self"] = nodeInfo{
		Name: self.Name,
		Addr: fmt.Sprintf("%s:%d", self.Addr.String(), self.Port),
	}
	var mList []nodeInfo
	for _, nd := range p.mlist.Members() {
		mList = append(mList, nodeInfo{
			Name: nd.Name,
			Addr: fmt.Sprintf("%s:%d", nd.Addr.String(), nd.Port),
		})
	}
	info["peers"] = mList
	return info
}

// Self returns the node information about the peer itself.
func (p *Peer) Self() *memberlist.Node {
	return p.mlist.LocalNode()
}

// Peers returns the peers in the cluster.
func (p *Peer) Peers() []*memberlist.Node {
	return p.mlist.Members()
}

// Settle waits until the mesh is ready (and sets the appropriate internal state when it is).
// The idea is that we don't want to start "working" before we get a chance to know stable number of members.
// Inspired from https://github.com/apache/cassandra/blob/7a40abb6a5108688fb1b10c375bb751cbb782ea4/src/java/org/apache/cassandra/gms/Gossiper.java
func (p *Peer) Settle() {
	const NumOkayRequired = 3
	utilerrors.Must(level.Info(p.logger).Log("msg", "Waiting for gossip to settle..."))
	start := time.Now()
	nPeers := 0
	nOkay := 0
	totalPolls := 0
	for {
		select {
		case <-p.stopc:
			elapsed := time.Since(start)
			utilerrors.Must(level.Info(p.logger).Log("msg", "gossip not settled but continuing anyway", "polls", totalPolls, "elapsed", elapsed))
			close(p.readyc)
			return
		case <-time.After(p.cfg.GossipInterval * 10):
		}
		elapsed := time.Since(start)
		n := len(p.Peers())
		if nOkay >= NumOkayRequired {
			utilerrors.Must(level.Info(p.logger).Log("msg", "gossip settled; proceeding", "elapsed", elapsed))
			break
		}
		if n == nPeers {
			nOkay++
			utilerrors.Must(level.Debug(p.logger).Log("msg", "gossip looks settled", "elapsed", elapsed))
		} else {
			nOkay = 0
			utilerrors.Must(level.Info(p.logger).Log("msg", "gossip not settled", "polls", totalPolls, "before", nPeers, "now", n, "elapsed", elapsed))
		}
		nPeers = n
		totalPolls++
	}
	close(p.readyc)
}

func resolvePeers(ctx context.Context, peers []string, res *net.Resolver) ([]string, error) {
	var resolvedPeers []string

	for _, peer := range peers {
		host, port, err := net.SplitHostPort(peer)
		if err != nil {
			return nil, errors.Wrapf(err, "split host/port for peer %s", peer)
		}

		retryCtx, cancel := context.WithCancel(ctx)

		ips, err := res.LookupIPAddr(ctx, host)
		if err != nil {
			// Assume direct address.
			resolvedPeers = append(resolvedPeers, peer)
			continue
		}

		if len(ips) == 0 {
			var lookupErrSpotted bool

			err := retry(2*time.Second, retryCtx.Done(), func() error {
				if lookupErrSpotted {
					// We need to invoke cancel in next run of retry when lookupErrSpotted to preserve LookupIPAddr error.
					cancel()
				}

				ips, err = res.LookupIPAddr(retryCtx, host)
				if err != nil {
					lookupErrSpotted = true
					return errors.Wrapf(err, "IP Addr lookup for peer %s", peer)
				}
				if len(ips) == 0 {
					return errors.New("empty IPAddr result. Retrying")
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}

		for _, ip := range ips {
			resolvedPeers = append(resolvedPeers, net.JoinHostPort(ip.String(), port))
		}
	}

	return resolvedPeers, nil
}

// retry executes f every interval seconds until timeout or no error is returned from f.
func retry(interval time.Duration, stopc <-chan struct{}, f func() error) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()

	var err error
	for {
		if err = f(); err == nil {
			return nil
		}
		select {
		case <-stopc:
			return err
		case <-tick.C:
		}
	}
}
