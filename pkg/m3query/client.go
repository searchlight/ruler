package m3query

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.searchlight.dev/ruler/pkg/logger"

	utilerrors "github.com/appscode/go/util/errors"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

const (
	QueryAPIPath = "/api/v1/query_range"
)

// https://github.com/m3db/m3/blob/2cf0172d284b8ba1028b5b6d49a72ff93c851644/src/query/api/v1/handler/prometheus/native/common.go#L230
type M3QueryResponse struct {
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
	Data   struct {
		ResultType string `json:"resultType,omitempty"`
		Result     []struct {
			Metric labels.Labels   `json:"metric"`
			Values [][]interface{} `json:"values"`
		} `json:"result,omitempty"`
	} `json:"data,omitempty"`
}

type Client struct {
	config        *Configs
	client        *http.Client
	url           *url.URL
	timeout       time.Duration
	maxConcurrent chan struct{}
}

func NewClient(cfg *Configs) (*Client, error) {
	// https://m3db.github.io/m3/query_engine/api/#read-using-prometheus-query
	u, err := url.Parse(cfg.Addr + QueryAPIPath)
	if err != nil {
		return nil, err
	}

	tlsConfig, err := NewTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		MaxIdleConns:        20000,
		MaxIdleConnsPerHost: 1000, // see https://github.com/golang/go/issues/13801
		DisableKeepAlives:   false,
		TLSClientConfig:     tlsConfig,
		DisableCompression:  true,
		// 5 minutes is typically above the maximum sane query interval. So we can
		// use keepalive for all configurations.
		IdleConnTimeout: 5 * time.Minute,
		DialContext: (&net.Dialer{ // from http.DefaultTransport
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second, // http.DefaultTransport
	}

	return &Client{
		client:        &http.Client{Transport: transport},
		timeout:       cfg.Timeout,
		maxConcurrent: make(chan struct{}, cfg.MaxConcurrent),
		url:           u,
		config:        cfg,
	}, nil
}

func (c *Client) GetQueryFunc() rules.QueryFunc {
	return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
		// to control maximum concurrent query call
		c.concurrentInc()
		defer c.concurrentDec()

		req, err := getQueryRequest(c.url, q, t)
		if err != nil {
			return nil, err
		}

		ctx2, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()

		resp, err := c.client.Do(req.WithContext(ctx2))
		if err != nil {
			return nil, err
		}
		defer utilerrors.Must(resp.Body.Close())

		qResp := &M3QueryResponse{}
		if err := json.NewDecoder(resp.Body).Decode(qResp); err != nil {
			return nil, err
		}
		if qResp.Error != "" {
			return nil, errors.New(qResp.Error)
		}
		return M3QueryResponsePromqlVector(qResp), nil
	}
}

func (c *Client) concurrentInc() {
	c.maxConcurrent <- struct{}{}
}

func (c *Client) concurrentDec() {
	<-c.maxConcurrent
}

// https://m3db.github.io/m3/query_engine/api/#read-using-prometheus-query
func getQueryRequest(u *url.URL, q string, t time.Time) (*http.Request, error) {
	qParams := u.Query()
	qParams.Set("query", q)
	qParams.Set("start", t.Format(time.RFC3339Nano))
	qParams.Set("end", t.Format(time.RFC3339Nano))
	qParams.Set("step", "1s")
	u.RawQuery = qParams.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ruler")
	return req, err
}

func M3QueryResponsePromqlVector(resp *M3QueryResponse) promql.Vector {
	vec := promql.Vector{}
	if resp != nil {
		rs := resp.Data.Result
		for _, s := range rs {
			sample := promql.Sample{
				Metric: s.Metric,
			}
			if len(s.Values) > 0 {
				updatedV := ""
				updatedT := int64(0)
				for _, vlist := range s.Values {
					t := int64(0)
					v := ""
					for _, val := range vlist {
						switch u := val.(type) {
						case string:
							v = u
						case float64:
							t = int64(u)
						default:
							f, err := strconv.ParseFloat(fmt.Sprint(val), 64)
							utilerrors.Must(level.Warn(logger.Logger).Log("failed to convert interface{} to float64", err))
							t = int64(f)
						}
						if v != "" {
							if updatedT < t {
								updatedT = t
								updatedV = v
							}
						}
					}
				}
				if updatedV != "" {
					v, err := strconv.ParseFloat(updatedV, 64)
					if err != nil {
						utilerrors.Must(level.Warn(logger.Logger).Log("failed to convert interface{} to float64", err))
					} else {
						// https://prometheus.io/docs/concepts/data_model/
						// converting to millisecond
						sample.Point = promql.Point{T: updatedT * 1000, V: v}
						vec = append(vec, sample)
					}
				}
			}
		}
	}

	return vec
}

// NewTLSConfig creates a new tls.Config from the given TLSConfig.
func NewTLSConfig(cfg *Configs) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}

	// If a CA cert is provided then let's read it in so we can validate the
	// scrape target's certificate properly.
	if len(cfg.CAFile) > 0 {
		caCertPool := x509.NewCertPool()
		// Load CA cert.
		caCert, err := ioutil.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("unable to use specified CA cert %s: %s", cfg.CAFile, err)
		}
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	if len(cfg.ServerName) > 0 {
		tlsConfig.ServerName = cfg.ServerName
	}
	// If a client cert & key is provided then configure TLS config accordingly.
	if len(cfg.CertFile) > 0 && len(cfg.KeyFile) == 0 {
		return nil, fmt.Errorf("client cert file %q specified without client key file", cfg.CertFile)
	} else if len(cfg.KeyFile) > 0 && len(cfg.CertFile) == 0 {
		return nil, fmt.Errorf("client key file %q specified without client cert file", cfg.KeyFile)
	} else if len(cfg.CertFile) > 0 && len(cfg.KeyFile) > 0 {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("unable to use specified client cert (%s) & key (%s): %s", cfg.CertFile, cfg.KeyFile, err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}
