package etcd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.searchlight.dev/ruler/pkg/ruler"

	utilerrors "github.com/appscode/go/util/errors"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"gopkg.in/yaml.v2"
)

const (
	rulerPrefix       = "ruler/configs/"
	userRulePrefixFmt = "ruler/configs/user/%s/"
	keyFmt            = "ruler/configs/user/%s/rule/%s"

	DialTimeout = 10 * time.Second
)

type Client struct {
	cl     *clientv3.Client
	kv     clientv3.KV
	ctx    context.Context
	logger log.Logger
}

func NewClient(c *Config, l log.Logger) (*Client, error) {
	cl, err := clientv3.New(clientv3.Config{
		Endpoints:   c.Endpoints,
		DialTimeout: DialTimeout,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create etcd client")
	}

	// TODO: should we use context with timer, if we use, then consider the time to get all configs
	return &Client{
		cl:     cl,
		kv:     clientv3.NewKV(cl),
		ctx:    context.Background(),
		logger: l,
	}, nil
}

func (c *Client) GetUserRuleGroups(userID string) ([]ruler.RuleGroupsWithInfo, error) {
	return c.getWithPrefix(fmt.Sprintf(userRulePrefixFmt, userID))
}

func (c *Client) GetRuleGroup(userID string, groupID string) (ruler.RuleGroupsWithInfo, error) {
	return c.get(getKey(userID, groupID))
}

func (c *Client) GetAllRuleGroups() ([]ruler.RuleGroupsWithInfo, error) {
	return c.getWithPrefix(rulerPrefix)
}

func (c *Client) SetRuleGroup(ruleGroup *ruler.RuleGroupsWithInfo) error {
	return c.put(ruleGroup)
}

func (c *Client) DeleteRuleGroup(userID string, groupID string) error {
	return c.delete(getKey(userID, groupID))
}

func (c *Client) get(key string) (ruler.RuleGroupsWithInfo, error) {
	rg := ruler.RuleGroupsWithInfo{}

	resp, err := c.kv.Get(c.ctx, key)
	if err != nil {
		return rg, err
	}
	if len(resp.Kvs) == 0 {
		return rg, nil
	}

	if err := yaml.Unmarshal(resp.Kvs[0].Value, &rg); err != nil {
		return rg, errors.Wrap(err, "failed to decode response")
	}
	return rg, nil
}

func (c *Client) getWithPrefix(prefix string) ([]ruler.RuleGroupsWithInfo, error) {
	resp, err := c.kv.Get(c.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var rgList []ruler.RuleGroupsWithInfo
	for _, rg := range resp.Kvs {
		r := ruler.RuleGroupsWithInfo{}
		if err := yaml.Unmarshal(rg.Value, &r); err != nil {
			return nil, errors.Wrap(err, "failed to decode response")
		}
		rgList = append(rgList, r)
	}
	return rgList, nil
}

func (c *Client) put(rg *ruler.RuleGroupsWithInfo) error {
	data, err := yaml.Marshal(rg)
	if err != nil {
		return errors.Wrap(err, "failed to marshal rule group")
	}

	_, err = c.kv.Put(c.ctx, getKey(rg.UserID, rg.ID), string(data))
	if err != nil {
		return errors.Wrap(err, "failed to store rule group")
	}
	return nil
}

func (c *Client) delete(key string) error {
	// TODO: should delete it or just set the delete timestamp.
	_, err := c.kv.Delete(c.ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to delete rule group")
	}
	return nil
}

// Watches the keys
// it's blocking
func (c *Client) Watch(ch chan ruler.RuleGroupsWithInfo) {
	watcher := c.cl.Watch(c.ctx, rulerPrefix, clientv3.WithPrefix())
	for resp := range watcher {
		for _, ev := range resp.Events {

			if ev.Type == clientv3.EventTypeDelete {
				userID, groupID := getUserAndGroupIDFromKey(string(ev.Kv.Key))
				ch <- ruler.RuleGroupsWithInfo{
					UserID:          userID,
					ID:              groupID,
					DeletedAtInUnix: time.Now().Unix(),
				}
			} else {
				r := ruler.RuleGroupsWithInfo{}
				if err := yaml.Unmarshal(ev.Kv.Value, &r); err != nil {
					utilerrors.Must(level.Warn(c.logger).Log("msg", "failed unmarshal response", "err", err))
				} else {
					ch <- r
				}
			}
		}
	}
}

func (c *Client) Close() {
	utilerrors.Must(c.cl.Close())
}

func getKey(usedID, groupID string) string {
	return fmt.Sprintf(keyFmt, usedID, groupID)
}

func getUserAndGroupIDFromKey(key string) (userID string, groupID string) {
	st := strings.Split(key, "/")
	if len(st) >= 4 {
		userID = st[3]
	}
	if len(st) >= 6 {
		groupID = st[5]
	}
	return userID, groupID
}
