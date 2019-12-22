package ruler

import (
	"time"

	"go.searchlight.dev/ruler/pkg/logger"
	"go.searchlight.dev/ruler/pkg/promquery"

	erragg "github.com/appscode/go/util/errors"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

type RuleGroupsWithInfo struct {
	UserID             string `json:"userID" yaml:"userID"`
	ID                 string `json:"id" yaml:"id"`
	rulefmt.RuleGroups `json:",inline" yaml:",inline"`
	UpdatedAtInUnix    int64 `json:"updatedAtInUnix" yaml:"updatedAtInUnix"`
	DeletedAtInUnix    int64 `json:"deletedAtInUnix" yaml:"deletedAtInUnix"`
}

func (r *RuleGroupsWithInfo) Validate() error {
	if r.ID == "" {
		return errors.New("rule group id is not provided")
	}
	if errs := r.RuleGroups.Validate(); len(errs) > 0 {
		return erragg.NewAggregate(errs)
	}
	return nil
}

// This will add provided labels as label matchers to query and to rule labels
// In conflict, it will overwrite
func (r *RuleGroupsWithInfo) AddLabelsToQueryExprAddRuleLabel(lbs []labels.Label) error {
	var err error
	for p, g := range r.Groups {
		for q, rg := range r.Groups[p].Rules {
			r.Groups[p].Rules[q].Expr, err = promquery.AddLabelMatchersToQuery(r.Groups[p].Rules[q].Expr, lbs)
			if err != nil {
				return errors.Wrapf(err, "failed to add label matchers to rule expr(%s) for rule group %s", rg.Expr, g.Name)
			}

			lMap := r.Groups[p].Rules[q].Labels
			if lMap == nil {
				lMap = map[string]string{}
			}
			for _, lb := range lbs {
				lMap[lb.Name] = lb.Value
			}
			r.Groups[p].Rules[q].Labels = lMap
		}
	}
	return nil
}

func (r RuleGroupsWithInfo) Parse() ([]ruleGroup, error) {
	groups := []ruleGroup{}

	for _, rg := range r.Groups {
		rls := make([]rules.Rule, 0, len(rg.Rules))
		for _, rl := range rg.Rules {
			expr, err := promql.ParseExpr(rl.Expr)
			if err != nil {
				return nil, err
			}

			if rl.Alert != "" {
				rls = append(rls, rules.NewAlertingRule(
					rl.Alert,
					expr,
					time.Duration(rl.For),
					labels.FromMap(rl.Labels),
					labels.FromMap(rl.Annotations),
					true,
					log.With(logger.Logger, "alert", rl.Alert),
				))
				continue
			}
			rls = append(rls, rules.NewRecordingRule(
				rl.Record,
				expr,
				labels.FromMap(rl.Labels),
			))
		}

		// Group names have to be unique in Prometheus, but only within one rules file.
		groups = append(groups, ruleGroup{
			ruleGroupName: rg.Name,
			interval:      time.Duration(rg.Interval),
			rules:         rls,
		})
	}
	return groups, nil
}

type RuleGetter interface {
	GetAllRuleGroups() ([]RuleGroupsWithInfo, error)
	GetAllUpdatedRuleGroups() ([]RuleGroupsWithInfo, error)
}

type RuleWatcher interface {
	Watch(ch chan RuleGroupsWithInfo)
}

type RuleClient interface {
	GetUserRuleGroups(userID string) ([]RuleGroupsWithInfo, error)
	GetRuleGroup(userID string, groupID string) (RuleGroupsWithInfo, error)
	GetAllRuleGroups() ([]RuleGroupsWithInfo, error)

	SetRuleGroup(ruleGroup *RuleGroupsWithInfo) error

	DeleteRuleGroup(userID string, groupID string) error
}
