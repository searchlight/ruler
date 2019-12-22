package ruler

import (
	"context"

	"go.searchlight.dev/ruler/pkg/m3coordinator"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
)

// appendableAppender adapts a m3coordinator client to both a ruler.Appendable
// and a storage.Appender.
type appendableAppender struct {
	writer  m3coordinator.Writer
	ctx     context.Context
	samples []prompb.TimeSeries
}

func (a *appendableAppender) Appender() (storage.Appender, error) {
	return a, nil
}

func (a *appendableAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	var lbs []prompb.Label
	for _, lbl := range l {
		lbs = append(lbs, prompb.Label{
			Name:  lbl.Name,
			Value: lbl.Value,
		})
	}
	a.samples = append(a.samples, prompb.TimeSeries{
		Labels: lbs,
		Samples: []prompb.Sample{
			{
				Timestamp: t,
				Value:     v,
			},
		},
	})
	return 0, nil
}

func (a *appendableAppender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	_, err := a.Add(l, t, v)
	return err
}

func (a *appendableAppender) Commit() error {
	err := a.writer.Write(a.ctx, a.samples)
	a.samples = nil
	return err
}

func (a *appendableAppender) Rollback() error {
	a.samples = nil
	return nil
}
