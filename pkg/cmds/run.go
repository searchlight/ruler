package cmds

import (
	"fmt"
	"net/http"

	"go.searchlight.dev/ruler/pkg/logger"
	"go.searchlight.dev/ruler/pkg/m3coordinator"
	"go.searchlight.dev/ruler/pkg/m3query"
	"go.searchlight.dev/ruler/pkg/ruler"
	"go.searchlight.dev/ruler/pkg/ruler/api"
	"go.searchlight.dev/ruler/pkg/storage/etcd"

	utilerrors "github.com/appscode/go/util/errors"
	"github.com/go-kit/kit/log"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
)

func NewCmdRun() *cobra.Command {
	rulerCfg := ruler.NewRulerConfig()
	etcdCfg := etcd.NewConfig()

	m3coordinatorCfg := &m3coordinator.Configs{}
	m3queryCfg := &m3query.Configs{}

	cmd := &cobra.Command{
		Use:               "run",
		Short:             "Launch ruler",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger.InitLogger()

			utilerrors.Must(logger.Logger.Log("Starting ruler"))

			glog.Infof("Starting ruler ...")
			if err := rulerCfg.Validate(); err != nil {
				return err
			}
			if err := m3coordinatorCfg.Validate(); err != nil {
				return err
			}
			if err := m3queryCfg.Validate(); err != nil {
				return err
			}
			if err := etcdCfg.Validate(); err != nil {
				return err
			}

			writer, err := m3coordinator.NewWriter(m3coordinatorCfg)
			if err != nil {
				return err
			}

			m3qClient, err := m3query.NewClient(m3queryCfg)
			if err != nil {
				return err
			}

			rulr, err := ruler.NewRuler(rulerCfg, m3qClient.GetQueryFunc(), writer)
			if err != nil {
				return err
			}
			defer rulr.Stop()

			ruleStoreClient, err := etcd.NewClient(etcdCfg, log.With(logger.Logger, "domain", "etcd rule storage"))
			if err != nil {
				return err
			}

			ruleGetter, err := ruler.NewRuleGetterWrapper(rulr.Distributor(), ruleStoreClient, ruleStoreClient)
			if err != nil {
				return err
			}

			rulerServer, err := ruler.NewServer(rulerCfg, rulr, ruleGetter)
			if err != nil {
				return err
			}
			defer rulerServer.Stop()

			rulerAPI := api.NewAPI(ruleStoreClient)

			r := mux.NewRouter()
			r.HandleFunc("/api/v1/cluster/status", rulr.ClusterStatus)
			r.HandleFunc("/api/v1/ring/status", rulr.HashRingStatus)
			rulerAPI.RegisterRoutes(r)
			if err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", rulerCfg.APIPort), r); err != nil {
				return err
			}
			return nil
		},
	}

	rulerCfg.AddFlags(cmd.Flags())
	m3coordinatorCfg.AddFlags(cmd.Flags())
	m3queryCfg.AddFlags(cmd.Flags())
	etcdCfg.AddFlags(cmd.Flags())

	return cmd
}
