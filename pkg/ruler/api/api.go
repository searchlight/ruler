package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.searchlight.dev/ruler/pkg/ruler"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"gopkg.in/yaml.v2"
)

// API implements the configs api.
type API struct {
	client ruler.RuleClient
	http.Handler
}

// NewAPI creates a new API.
func NewAPI(client ruler.RuleClient) *API {
	a := &API{client: client}
	r := mux.NewRouter()
	a.RegisterRoutes(r)
	a.Handler = r
	return a
}

// RegisterRoutes registers the configs API HTTP routes with the provided Router.
func (a *API) RegisterRoutes(r *mux.Router) {
	for _, route := range []struct {
		name, method, path string
		handler            http.HandlerFunc
	}{
		{"get_rules", "GET", "/api/v1/rules", a.getConfig},
		{"set_rules", "POST", "/api/v1/rules", a.setConfig},
		{"delete_rules", "DELETE", "/api/v1/rules/{id}", a.deleteConfig},
		{"private_get_all_rules", "GET", "/private/api/v1/rules", a.getAllConfig},
	} {
		r.Handle(route.path, route.handler).Methods(route.method).Name(route.name)
	}
}

// getConfig returns the request configuration.
func (a *API) getConfig(w http.ResponseWriter, r *http.Request) {
	userID, err := ruler.ExtractUserIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	allRuleGroups, err := a.client.GetUserRuleGroups(userID)
	if err != nil {
		glog.Errorf("for userID %s: get rules: %v", userID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(allRuleGroups); err != nil {
		glog.Errorf("for userID %s: get rules: %v", userID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (a *API) setConfig(w http.ResponseWriter, r *http.Request) {
	userID, err := ruler.ExtractUserIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	ruleGroups := &ruler.RuleGroupsWithInfo{}
	if err := yaml.NewDecoder(r.Body).Decode(ruleGroups); err != nil {
		glog.Errorf("for userID %s: error decoding json body: %v", userID, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := ruleGroups.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Add client labels for multi-tenancy purpose
	if err := ruleGroups.AddLabelsToQueryExprAddRuleLabel(ruler.GetLables(userID)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ruleGroups.UpdatedAtInUnix = time.Now().Unix()
	ruleGroups.UserID = userID
	err = a.client.SetRuleGroup(ruleGroups)
	if err != nil {
		glog.Errorf("for userID %s: error storing rule group: %v", userID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *API) deleteConfig(w http.ResponseWriter, r *http.Request) {
	userID, err := ruler.ExtractUserIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	groupID := mux.Vars(r)["id"]
	if groupID == "" {
		http.Error(w, fmt.Sprintf("invalid group id, url: %s", r.URL.String()), http.StatusBadRequest)
		return
	}

	err = a.client.DeleteRuleGroup(userID, groupID)
	if err != nil {
		glog.Errorf("for userID %s: error deleting rule group: %v", userID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// getAllConfig returns all the rules
func (a *API) getAllConfig(w http.ResponseWriter, r *http.Request) {

	allRuleGroups, err := a.client.GetAllRuleGroups()
	if err != nil {
		glog.Errorf("for private: get all rules: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(allRuleGroups); err != nil {
		glog.Errorf("for private: get all rules: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
