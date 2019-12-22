#!/usr/bin/env bash

curl -X POST \
-H "X-AppsCode-UserID: 1" \
-H "Content-Type: application/json" \
-d "@/home/ac/go/src/go.searchlight.dev/ruler/artifacts/k8s/rules/alert-rule-1.json" \
http://localhost:18443/api/v1/rules

curl -X POST \
-H "X-AppsCode-UserID: 2" \
-H "Content-Type: application/json" \
-d "@/home/ac/go/src/go.searchlight.dev/ruler/artifacts/k8s/rules/alert-rule-2.json" \
http://localhost:18443/api/v1/rules