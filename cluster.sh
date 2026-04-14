#!/bin/bash
if [ "$1" == "stop" ]; then
  gcloud container clusters resize transit-cluster --num-nodes 0 --zone asia-southeast1-a --quiet
  echo "Cluster stopped — nhớ bật lại trước khi làm việc"
elif [ "$1" == "start" ]; then
  gcloud container clusters resize transit-cluster --num-nodes 3 --zone asia-southeast1-a --quiet
  gcloud container clusters get-credentials transit-cluster --zone asia-southeast1-a
  echo "Cluster running"
fi
