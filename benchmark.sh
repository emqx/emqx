#!/bin/bash
set -euo pipefail

export BENCH_OUT_FILE="$1"

ntopics=1

topics=""

for i in $(seq 1 ${ntopics}) ; do
    topics="${topics} -t ${i}/%c/${i}/%i/+/req"
done

./emqtt_bench sub -c 1000 -h 10.0.0.6 -i 1 $topics
