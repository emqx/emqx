#!/usr/bin/env bash

## Smoke test: connect a small batch of MQTT clients to a real EMQX release,
## reject connect-time warning/error logs, and guard the live memory retained by
## each idle connection.
##
## Usage: run.sh IMAGE_TAG   (e.g. run.sh emqx/emqx-enterprise:latest)

set -euo pipefail

IMAGE_TAG="${1:-${_EMQX_DOCKER_IMAGE_TAG:-}}"
[ -z "${IMAGE_TAG}" ] && { echo "Usage: $0 IMAGE_TAG"; exit 1; }

## Measured on dev-63 at f6ac260d00 with 20,000 idle MQTT v5 clients using
## the default socket backend: 298-301 state words and 3,896 bytes after
## hibernation. Re-measure with the memory_probe expression below.
MAX_STATE_WORDS=360
MAX_HIBERNATED_MEMORY_BYTES=4600
CLIENT_COUNT=200
MIN_HIBERNATED_COUNT=$((CLIENT_COUNT * 9 / 10))

CONTAINER="emqx-conn-memory-smoke"
CLIENT_CONTAINER="emqtt-conn-memory-smoke"
EMQTT_BENCH_IMAGE="${EMQTT_BENCH_IMAGE:-emqx/emqtt-bench:0.6.2}"
START_SECONDS=${SECONDS}

cleanup() {
  docker rm -f "${CLIENT_CONTAINER}" >/dev/null 2>&1 || true
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
}

capture_logs() {
  docker logs "${CONTAINER}" 2>&1 || true
  docker exec "${CONTAINER}" sh -c '
    for logfile in /opt/emqx/log/emqx.log.*; do
      [ -f "${logfile}" ] && cat "${logfile}"
    done
  ' 2>/dev/null || true
}

dump_logs() {
  echo "--- ${CONTAINER} logs ---"
  capture_logs
  echo "--- ${CLIENT_CONTAINER} logs ---"
  docker logs "${CLIENT_CONTAINER}" 2>&1 || true
}

on_exit() {
  result=$?
  trap - EXIT
  if [ "${result}" -ne 0 ]; then
    dump_logs
  fi
  cleanup
  exit "${result}"
}
trap on_exit EXIT
cleanup

eval_emqx() {
  docker exec "${CONTAINER}" emqx eval "$1" 2>/dev/null | tr -d '[:space:]'
}

count_severe_logs() {
  grep -Ec '^\S+ \[(warning|error|critical|alert|emergency)\]' <<<"$1" || true
}

count_hook_callback_exceptions() {
  grep -c 'hook_callback_exception' <<<"$1" || true
}

echo "Starting ${CONTAINER} from ${IMAGE_TAG} ..."
docker run -d --name "${CONTAINER}" \
  -e EMQX_NODE__NAME="emqx@127.0.0.1" \
  -e EMQX_LOG__CONSOLE__LEVEL=info \
  -e EMQX_MQTT__IDLE_TIMEOUT=2s \
  "${IMAGE_TAG}" >/dev/null

echo "Waiting for the TCP listener ..."
ready=false
for _ in $(seq 1 30); do
  if docker exec "${CONTAINER}" emqx ctl listeners 2>/dev/null \
       | grep -A6 'tcp:default' | grep -qE 'running *: true'; then
    ready=true
    break
  fi
  sleep 1
done
if [ "${ready}" != true ]; then
  echo "EMQX did not become ready within 30 seconds"
  exit 1
fi

before_logs="$(capture_logs)"
before_severe="$(count_severe_logs "${before_logs}")"
before_hook_exceptions="$(count_hook_callback_exceptions "${before_logs}")"

echo "Connecting ${CLIENT_COUNT} MQTT v5 clients ..."
docker run --rm -d --name "${CLIENT_CONTAINER}" \
  --network "container:${CONTAINER}" \
  "${EMQTT_BENCH_IMAGE}" \
  conn -h 127.0.0.1 -p 1883 -V 5 -c "${CLIENT_COUNT}" -R "${CLIENT_COUNT}" -k 300 \
  --prefix conn-memory-smoke --shortids true --log_to null >/dev/null

connected=0
for _ in $(seq 1 10); do
  connected="$(eval_emqx 'emqx_cm:get_connected_client_count().')"
  if [ "${connected}" = "${CLIENT_COUNT}" ]; then
    break
  fi
  sleep 1
done
if [ "${connected}" != "${CLIENT_COUNT}" ]; then
  echo "Expected ${CLIENT_COUNT} connected clients, got '${connected}'"
  exit 1
fi

## Let async logger writes from the last CONNECT reach both handlers.
sleep 1
after_logs="$(capture_logs)"
after_severe="$(count_severe_logs "${after_logs}")"
after_hook_exceptions="$(count_hook_callback_exceptions "${after_logs}")"

echo "connect log counts: warning-or-above ${before_severe}->${after_severe}, hook_callback_exception ${before_hook_exceptions}->${after_hook_exceptions}"
if [ "${after_severe}" != "${before_severe}" ]; then
  echo "Connects emitted warning-or-above log lines"
  exit 1
fi
if [ "${after_hook_exceptions}" -ne 0 ]; then
  echo "Connects emitted hook_callback_exception"
  exit 1
fi

hibernated=0
for _ in $(seq 1 10); do
  hibernated="$(eval_emqx '
    length([
      P
     || P <- emqx_cm:all_channels(),
        process_info(P, current_function) =:= {current_function, {erlang, hibernate, 3}}
    ]).')"
  if [ "${hibernated}" -ge "${MIN_HIBERNATED_COUNT}" ]; then
    break
  fi
  sleep 1
done
if [ "${hibernated}" -lt "${MIN_HIBERNATED_COUNT}" ]; then
  echo "Expected at least ${MIN_HIBERNATED_COUNT} hibernated channels, got '${hibernated}'"
  exit 1
fi

## Keep memory reads ahead of sys:get_state/1 because that system message wakes
## a hibernated channel. The memory budget applies to the maximum, not the average.
memory_probe='Pids = emqx_cm:all_channels(),
Hibernated = [
    P
 || P <- Pids,
    process_info(P, current_function) =:= {current_function, {erlang, hibernate, 3}}
],
Snapshots = [
    {P, proplists:get_value(memory, process_info(P, [memory]))}
 || P <- Hibernated
],
MemoryBytes = [M || {_, M} <- Snapshots],
StateWords = erts_debug:flat_size(sys:get_state(hd(Hibernated))),
{length(Pids), length(Hibernated), StateWords,
 lists:min(MemoryBytes), lists:max(MemoryBytes)}.'
measurement="$(eval_emqx "${memory_probe}")"

if [[ ! "${measurement}" =~ ^\{([0-9]+),([0-9]+),([0-9]+),([0-9]+),([0-9]+)\}$ ]]; then
  echo "Could not parse memory probe result: '${measurement}'"
  exit 1
fi
measured_connections="${BASH_REMATCH[1]}"
measured_hibernated="${BASH_REMATCH[2]}"
state_words="${BASH_REMATCH[3]}"
min_memory_bytes="${BASH_REMATCH[4]}"
max_memory_bytes="${BASH_REMATCH[5]}"

echo "memory probe: connections=${measured_connections}, hibernated=${measured_hibernated}, state=${state_words} words (budget <=${MAX_STATE_WORDS}), memory=${min_memory_bytes}-${max_memory_bytes} bytes (budget <=${MAX_HIBERNATED_MEMORY_BYTES})"

if [ "${state_words}" -gt "${MAX_STATE_WORDS}" ] || \
   [ "${max_memory_bytes}" -gt "${MAX_HIBERNATED_MEMORY_BYTES}" ]; then
  echo "Per-connection memory budget exceeded; channel breakdown follows:"
  docker exec "${CONTAINER}" emqx eval '
    Pids = emqx_cm:all_channels(),
    Hibernated = [
        P
     || P <- Pids,
        process_info(P, current_function) =:= {current_function, {erlang, hibernate, 3}}
    ],
    WithMemory = [
        {proplists:get_value(memory, process_info(P, [memory])), P}
     || P <- Hibernated
    ],
    {_, P} = lists:last(lists:sort(WithMemory)),
    Info = process_info(P, [memory, heap_size, total_heap_size, stack_size,
                            message_queue_len, current_function, status, dictionary]),
    State = sys:get_state(P),
    Size = fun(Term) -> erts_debug:flat_size(Term) end,
    Deep = fun(Depth, Recur, Term) when is_tuple(Term), Depth > 0 ->
                   [{I, Size(element(I, Term)), Recur(Depth - 1, Recur, element(I, Term))}
                    || I <- lists:seq(1, tuple_size(Term)), Size(element(I, Term)) > 8];
              (Depth, Recur, Term) when is_map(Term), Depth > 0 ->
                   [{K, Size(V), Recur(Depth - 1, Recur, V)}
                    || {K, V} <- maps:to_list(Term), Size(V) > 8];
              (_, _, _) -> []
           end,
    Dictionary = proplists:get_value(dictionary, Info),
    #{process_info => [{K, V} || {K, V} <- Info, K =/= dictionary],
      dictionary_keys_and_words => [{K, Size(V)} || {K, V} <- Dictionary],
      state_words => Size(State),
      state_tuple_size => tuple_size(State),
      state_breakdown => Deep(3, Deep, State)}.' || true
  exit 1
fi

echo "Connection memory smoke test passed in $((SECONDS - START_SECONDS)) seconds"
