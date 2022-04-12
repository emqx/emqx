# emqx-prometheus

EMQX Prometheus Agent

## push emqx stats/metrics to prometheus PushGateway

```
prometheus.push.gateway.server = http://127.0.0.1:9091

prometheus.interval = 15000
```

## pull emqx stats/metrics

```
Method: GET
Path: api/v4/emqx_prometheus?type=prometheus
params: type: [prometheus| json]

prometheus data

# TYPE erlang_vm_ets_limit gauge
erlang_vm_ets_limit 256000
# TYPE erlang_vm_logical_processors gauge
erlang_vm_logical_processors 4
# TYPE erlang_vm_logical_processors_available gauge
erlang_vm_logical_processors_available NaN
# TYPE erlang_vm_logical_processors_online gauge
erlang_vm_logical_processors_online 4
# TYPE erlang_vm_port_count gauge
erlang_vm_port_count 17
# TYPE erlang_vm_port_limit gauge
erlang_vm_port_limit 1048576


json data

{
  "stats": {key:value},
  "metrics": {key:value},
  "packets": {key:value},
  "messages": {key:value},
  "delivery": {key:value},
  "client": {key:value},
  "session": {key:value}
}

```


## Before EMQX v4.0.0
The prometheus data simple is:


```bash
# TYPE erlang_vm_ets_limit gauge
erlang_vm_ets_limit 2097152
# TYPE erlang_vm_logical_processors gauge
erlang_vm_logical_processors 2
# TYPE erlang_vm_logical_processors_available gauge
erlang_vm_logical_processors_available 2
# TYPE erlang_vm_logical_processors_online gauge
erlang_vm_logical_processors_online 2
# TYPE erlang_vm_port_count gauge
erlang_vm_port_count 19
# TYPE erlang_vm_port_limit gauge
erlang_vm_port_limit 1048576
# TYPE erlang_vm_process_count gauge
erlang_vm_process_count 460
# TYPE erlang_vm_process_limit gauge
erlang_vm_process_limit 2097152
# TYPE erlang_vm_schedulers gauge
erlang_vm_schedulers 2
# TYPE erlang_vm_schedulers_online gauge
erlang_vm_schedulers_online 2
# TYPE erlang_vm_smp_support untyped
erlang_vm_smp_support 1
# TYPE erlang_vm_threads untyped
erlang_vm_threads 1
# TYPE erlang_vm_thread_pool_size gauge
erlang_vm_thread_pool_size 32
# TYPE erlang_vm_time_correction untyped
erlang_vm_time_correction 1
# TYPE erlang_vm_statistics_context_switches counter
erlang_vm_statistics_context_switches 39850
# TYPE erlang_vm_statistics_garbage_collection_number_of_gcs counter
erlang_vm_statistics_garbage_collection_number_of_gcs 17116
# TYPE erlang_vm_statistics_garbage_collection_words_reclaimed counter
erlang_vm_statistics_garbage_collection_words_reclaimed 55711819
# TYPE erlang_vm_statistics_garbage_collection_bytes_reclaimed counter
erlang_vm_statistics_garbage_collection_bytes_reclaimed 445694552
# TYPE erlang_vm_statistics_bytes_received_total counter
erlang_vm_statistics_bytes_received_total 400746
# TYPE erlang_vm_statistics_bytes_output_total counter
erlang_vm_statistics_bytes_output_total 337197
# TYPE erlang_vm_statistics_reductions_total counter
erlang_vm_statistics_reductions_total 21157980
# TYPE erlang_vm_statistics_run_queues_length_total gauge
erlang_vm_statistics_run_queues_length_total 0
# TYPE erlang_vm_statistics_runtime_milliseconds counter
erlang_vm_statistics_runtime_milliseconds 6559
# TYPE erlang_vm_statistics_wallclock_time_milliseconds counter
erlang_vm_statistics_wallclock_time_milliseconds 261243
# TYPE erlang_vm_memory_atom_bytes_total gauge
erlang_vm_memory_atom_bytes_total{usage="used"} 1814822
erlang_vm_memory_atom_bytes_total{usage="free"} 22459
# TYPE erlang_vm_memory_bytes_total gauge
erlang_vm_memory_bytes_total{kind="system"} 109820104
erlang_vm_memory_bytes_total{kind="processes"} 44983656
# TYPE erlang_vm_dets_tables gauge
erlang_vm_dets_tables 1
# TYPE erlang_vm_ets_tables gauge
erlang_vm_ets_tables 139
# TYPE erlang_vm_memory_processes_bytes_total gauge
erlang_vm_memory_processes_bytes_total{usage="used"} 44983656
erlang_vm_memory_processes_bytes_total{usage="free"} 0
# TYPE erlang_vm_memory_system_bytes_total gauge
erlang_vm_memory_system_bytes_total{usage="atom"} 1837281
erlang_vm_memory_system_bytes_total{usage="binary"} 595872
erlang_vm_memory_system_bytes_total{usage="code"} 40790577
erlang_vm_memory_system_bytes_total{usage="ets"} 37426896
erlang_vm_memory_system_bytes_total{usage="other"} 29169478
# TYPE erlang_mnesia_held_locks gauge
erlang_mnesia_held_locks 0
# TYPE erlang_mnesia_lock_queue gauge
erlang_mnesia_lock_queue 0
# TYPE erlang_mnesia_transaction_participants gauge
erlang_mnesia_transaction_participants 0
# TYPE erlang_mnesia_transaction_coordinators gauge
erlang_mnesia_transaction_coordinators 0
# TYPE erlang_mnesia_failed_transactions counter
erlang_mnesia_failed_transactions 2
# TYPE erlang_mnesia_committed_transactions counter
erlang_mnesia_committed_transactions 239
# TYPE erlang_mnesia_logged_transactions counter
erlang_mnesia_logged_transactions 60
# TYPE erlang_mnesia_restarted_transactions counter
erlang_mnesia_restarted_transactions 0
# TYPE emqx_packets_auth_received counter
emqx_packets_auth_received 0
# TYPE emqx_packets_auth_sent counter
emqx_packets_auth_sent 0
# TYPE emqx_packets_received counter
emqx_packets_received 0
# TYPE emqx_packets_sent counter
emqx_packets_sent 0
# TYPE emqx_packets_connect counter
emqx_packets_connect 0
# TYPE emqx_packets_connack_sent counter
emqx_packets_connack_sent 0
# TYPE emqx_packets_connack_error counter
emqx_packets_connack_error 0
# TYPE emqx_packets_connack_auth_error counter
emqx_packets_connack_auth_error 0
# TYPE emqx_packets_disconnect_received counter
emqx_packets_disconnect_received 0
# TYPE emqx_packets_disconnect_sent counter
emqx_packets_disconnect_sent 0
# TYPE emqx_packets_subscribe counter
emqx_packets_subscribe 0
# TYPE emqx_packets_subscribe_error counter
emqx_packets_subscribe_error 0
# TYPE emqx_packets_subscribe_auth_error counter
emqx_packets_subscribe_auth_error 0
# TYPE emqx_packets_suback counter
emqx_packets_suback 0
# TYPE emqx_packets_unsubscribe counter
emqx_packets_unsubscribe 0
# TYPE emqx_packets_unsubscribe_error counter
emqx_packets_unsubscribe_error 0
# TYPE emqx_packets_unsuback counter
emqx_packets_unsuback 0
# TYPE emqx_packets_publish_received counter
emqx_packets_publish_received 0
# TYPE emqx_packets_publish_sent counter
emqx_packets_publish_sent 0
# TYPE emqx_packets_publish_auth_error counter
emqx_packets_publish_auth_error 0
# TYPE emqx_packets_publish_error counter
emqx_packets_publish_error 0
# TYPE emqx_packets_puback_received counter
emqx_packets_puback_received 0
# TYPE emqx_packets_puback_sent counter
emqx_packets_puback_sent 0
# TYPE emqx_packets_puback_missed counter
emqx_packets_puback_missed 0
# TYPE emqx_packets_pubrec_received counter
emqx_packets_pubrec_received 0
# TYPE emqx_packets_pubrec_sent counter
emqx_packets_pubrec_sent 0
# TYPE emqx_packets_pubrec_missed counter
emqx_packets_pubrec_missed 0
# TYPE emqx_packets_pubrel_received counter
emqx_packets_pubrel_received 0
# TYPE emqx_packets_pubrel_sent counter
emqx_packets_pubrel_sent 0
# TYPE emqx_packets_pubrel_missed counter
emqx_packets_pubrel_missed 0
# TYPE emqx_packets_pubcomp_received counter
emqx_packets_pubcomp_received 0
# TYPE emqx_packets_pubcomp_sent counter
emqx_packets_pubcomp_sent 0
# TYPE emqx_packets_pubcomp_missed counter
emqx_packets_pubcomp_missed 0
# TYPE emqx_packets_pingreq counter
emqx_packets_pingreq 0
# TYPE emqx_packets_pingresp counter
emqx_packets_pingresp 0
# TYPE emqx_bytes_received counter
emqx_bytes_received 0
# TYPE emqx_bytes_sent counter
emqx_bytes_sent 0
# TYPE emqx_connections_count gauge
emqx_connections_count 0
# TYPE emqx_connections_max gauge
emqx_connections_max 0
# TYPE emqx_retained_count gauge
emqx_retained_count 3
# TYPE emqx_retained_max gauge
emqx_retained_max 3
# TYPE emqx_sessions_count gauge
emqx_sessions_count 0
# TYPE emqx_sessions_max gauge
emqx_sessions_max 0
# TYPE emqx_subscriptions_count gauge
emqx_subscriptions_count 0
# TYPE emqx_subscriptions_max gauge
emqx_subscriptions_max 0
# TYPE emqx_topics_count gauge
emqx_topics_count 0
# TYPE emqx_topics_max gauge
emqx_topics_max 0
# TYPE emqx_vm_cpu_use gauge
emqx_vm_cpu_use 100.0
# TYPE emqx_vm_cpu_idle gauge
emqx_vm_cpu_idle 0.0
# TYPE emqx_vm_run_queue gauge
emqx_vm_run_queue 1
# TYPE emqx_vm_process_messages_in_queues gauge
emqx_vm_process_messages_in_queues 0
# TYPE emqx_messages_received counter
emqx_messages_received 0
# TYPE emqx_messages_sent counter
emqx_messages_sent 0
# TYPE emqx_messages_dropped counter
emqx_messages_dropped 0
# TYPE emqx_messages_retained counter
emqx_messages_retained 3
# TYPE emqx_messages_qos0_received counter
emqx_messages_qos0_received 0
# TYPE emqx_messages_qos0_sent counter
emqx_messages_qos0_sent 0
# TYPE emqx_messages_qos1_received counter
emqx_messages_qos1_received 0
# TYPE emqx_messages_qos1_sent counter
emqx_messages_qos1_sent 0
# TYPE emqx_messages_qos2_received counter
emqx_messages_qos2_received 0
# TYPE emqx_messages_qos2_expired counter
emqx_messages_qos2_expired 0
# TYPE emqx_messages_qos2_sent counter
emqx_messages_qos2_sent 0
# TYPE emqx_messages_qos2_dropped counter
emqx_messages_qos2_dropped 0
# TYPE emqx_messages_forward counter
emqx_messages_forward 0
```


License
-------

Apache License Version 2.0

Author
------

EMQX Team.

