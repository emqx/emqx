Prometheus VM and Mnesia collector metric names now use the `prometheus.erl` 6.x promtool-compliant names.

Affected metric renames:

- `erlang_mnesia_failed_transactions` -> `erlang_mnesia_failed_transactions_total`
- `erlang_mnesia_committed_transactions` -> `erlang_mnesia_committed_transactions_total`
- `erlang_mnesia_logged_transactions` -> `erlang_mnesia_logged_transactions_total`
- `erlang_mnesia_restarted_transactions` -> `erlang_mnesia_restarted_transactions_total`
- `erlang_vm_memory_atom_bytes_total` -> `erlang_vm_memory_atom_bytes`
- `erlang_vm_memory_bytes_total` -> `erlang_vm_memory_bytes`
- `erlang_vm_memory_processes_bytes_total` -> `erlang_vm_memory_processes_bytes`
- `erlang_vm_memory_system_bytes_total` -> `erlang_vm_memory_system_bytes`
- `erlang_vm_statistics_context_switches` -> `erlang_vm_statistics_context_switches_total`
- `erlang_vm_statistics_garbage_collection_number_of_gcs` -> `erlang_vm_statistics_garbage_collection_number_of_gcs_total`
- `erlang_vm_statistics_garbage_collection_words_reclaimed` -> `erlang_vm_statistics_garbage_collection_words_reclaimed_total`
- `erlang_vm_statistics_garbage_collection_bytes_reclaimed` -> `erlang_vm_statistics_garbage_collection_bytes_reclaimed_total`
- `erlang_vm_statistics_runtime_milliseconds` -> `erlang_vm_statistics_runtime_seconds_total`
- `erlang_vm_statistics_wallclock_time_milliseconds` -> `erlang_vm_statistics_wallclock_time_seconds_total`
- `erlang_vm_port_count` -> `erlang_vm_ports`
- `erlang_vm_process_count` -> `erlang_vm_processes`
- `erlang_vm_atom_count` -> `erlang_vm_atoms`
