%% -*- mode:erlang -*-
%% Collection of tests related to optimistic transactions in builtin backends
{groups, "../../emqx_ds_backends/test", emqx_ds_backends_SUITE, [emqx_ds_builtin_local, emqx_ds_builtin_raft],
 {cases,
  [ t_13_smoke_ttv_tx
  , t_14_ttv_wildcard_deletes
  , t_15_ttv_write_serial
  , t_16_ttv_preconditions
  , t_17_tx_wrapper
  , t_18_async_trans
  , t_20_tx_monotonic_ts
  , t_24_tx_side_effects
  , t_27_tx_read_conflicts
  , t_28_ttv_time_limited
  ]}}.
