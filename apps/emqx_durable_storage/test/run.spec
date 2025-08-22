%% -*- mode:erlang -*-
%%
%% Run all testcases related to the durable storage:
{suites, ".", all}.
{suites, "../../emqx_ds_backends/test", all}.
{suites, "../../emqx_ds_builtin_raft/test", all}.
{suites, "../../emqx_ds_builtin_local/test", all}.
