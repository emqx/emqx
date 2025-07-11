%% -*- mode:erlang -*-
{config, ["sessds.cfg"]}.

{suites, "../../emqx_durable_timer/test", emqx_durable_timer_SUITE}.

%% {cases, ".", emqx_persistent_session_ds_SUITE, [t_state_fuzz]}.

%% {suites, ".", [emqx_persistent_messages_SUITE, emqx_persistent_session_ds_SUITE, emqx_persistent_session_ds_router_SUITE]}.

%% {groups, ".", emqx_persistent_session_SUITE, [tcp]}.
