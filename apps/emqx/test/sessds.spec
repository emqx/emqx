%% -*- mode:erlang -*-
{config, ["sessds.cfg"]}.

{suites, "../../emqx_durable_timer/test", emqx_durable_timer_SUITE}.
{suites, ".", [ emqx_persistent_messages_SUITE,
                emqx_persistent_session_ds_SUITE,
                emqx_persistent_session_ds_router_SUITE
              ]}.
{groups, ".", emqx_persistent_session_SUITE, [[persistence_enabled, tcp]]}.
{groups, ".", emqx_takeover_SUITE, [persistence_enabled]}.
{groups, "../../emqx_management/test", emqx_mgmt_api_publish_SUITE, [persistence_enabled]}.
{groups, "../../emqx_management/test", emqx_mgmt_api_clients_SUITE, [persistence_enabled]}.
{groups, "../../emqx_management/test", emqx_mgmt_api_subscription_SUITE, [persistence_enabled]}.
{suites, "../../emqx_ds_shared_sub/test", all}.
