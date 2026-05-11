%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_plugin_config_fixture).

-export([
    setup/0,
    teardown/0,
    reset/0,
    get_config/2,
    put_plugin_config/2,
    name_vsn/0
]).

-define(CONFIG_KEY, {?MODULE, config}).

setup() ->
    reset(),
    ok = meck:new(emqx_plugins, [passthrough, no_history]),
    ok = meck:new(emqx_mgmt_api_plugins, [passthrough, no_history]),
    ok = meck:expect(emqx_plugins, get_config, fun ?MODULE:get_config/2),
    ok = meck:expect(emqx_mgmt_api_plugins, put_plugin_config, fun ?MODULE:put_plugin_config/2),
    ok = emqx_agent_config:init_config(),
    ok.

teardown() ->
    reset(),
    _ = emqx_agent_config:init_config(),
    _ = catch meck:unload(emqx_mgmt_api_plugins),
    _ = catch meck:unload(emqx_plugins),
    ok.

reset() ->
    _ = persistent_term:erase(?CONFIG_KEY),
    ok.

get_config(NameVsn, Default) ->
    case NameVsn =:= name_vsn() of
        true -> persistent_term:get(?CONFIG_KEY, Default);
        false -> meck:passthrough([NameVsn, Default])
    end.

put_plugin_config(NameVsn, Config) ->
    case NameVsn =:= name_vsn() of
        true ->
            OldConfig = get_config(NameVsn, #{}),
            persistent_term:put(?CONFIG_KEY, Config),
            _ = emqx_agent_app:on_config_changed(OldConfig, Config),
            ok;
        false ->
            meck:passthrough([NameVsn, Config])
    end.

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_agent, vsn),
    iolist_to_binary([<<"emqx_agent-">>, Vsn]).
