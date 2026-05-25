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
    ok = put_plugin_config(name_vsn(), #{}),
    ok = emqx_agent_config:init_config(),
    ok.

teardown() ->
    _ = put_plugin_config(name_vsn(), #{}),
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
            case decode_config(Config) of
                {ok, Decoded} ->
                    OldConfig = get_config(NameVsn, #{}),
                    persistent_term:put(?CONFIG_KEY, Decoded),
                    _ = emqx_agent_app:on_config_changed(OldConfig, Decoded),
                    ok;
                {error, _} = Error ->
                    Error
            end;
        false ->
            meck:passthrough([NameVsn, Config])
    end.

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_agent, vsn),
    iolist_to_binary([<<"emqx_agent-">>, Vsn]).

decode_config(Config) ->
    try
        PrivDir = code:priv_dir(emqx_agent),
        {ok, AvscBin} = file:read_file(filename:join(PrivDir, "config_schema.avsc")),
        Store0 = avro_schema_store:new([map]),
        Store = avro_schema_store:import_schema_json(name_vsn(), AvscBin, Store0),
        Opts = avro:make_decoder_options([
            {map_type, map},
            {record_type, map},
            {encoding, avro_json}
        ]),
        Decoded = avro_json_decoder:decode_value(
            emqx_utils_json:encode(Config), name_vsn(), Store, Opts
        ),
        {ok, Decoded}
    catch
        Class:Reason ->
            {error, {Class, Reason}}
    end.
