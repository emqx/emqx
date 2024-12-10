%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_peersni_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("esockd/include/esockd.hrl").

-define(SERVER_NAME, <<"localhost">>).

%%--------------------------------------------------------------------
%% setups
%%--------------------------------------------------------------------

all() ->
    [
        {group, tcp_ppv2},
        {group, ws_ppv2},
        {group, ssl},
        {group, wss}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp_ppv2, [], TCs},
        {ws_ppv2, [], TCs},
        {ssl, [], TCs},
        {wss, [], TCs}
    ].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_group(tcp_ppv2, Config) ->
    ClientFn = emqx_cth_listener:reload_listener_with_ppv2(
        [listeners, tcp, default],
        ?SERVER_NAME
    ),
    [{client_fn, ClientFn} | Config];
init_per_group(ws_ppv2, Config) ->
    ClientFn = emqx_cth_listener:reload_listener_with_ppv2(
        [listeners, ws, default],
        ?SERVER_NAME
    ),
    [{client_fn, ClientFn} | Config];
init_per_group(ssl, Config) ->
    ClientFn = fun(ClientId, Opts) ->
        Opts1 = Opts#{
            host => ?SERVER_NAME,
            port => 8883,
            ssl => true,
            ssl_opts => [
                {verify, verify_none},
                {server_name_indication, binary_to_list(?SERVER_NAME)}
            ]
        },
        {ok, C} = emqtt:start_link(Opts1#{clientid => ClientId}),
        case emqtt:connect(C) of
            {ok, _} -> {ok, C};
            {error, _} = Err -> Err
        end
    end,
    [{client_fn, ClientFn} | Config];
init_per_group(wss, Config) ->
    ClientFn = fun(ClientId, Opts) ->
        Opts1 = Opts#{
            host => ?SERVER_NAME,
            port => 8084,
            ws_transport_options => [
                {transport, tls},
                {protocols, [http]},
                {transport_opts, [
                    {verify, verify_none},
                    {server_name_indication, binary_to_list(?SERVER_NAME)},
                    {customize_hostname_check, []}
                ]}
            ]
        },
        {ok, C} = emqtt:start_link(Opts1#{clientid => ClientId}),
        case emqtt:ws_connect(C) of
            {ok, _} -> {ok, C};
            {error, _} = Err -> Err
        end
    end,
    [{client_fn, ClientFn} | Config];
init_per_group(_, Config) ->
    Config.

end_per_group(tcp_ppv2, _Config) ->
    emqx_cth_listener:reload_listener_without_ppv2([listeners, tcp, default]);
end_per_group(ws_ppv2, _Config) ->
    emqx_cth_listener:reload_listener_without_ppv2([listeners, ws, default]);
end_per_group(_, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config);
        _ -> Config
    end.

end_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

%%--------------------------------------------------------------------
%% cases
%%--------------------------------------------------------------------

t_peersni_saved_into_conninfo(Config) ->
    process_flag(trap_exit, true),

    ClientId = <<"test-clientid1">>,
    ClientFn = proplists:get_value(client_fn, Config),

    {ok, Client} = ClientFn(ClientId, _Opts = #{}),
    ?assertMatch(#{clientinfo := #{peersni := ?SERVER_NAME}}, get_chan_info(ClientId)),

    ok = emqtt:disconnect(Client).

t_parse_peersni_to_client_attr(Config) ->
    process_flag(trap_exit, true),

    %% set the peersni to the client attribute
    {ok, Variform} = emqx_variform:compile("nth(1, tokens(peersni, 'h'))"),
    emqx_config:put([mqtt, client_attrs_init], [
        #{expression => Variform, set_as_attr => tns}
    ]),

    ClientId = <<"test-clientid2">>,
    ClientFn = proplists:get_value(client_fn, Config),
    {ok, Client} = ClientFn(ClientId, _Opts = #{}),

    ?assertMatch(
        #{clientinfo := #{client_attrs := #{tns := <<"local">>}}}, get_chan_info(ClientId)
    ),

    ok = emqtt:disconnect(Client).

get_chan_info(ClientId) ->
    ?retry(
        3_000,
        100,
        #{} = emqx_cm:get_chan_info(ClientId)
    ).
