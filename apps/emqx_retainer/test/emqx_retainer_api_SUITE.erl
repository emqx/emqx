%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_retainer_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_retainer.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_ct_http, [ request_api/3
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
                      , delete_default_app/0
                      , default_auth_header/0
                      ]).

-define(HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

all() ->
%%    TODO: V5 API
%%    emqx_ct:all(?MODULE).
    [].

groups() ->
    [].

init_per_suite(Config) ->
    application:stop(emqx_retainer),
    emqx_ct_helpers:start_apps([emqx_retainer, emqx_management], fun set_special_configs/1),
    create_default_app(),
    Config.

end_per_suite(_Config) ->
    delete_default_app(),
    emqx_ct_helpers:stop_apps([emqx_retainer]).

init_per_testcase(_, Config) ->
    Config.

set_special_configs(emqx_retainer) ->
    init_emqx_retainer_conf(0);
set_special_configs(emqx_management) ->
    emqx_config:put([emqx_management], #{listeners => [#{protocol => http, port => 8081}],
        applications =>[#{id => "admin", secret => "public"}]}),
    ok;
set_special_configs(_) ->
    ok.

init_emqx_retainer_conf(Expiry) ->
    emqx_config:put([emqx_retainer],
                    #{enable => true,
                      storage_type => ram,
                      max_retained_messages => 0,
                      max_payload_size => 1024 * 1024,
                      expiry_interval => Expiry}).
%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_config(_Config) ->
    {ok, Return} = request_http_rest_lookup(["retainer"]),
    NowCfg = get_http_data(Return),
    NewCfg = NowCfg#{<<"expiry_interval">> => timer:seconds(60)},
    RetainerConf = #{<<"emqx_retainer">> => NewCfg},

    {ok, _} = request_http_rest_update(["retainer?action=test"], RetainerConf),
    {ok, TestReturn} = request_http_rest_lookup(["retainer"]),
    ?assertEqual(NowCfg, get_http_data(TestReturn)),

    {ok, _} = request_http_rest_update(["retainer"], RetainerConf),
    {ok, UpdateReturn} = request_http_rest_lookup(["retainer"]),
    ?assertEqual(NewCfg, get_http_data(UpdateReturn)),
    ok.

t_enable_disable(_Config) ->
    Conf = switch_emqx_retainer(undefined, true),

    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    emqtt:publish(C1, <<"retained">>, <<"this is a retained message">>, [{qos, 0}, {retain, true}]),
    timer:sleep(100),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    _ = switch_emqx_retainer(Conf, false),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),
    emqtt:publish(C1, <<"retained">>, <<"this is a retained message">>, [{qos, 0}, {retain, true}]),
    timer:sleep(100),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),

    ok = emqtt:disconnect(C1).

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------
request_http_rest_lookup(Path) ->
    request_api(get, uri([Path]), default_auth_header()).

request_http_rest_update(Path, Params) ->
    request_api(put, uri([Path]), [], default_auth_header(), Params).

uri(Parts) when is_list(Parts) ->
    NParts = [b2l(E) || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION | NParts]).

%% @private
b2l(B) when is_binary(B) ->
    binary_to_list(B);
b2l(L) when is_list(L) ->
    L.

receive_messages(Count) ->
    receive_messages(Count, []).
receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            ct:log("Msg: ~p ~n", [Msg]),
            receive_messages(Count-1, [Msg|Msgs]);
        Other ->
            ct:log("Other Msg: ~p~n",[Other]),
            receive_messages(Count, Msgs)
    after 2000 ->
            Msgs
    end.

switch_emqx_retainer(undefined, IsEnable) ->
    {ok, Return} = request_http_rest_lookup(["retainer"]),
    NowCfg = get_http_data(Return),
    switch_emqx_retainer(NowCfg, IsEnable);

switch_emqx_retainer(NowCfg, IsEnable) ->
    NewCfg = NowCfg#{<<"enable">> => IsEnable},
    RetainerConf = #{<<"emqx_retainer">> => NewCfg},
    {ok, _} = request_http_rest_update(["retainer"], RetainerConf),
    NewCfg.
