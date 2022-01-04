%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(CONF_DEFAULT, <<"
exhook {
  servers = [
    { name = default,
      url = \"http://127.0.0.1:9000\"
    }]
}
">>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Cfg) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    meck:new(emqx_alarm, [non_strict, passthrough, no_link]),
    meck:expect(emqx_alarm, activate, 3, ok),
    meck:expect(emqx_alarm, deactivate, 3, ok),

    _ = emqx_exhook_demo_svr:start(),
    ok = emqx_config:init_load(emqx_exhook_schema, ?CONF_DEFAULT),
    emqx_common_test_helpers:start_apps([emqx_exhook]),
    Cfg.

end_per_suite(_Cfg) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_alarm),

    emqx_common_test_helpers:stop_apps([emqx_exhook]),
    emqx_exhook_demo_svr:stop().

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    timer:sleep(200),
    Config.

end_per_testcase(_, Config) ->
    case erlang:whereis(node()) of
        undefined -> ok;
        P ->
            erlang:unlink(P),
            erlang:exit(P, kill)
    end,
    Config.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_noserver_nohook(_) ->
    emqx_exhook_mgr:disable(<<"default">>),
    ?assertEqual([], ets:tab2list(emqx_hooks)),
    {ok, _} = emqx_exhook_mgr:enable(<<"default">>),
    ?assertNotEqual([], ets:tab2list(emqx_hooks)).

t_access_failed_if_no_server_running(_) ->
    emqx_exhook_mgr:disable(<<"default">>),
    ClientInfo = #{clientid => <<"user-id-1">>,
                   username => <<"usera">>,
                   peerhost => {127,0,0,1},
                   sockport => 1883,
                   protocol => mqtt,
                   mountpoint => undefined
                  },
    ?assertMatch({stop, {error, not_authorized}},
                 emqx_exhook_handler:on_client_authenticate(ClientInfo, #{auth_result => success})),

    ?assertMatch({stop, deny},
                 emqx_exhook_handler:on_client_authorize(ClientInfo, publish, <<"t/1">>, allow)),

    Message = emqx_message:make(<<"t/1">>, <<"abc">>),
    ?assertMatch({stop, Message},
                 emqx_exhook_handler:on_message_publish(Message)),
    emqx_exhook_mgr:enable(<<"default">>).

%%--------------------------------------------------------------------
%% Utils
%%--------------------------------------------------------------------

meck_print() ->
    meck:new(emqx_ctl, [passthrough, no_history, no_link]),
    meck:expect(emqx_ctl, print, fun(_) -> ok end),
    meck:expect(emqx_ctl, print, fun(_, Args) -> Args end).

unmeck_print() ->
    meck:unload(emqx_ctl).

loaded_exhook_hookpoints() ->
    lists:filtermap(fun(E) ->
                            Name = element(2, E),
                            Callbacks = element(3, E),
                            case lists:any(fun is_exhook_callback/1, Callbacks) of
                                true -> {true, Name};
                                _ -> false
                            end
                    end, ets:tab2list(emqx_hooks)).

is_exhook_callback(Cb) ->
    Action = element(2, Cb),
    emqx_exhook_handler == element(1, Action).
