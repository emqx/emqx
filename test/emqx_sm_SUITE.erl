%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sm_SUITE).

-include("emqx.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> [t_open_close_session].

t_open_close_session(_) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    {ok, ClientPid} = emqx_mock_client:start_link(<<"client">>),
    Attrs = #{clean_start         => true,
              client_id           => <<"client">>,
              conn_pid            => ClientPid,
              zone                => internal,
              username            => <<"emqx">>,
              expiry_interval     => 0,
              max_inflight        => 0,
              topic_alias_maximum => 0,
              will_msg            => undefined},
    {ok, SPid} = emqx_sm:open_session(Attrs),
    ?assertEqual([SPid], emqx_sm:lookup_session_pids(<<"client">>)),
    {ok, NewConnPid} = emqx_mock_client:start_link(<<"client">>),
    {ok, SPid, true} = emqx_sm:open_session(Attrs#{clean_start => false, conn_pid => NewConnPid}),
    ?assertEqual([SPid], emqx_sm:lookup_session_pids(<<"client">>)),
    SAttrs = emqx_sm:get_session_attrs(<<"client">>, SPid),
    ?assertEqual(<<"client">>, proplists:get_value(client_id, SAttrs)),
    emqx_sm:set_session_stats(<<"client">>, SPid, [{inflight, 10}]),
    ?assertEqual([{inflight, 10}], emqx_sm:get_session_stats(<<"client">>, SPid)),
    ok = emqx_sm:close_session(SPid),
    ?assertEqual([], emqx_sm:lookup_session_pids(<<"client">>)),
    emqx_ct_broker_helpers:run_teardown_steps().
