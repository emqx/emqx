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
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(ATTRS, #{clean_start         => true,
                 client_id           => <<"client">>,
                 zone                => internal,
                 username            => <<"emqx">>,
                 expiry_interval     => 0,
                 max_inflight        => 0,
                 topic_alias_maximum => 0,
                 will_msg            => undefined}).

all() -> [{group, sm}].

groups() ->
    [{sm, [non_parallel_tests],
      [t_open_close_session,
       t_resume_session,
       t_discard_session,
       t_register_unregister_session,
       t_get_set_session_attrs,
       t_get_set_session_stats,
       t_lookup_session_pids]}].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

t_open_close_session(_) ->
    {ok, ClientPid} = emqx_mock_client:start_link(<<"client">>),
    {ok, SPid} = emqx_sm:open_session(?ATTRS#{conn_pid => ClientPid}),
    ?assertEqual(ok, emqx_sm:close_session(SPid)).

t_resume_session(_) ->
    {ok, ClientPid} = emqx_mock_client:start_link(<<"client">>),
    {ok, SPid} = emqx_sm:open_session(?ATTRS#{conn_pid => ClientPid}),
    ?assertEqual({ok, SPid}, emqx_sm:resume_session(<<"client">>, ?ATTRS#{conn_pid => ClientPid})).

t_discard_session(_) ->
    {ok, ClientPid} = emqx_mock_client:start_link(<<"client1">>),
    {ok, _SPid} = emqx_sm:open_session(?ATTRS#{conn_pid => ClientPid}),
    ?assertEqual(ok, emqx_sm:discard_session(<<"client1">>)).

t_register_unregister_session(_) ->
    Pid = self(),
    {ok, _ClientPid} = emqx_mock_client:start_link(<<"client">>),
    ?assertEqual(ok, emqx_sm:register_session(<<"client">>)),
    ?assertEqual(ok, emqx_sm:register_session(<<"client">>, Pid)),
    ?assertEqual(ok, emqx_sm:unregister_session(<<"client">>)),
    ?assertEqual(ok, emqx_sm:unregister_session(<<"client">>), Pid).

t_get_set_session_attrs(_) ->
    {ok, ClientPid} = emqx_mock_client:start_link(<<"client">>),
    {ok, SPid} = emqx_sm:open_session(?ATTRS#{conn_pid => ClientPid}),
    ?assertEqual(true, emqx_sm:set_session_attrs(<<"client">>, [?ATTRS#{conn_pid => ClientPid}])),
    ?assertEqual(true, emqx_sm:set_session_attrs(<<"client">>, SPid, [?ATTRS#{conn_pid => ClientPid}])),
    [SAttr] = emqx_sm:get_session_attrs(<<"client">>, SPid),
    ?assertEqual(<<"client">>, maps:get(client_id, SAttr)).

t_get_set_session_stats(_) ->
    {ok, ClientPid} = emqx_mock_client:start_link(<<"client">>),
    {ok, SPid} = emqx_sm:open_session(?ATTRS#{conn_pid => ClientPid}),
    ?assertEqual(true, emqx_sm:set_session_stats(<<"client">>, [{inflight, 10}])),
    ?assertEqual(true, emqx_sm:set_session_stats(<<"client">>, SPid, [{inflight, 10}])),
    ?assertEqual([{inflight, 10}], emqx_sm:get_session_stats(<<"client">>, SPid)).

t_lookup_session_pids(_) ->
    {ok, ClientPid} = emqx_mock_client:start_link(<<"client">>),
    {ok, SPid} = emqx_sm:open_session(?ATTRS#{conn_pid => ClientPid}),
    ?assertEqual([SPid], emqx_sm:lookup_session_pids(<<"client">>)).
