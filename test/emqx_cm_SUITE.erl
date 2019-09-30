%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_cm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% TODO: Add more test cases
%%--------------------------------------------------------------------

t_reg_unreg_channel(_) ->
    ok = emqx_cm:register_channel(<<"clientid">>),
    ?assertEqual([self()], emqx_cm:lookup_channels(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual([], emqx_cm:lookup_channels(<<"clientid">>)).

t_get_set_chan_attrs(_) ->
    Attrs = #{proto_ver => 4, proto_name => <<"MQTT">>},
    ok = emqx_cm:register_channel(<<"clientid">>),
    ok = emqx_cm:set_chan_attrs(<<"clientid">>, Attrs),
    ?assertEqual(Attrs, emqx_cm:get_chan_attrs(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual(undefined, emqx_cm:get_chan_attrs(<<"clientid">>)).

t_get_set_chan_stats(_) ->
    Stats = [{recv_oct, 10}, {send_oct, 8}],
    ok = emqx_cm:register_channel(<<"clientid">>),
    ok = emqx_cm:set_chan_stats(<<"clientid">>, Stats),
    ?assertEqual(Stats, emqx_cm:get_chan_stats(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual(undefined, emqx_cm:get_chan_stats(<<"clientid">>)).

t_open_session(_) ->
    ClientInfo = #{zone => external,
                   clientid => <<"clientid">>,
                   username => <<"username">>,
                   peerhost => {127,0,0,1}},
    ConnInfo = #{peername => {{127,0,0,1}, 5000},
                 receive_maximum => 100},
    {ok, #{session := Session1, present := false}}
        = emqx_cm:open_session(true, ClientInfo, ConnInfo),
    ?assertEqual(100, emqx_session:info(inflight_max, Session1)),
    {ok, #{session := Session2, present := false}}
        = emqx_cm:open_session(false, ClientInfo, ConnInfo),
    ?assertEqual(100, emqx_session:info(inflight_max, Session2)).

t_discard_session(_) ->
    ok = emqx_cm:discard_session(<<"clientid">>).

t_takeover_session(_) ->
    {error, not_found} = emqx_cm:takeover_session(<<"clientid">>).

t_lock_clientid(_) ->
    {true, _Nodes} = emqx_cm_locker:lock(<<"clientid">>),
    {true, _Nodes} = emqx_cm_locker:lock(<<"clientid">>),
    {true, _Nodes} = emqx_cm_locker:unlock(<<"clientid">>),
    {true, _Nodes} = emqx_cm_locker:unlock(<<"clientid">>).

