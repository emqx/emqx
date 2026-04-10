%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_proxy_conn_tests).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_coap.hrl").

get_connection_id_reuses_bound_clientid_without_query_clientid_test() ->
    Peer = {{127, 0, 0, 1}, 5683},
    State0 = emqx_coap_proxy_conn:initialize(#{}),
    Data1 = build_request_data(#{
        <<"clientid">> => <<"client1">>,
        <<"token">> => <<"token1">>
    }),
    {ok, <<"client1">>, _Packets1, State1} =
        emqx_coap_proxy_conn:get_connection_id(udp, Peer, State0, Data1),

    Data2 = build_request_data(#{
        <<"token">> => <<"token1">>
    }),
    {ok, <<"client1">>, _Packets2, _State2} =
        emqx_coap_proxy_conn:get_connection_id(udp, Peer, State1, Data2).

get_connection_id_falls_back_to_peerid_when_clientid_missing_test() ->
    Peer = {{127, 0, 0, 1}, 5683},
    State0 = emqx_coap_proxy_conn:initialize(#{}),
    Data = build_request_data(#{
        <<"token">> => <<"token1">>
    }),
    {ok, {peer, Peer}, _Packets, _State1} =
        emqx_coap_proxy_conn:get_connection_id(udp, Peer, State0, Data).

build_request_data(UriQuery) ->
    Msg0 = emqx_coap_message:request(
        con,
        post,
        <<>>,
        #{
            uri_path => [<<"ps">>, <<"coap">>, <<"test">>],
            uri_query => UriQuery
        }
    ),
    Msg = Msg0#coap_message{id = 1},
    emqx_coap_frame:serialize_pkt(Msg, emqx_coap_frame:serialize_opts()).
