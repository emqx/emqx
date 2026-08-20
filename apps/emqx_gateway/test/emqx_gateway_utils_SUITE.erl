%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_utils_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_gateway_test_utils:load_all_gateway_apps(),
    Config.

end_per_suite(Config) ->
    Config.

t_global_chain(_Config) ->
    Names = emqx_gateway_schema:gateway_names(),
    lists:foreach(
        fun(Name) ->
            %% no exception is expected
            _ = emqx_gateway_utils:global_chain(Name)
        end,
        Names
    ),
    ?assertError({invalid_protocol_name, 'Others'}, emqx_gateway_utils:global_chain('Others')).

-doc """
Gateway WebSocket options default to empty `proxy_address_header` and
`proxy_port_header`, so forwarded headers are not consulted by default.
""".
t_ws_opts_proxy_header_defaults(_Config) ->
    Sc = #{
        roots => [websocket],
        fields => #{websocket => emqx_gateway_schema:ws_opts(#{})}
    },
    ?assertMatch(
        #{
            <<"websocket">> := #{
                <<"proxy_address_header">> := "",
                <<"proxy_port_header">> := ""
            }
        },
        hocon_tconf:check_plain(Sc, #{<<"websocket">> => #{}}, #{required => false})
    ).
