%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_lib).

%% connectivity check
-export([
    http_connectivity/2,
    tcp_connectivity/3
]).

-export([resolve_dns/2]).

-spec http_connectivity(uri_string:uri_string(), timeout()) ->
    ok | {error, Reason :: term()}.
http_connectivity(Url, Timeout) ->
    case emqx_http_lib:uri_parse(Url) of
        {ok, #{host := Host, port := Port}} ->
            tcp_connectivity(Host, Port, Timeout);
        {error, Reason} ->
            {error, Reason}
    end.

-spec tcp_connectivity(
    Host :: inet:socket_address() | inet:hostname(),
    Port :: inet:port_number(),
    timeout()
) ->
    ok | {error, Reason :: term()}.
tcp_connectivity(Host, Port, Timeout) ->
    case gen_tcp:connect(Host, Port, emqx_utils:ipv6_probe([]), Timeout) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Mostly for meck.
resolve_dns(DNS, Type) ->
    inet_res:lookup(DNS, in, Type).
