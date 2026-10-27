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

%% @doc https://www.erlang-solutions.com/blog/erlang-and-elixir-distribution-without-epmd.html

-module(ekka_epmd).

-include_lib("mria/include/mria.hrl").

%% epmd_module callbacks
-export([start_link/0]).
-export([port_please/2, port_please/3]).
-export([names/0, names/1]).
-export([register_node/2, register_node/3]).
%% -export([address_please/3]).

%% The supervisor module erl_distribution tries to add us as a child
%% process.  We don't need a child process, so return 'ignore'.
start_link() ->
    ignore.

register_node(_Name, _Port) ->
    %% This is where we would connect to epmd and tell it which port
    %% we're listening on, but since we're epmd-less, we don't do that.

    %% Need to return a "creation" number between 1 and 3.
    Creation = rand:uniform(3),
    {ok, Creation}.

%% As of Erlang/OTP 19.1, register_node/3 is used instead of
%% register_node/2, passing along the address family, 'inet_tcp' or
%% 'inet6_tcp'.  This makes no difference for our purposes.
register_node(Name, Port, _Family) ->
    register_node(Name, Port).

port_please(Name, _IP) ->
    Port = ekka_dist:port(Name),
    %% The distribution protocol version number has been 5 ever since
    %% Erlang/OTP R6.
    Version = 5,
    {port, Port, Version}.

port_please(Name, IP, _Timeout) ->
    port_please(Name, IP).

names() ->
    {ok, H} = inet:gethostname(),
    names(H).

names(_Hostname) ->
    %% Since we don't have epmd, we don't really know what other nodes
    %% there are.
    {error, address}.
