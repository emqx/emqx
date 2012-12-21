%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(tcp_listener_sup).

-behaviour(supervisor).

-export([start_link/7, start_link/8]).

-export([init/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfargs() :: {atom(), atom(), [any()]}).

-spec(start_link/7 ::
        (inet:ip_address(), inet:port_number(), [gen_tcp:listen_option()],
         mfargs(), mfargs(), mfargs(), string()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(start_link/8 ::
        (inet:ip_address(), inet:port_number(), [gen_tcp:listen_option()],
         mfargs(), mfargs(), mfargs(), integer(), string()) ->
                           rabbit_types:ok_pid_or_error()).

-endif.

%%----------------------------------------------------------------------------

start_link(IPAddress, Port, SocketOpts, OnStartup, OnShutdown,
           AcceptCallback, Label) ->
    start_link(IPAddress, Port, SocketOpts, OnStartup, OnShutdown,
               AcceptCallback, 1, Label).

start_link(IPAddress, Port, SocketOpts, OnStartup, OnShutdown,
           AcceptCallback, ConcurrentAcceptorCount, Label) ->
    supervisor:start_link(
      ?MODULE, {IPAddress, Port, SocketOpts, OnStartup, OnShutdown,
                AcceptCallback, ConcurrentAcceptorCount, Label}).

init({IPAddress, Port, SocketOpts, OnStartup, OnShutdown,
      AcceptCallback, ConcurrentAcceptorCount, Label}) ->
    %% This is gross. The tcp_listener needs to know about the
    %% tcp_acceptor_sup, and the only way I can think of accomplishing
    %% that without jumping through hoops is to register the
    %% tcp_acceptor_sup.
    Name = tcp_name(tcp_acceptor_sup, IPAddress, Port),
    {ok, {{one_for_all, 10, 10},
          [{tcp_acceptor_sup, {tcp_acceptor_sup, start_link,
                               [Name, AcceptCallback]},
            transient, infinity, supervisor, [tcp_acceptor_sup]},
           {tcp_listener, {tcp_listener, start_link,
                           [IPAddress, Port, SocketOpts,
                            ConcurrentAcceptorCount, Name,
                            OnStartup, OnShutdown, Label]},
            transient, 16#ffffffff, worker, [tcp_listener]}]}}.

tcp_name(Prefix, IPAddress, Port)
  when is_atom(Prefix) andalso is_number(Port) ->
    list_to_atom(
		lists:flatten(
			io_lib:format(
				"~w_~s:~w", [Prefix, inet_parse:ntoa(IPAddress), Port]))).


