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

-module(tcp_listener).

-include("emqtt.hrl").

-behaviour(gen_server).

-export([start_link/8]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {sock, on_startup, on_shutdown, label}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(mfargs() :: {atom(), atom(), [any()]}).

-spec(start_link/8 ::
        (inet:ip_address(), inet:port_number(), [gen_tcp:listen_option()],
         integer(), atom(), mfargs(), mfargs(), string()) ->
                           rabbit_types:ok_pid_or_error()).

-endif.

%%--------------------------------------------------------------------

start_link(IPAddress, Port, SocketOpts,
           ConcurrentAcceptorCount, AcceptorSup,
           OnStartup, OnShutdown, Label) ->
    gen_server:start_link(
      ?MODULE, {IPAddress, Port, SocketOpts,
                ConcurrentAcceptorCount, AcceptorSup,
                OnStartup, OnShutdown, Label}, []).

%%--------------------------------------------------------------------

init({IPAddress, Port, SocketOpts,
      ConcurrentAcceptorCount, AcceptorSup,
      {M,F,A} = OnStartup, OnShutdown, Label}) ->
    process_flag(trap_exit, true),
    case gen_tcp:listen(Port, SocketOpts ++ [{ip, IPAddress},
                                             {active, false}]) of
        {ok, LSock} ->
            lists:foreach(fun (_) ->
                                  {ok, _APid} = supervisor:start_child(
                                                  AcceptorSup, [LSock])
                          end,
                          lists:duplicate(ConcurrentAcceptorCount, dummy)),
            {ok, {LIPAddress, LPort}} = inet:sockname(LSock),
            ?INFO("started ~s on ~s:~p~n",
				[Label, ntoab(LIPAddress), LPort]),
            apply(M, F, A ++ [IPAddress, Port]),
            {ok, #state{sock = LSock,
                        on_startup = OnStartup, on_shutdown = OnShutdown,
                        label = Label}};
        {error, Reason} ->
            ?ERROR("failed to start ~s on ~s:~p - ~p (~s)~n",
				[Label, ntoab(IPAddress), Port,
				 Reason, inet:format_error(Reason)]),
            {stop, {cannot_listen, IPAddress, Port, Reason}}
    end.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{sock=LSock, on_shutdown = {M,F,A}, label=Label}) ->
    {ok, {IPAddress, Port}} = inet:sockname(LSock),
    gen_tcp:close(LSock),
    ?ERROR("stopped ~s on ~s:~p~n",
           [Label, ntoab(IPAddress), Port]),
    apply(M, F, A ++ [IPAddress, Port]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Format IPv4-mapped IPv6 addresses as IPv4, since they're what we see
%% when IPv6 is enabled but not used (i.e. 99% of the time).
ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
    inet_parse:ntoa(IP).

ntoab(IP) ->
    Str = ntoa(IP),
    case string:str(Str, ":") of
        0 -> Str;
        _ -> "[" ++ Str ++ "]"
    end.

