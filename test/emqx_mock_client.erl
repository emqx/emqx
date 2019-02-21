%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mock_client).

-behaviour(gen_server).

-export([start_link/1, open_session/3, open_session/4,
         close_session/1, stop/1, get_last_message/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {clean_start, client_id, client_pid, last_msg, session_pid}).

start_link(ClientId) ->
    gen_server:start_link(?MODULE, [ClientId], []).

open_session(ClientPid, ClientId, Zone) ->
    open_session(ClientPid, ClientId, Zone, _Attrs = #{}).

open_session(ClientPid, ClientId, Zone, Attrs0) ->
    Attrs1 = default_session_attributes(Zone, ClientId, ClientPid),
    Attrs = maps:merge(Attrs1, Attrs0),
    gen_server:call(ClientPid, {start_session, ClientPid, ClientId, Attrs}).

%% close session and terminate the client itself
close_session(ClientPid) ->
    gen_server:call(ClientPid, stop_session, infinity).

stop(CPid) ->
    gen_server:call(CPid, stop, infinity).

get_last_message(Pid) ->
    gen_server:call(Pid, get_last_message, infinity).

init([ClientId]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{clean_start = true,
                client_id = ClientId,
                last_msg = undefined
               }
    }.

handle_call({start_session, ClientPid, ClientId, Attrs}, _From, State) ->
    {ok, SessPid} = emqx_sm:open_session(Attrs),
    {reply, {ok, SessPid},
     State#state{clean_start = true,
                 client_id = ClientId,
                 client_pid = ClientPid,
                 session_pid = SessPid
                }};
handle_call(stop_session, _From, #state{session_pid = Pid} = State) ->
    is_pid(Pid) andalso is_process_alive(Pid) andalso emqx_sm:close_session(Pid),
    {stop, normal, ok, State#state{session_pid = undefined}};
handle_call(get_last_message, _From, #state{last_msg = Msg} = State) ->
    {reply, Msg, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({deliver, Msg}, State) ->
    {noreply, State#state{last_msg = Msg}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

default_session_attributes(Zone, ClientId, ClientPid) ->
    #{zone                => Zone,
      client_id           => ClientId,
      conn_pid            => ClientPid,
      clean_start         => true,
      username            => undefined,
      expiry_interval     => 0,
      max_inflight        => 0,
      topic_alias_maximum => 0,
      will_msg            => undefined
     }.

