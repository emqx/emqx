%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_registry).

-author("Feng Lee <feng@emqx.io>").

-include("emqx_coap.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[CoAP-Registry]").

-behaviour(gen_server).

%% API.
-export([ start_link/0
        , register_name/2
        , unregister_name/1
        , whereis_name/1
        , send/2
        , stop/0
        ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {}).

-define(RESPONSE_TAB, coap_response_process).
-define(RESPONSE_REF_TAB, coap_response_process_ref).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_name(Name, Pid) ->
    gen_server:call(?MODULE, {register_name, Name, Pid}).

unregister_name(Name) ->
    gen_server:call(?MODULE, {unregister_name, Name}).

whereis_name(Name) ->
    case ets:lookup(?RESPONSE_TAB, Name) of
        [] ->                    undefined;
        [{Name, Pid, _MRef}] ->  Pid
    end.

send(Name, Msg) ->
    case whereis_name(Name) of
        undefined ->
            exit({badarg, {Name, Msg}});
        Pid when is_pid(Pid) ->
            Pid ! Msg,
            Pid
    end.

stop() ->
    gen_server:stop(?MODULE).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    _ = ets:new(?RESPONSE_TAB, [set, named_table, protected]),
    _ = ets:new(?RESPONSE_REF_TAB, [set, named_table, protected]),
    {ok, #state{}}.

handle_call({register_name, Name, Pid}, _From, State) ->
    case ets:member(?RESPONSE_TAB, Name) of
        false ->
            MRef = monitor_client(Pid),
            ets:insert(?RESPONSE_TAB, {Name, Pid, MRef}),
            ets:insert(?RESPONSE_REF_TAB, {MRef, Name, Pid}),
            {reply, yes, State};
        true  ->   {reply, no, State}
    end;

handle_call({unregister_name, Name}, _From, State) ->
    case ets:lookup(?RESPONSE_TAB, Name) of
        [] ->
            ok;
        [{Name, _Pid, MRef}] ->
            erase_monitor(MRef),
            ets:delete(?RESPONSE_TAB, Name),
            ets:delete(?RESPONSE_REF_TAB, MRef)
    end,
	{reply, ok, State};

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.


handle_info({'DOWN', MRef, process, DownPid, _Reason}, State) ->
    case ets:lookup(?RESPONSE_REF_TAB, MRef) of
        [{MRef, Name, _Pid}] ->
            ets:delete(?RESPONSE_TAB, Name),
            ets:delete(?RESPONSE_REF_TAB, MRef),
            erase_monitor(MRef);
        [] ->
            ?LOG(error, "MRef of client ~p not found", [DownPid])
    end,
    {noreply, State};


handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
    ets:delete(?RESPONSE_TAB),
    ets:delete(?RESPONSE_REF_TAB),
    ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.



%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

monitor_client(Pid) ->
    erlang:monitor(process, Pid).

erase_monitor(MRef) ->
    catch erlang:demonitor(MRef, [flush]).
