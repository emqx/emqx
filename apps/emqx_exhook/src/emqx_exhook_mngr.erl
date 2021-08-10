%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Manage the server status and reload strategy
-module(emqx_exhook_mngr).

-behaviour(gen_server).

-include("emqx_exhook.hrl").
-include_lib("emqx/include/logger.hrl").

%% APIs
-export([start_link/2]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
          %% Running servers
          running :: map(),
          %% Wait to reload servers
          waiting :: map(),
          %% Marked stopped servers
          stopped :: map(),
          %% Auto reconnect timer interval
          auto_reconnect :: false | non_neg_integer(),
          %% Timer references
          trefs :: map()
         }).

-type servers() :: [{Name :: atom(), server_options()}].

-type server_options() :: [ {scheme, http | https}
                          | {host, string()}
                          | {port, inet:port_number()}
                          ].

-define(CNTER, emqx_exhook_counter).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(servers(), false | non_neg_integer())
    ->ignore
     | {ok, pid()}
     | {error, any()}.
start_link(Servers, AutoReconnect) ->
    gen_server:start_link(?MODULE, [Servers, AutoReconnect], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Servers, AutoReconnect]) ->
    %% XXX: Due to the ExHook Module in the enterprise,
    %% this process may start multiple times and they will share this table
    try
        _ = ets:new(?CNTER, [named_table, public]), ok
    catch
        error:badarg:_ ->
            ok
    end,

    %% Load the hook servers
    {Waiting, Running} = load_all_servers(Servers),
    {ok, ensure_reload_timer(
           #state{waiting = Waiting,
                  running = Running,
                  stopped = #{},
                  auto_reconnect = AutoReconnect,
                  trefs = #{}
                 }
          )}.

%% @private
load_all_servers(Servers) ->
    load_all_servers(Servers, #{}, #{}).
load_all_servers([], Waiting, Running) ->
    {Waiting, Running};
load_all_servers([{Name, Options}|More], Waiting, Running) ->
    {NWaiting, NRunning} = case emqx_exhook:enable(Name, Options) of
        ok ->
            {Waiting, Running#{Name => Options}};
        {error, _} ->
            {Waiting#{Name => Options}, Running}
    end,
    load_all_servers(More, NWaiting, NRunning).

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, _Ref, {reload, Name}},
            State0 = #state{waiting = Waiting,
                            running = Running,
                            trefs = TRefs}) ->
    State = State0#state{trefs = maps:remove(Name, TRefs)},
    case maps:get(Name, Waiting, undefined) of
        undefined ->
            {noreply, State};
        Options ->
            case emqx_exhook:enable(Name, Options) of
                ok ->
                    ?LOG(warning, "Reconnect to exhook callback server "
                                  "\"~s\" successfully!", [Name]),
                    {noreply, State#state{
                                running = maps:put(Name, Options, Running),
                                waiting = maps:remove(Name, Waiting)}
                    };
                {error, _} ->
                    {noreply, ensure_reload_timer(State)}
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    _ = emqx_exhook:disable_all(),
    _ = unload_exhooks(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

unload_exhooks() ->
    [emqx:unhook(Name, {M, F}) ||
     {Name, {M, F, _A}} <- ?ENABLED_HOOKS].

ensure_reload_timer(State = #state{auto_reconnect = false}) ->
    State;
ensure_reload_timer(State = #state{waiting = Waiting,
                                   trefs = TRefs,
                                   auto_reconnect = Intv}) ->
    NRefs = maps:fold(fun(Name, _, AccIn) ->
        case maps:get(Name, AccIn, undefined) of
            undefined ->
                Ref = erlang:start_timer(Intv, self(), {reload, Name}),
                AccIn#{Name => Ref};
            _HasRef ->
                AccIn
        end
    end, TRefs, Waiting),
    State#state{trefs = NRefs}.
