%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([start_link/4]).

%% Mgmt API
-export([ enable/2
        , disable/2
        , list/1
        ]).

%% Helper funcs
-export([ running/0
        , server/1
        , put_request_failed_action/1
        , get_request_failed_action/0
        , put_pool_size/1
        , get_pool_size/0
        ]).

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
          running :: map(),         %% XXX: server order?
          %% Wait to reload servers
          waiting :: map(),
          %% Marked stopped servers
          stopped :: map(),
          %% Auto reconnect timer interval
          auto_reconnect :: false | non_neg_integer(),
          %% Request options
          request_options :: grpc_client:options(),
          %% Timer references
          trefs :: map(),
          %% Hooks execute options
          hooks_options :: hooks_options()
         }).

-export_type([ server_options/0
             , hooks_options/0]).

-type servers() :: [{Name :: atom(), server_options()}].

-type server_options() :: [ {scheme, http | https}
                          | {host, string()}
                          | {port, inet:port_number()}
                          ].

-type hooks_options() :: #{hook_priority => integer()}.

-define(DEFAULT_HOOK_OPTS, #{hook_priority => ?DEFAULT_HOOK_PRIORITY}).

-define(DEFAULT_TIMEOUT, 60000).

-define(CNTER, emqx_exhook_counter).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link(servers(), false | non_neg_integer(), grpc_client:options(), hooks_options())
    ->ignore
     | {ok, pid()}
     | {error, any()}.
start_link(Servers, AutoReconnect, ReqOpts, HooksOpts) ->
    gen_server:start_link(?MODULE, [Servers, AutoReconnect, ReqOpts, HooksOpts], []).

-spec enable(pid(), atom() | string()) -> ok | {error, term()}.
enable(Pid, Name) ->
    call(Pid, {load, Name}).

-spec disable(pid(), atom() | string()) -> ok | {error, term()}.
disable(Pid, Name) ->
    call(Pid, {unload, Name}).

list(Pid) ->
    call(Pid, list).

call(Pid, Req) ->
    gen_server:call(Pid, Req, ?DEFAULT_TIMEOUT).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Servers, AutoReconnect, ReqOpts0, HooksOpts]) ->
    process_flag(trap_exit, true),
    %% XXX: Due to the ExHook Module in the enterprise,
    %% this process may start multiple times and they will share this table
    try
        _ = ets:new(?CNTER, [named_table, public]), ok
    catch
        error:badarg:_ ->
            ok
    end,

    %% put the global option
    put_request_failed_action(
      maps:get(request_failed_action, ReqOpts0, deny)
     ),
    put_pool_size(
      maps:get(pool_size, ReqOpts0, erlang:system_info(schedulers))
     ),

    %% Load the hook servers
    ReqOpts = maps:without([request_failed_action], ReqOpts0),
    {Waiting, Running} = load_all_servers(Servers, ReqOpts, HooksOpts),
    {ok, ensure_reload_timer(
           #state{waiting = Waiting,
                  running = Running,
                  stopped = #{},
                  request_options = ReqOpts,
                  auto_reconnect = AutoReconnect,
                  trefs = #{},
                  hooks_options = HooksOpts
                 }
          )}.

%% @private
load_all_servers(Servers, ReqOpts, HooksOpts) ->
    load_all_servers(Servers, ReqOpts, HooksOpts, #{}, #{}).

load_all_servers([], _Request, _HooksOpts, Waiting, Running) ->
    {Waiting, Running};
load_all_servers([{Name, Options} | More], ReqOpts, HooksOpts, Waiting, Running) ->
    {NWaiting, NRunning} =
        case emqx_exhook_server:load(Name, Options, ReqOpts, HooksOpts) of
            {ok, ServerState} ->
                save(Name, ServerState),
                {Waiting, Running#{Name => Options}};
            {error, _} ->
                {Waiting#{Name => Options}, Running}
        end,
    load_all_servers(More, ReqOpts, HooksOpts, NWaiting, NRunning).

handle_call({load, Name}, _From, State) ->
    {Result, NState} = do_load_server(Name, State),
    {reply, Result, NState};

handle_call({unload, Name}, _From, State) ->
    case do_unload_server(Name, State) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {ok, NState} ->
            {reply, ok, NState}
    end;

handle_call(list, _From, State = #state{
                                    running = Running,
                                    waiting = Waiting,
                                    stopped = Stopped}) ->
    ServerNames = maps:keys(Running)
                    ++ maps:keys(Waiting)
                    ++ maps:keys(Stopped),
    {reply, ServerNames, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, _Ref, {reload, Name}}, State) ->
    {Result, NState} = do_load_server(Name, State),
    case Result of
        ok ->
            {noreply, NState};
        {error, not_found} ->
            {noreply, NState};
        {error, {already_started, Pid}} ->
            ?LOG(warning, "Server ~s already started on ~p, try to restart it", [Name, Pid]),
            case server(Name) of
                undefined ->
                    %% force close grpc client pool
                    grpc_client_sup:stop_channel_pool(Name);
                ServerState ->
                   emqx_exhook_server:unload(ServerState)
            end,
            %% try again immediately
            handle_info({timeout, _Ref, {reload, Name}}, State);
        {error, Reason} ->
            ?LOG(warning, "Failed to reload exhook callback server \"~s\", "
                          "Reason: ~0p", [Name, Reason]),
            {noreply, ensure_reload_timer(NState)}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State = #state{running = Running}) ->
    _ = unload_exhooks(),
    _ = maps:fold(fun(Name, _, AccIn) ->
            {ok, NAccIn} = do_unload_server(Name, AccIn),
            NAccIn
        end, State, Running),
    ok.

%% in the emqx_exhook:v4.3.5, we have added one new field in the state last:
%%  - hooks_options :: map()
code_change({down, _Vsn}, State, [ToVsn]) ->
    case re:run(ToVsn, "4\\.4\\.0") of
        {match, _} ->
            NState = list_to_tuple(
                       lists:droplast(
                         tuple_to_list(State))),
            {ok, NState};
        _ ->
            {ok, State}
    end;
code_change(_Vsn, State, [FromVsn]) ->
    case re:run(FromVsn, "4\\.4\\.0") of
        {match, _} ->
            NState = list_to_tuple(
                       tuple_to_list(State) ++ [?DEFAULT_HOOK_OPTS]),
            {ok, NState};
        _ ->
            {ok, State}
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

unload_exhooks() ->
    [emqx:unhook(Name, {M, F}) ||
     {Name, {M, F, _A}} <- ?ENABLED_HOOKS].

do_load_server(Name, State0 = #state{
                                 waiting = Waiting,
                                 running = Running,
                                 stopped = Stopped,
                                 request_options = ReqOpts,
                                 hooks_options = HooksOpts}) ->
    State = clean_reload_timer(Name, State0),
    case maps:get(Name, Running, undefined) of
        undefined ->
            case maps:get(Name, Stopped,
                          maps:get(Name, Waiting, undefined)) of
                undefined ->
                    {{error, not_found}, State};
                Options ->
                    case emqx_exhook_server:load(Name, Options, ReqOpts, HooksOpts) of
                        {ok, ServerState} ->
                            save(Name, ServerState),
                            ?LOG(info, "Load exhook callback server "
                                          "\"~s\" successfully!", [Name]),
                            {ok, State#state{
                                   running = maps:put(Name, Options, Running),
                                   waiting = maps:remove(Name, Waiting),
                                   stopped = maps:remove(Name, Stopped)
                                  }
                            };
                        {error, Reason} ->
                            {{error, Reason}, State}
                    end
            end;
        _ ->
            {{error, already_started}, State}
    end.

do_unload_server(Name, State = #state{running = Running, stopped = Stopped}) ->
    case maps:take(Name, Running) of
        error -> {error, not_running};
        {Options, NRunning} ->
            ok = emqx_exhook_server:unload(server(Name)),
            ok = unsave(Name),
            {ok, State#state{running = NRunning,
                             stopped = maps:put(Name, Options, Stopped)
                            }}
    end.

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

clean_reload_timer(Name, State = #state{trefs = TRefs}) ->
    case maps:take(Name, TRefs) of
        error -> State;
        {TRef, NTRefs} ->
            _ = erlang:cancel_timer(TRef),
            State#state{trefs = NTRefs}
    end.

%%--------------------------------------------------------------------
%% Server state persistent

put_request_failed_action(Val) ->
    persistent_term:put({?APP, request_failed_action}, Val).

get_request_failed_action() ->
    persistent_term:get({?APP, request_failed_action}).

put_pool_size(Val) ->
    persistent_term:put({?APP, pool_size}, Val).

get_pool_size() ->
    %% Avoid the scenario that the parameter is not set after
    %% the hot upgrade completed.
    persistent_term:get({?APP, pool_size}, erlang:system_info(schedulers)).

save(Name, ServerState) ->
    Saved = persistent_term:get(?APP, []),
    persistent_term:put(?APP, lists:usort(lists:reverse([Name | Saved]))),
    persistent_term:put({?APP, Name}, ServerState).

unsave(Name) ->
    case persistent_term:get(?APP, []) of
        [] ->
            persistent_term:erase(?APP);
        Saved ->
            persistent_term:put(?APP, lists:delete(Name, Saved))
    end,
    persistent_term:erase({?APP, Name}),
    ok.

running() ->
    persistent_term:get(?APP, []).

server(Name) ->
    case catch persistent_term:get({?APP, Name}) of
        {'EXIT', {badarg,_}} -> undefined;
        Service -> Service
    end.
