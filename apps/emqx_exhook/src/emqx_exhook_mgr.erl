%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_exhook_mgr).

-behaviour(gen_server).

-include("emqx_exhook.hrl").
-include_lib("emqx/include/logger.hrl").

%% APIs
-export([start_link/0]).

%% Mgmt API
-export([ list/0
        , lookup/1
        , enable/1
        , disable/1
        , server_info/1
        , all_servers_info/0
        , server_hooks_metrics/1
        ]).

%% Helper funcs
-export([ running/0
        , server/1
        , hooks/1
        , init_ref_counter_table/0
        ]).

-export([ update_config/2
        , pre_config_update/3
        , post_config_update/5
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([roots/0]).

-type state() :: #{%% Running servers
                   running := servers(),
                   %% Wait to reload servers
                   waiting := servers(),
                   %% Marked stopped servers
                   stopped := servers(),
                   %% Timer references
                   trefs := map(),
                   orders := orders()
                  }.

-type server_name() :: binary().
-type servers() :: #{server_name() => server()}.
-type server() :: server_options().
-type server_options() :: map().

-type position() :: front
                  | rear
                  | {before, binary()}
                  | {'after', binary()}.

-type orders() :: #{server_name() => integer()}.

-type server_info() :: #{name := server_name(),
                         status := running | waiting | stopped,

                         atom() => term()
                        }.

-define(DEFAULT_TIMEOUT, 60000).
-define(REFRESH_INTERVAL, timer:seconds(5)).

-export_type([servers/0, server/0, server_info/0]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec start_link() -> ignore
              | {ok, pid()}
              | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

list() ->
    call(list).

-spec lookup(server_name()) -> not_found | server_info().
lookup(Name) ->
    call({lookup, Name}).

enable(Name) ->
    update_config([exhook, servers], {enable, Name, true}).

disable(Name) ->
    update_config([exhook, servers], {enable, Name, false}).

server_info(Name) ->
    call({?FUNCTION_NAME, Name}).

all_servers_info() ->
    call(?FUNCTION_NAME).

server_hooks_metrics(Name) ->
    call({?FUNCTION_NAME, Name}).

call(Req) ->
    gen_server:call(?MODULE, Req, ?DEFAULT_TIMEOUT).

init_ref_counter_table() ->
    _ = ets:new(?HOOKS_REF_COUNTER, [named_table, public]).

%%=====================================================================
%% Hocon schema
roots() ->
    emqx_exhook_schema:server_config().

update_config(KeyPath, UpdateReq) ->
    case emqx_conf:update(KeyPath, UpdateReq, #{override_to => cluster}) of
        {ok, UpdateResult}  ->
            #{post_config_update := #{?MODULE := Result}} = UpdateResult,
            {ok, Result};
        Error ->
            Error
    end.

pre_config_update(_, {add, Conf}, OldConf) ->
    {ok, OldConf ++ [Conf]};

pre_config_update(_, {update, Name, Conf}, OldConf) ->
    case replace_conf(Name, fun(_) -> Conf end, OldConf) of
        not_found -> {error, not_found};
        NewConf -> {ok, NewConf}
    end;

pre_config_update(_, {delete, ToDelete}, OldConf) ->
    {ok, lists:dropwhile(fun(#{<<"name">> := Name}) -> Name =:= ToDelete end,
                         OldConf)};

pre_config_update(_, {move, Name, Position}, OldConf) ->
    case do_move(Name, Position, OldConf) of
        not_found -> {error, not_found};
        NewConf -> {ok, NewConf}
    end;

pre_config_update(_, {enable, Name, Enable}, OldConf) ->
    case replace_conf(Name,
                      fun(Conf) -> Conf#{<<"enable">> => Enable} end, OldConf) of
        not_found -> {error, not_found};
        NewConf -> {ok, NewConf}
    end.

post_config_update(_KeyPath, UpdateReq, NewConf, _OldConf, _AppEnvs) ->
    {ok, call({update_config, UpdateReq, NewConf})}.

%%=====================================================================

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    emqx_conf:add_handler([exhook, servers], ?MODULE),
    ServerL = emqx:get_config([exhook, servers]),
    {Waiting, Running, Stopped} = load_all_servers(ServerL),
    Orders = reorder(ServerL),
    refresh_tick(),
    {ok, ensure_reload_timer(
           #{waiting => Waiting,
             running => Running,
             stopped => Stopped,
             trefs => #{},
             orders => Orders
            })}.

-spec load_all_servers(list(server_options())) -> {servers(), servers(), servers()}.
load_all_servers(ServerL) ->
    load_all_servers(ServerL, #{}, #{}, #{}).

load_all_servers([#{name := Name} = Options | More], Waiting, Running, Stopped) ->
    case emqx_exhook_server:load(Name, Options) of
        {ok, ServerState} ->
            save(Name, ServerState),
            load_all_servers(More, Waiting, Running#{Name => Options}, Stopped);
        {error, _} ->
            load_all_servers(More, Waiting#{Name => Options}, Running, Stopped);
        disable ->
            load_all_servers(More, Waiting, Running, Stopped#{Name => Options})
    end;

load_all_servers([], Waiting, Running, Stopped) ->
    {Waiting, Running, Stopped}.

handle_call(list, _From, State = #{running := Running,
                                   waiting := Waiting,
                                   stopped := Stopped,
                                   orders := Orders}) ->

    R = get_servers_info(running, Running),
    W = get_servers_info(waiting, Waiting),
    S = get_servers_info(stopped, Stopped),

    Servers = R ++ W ++ S,
    OrderServers = sort_name_by_order(Servers, Orders),

    {reply, OrderServers, State};

handle_call({update_config, {move, _Name, _Position}, NewConfL},
            _From,
            State) ->
    Orders = reorder(NewConfL),
    {reply, ok, State#{orders := Orders}};

handle_call({update_config, {delete, ToDelete}, _}, _From, State) ->
    {ok, #{orders := Orders,
           stopped := Stopped
          } = State2} = do_unload_server(ToDelete, State),

    State3 = State2#{stopped := maps:remove(ToDelete, Stopped),
                     orders := maps:remove(ToDelete, Orders)
                    },

    emqx_exhook_metrics:on_server_deleted(ToDelete),

    {reply, ok, State3};

handle_call({update_config, {add, RawConf}, NewConfL},
            _From,
            #{running := Running, waiting := Waitting, stopped := Stopped} = State) ->
    {_, #{name := Name} = Conf} = emqx_config:check_config(?MODULE, RawConf),

    case emqx_exhook_server:load(Name, Conf) of
        {ok, ServerState} ->
            save(Name, ServerState),
            State2 = State#{running := Running#{Name => Conf}};
        {error, _} ->
            StateT = State#{waiting := Waitting#{Name => Conf}},
            State2 = ensure_reload_timer(StateT);
        disable ->
            State2 = State#{stopped := Stopped#{Name => Conf}}
    end,
    Orders = reorder(NewConfL),
    {reply, ok, State2#{orders := Orders}};

handle_call({lookup, Name}, _From, State) ->
    case where_is_server(Name, State) of
        not_found ->
            Result = not_found;
        {Where, #{Name := Conf}} ->
            Result = maps:merge(Conf, #{status => Where})
    end,
    {reply, Result, State};

handle_call({update_config, {update, Name, _Conf}, NewConfL}, _From, State) ->
    {Result, State2} = restart_server(Name, NewConfL, State),
    {reply, Result, State2};

handle_call({update_config, {enable, Name, _Enable}, NewConfL}, _From, State) ->
    {Result, State2} = restart_server(Name, NewConfL, State),
    {reply, Result, State2};

handle_call({server_info, Name}, _From, State) ->
    case where_is_server(Name, State) of
        not_found ->
            Result = not_found;
        {Status, _} ->
            HooksMetrics = emqx_exhook_metrics:server_metrics(Name),
            Result = #{ status => Status
                      , metrics => HooksMetrics
                      }
    end,
    {reply, Result, State};

handle_call(all_servers_info, _From, #{running := Running,
                                       waiting := Waiting,
                                       stopped := Stopped} = State) ->
    MakeStatus = fun(Status, Servers, Acc) ->
                         lists:foldl(fun(Name, IAcc) -> IAcc#{Name => Status} end,
                                     Acc,
                                     maps:keys(Servers))
                 end,
    Status = lists:foldl(fun({Status, Servers}, Acc) -> MakeStatus(Status, Servers, Acc) end,
                         #{},
                         [{running, Running}, {waiting, Waiting}, {stopped, Stopped}]),

    Metrics = emqx_exhook_metrics:servers_metrics(),

    Result = #{ status => Status
              , metrics => Metrics
              },

    {reply, Result, State};

handle_call({server_hooks_metrics, Name}, _From, State) ->
    Result = emqx_exhook_metrics:hooks_metrics(Name),
    {reply, Result, State};

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
        {error, Reason} ->
            ?SLOG(warning,
                  #{msg => "failed_to_reload_exhook_callback_server",
                    reason => Reason,
                    name => Name}),
            {noreply, ensure_reload_timer(NState)}
    end;

handle_info(refresh_tick, State) ->
    refresh_tick(),
    emqx_exhook_metrics:update(?REFRESH_INTERVAL),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State = #{running := Running}) ->
    _ = maps:fold(fun(Name, _, AccIn) ->
                          {ok, NAccIn} =  do_unload_server(Name, AccIn),
                          NAccIn
                  end, State, Running),
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

-spec do_load_server(server_name(), state()) -> {{error, not_found}, state()}
              | {{error, already_started}, state()}
              | {ok, state()}.
do_load_server(Name, State = #{orders := Orders}) ->
    case where_is_server(Name, State) of
        not_found ->
            {{error, not_found}, State};
        {running, _} ->
            {ok, State};
        {Where, Map} ->
            State2 = clean_reload_timer(Name, State),
            {Options, Map2} = maps:take(Name, Map),
            State3 = State2#{Where := Map2},
            #{running := Running,
              stopped := Stopped} = State3,
            case emqx_exhook_server:load(Name, Options) of
                    {ok, ServerState} ->
                        save(Name, ServerState),
                        update_order(Orders),
                        ?SLOG(info, #{msg => "load_exhook_callback_server_ok",
                                      name => Name}),
                    {ok, State3#{running := maps:put(Name, Options, Running)}};
                {error, Reason} ->
                    {{error, Reason}, State};
                disable ->
                    {ok, State3#{stopped := Stopped#{Name => Options}}}
            end
    end.

-spec do_unload_server(server_name(), state()) -> {ok, state()}.
do_unload_server(Name, #{stopped := Stopped} = State) ->
    case where_is_server(Name, State) of
        {stopped, _} -> {ok, State};
        {waiting, Waiting} ->
            {Options, Waiting2} = maps:take(Name, Waiting),
            {ok, clean_reload_timer(Name,
                                    State#{waiting := Waiting2,
                                           stopped := maps:put(Name, Options, Stopped)
                                          }
                                   )};
        {running, Running} ->
            Service = server(Name),
            ok = unsave(Name),
            ok = emqx_exhook_server:unload(Service),
            {Options, Running2} = maps:take(Name, Running),
            {ok, State#{running := Running2,
                        stopped := maps:put(Name, Options, Stopped)
                       }};
        not_found -> {ok, State}
    end.

-spec ensure_reload_timer(state()) -> state().
ensure_reload_timer(State = #{waiting := Waiting,
                              stopped := Stopped,
                              trefs := TRefs}) ->
    Iter = maps:iterator(Waiting),

    {Waitting2, Stopped2, TRefs2} =
        ensure_reload_timer(maps:next(Iter), Waiting, Stopped, TRefs),

    State#{waiting := Waitting2,
           stopped := Stopped2,
           trefs := TRefs2}.

ensure_reload_timer(none, Waiting, Stopped, TimerRef) ->
    {Waiting, Stopped, TimerRef};

ensure_reload_timer({Name, #{auto_reconnect := Intv}, Iter},
                    Waiting,
                    Stopped,
                    TimerRef) when is_integer(Intv) ->
    Next = maps:next(Iter),
    case maps:is_key(Name, TimerRef) of
        true ->
            ensure_reload_timer(Next, Waiting, Stopped, TimerRef);
        _ ->
            Ref = erlang:start_timer(Intv, self(), {reload, Name}),
            TimerRef2 = maps:put(Name, Ref, TimerRef),
            ensure_reload_timer(Next, Waiting, Stopped, TimerRef2)
    end;

ensure_reload_timer({Name, Opts, Iter}, Waiting, Stopped, TimerRef) ->
    ensure_reload_timer(maps:next(Iter),
                        maps:remove(Name, Waiting),
                        maps:put(Name, Opts, Stopped),
                        TimerRef).

-spec clean_reload_timer(server_name(), state()) -> state().
clean_reload_timer(Name, State = #{trefs := TRefs}) ->
    case maps:take(Name, TRefs) of
        error -> State;
        {TRef, NTRefs} ->
            _ = erlang:cancel_timer(TRef),
            State#{trefs := NTRefs}
    end.

-spec do_move(binary(), position(), list(server_options())) ->
          not_found | list(server_options()).
do_move(Name, Position, ConfL) ->
    move(ConfL, Name, Position, []).

move([#{<<"name">> := Name} = Server | T], Name, Position, HeadL) ->
    move_to(Position, Server, lists:reverse(HeadL) ++ T);

move([Server | T], Name, Position, HeadL) ->
    move(T, Name, Position, [Server | HeadL]);

move([], _Name, _Position, _HeadL) ->
    not_found.

move_to(?CMD_MOVE_FRONT, Server, ServerL) ->
    [Server | ServerL];

move_to(?CMD_MOVE_REAR, Server, ServerL) ->
    ServerL ++ [Server];

move_to(Position, Server, ServerL) ->
    move_to(ServerL, Position, Server, []).

move_to([#{<<"name">> := Name} | _] = T, ?CMD_MOVE_BEFORE(Name), Server, HeadL) ->
    lists:reverse(HeadL) ++ [Server | T];

move_to([#{<<"name">> := Name} = H | T], ?CMD_MOVE_AFTER(Name), Server, HeadL) ->
    lists:reverse(HeadL) ++ [H, Server | T];

move_to([H | T], Position, Server, HeadL) ->
    move_to(T, Position, Server, [H | HeadL]);

move_to([], _Position, _Server, _HeadL) ->
    not_found.

-spec reorder(list(server_options())) -> orders().
reorder(ServerL) ->
    Orders = reorder(ServerL, 1, #{}),
    update_order(Orders),
    Orders.

reorder([#{name := Name} | T], Order, Orders) ->
    reorder(T, Order + 1, Orders#{Name => Order});

reorder([], _Order, Orders) ->
    Orders.

get_servers_info(Status, Map) ->
    Fold = fun(Name, Conf, Acc) ->
                   [maps:merge(Conf, #{status => Status,
                                       hooks => hooks(Name)}) | Acc]
           end,
    maps:fold(Fold, [], Map).

where_is_server(Name, #{running := Running}) when is_map_key(Name, Running) ->
    {running, Running};

where_is_server(Name, #{waiting := Waiting}) when is_map_key(Name, Waiting) ->
    {waiting, Waiting};

where_is_server(Name, #{stopped := Stopped}) when is_map_key(Name, Stopped) ->
    {stopped, Stopped};

where_is_server(_, _) ->
    not_found.

-type replace_fun() :: fun((server_options()) -> server_options()).

-spec replace_conf(binary(), replace_fun(), list(server_options())) -> not_found
              | list(server_options()).
replace_conf(Name, ReplaceFun, ConfL) ->
    replace_conf(ConfL, Name, ReplaceFun, []).

replace_conf([#{<<"name">> := Name} = H | T], Name, ReplaceFun, HeadL) ->
    New = ReplaceFun(H),
    lists:reverse(HeadL) ++ [New | T];

replace_conf([H | T], Name, ReplaceFun, HeadL) ->
    replace_conf(T, Name, ReplaceFun, [H | HeadL]);

replace_conf([], _, _, _) ->
    not_found.

-spec restart_server(binary(), list(server_options()), state()) -> {ok, state()}
              | {{error, term()}, state()}.
restart_server(Name, ConfL, State) ->
    case lists:search(fun(#{name := CName}) -> CName =:= Name end, ConfL) of
        false ->
            {{error, not_found}, State};
        {value, Conf} ->
            case where_is_server(Name, State) of
                not_found ->
                    {{error, not_found}, State};
                {Where, Map} ->
                    State2 = State#{Where := Map#{Name := Conf}},
                    {ok, State3} = do_unload_server(Name, State2),
                    case do_load_server(Name, State3) of
                        {ok, State4} ->
                            {ok, State4};
                        {Error, State4} ->
                            {Error, State4}
                    end
            end
    end.

sort_name_by_order(Names, Orders) ->
    lists:sort(fun(A, B) when is_binary(A) ->
                       maps:get(A, Orders) < maps:get(B, Orders);
                  (#{name := A}, #{name := B}) ->
                       maps:get(A, Orders) < maps:get(B, Orders)
               end,
               Names).

refresh_tick() ->
    erlang:send_after(?REFRESH_INTERVAL, self(), ?FUNCTION_NAME).

%%--------------------------------------------------------------------
%% Server state persistent
save(Name, ServerState) ->
    Saved = persistent_term:get(?APP, []),
    persistent_term:put(?APP, lists:reverse([Name | Saved])),
    persistent_term:put({?APP, Name}, ServerState).

unsave(Name) ->
    case persistent_term:get(?APP, []) of
        [] ->
            ok;
        Saved ->
            case lists:member(Name, Saved) of
                false ->
                    ok;
                true ->
                    persistent_term:put(?APP, lists:delete(Name, Saved))
            end
    end,
    persistent_term:erase({?APP, Name}),
    ok.

running() ->
    persistent_term:get(?APP, []).

server(Name) ->
    case persistent_term:get({?APP, Name}, undefined) of
        undefined -> undefined;
        Service -> Service
    end.

update_order(Orders) ->
    Running = running(),
    Running2 = sort_name_by_order(Running, Orders),
    persistent_term:put(?APP, Running2).

hooks(Name) ->
    case server(Name) of
        undefined ->
            [];
        Service ->
            emqx_exhook_server:hooks(Service)
    end.
