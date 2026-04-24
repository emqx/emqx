%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages).

-include("emqx_offline_messages.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(gen_server).

%% API
-export([
    current_config/0,
    start_link/0,
    child_spec/0
]).

%% Plugin callbacks
-export([
    on_config_changed/2,
    on_health_check/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 15000).

%%--------------------------------------------------------------------
%% Events
%%--------------------------------------------------------------------

-record(state, {}).

-record(on_config_changed, {
    old_conf :: map(),
    new_conf :: map()
}).

-record(on_health_check, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?SERVER,
        start => {?MODULE, start_link, []},
        type => worker,
        modules => [?MODULE],
        restart => permanent,
        shutdown => ?TIMEOUT
    }.

%%--------------------------------------------------------------------
%% EMQX Plugin callbacks
%%--------------------------------------------------------------------

on_config_changed(OldConf, NewConf) ->
    try
        gen_server:call(
            ?SERVER, #on_config_changed{old_conf = OldConf, new_conf = NewConf}, ?TIMEOUT
        )
    catch
        exit:{noproc, _} ->
            ok
    end.

on_health_check() ->
    try
        gen_server:call(?SERVER, #on_health_check{}, ?TIMEOUT)
    catch
        exit:{noproc, _} ->
            {error, <<"Plugin is not running">>}
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init(map()) -> {ok, map()}.
init([]) ->
    erlang:process_flag(trap_exit, true),
    ok = init_metrics(),
    ok = handle_on_config_changed(#{}, current_config()),
    {ok, #state{}}.

handle_call(#on_config_changed{old_conf = OldConf, new_conf = NewConf}, _From, State) ->
    {reply, handle_on_config_changed(OldConf, NewConf), State};
handle_call(#on_health_check{}, _From, State) ->
    {reply, handle_on_health_check(), State};
handle_call(Request, From, State) ->
    ?SLOG(error, #{
        msg => "offline_messages_unexpected_call", request => Request, from => From
    }),
    {reply, {error, unexpected_call}, State}.

handle_cast(Request, State) ->
    ?SLOG(error, #{
        msg => "offline_messages_unexpected_cast", request => Request
    }),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{
        msg => "offline_messages_unexpected_info", info => Info
    }),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = handle_on_config_changed(current_config(), #{}),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec handle_on_config_changed(map(), map()) -> ok.
handle_on_config_changed(OldConf, NewConf) ->
    ?SLOG(info, #{
        msg => "offline_messages_config_changed", old_conf => OldConf, new_conf => NewConf
    }),
    %% MySQL
    DefaultConf = #{<<"enable">> => false},
    OldMysqlConf = maps:get(<<"mysql">>, OldConf, DefaultConf),
    NewMysqlConf = maps:get(<<"mysql">>, NewConf, DefaultConf),
    ok = emqx_offline_messages_mysql:on_config_changed(OldMysqlConf, NewMysqlConf),
    %% Redis
    OldRedisConf = maps:get(<<"redis">>, OldConf, DefaultConf),
    NewRedisConf = maps:get(<<"redis">>, NewConf, DefaultConf),
    ok = emqx_offline_messages_redis:on_config_changed(OldRedisConf, NewRedisConf),
    ok.

handle_on_health_check() ->
    Config = current_config(),
    DefaultConf = #{<<"enable">> => false},
    MysqlConf = maps:get(<<"mysql">>, Config, DefaultConf),
    RedisConf = maps:get(<<"redis">>, Config, DefaultConf),
    MysqlStatus = emqx_offline_messages_mysql:on_health_check(MysqlConf),
    RedisStatus = emqx_offline_messages_redis:on_health_check(RedisConf),
    Errors = status_to_error_list(MysqlStatus) ++ status_to_error_list(RedisStatus),
    case Errors of
        [] ->
            ok;
        Errors ->
            {error, iolist_to_binary(lists:join(",", Errors))}
    end.

status_to_error_list(ok) -> [];
status_to_error_list({error, Error}) -> [Error].

current_config() ->
    emqx_plugins:get_config(name_vsn(), #{}).

name_vsn() ->
    {ok, Vsn} = application:get_key(?PLUGIN_NAME, vsn),
    iolist_to_binary([atom_to_binary(?PLUGIN_NAME), <<"-">>, Vsn]).

init_metrics() ->
    ?SLOG(info, #{msg => "offline_messages_init_metrics"}),
    emqx_metrics_worker:create_metrics(
        ?METRICS_WORKER, message_acked, [success, fail]
    ),
    emqx_metrics_worker:create_metrics(
        ?METRICS_WORKER, session_subscribed, [success, fail]
    ),
    ok.
