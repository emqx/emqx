%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_retainer).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0]).

-export([
    get_expiry_time/1,
    clean/0,
    delete/1,
    read_message/1,
    page_read/3,
    page_read/4,
    retained_count/0,
    is_enabled/0,
    is_started/0
]).

%% Hooks
-export([
    on_session_subscribed/3,
    on_message_publish/1,
    post_config_update/5,
    on_config_zones_updated/2
]).

%% Internal APIs
-export([
    update_config/1,
    stats_fun/0,
    backend_module/0,
    backend_module/1,
    backend_state/1,
    context/0,
    with_backend/1,
    with_backend/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export_type([
    deadline/0,
    cursor/0,
    context/0
]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

-type state() :: #{
    is_started := boolean(),
    clear_timer := undefined | reference()
}.

-type backend_state() :: term().

-type context() ::
    #{
        module := module(),
        state := backend_state()
    }
    | undefined.

-type topic() :: emqx_types:topic().
-type message() :: emqx_types:message().
-type deadline() :: emqx_utils_calendar:epoch_millisecond().
-type cursor() :: undefined | term().
-type has_next() :: boolean().
-type index_incarnation() :: integer().

-define(CONTEXT_KEY, {?MODULE, context}).

-callback create(hocon:config()) -> backend_state().
-callback update(backend_state(), hocon:config()) -> ok | need_recreate.
-callback close(backend_state()) -> ok.
-callback delete_message(backend_state(), topic()) -> ok.
-callback store_retained(backend_state(), message()) -> ok.
-callback page_read(
    backend_state(),
    emqx_maybe:t(topic()),
    deadline(),
    non_neg_integer(),
    non_neg_integer()
) ->
    {ok, has_next(), list(message())}.
-callback read_message(backend_state(), topic()) -> {ok, list(message())}.
-callback match_messages(backend_state(), topic(), cursor()) -> {ok, list(message()), cursor()}.
-callback delete_cursor(backend_state(), cursor()) -> ok.
-callback clean(backend_state()) -> ok.
-callback size(backend_state()) -> non_neg_integer().
-callback current_index_incarnation(backend_state()) -> index_incarnation().
-callback cursor_index_incarnation(backend_state(), cursor()) -> index_incarnation().

%%------------------------------------------------------------------------------
%% Hook API
%%------------------------------------------------------------------------------
-spec on_session_subscribed(_, _, emqx_types:subopts()) -> any().
on_session_subscribed(_, #share{} = _Topic, _SubOpts) ->
    ok;
on_session_subscribed(_, Topic, #{rh := Rh} = Opts) ->
    IsNew = maps:get(is_new, Opts, true),
    case Rh =:= 0 orelse (Rh =:= 1 andalso IsNew) of
        true -> emqx_retainer_dispatcher:dispatch(Topic);
        _ -> ok
    end.

%% RETAIN flag set to 1 and payload containing zero bytes
on_message_publish(
    Msg = #message{
        flags = #{retain := true},
        topic = Topic,
        payload = <<>>
    }
) ->
    emqx_retainer_publisher:delete_message(Topic),
    case get_stop_publish_clear_msg() of
        true ->
            {ok, emqx_message:set_header(allow_publish, false, Msg)};
        _ ->
            {ok, Msg}
    end;
on_message_publish(Msg = #message{flags = #{retain := true}}) ->
    Msg1 = emqx_message:set_header(retained, true, Msg),
    emqx_retainer_publisher:store_retained(Msg1),
    {ok, Msg};
on_message_publish(Msg) ->
    {ok, Msg}.

%%------------------------------------------------------------------------------
%% Config API
%%------------------------------------------------------------------------------

post_config_update([retainer], _UpdateReq, NewConf, OldConf, _AppEnvs) ->
    ok = call({update_config, NewConf, OldConf}).

on_config_zones_updated(_OldZones, NewZones) ->
    case is_enabled(NewZones) of
        true -> call(start);
        false -> call(stop)
    end.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

%% @doc Start the retainer
-spec start_link() -> emqx_types:startlink_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_expiry_time(message()) -> non_neg_integer().
get_expiry_time(#message{headers = #{properties := #{'Message-Expiry-Interval' := 0}}}) ->
    0;
get_expiry_time(#message{
    headers = #{properties := #{'Message-Expiry-Interval' := Interval}},
    timestamp = Ts
}) ->
    Ts + Interval * 1000;
get_expiry_time(#message{timestamp = Ts}) ->
    Interval = emqx_conf:get([retainer, msg_expiry_interval]),
    case Interval of
        0 -> 0;
        _ -> Ts + Interval
    end.

-spec update_config(hocon:config()) -> {ok, _} | {error, _}.
update_config(Conf) ->
    emqx_conf:update([retainer], Conf, #{override_to => cluster}).

-spec clean() -> ok.
clean() ->
    with_backend(fun(Mod, BackendState) -> Mod:clean(BackendState) end).

-spec delete(topic()) -> ok.
delete(Topic) ->
    with_backend(fun(Mod, BackendState) -> Mod:delete_message(BackendState, Topic) end).

-spec retained_count() -> non_neg_integer().
retained_count() ->
    with_backend(fun(Mod, BackendState) -> Mod:size(BackendState) end, 0).

-spec read_message(topic()) -> {ok, list(message())}.
read_message(Topic) ->
    with_backend(
        fun(Mod, BackendState) -> Mod:read_message(BackendState, Topic) end,
        {ok, []}
    ).

-spec page_read(emqx_maybe:t(topic()), non_neg_integer(), non_neg_integer()) ->
    {ok, has_next(), list(message())}.
page_read(Topic, Page, Limit) ->
    page_read(Topic, erlang:system_time(millisecond), Page, Limit).

-spec page_read(emqx_maybe:t(topic()), deadline(), non_neg_integer(), non_neg_integer()) ->
    {ok, has_next(), list(message())}.
page_read(Topic, Deadline, Page, Limit) ->
    with_backend(
        fun(Mod, BackendState) -> Mod:page_read(BackendState, Topic, Deadline, Page, Limit) end,
        {ok, false, []}
    ).

-spec context() -> context().
context() ->
    persistent_term:get(?CONTEXT_KEY, undefined).

-spec is_started() -> boolean().
is_started() ->
    call(?FUNCTION_NAME).

-spec is_enabled() -> boolean().
is_enabled() ->
    Zones = emqx_config:get([zones], #{}),
    is_enabled(Zones).

-spec is_enabled(emqx_config:config()) -> boolean().
is_enabled(Zones) ->
    lists:any(fun is_enabled_for_zone/1, maps:values(Zones)).

%%------------------------------------------------------------------------------
%% Internal APIs
%%------------------------------------------------------------------------------

stats_fun() ->
    emqx_stats:setstat('retained.count', 'retained.max', retained_count()).

-spec get_basic_usage_info() -> #{retained_messages => non_neg_integer()}.
get_basic_usage_info() ->
    try
        #{retained_messages => retained_count()}
    catch
        _:_ ->
            #{retained_messages => 0}
    end.

with_backend(Fun, Default) ->
    Context = context(),
    case backend_module(Context) of
        undefined ->
            Default;
        Mod ->
            BackendState = backend_state(Context),
            Fun(Mod, BackendState)
    end.

with_backend(Fun) ->
    with_backend(Fun, ok).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    erlang:process_flag(trap_exit, true),
    emqx_conf:add_handler([retainer], ?MODULE),
    ok = emqx_hooks:add('config.zones_updated', {?MODULE, on_config_zones_updated, []}, ?HP_LOWEST),
    ok = emqx_hooks:add(
        'retainer.request_next',
        {emqx_retainer_dispatcher, on_retainer_request_next, []},
        ?HP_RETAINER
    ),
    State = new_state(),
    RetainerConfig = emqx:get_config([retainer]),
    {ok,
        case is_enabled() of
            false ->
                %% Cleanup in case of previous crash
                stop_retainer(State);
            true ->
                BackendConfig = enabled_backend_config(RetainerConfig),
                start_retainer(State, RetainerConfig, BackendConfig)
        end}.

handle_call({update_config, _NewConf, _OldConf}, _From, #{is_started := false} = State) ->
    {reply, ok, State};
handle_call({update_config, NewConf, OldConf}, _From, #{is_started := true} = State) ->
    State2 = update_config(State, NewConf, OldConf),
    ok = emqx_retainer_limiter:update(),
    {reply, ok, State2};
handle_call(start, _From, #{is_started := IsStarted} = State0) ->
    State = update_status(State0, IsStarted, true),
    {reply, ok, State};
handle_call(stop, _From, #{is_started := IsStarted} = State0) ->
    State = update_status(State0, IsStarted, false),
    {reply, ok, State};
handle_call(is_started, _From, State = #{is_started := IsStarted}) ->
    {reply, IsStarted, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(
    {timeout, TRef, clear_expired}, #{clear_timer := TRef, is_started := IsStarted} = State
) ->
    IsStarted andalso start_clear_expired(),
    ClearInterval = emqx_conf:get([retainer, msg_clear_interval]),
    {noreply, State#{clear_timer := maybe_start_timer(ClearInterval, clear_expired)}};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{is_started := IsStarted} = State) ->
    emqx_conf:remove_handler([retainer]),
    emqx_hooks:del('config.zones_updated', {?MODULE, on_config_zones_updated}),
    emqx_hooks:del('retainer.request_next', {emqx_retainer_dispatcher, on_retainer_request_next}),
    IsStarted andalso stop_retainer(State),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

call(Req) ->
    gen_server:call(?MODULE, Req, infinity).

get_stop_publish_clear_msg() ->
    emqx_conf:get([retainer, stop_publish_clear_msg], false).

-spec new_state() -> state().
new_state() ->
    #{
        is_started => false,
        clear_timer => undefined
    }.

is_enabled_for_zone(ZoneConfig) ->
    emqx_utils_maps:deep_get([mqtt, retain_available], ZoneConfig, false).

-spec start_clear_expired() -> ok.
start_clear_expired() ->
    Opts = #{
        deadline => erlang:system_time(millisecond),
        limit => emqx_conf:get([retainer, msg_clear_limit], all)
    },
    _Result = emqx_retainer_sup:start_gc(context(), Opts),
    ok.

-spec update_config(state(), hocon:config(), hocon:config()) -> state().
update_config(
    #{clear_timer := ClearTimer} = State,
    NewConfig,
    OldConfig
) ->
    #{msg_clear_interval := ClearInterval} = NewConfig,
    OldBackendConfig = enabled_backend_config(OldConfig),
    NewBackendConfig = enabled_backend_config(NewConfig),
    OldMod = config_backend_module(OldBackendConfig),
    NewMod = config_backend_module(NewBackendConfig),

    SameBackendType = NewMod =:= OldMod,
    case SameBackendType andalso ok =:= OldMod:update(context(), NewBackendConfig) of
        true ->
            State#{
                clear_timer := update_timer(
                    ClearTimer,
                    ClearInterval,
                    clear_expired
                )
            };
        false ->
            State2 = stop_retainer(State),
            start_retainer(State2, NewConfig, NewBackendConfig)
    end.

-spec update_status(state(), boolean(), boolean()) -> state().
update_status(State0, From, To) ->
    State = do_update_status(State0, From, To),
    ?tp(retainer_status_updated, #{from => From, to => To}),
    State.

do_update_status(State, Status, Status) ->
    State;
do_update_status(State, false, true) ->
    start_retainer(State);
do_update_status(State, true, false) ->
    stop_retainer(State).

-spec start_retainer(state()) -> state().
start_retainer(State) ->
    RetainerConfig = emqx:get_config([retainer]),
    BackendConfig = enabled_backend_config(RetainerConfig),
    start_retainer(State, RetainerConfig, BackendConfig).

-spec start_retainer(state(), hocon:config(), hocon:config()) -> state().
start_retainer(
    State,
    #{msg_clear_interval := ClearInterval} = _RetainerConfig,
    BackendConfig
) ->
    ok = create_context(BackendConfig),
    ok = emqx_retainer_sup:start_workers(),
    ok = load_hooks(),
    State#{
        is_started := true,
        clear_timer := maybe_start_timer(ClearInterval, clear_expired)
    }.

-spec stop_retainer(state()) -> state().
stop_retainer(
    #{
        clear_timer := ClearTimer
    } = State
) ->
    ok = unload_hooks(),
    ok = emqx_retainer_sup:stop_workers(),
    ok = close_context(),
    State#{
        is_started := false,
        clear_timer := stop_timer(ClearTimer)
    }.

-spec stop_timer(undefined | reference()) -> undefined.
stop_timer(undefined) ->
    undefined;
stop_timer(TimerRef) ->
    _ = emqx_utils:cancel_timer(TimerRef),
    undefined.

maybe_start_timer(0, _) ->
    undefined;
maybe_start_timer(undefined, _) ->
    undefined;
maybe_start_timer(Ms, Content) ->
    start_timer(Ms, Content).

start_timer(Ms, Content) ->
    emqx_utils:start_timer(Ms, self(), Content).

update_timer(undefined, Ms, Context) ->
    maybe_start_timer(Ms, Context);
update_timer(Timer, 0, _) ->
    stop_timer(Timer);
update_timer(Timer, undefined, _) ->
    stop_timer(Timer);
update_timer(Timer, _, _) ->
    Timer.

-spec enabled_backend_config(hocon:config()) -> hocon:config() | no_return().
enabled_backend_config(#{backend := Backend, external_backends := ExternalBackends} = Config) ->
    AllBackends = [Backend | maps:values(ExternalBackends)],
    case lists:search(fun(#{enable := Enable}) -> Enable end, AllBackends) of
        {value, EnabledBackend} -> EnabledBackend;
        false -> error({no_enabled_backend, Config})
    end.

-spec config_backend_module(hocon:config()) -> module().
config_backend_module(Config) ->
    case Config of
        #{type := built_in_database} -> emqx_retainer_mnesia;
        #{module := Module} -> Module
    end.

-spec backend_module(context()) -> module() | undefined.
backend_module(#{module := Module}) -> Module;
backend_module(undefined) -> undefined.

-spec backend_state(context()) -> backend_state().
backend_state(#{state := State}) -> State.

-spec backend_module() -> module() | no_return().
backend_module() ->
    Config = enabled_backend_config(emqx:get_config([retainer])),
    config_backend_module(Config).

-spec create_context(hocon:config()) -> ok.
create_context(Cfg) ->
    Mod = config_backend_module(Cfg),
    Context = #{
        module => Mod,
        state => Mod:create(Cfg)
    },
    _ = persistent_term:put(?CONTEXT_KEY, Context),
    ok.

-spec close_context() -> ok | {error, term()}.
close_context() ->
    try
        with_backend(fun(Mod, BackendState) -> Mod:close(BackendState) end)
    after
        persistent_term:erase(?CONTEXT_KEY)
    end.

-spec load_hooks() -> ok.
load_hooks() ->
    ok = emqx_hooks:put(
        'session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_RETAINER
    ),
    ok = emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER),
    emqx_stats:update_interval(emqx_retainer_stats, fun ?MODULE:stats_fun/0),
    ok.

-spec unload_hooks() -> ok.
unload_hooks() ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok = emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_stats:cancel_update(emqx_retainer_stats),
    ok.
