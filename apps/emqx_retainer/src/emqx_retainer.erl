%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-export([start_link/0]).

-export([
    on_session_subscribed/4,
    on_message_publish/2
]).

-export([
    delete_message/2,
    store_retained/2
]).

-export([
    get_expiry_time/1,
    update_config/1,
    clean/0,
    delete/1,
    read_message/1,
    page_read/3,
    page_read/4,
    post_config_update/5,
    stats_fun/0,
    retained_count/0,
    backend_module/0,
    backend_module/1,
    backend_state/1,
    enabled/0
]).

%% For testing only
-export([
    context/0
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
    enable := boolean(),
    context := undefined | context(),
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

-define(DEF_MAX_PAYLOAD_SIZE, (1024 * 1024)).
-define(DEF_EXPIRY_INTERVAL, 0).
-define(MAX_PAYLOAD_SIZE_CONFIG_PATH, [retainer, max_payload_size]).

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

%%------------------------------------------------------------------------------
%% Hook API
%%------------------------------------------------------------------------------
-spec on_session_subscribed(_, _, emqx_types:subopts(), _) -> any().
on_session_subscribed(_, #share{} = _Topic, _SubOpts, _) ->
    ok;
on_session_subscribed(_, Topic, #{rh := Rh} = Opts, Context) ->
    IsNew = maps:get(is_new, Opts, true),
    case Rh =:= 0 orelse (Rh =:= 1 andalso IsNew) of
        true -> dispatch(Context, Topic);
        _ -> ok
    end.

%% RETAIN flag set to 1 and payload containing zero bytes
on_message_publish(
    Msg = #message{
        flags = #{retain := true},
        topic = Topic,
        payload = <<>>
    },
    Context
) ->
    delete_message(Context, Topic),
    case get_stop_publish_clear_msg() of
        true ->
            {ok, emqx_message:set_header(allow_publish, false, Msg)};
        _ ->
            {ok, Msg}
    end;
on_message_publish(Msg = #message{flags = #{retain := true}}, Context) ->
    Msg1 = emqx_message:set_header(retained, true, Msg),
    store_retained(Context, Msg1),
    {ok, Msg};
on_message_publish(Msg, _) ->
    {ok, Msg}.

%%------------------------------------------------------------------------------
%% Config API
%%------------------------------------------------------------------------------

post_config_update(_, _UpdateReq, NewConf, OldConf, _AppEnvs) ->
    call({update_config, NewConf, OldConf}).

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
    Interval = emqx_conf:get([retainer, msg_expiry_interval], ?DEF_EXPIRY_INTERVAL),
    case Interval of
        0 -> 0;
        _ -> Ts + Interval
    end.

-spec update_config(hocon:config()) -> {ok, _} | {error, _}.
update_config(Conf) ->
    emqx_conf:update([retainer], Conf, #{override_to => cluster}).

-spec clean() -> ok.
clean() ->
    call(?FUNCTION_NAME).

-spec delete(topic()) -> ok.
delete(Topic) ->
    call({?FUNCTION_NAME, Topic}).

-spec retained_count() -> non_neg_integer().
retained_count() ->
    call(?FUNCTION_NAME).

-spec read_message(topic()) -> {ok, list(message())}.
read_message(Topic) ->
    call({?FUNCTION_NAME, Topic}).

-spec page_read(emqx_maybe:t(topic()), non_neg_integer(), non_neg_integer()) ->
    {ok, has_next(), list(message())}.
page_read(Topic, Page, Limit) ->
    page_read(Topic, erlang:system_time(millisecond), Page, Limit).

-spec page_read(emqx_maybe:t(topic()), deadline(), non_neg_integer(), non_neg_integer()) ->
    {ok, has_next(), list(message())}.
page_read(Topic, Deadline, Page, Limit) ->
    call({?FUNCTION_NAME, Topic, Deadline, Page, Limit}).

-spec enabled() -> boolean().
enabled() ->
    call(?FUNCTION_NAME).

-spec context() -> ok.
context() ->
    call(?FUNCTION_NAME).

%%------------------------------------------------------------------------------
%% Internal APIs
%%------------------------------------------------------------------------------

stats_fun() ->
    gen_server:cast(?MODULE, ?FUNCTION_NAME).

-spec get_basic_usage_info() -> #{retained_messages => non_neg_integer()}.
get_basic_usage_info() ->
    try
        #{retained_messages => retained_count()}
    catch
        _:_ ->
            #{retained_messages => 0}
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    erlang:process_flag(trap_exit, true),
    emqx_conf:add_handler([retainer], ?MODULE),
    State = new_state(),
    RetainerConfig = emqx:get_config([retainer]),
    {ok,
        case maps:get(enable, RetainerConfig) of
            false ->
                State;
            true ->
                BackendConfig = enabled_backend_config(RetainerConfig),
                enable_retainer(State, RetainerConfig, BackendConfig)
        end}.

handle_call({update_config, NewConf, OldConf}, _, State) ->
    State2 = update_config(State, NewConf, OldConf),
    emqx_retainer_dispatcher:refresh_limiter(NewConf),
    {reply, ok, State2};
handle_call(clean, _, #{context := Context} = State) ->
    _ = clean(Context),
    {reply, ok, State};
handle_call({delete, Topic}, _, #{context := Context} = State) ->
    _ = delete_message(Context, Topic),
    {reply, ok, State};
handle_call({read_message, Topic}, _, #{context := Context} = State) ->
    {reply, read_message(Context, Topic), State};
handle_call({page_read, Topic, Deadline, Page, Limit}, _, #{context := Context} = State) ->
    {reply, page_read(Context, Topic, Deadline, Page, Limit), State};
handle_call(retained_count, _From, State = #{context := Context}) ->
    {reply, count(Context), State};
handle_call(enabled, _From, State = #{enable := Enable}) ->
    {reply, Enable, State};
handle_call(context, _From, State = #{context := Context}) ->
    {reply, Context, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(stats_fun, #{context := Context} = State) ->
    emqx_stats:setstat('retained.count', 'retained.max', count(Context)),
    {noreply, State};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, TRef, clear_expired}, #{context := Context, clear_timer := TRef} = State) ->
    ok = start_clear_expired(Context),
    Interval = emqx_conf:get([retainer, msg_clear_interval], ?DEF_EXPIRY_INTERVAL),
    {noreply, State#{clear_timer := maybe_start_timer(Interval, clear_expired)}, hibernate};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{clear_timer := ClearTimer}) ->
    emqx_conf:remove_handler([retainer]),
    _ = stop_timer(ClearTimer),
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
        enable => false,
        context => undefined,
        clear_timer => undefined
    }.

payload_size_limit() ->
    emqx_conf:get(?MAX_PAYLOAD_SIZE_CONFIG_PATH, ?DEF_MAX_PAYLOAD_SIZE).

dispatch(Context, Topic) ->
    emqx_retainer_dispatcher:dispatch(Context, Topic).

-spec delete_message(context(), topic()) -> ok.
delete_message(Context, Topic) ->
    with_backend_module(
        Context,
        fun(Mod, BackendState) ->
            Mod:delete_message(BackendState, Topic)
        end,
        ok
    ).

-spec read_message(context(), topic()) -> {ok, list(message())}.
read_message(Context, Topic) ->
    with_backend_module(
        Context,
        fun(Mod, BackendState) -> Mod:read_message(BackendState, Topic) end,
        {ok, []}
    ).

-spec page_read(context(), emqx_maybe:t(topic()), deadline(), non_neg_integer(), non_neg_integer()) ->
    {ok, has_next(), list(message())}.
page_read(Context, Topic, Deadline, Page, Limit) ->
    with_backend_module(
        Context,
        fun(Mod, BackendState) -> Mod:page_read(BackendState, Topic, Deadline, Page, Limit) end,
        {ok, false, []}
    ).

-spec count(context()) -> non_neg_integer().
count(Context) ->
    with_backend_module(Context, fun(Mod, BackendState) -> Mod:size(BackendState) end, 0).

with_backend_module(Context, Fun, Default) ->
    case backend_module(Context) of
        undefined ->
            Default;
        Mod ->
            BackendState = backend_state(Context),
            Fun(Mod, BackendState)
    end.

-spec start_clear_expired(context()) -> ok.
start_clear_expired(Context) ->
    Opts = #{
        deadline => erlang:system_time(millisecond),
        limit => emqx_conf:get([retainer, msg_clear_limit], all)
    },
    _Result = emqx_retainer_sup:start_gc(Context, Opts),
    ok.

-spec store_retained(context(), message()) -> ok.
store_retained(Context, #message{topic = Topic, payload = Payload} = Msg) ->
    Size = iolist_size(Payload),
    case payload_size_limit() of
        Limit when Limit > 0 andalso Limit < Size ->
            ?SLOG(error, #{
                msg => "retain_failed_for_payload_size_exceeded_limit",
                topic => Topic,
                config => emqx_hocon:format_path(?MAX_PAYLOAD_SIZE_CONFIG_PATH),
                size => Size,
                limit => Limit
            });
        _ ->
            with_backend_module(
                Context,
                fun(Mod, BackendState) -> Mod:store_retained(BackendState, Msg) end,
                ok
            )
    end.

-spec clean(context()) -> ok.
clean(Context) ->
    with_backend_module(Context, fun(Mod, BackendState) -> Mod:clean(BackendState) end, ok).

-spec update_config(state(), hocon:config(), hocon:config()) -> state().
update_config(State, NewConfig, OldConfig) ->
    update_config(
        maps:get(enable, NewConfig),
        maps:get(enable, OldConfig),
        State,
        NewConfig,
        OldConfig
    ).

-spec update_config(boolean(), boolean(), state(), hocon:config(), hocon:config()) -> state().
update_config(false, _, State, _, _) ->
    disable_retainer(State);
update_config(true, false, State, NewConfig, _) ->
    enable_retainer(State, NewConfig, enabled_backend_config(NewConfig));
update_config(
    true,
    true,
    #{clear_timer := ClearTimer, context := Context} = State,
    NewConfig,
    OldConfig
) ->
    #{msg_clear_interval := ClearInterval} = NewConfig,
    OldBackendConfig = enabled_backend_config(OldConfig),
    NewBackendConfig = enabled_backend_config(NewConfig),
    OldMod = config_backend_module(OldBackendConfig),
    NewMod = config_backend_module(NewBackendConfig),

    SameBackendType = NewMod =:= OldMod,
    case SameBackendType andalso ok =:= OldMod:update(Context, NewBackendConfig) of
        true ->
            State#{
                clear_timer := update_timer(
                    ClearTimer,
                    ClearInterval,
                    clear_expired
                )
            };
        false ->
            State2 = disable_retainer(State),
            enable_retainer(State2, NewConfig, NewBackendConfig)
    end.

-spec enable_retainer(state(), hocon:config(), hocon:config()) -> state().
enable_retainer(
    State,
    #{msg_clear_interval := ClearInterval} = _RetainerConfig,
    BackendConfig
) ->
    Context = create(BackendConfig),
    ok = load(Context),
    State#{
        enable := true,
        context := Context,
        clear_timer := maybe_start_timer(ClearInterval, clear_expired)
    }.

-spec disable_retainer(state()) -> state().
disable_retainer(
    #{
        clear_timer := ClearTimer,
        context := Context
    } = State
) ->
    ok = unload(),
    ok = close(Context),
    State#{
        enable := false,
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

-spec create(hocon:config()) -> context().
create(Cfg) ->
    Mod = config_backend_module(Cfg),
    #{
        module => Mod,
        state => Mod:create(Cfg)
    }.

-spec close(context()) -> ok | {error, term()}.
close(Context) ->
    with_backend_module(Context, fun(Mod, BackendState) -> Mod:close(BackendState) end, ok).

-spec load(context()) -> ok.
load(Context) ->
    ok = emqx_hooks:put(
        'session.subscribed', {?MODULE, on_session_subscribed, [Context]}, ?HP_RETAINER
    ),
    ok = emqx_hooks:put('message.publish', {?MODULE, on_message_publish, [Context]}, ?HP_RETAINER),
    emqx_stats:update_interval(emqx_retainer_stats, fun ?MODULE:stats_fun/0),
    ok.

unload() ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok = emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_stats:cancel_update(emqx_retainer_stats),
    ok.
