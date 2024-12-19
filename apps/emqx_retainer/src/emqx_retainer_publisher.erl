%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This module uses emqx_htb_limiter to limit the rate of retained messages.
%% emqx_htb_limiter works as follows:
%% * we try to consume tokens
%% * if we hit rate limits, the caller is suspended for at most `max_retry_time` till
%% there are enough tokens.
%% ** if we succeed, within the time limit, we get {ok, Limiter}
%% ** if we fail, we get {drop, Limiter}
%%
%% To avoid overflow and to respect the rate limits, we do the following:
%% * set `max_retry_time` to a 1s (hidden default value)
%% * if we hit rate limits, and failed to consume tokens we drop the operation (delete or store)
%% and drop all the accumulated operations as well.

-module(emqx_retainer_publisher).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/2,
    store_retained/1,
    delete_message/1,
    refresh_limiter/0,
    refresh_limiter/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(POOL, ?PUBLISHER_POOL).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%--------------------------------------------------------------------
%% Constants
%%--------------------------------------------------------------------

-define(DEF_MAX_PAYLOAD_SIZE, (1024 * 1024)).
-define(MAX_PAYLOAD_SIZE_CONFIG_PATH, [retainer, max_payload_size]).
-define(PUBLISHER_LIMITER_FULL_CONFIG_PATH, [retainer, flow_control, publish_limiter]).
-define(PUBLISHER_LIMITER_CONFIG_PATH, [flow_control, publish_limiter]).
-define(PUBLISHER_LIMITER_EXPOSED_CONFIG_PATH, [retainer, publish_rate]).

%% States

-define(unblocked, unblocked).
-define(blocked, blocked).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(store_retained, {
    message :: emqx_types:message()
}).

-record(refresh_limiter, {
    conf :: hocon:config()
}).

-record(delete_message, {
    topic :: binary()
}).

-record(unblock, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec store_retained(emqx_types:message()) -> ok.
store_retained(Message) ->
    Worker = gproc_pool:pick_worker(?POOL, self()),
    gen_server:cast(Worker, #store_retained{message = Message}).

-spec delete_message(binary()) -> ok.
delete_message(Topic) ->
    Worker = gproc_pool:pick_worker(?POOL, self()),
    gen_server:cast(Worker, #delete_message{topic = Topic}).

%% reset the client's limiter after updated the limiter's config
refresh_limiter() ->
    Conf = emqx:get_config([retainer]),
    refresh_limiter(Conf).

refresh_limiter(Conf) ->
    Workers = gproc_pool:active_workers(?POOL),
    lists:foreach(
        fun({_, Pid}) ->
            gen_server:cast(Pid, #refresh_limiter{conf = Conf})
        end,
        Workers
    ).

-spec start_link(atom(), pos_integer()) -> {ok, pid()}.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        []
    ).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    erlang:process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    BucketCfg = emqx:get_config(?PUBLISHER_LIMITER_FULL_CONFIG_PATH, undefined),
    {ok, Limiter} = emqx_limiter_server:connect(?PUBLISHER_LIMITER_ID, internal, BucketCfg),
    {ok, #{pool => Pool, id => Id, limiter => Limiter, state => ?unblocked}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => retainer_publisher_unexpected_call, call => Req}),
    {reply, ignored, State}.

handle_cast(#store_retained{message = #message{topic = Topic}}, #{state := ?blocked} = State) ->
    ?tp(warning, retain_failed_for_rate_exceeded_limit, #{
        reason => blocked,
        topic => Topic,
        config => format_hocon_path(?PUBLISHER_LIMITER_EXPOSED_CONFIG_PATH)
    }),
    {noreply, State};
handle_cast(#store_retained{message = Message}, #{limiter := Limiter} = State0) ->
    State1 = maybe_block(State0, store_retained(Message, Limiter)),
    {noreply, State1};
handle_cast(#delete_message{topic = Topic}, #{state := ?blocked} = State) ->
    ?tp(warning, retained_delete_failed_for_rate_exceeded_limit, #{
        topic => Topic,
        reason => blocked,
        config => format_hocon_path(?PUBLISHER_LIMITER_EXPOSED_CONFIG_PATH)
    }),
    {noreply, State};
handle_cast(#delete_message{topic = Topic}, #{limiter := Limiter} = State0) ->
    State1 = maybe_block(State0, delete_message(Topic, Limiter)),
    {noreply, State1};
handle_cast(#refresh_limiter{conf = Conf}, State) ->
    BucketCfg = emqx_utils_maps:deep_get(?PUBLISHER_LIMITER_CONFIG_PATH, Conf, undefined),
    {ok, Limiter} = emqx_limiter_server:connect(?PUBLISHER_LIMITER_ID, internal, BucketCfg),
    {noreply, State#{limiter := Limiter}};
handle_cast(#unblock{}, State) ->
    {noreply, unblock(State)};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => retainer_publisher_unexpected_cast, cast => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => retainer_publisher_unexpected_info, info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

store_retained(#message{topic = Topic, payload = Payload} = Msg, Limiter0) ->
    Size = iolist_size(Payload),
    case payload_size_limit() of
        PayloadSizeLimit when PayloadSizeLimit > 0 andalso PayloadSizeLimit < Size ->
            ?tp(warning, retain_failed_for_payload_size_exceeded_limit, #{
                topic => Topic,
                config => format_hocon_path(?MAX_PAYLOAD_SIZE_CONFIG_PATH),
                size => Size,
                limit => PayloadSizeLimit
            }),
            {ok, Limiter0};
        _ ->
            case emqx_htb_limiter:consume(1, Limiter0) of
                {ok, Limiter1} ->
                    _ = emqx_retainer:with_backend(
                        fun(Mod, State) -> Mod:store_retained(State, Msg) end
                    ),
                    {ok, Limiter1};
                {drop, Limiter1} ->
                    ?tp(warning, retain_failed_for_rate_exceeded_limit, #{
                        topic => Topic,
                        config => format_hocon_path(?PUBLISHER_LIMITER_EXPOSED_CONFIG_PATH)
                    }),
                    {block, Limiter1}
            end
    end.

delete_message(Topic, Limiter0) ->
    case emqx_htb_limiter:consume(1, Limiter0) of
        {ok, Limiter1} ->
            _ = emqx_retainer:with_backend(
                fun(Mod, State) -> Mod:delete_message(State, Topic) end
            ),
            {ok, Limiter1};
        {drop, Limiter1} ->
            ?tp(info, retained_delete_failed_for_rate_exceeded_limit, #{
                topic => Topic,
                config => format_hocon_path(?PUBLISHER_LIMITER_EXPOSED_CONFIG_PATH)
            }),
            {block, Limiter1}
    end.

payload_size_limit() ->
    emqx_conf:get(?MAX_PAYLOAD_SIZE_CONFIG_PATH, ?DEF_MAX_PAYLOAD_SIZE).

maybe_block(State, {ok, Limiter}) ->
    State#{limiter => Limiter};
maybe_block(State, {block, Limiter}) ->
    gen_server:cast(self(), #unblock{}),
    State#{state => ?blocked, limiter => Limiter}.

unblock(State) ->
    State#{state => ?unblocked}.

format_hocon_path(Path) ->
    iolist_to_binary(emqx_hocon:format_path(Path)).
