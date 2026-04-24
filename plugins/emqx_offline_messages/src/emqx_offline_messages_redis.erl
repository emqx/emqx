%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_redis).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-include("emqx_offline_messages.hrl").

-export([
    on_config_changed/2,
    on_health_check/1
]).

-export([
    on_client_connected/3,
    on_session_subscribed/4,
    on_session_unsubscribed/4,
    on_message_publish/2,
    on_message_acked/3
]).

-define(RESOURCE_ID, <<"offline_messages_redis">>).
-define(RESOURCE_GROUP, <<"omp">>).
-define(FETCH_MSG_BATCH_SIZE, 100).

-define(DEFAULT_MESSAGE_TTL, 7200).
-define(DEFAULT_SUBSCRIPTION_KEY_PREFIX, <<"mqtt:sub">>).
-define(DEFAULT_MESSAGE_KEY_PREFIX, <<"mqtt:msg">>).

-type context() :: map().

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec on_config_changed(map(), map()) -> ok.
on_config_changed(#{<<"enable">> := false}, #{<<"enable">> := false}) ->
    ok;
on_config_changed(#{<<"enable">> := true} = Conf, #{<<"enable">> := true} = Conf) ->
    ok;
on_config_changed(#{<<"enable">> := true} = _OldConf, #{<<"enable">> := true} = NewConf) ->
    ok = stop(),
    ok = start(NewConf);
on_config_changed(#{<<"enable">> := true} = _OldConf, #{<<"enable">> := false} = _NewConf) ->
    ok = stop();
on_config_changed(#{<<"enable">> := false} = _OldConf, #{<<"enable">> := true} = NewConf) ->
    ok = start(NewConf).

-spec on_health_check(map()) -> ok | {error, binary()}.
on_health_check(#{<<"enable">> := false}) ->
    ok;
on_health_check(#{<<"enable">> := true}) ->
    emqx_offline_messages_utils:resource_health_status(<<"Redis">>, ?RESOURCE_ID).

%%--------------------------------------------------------------------
%% start/stop
%%--------------------------------------------------------------------

-spec stop() -> ok.
stop() ->
    unhook(),
    ok = stop_resource().

-spec start(map()) -> ok.
start(ConfigRaw) ->
    ?SLOG(info, #{msg => offline_messages_redis_start, config => ConfigRaw}),
    {RedisConfig, ResourceOpts} = make_redis_resource_config(ConfigRaw),
    ok = start_resource(RedisConfig, ResourceOpts),

    Context = #{
        message_key_prefix => maps:get(
            <<"message_key_prefix">>, ConfigRaw, ?DEFAULT_MESSAGE_KEY_PREFIX
        ),
        subscription_key_prefix => maps:get(
            <<"subscription_key_prefix">>, ConfigRaw, ?DEFAULT_SUBSCRIPTION_KEY_PREFIX
        ),
        message_ttl => maps:get(
            <<"message_ttl">>, ConfigRaw, ?DEFAULT_MESSAGE_TTL
        ),
        topic_filters => emqx_offline_messages_utils:topic_filters(ConfigRaw)
    },
    hook(Context).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

on_client_connected(
    #{clientid := ClientId} = _ClientInfo,
    _ConnInfo,
    Context
) ->
    Cmd = [<<"HGETALL">>, sub_table(Context, ClientId)],
    _ =
        case sync_cmd(Cmd) of
            {ok, Hash} ->
                Subscriptions = to_subscriptions(Hash),
                ok = emqx_offline_messages_utils:induce_subscriptions(Subscriptions),
                ok;
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "offline_messages_redis_client_connected_error",
                    clientid => ClientId,
                    reason => Reason
                })
        end,
    ok.

on_session_subscribed(
    #{clientid := ClientId} = _ClientInfo,
    Topic,
    SubOpts,
    Context
) ->
    ?SLOG(info, #{
        msg => offline_messages_redis_session_subscribed,
        clientid => ClientId,
        topic => Topic,
        subopts => SubOpts
    }),
    ok = insert_subscription(ClientId, Topic, SubOpts, Context),
    ok = fetch_and_deliver_messages(Topic, Context).

insert_subscription(
    ClientId, Topic, SubOpts, Context
) ->
    QoS = maps:get(qos, SubOpts, 0),
    Cmd = [<<"HSET">>, sub_table(Context, ClientId), Topic, QoS],
    _ =
        case sync_cmd(Cmd) of
            {ok, _} ->
                ok;
            {error, Reason} ->
                ?SLOG(error, #{
                    msg => "offline_messages_redis_insert_subscription_error",
                    reason => Reason
                })
        end,
    ok.

fetch_and_deliver_messages(Topic, Context) ->
    case fetch_message_ids(Topic, Context) of
        {ok, MsgIds} ->
            Messages = fetch_messages(MsgIds, Context),
            ok = emqx_offline_messages_utils:deliver_messages(Topic, Messages),
            emqx_metrics_worker:inc(?METRICS_WORKER, session_subscribed, success);
        {error, Reason} ->
            emqx_metrics_worker:inc(?METRICS_WORKER, session_subscribed, fail),
            ?SLOG(error, #{
                msg => "offline_messages_redis_fetch_message_ids_error",
                reason => Reason
            })
    end.

fetch_message_ids(Topic, #{message_ttl := TTL} = Context) ->
    MsgTab = msg_table(Context, Topic),
    case sync_cmd([<<"ZRANGE">>, MsgTab, 0, -1, <<"WITHSCORES">>]) of
        {ok, RawMsgIds} ->
            ?SLOG(debug, #{
                msg => "offline_messages_redis_fetch_message_ids",
                raw_msg_ids => RawMsgIds
            }),
            MsgIdsWithCreatedTS = parse_msg_ids(RawMsgIds),
            Now = erlang:system_time(millisecond),
            Deadline = Now - erlang:convert_time_unit(TTL, second, millisecond),
            %% NOTE
            %% The MsgIds here are base62 encoded
            MsgIds = [
                MsgId
             || {MsgId, CreatedTS} <- MsgIdsWithCreatedTS, CreatedTS > Deadline
            ],
            ?SLOG(debug, #{
                msg => "offline_messages_redis_fetch_message_ids_parsed",
                msg_ids => MsgIds
            }),
            {ok, MsgIds};
        {error, _} = Error ->
            Error
    end.

parse_msg_ids([MsgId, TS | KVs]) ->
    [{MsgId, parse_zscore(TS)} | parse_msg_ids(KVs)];
parse_msg_ids([]) ->
    [].

parse_zscore(ZScore) when is_binary(ZScore) ->
    try
        binary_to_float(ZScore)
    catch
        error:badarg ->
            binary_to_integer(ZScore)
    end.

fetch_messages(MsgIds, Context) ->
    fetch_messages(MsgIds, [], [], Context).

fetch_messages([], [], Acc, _Context) ->
    lists:reverse(Acc);
fetch_messages(MsgIds, MsgIdBatchAcc, Acc0, Context) when
    length(MsgIdBatchAcc) >= ?FETCH_MSG_BATCH_SIZE orelse
        (length(MsgIds) =:= 0 andalso length(MsgIdBatchAcc) > 0)
->
    Cmds = [
        [<<"HGETALL">>, msg_table(Context, MsgId)]
     || MsgId <- MsgIdBatchAcc
    ],
    case sync_cmds(Cmds) of
        {ok, Results} ->
            Acc = append_results(lists:reverse(Results), Acc0),
            fetch_messages(MsgIds, [], Acc, Context);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "offline_messages_redis_fetch_messages_error",
                reason => Reason
            }),
            fetch_messages(MsgIds, [], Acc0, Context)
    end;
fetch_messages([MsgId | MsgIds], MsgIdBatchAcc, Acc, Context) ->
    fetch_messages(MsgIds, [MsgId | MsgIdBatchAcc], Acc, Context).

append_results([], Acc) ->
    Acc;
append_results([{ok, []} | Results], Acc) ->
    append_results(Results, Acc);
append_results([{ok, Hash} | Results], Acc) ->
    append_results(Results, [hash_to_message(Hash) | Acc]);
append_results([{error, Reason} | Results], Acc) ->
    ?SLOG(warning, #{
        msg => "offline_messages_redis_fetch_message_error",
        reason => Reason
    }),
    append_results(Results, Acc).

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, Context) ->
    ?SLOG(info, #{
        msg => offline_messages_redis_session_unsubscribed,
        clientid => ClientId,
        topic => Topic,
        opts => Opts
    }),
    ok = delete_subscription(ClientId, Topic, Context).

delete_subscription(ClientId, Topic, Context) ->
    Cmd = [<<"HDEL">>, sub_table(Context, ClientId), Topic],
    case sync_cmd(Cmd) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "offline_messages_redis_delete_subscription_error",
                topic => Topic,
                reason => Reason
            }),
            ok
    end.

on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Context) ->
    {ok, Message};
on_message_publish(Message, #{message_ttl := TTL, topic_filters := TopicFilters} = Context) ->
    _ =
        case emqx_offline_messages_utils:need_persist_message(Message, TopicFilters) of
            false ->
                ?SLOG(debug, #{
                    msg => offline_messages_redis_message_publish_qos0,
                    message => Message
                });
            true ->
                Topic = emqx_message:topic(Message),
                MsgId = emqx_message:id(Message),
                Now =
                    erlang:system_time(native) /
                        erlang:convert_time_unit(1, millisecond, native),
                Deadline = Now - erlang:convert_time_unit(TTL, second, millisecond),
                MsgIDb62 = to_b62(MsgId),
                Cmds = [
                    [<<"HMSET">>, msg_table(Context, MsgIDb62)] ++ message_to_hash(Message),
                    [<<"ZADD">>, msg_table(Context, Topic), float_to_binary(Now), MsgIDb62],
                    [<<"EXPIRE">>, msg_table(Context, MsgIDb62), TTL],
                    [
                        <<"ZREMRANGEBYSCORE">>,
                        msg_table(Context, Topic),
                        <<"-inf">>,
                        float_to_binary(Deadline)
                    ]
                ],
                case sync_cmds(Cmds) of
                    {ok, _} ->
                        ok;
                    {error, Reason} ->
                        ?SLOG(error, #{
                            msg => "offline_messages_redis_message_publish_error",
                            reason => Reason
                        })
                end
        end,
    {ok, Message}.

on_message_acked(
    _ClientInfo,
    #message{id = MsgId} = Message,
    Context
) ->
    Topic = emqx_message:topic(Message),
    MsgId = emqx_message:id(Message),
    MsgIDb62 = to_b62(MsgId),
    Cmds = [
        [<<"DEL">>, msg_table(Context, MsgIDb62)],
        [<<"ZREM">>, msg_table(Context, Topic), MsgIDb62]
    ],
    case sync_cmds(Cmds) of
        {ok, _} ->
            emqx_metrics_worker:inc(?METRICS_WORKER, message_acked, success);
        {error, Reason} ->
            emqx_metrics_worker:inc(?METRICS_WORKER, message_acked, fail),
            ?SLOG(error, #{
                msg => "offline_messages_redis_message_puback_error",
                reason => Reason
            })
    end,
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Message helpers

message_to_hash(#message{
    id = MsgId,
    from = From,
    qos = QoS,
    topic = Topic,
    payload = Payload,
    timestamp = Ts,
    flags = #{retain := Retain}
}) ->
    [
        <<"id">>,
        to_b62(MsgId),
        <<"from">>,
        format_from(From),
        <<"qos">>,
        QoS,
        <<"topic">>,
        Topic,
        <<"payload">>,
        Payload,
        <<"ts">>,
        Ts,
        <<"retain">>,
        atom_to_binary(Retain, utf8)
    ].

hash_to_message(KVs) ->
    hash_to_message(KVs, #message{}).

hash_to_message([<<"id">>, MsgId | KVs], Acc) ->
    hash_to_message(KVs, Acc#message{id = from_b62(MsgId)});
hash_to_message([<<"from">>, From | KVs], Acc) ->
    hash_to_message(KVs, Acc#message{from = From});
hash_to_message([<<"qos">>, QoS | KVs], Acc) ->
    hash_to_message(KVs, Acc#message{qos = binary_to_integer(QoS)});
hash_to_message([<<"topic">>, Topic | KVs], Acc) ->
    hash_to_message(KVs, Acc#message{topic = Topic});
hash_to_message([<<"payload">>, Payload | KVs], Acc) ->
    hash_to_message(KVs, Acc#message{payload = Payload});
hash_to_message([<<"ts">>, Ts | KVs], Acc) ->
    hash_to_message(KVs, Acc#message{timestamp = binary_to_integer(Ts)});
hash_to_message([<<"retain">>, Retain | KVs], Acc) ->
    hash_to_message(KVs, Acc#message{flags = #{retain => binary_to_bool(Retain)}});
hash_to_message([_K, _V | KVs], Acc) ->
    hash_to_message(KVs, Acc);
hash_to_message([], Acc) ->
    Acc.

format_from(From) when is_atom(From) ->
    atom_to_binary(From, utf8);
format_from(From) ->
    From.

%% Subscription helpers

to_subscriptions([Topic, QoSBin | KVs]) ->
    [{Topic, #{qos => binary_to_integer(QoSBin)}} | to_subscriptions(KVs)];
to_subscriptions([]) ->
    [].

%% Resource helpers

make_redis_resource_config(ConfigRaw0) ->
    RedisConfigRaw0 = maps:with(
        [
            <<"servers">>,
            <<"sentinel">>,
            <<"redis_type">>,
            <<"pool_size">>,
            <<"username">>,
            <<"password">>,
            <<"database">>,
            <<"ssl">>
        ],
        ConfigRaw0
    ),

    RedisConfigRaw1 = fix_servers(RedisConfigRaw0),
    RedisConfigRaw2 = drop_unused_fields(RedisConfigRaw1),
    RedisConfigRaw = maybe_drop_database_field(RedisConfigRaw2),

    ?SLOG(info, #{
        msg => offline_messages_redis_make_redis_resource_config, config => RedisConfigRaw
    }),
    RedisConfig = emqx_offline_messages_utils:check_config(emqx_redis, RedisConfigRaw),
    ResourceOpts = emqx_offline_messages_utils:make_resource_opts(ConfigRaw0),

    {RedisConfig, ResourceOpts}.

fix_servers(#{<<"servers">> := Servers, <<"redis_type">> := <<"single">>} = RawConfig0) ->
    RawConfig1 = maps:without([<<"servers">>, <<"sentinel">>], RawConfig0),
    RawConfig1#{<<"server">> => Servers};
fix_servers(#{<<"redis_type">> := <<"cluster">>} = RawConfig) ->
    maps:without([<<"sentinel">>], RawConfig);
fix_servers(#{<<"redis_type">> := <<"sentinel">>} = RawConfig) ->
    RawConfig.

drop_unused_fields(RawConfig) ->
    lists:foldl(
        fun(Key, Acc) ->
            case Acc of
                #{Key := <<>>} ->
                    maps:remove(Key, Acc);
                _ ->
                    Acc
            end
        end,
        RawConfig,
        [<<"pool_size">>, <<"username">>, <<"password">>, <<"database">>, <<"sentinel">>]
    ).

maybe_drop_database_field(#{<<"redis_type">> := <<"cluster">>} = RawConfig) ->
    maps:remove(<<"database">>, RawConfig);
maybe_drop_database_field(RawConfig) ->
    RawConfig.

start_resource(RedisConfig, ResourceOpts) ->
    ?SLOG(info, #{
        msg => offline_messages_redis_resource_start,
        config => RedisConfig,
        resource_opts => ResourceOpts,
        resource_id => ?RESOURCE_ID,
        resource_group => ?RESOURCE_GROUP
    }),
    {ok, _} = emqx_resource:create_local(
        ?RESOURCE_ID,
        ?RESOURCE_GROUP,
        emqx_offline_messages_redis_connector,
        RedisConfig,
        ResourceOpts
    ),
    ok.

stop_resource() ->
    ?SLOG(info, #{
        msg => offline_messages_redis_resource_stop,
        resource_id => ?RESOURCE_ID
    }),
    ok = emqx_resource:remove_local(?RESOURCE_ID).

sync_cmd(Cmd) ->
    emqx_resource:simple_sync_query(?RESOURCE_ID, {cmd, Cmd}).

sync_cmds(Cmds) ->
    emqx_resource:simple_sync_query(?RESOURCE_ID, {cmds, Cmds}).

%% Hook helpers

unhook() ->
    unhook('client.connected', {?MODULE, on_client_connected}),
    unhook('session.subscribed', {?MODULE, on_session_subscribed}),
    unhook('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    unhook('message.publish', {?MODULE, on_message_publish}),
    unhook('message.acked', {?MODULE, on_message_acked}).

-spec hook(context()) -> ok.
hook(Context) ->
    hook('client.connected', {?MODULE, on_client_connected, [Context]}),
    hook('session.subscribed', {?MODULE, on_session_subscribed, [Context]}),
    hook('session.unsubscribed', {?MODULE, on_session_unsubscribed, [Context]}),
    hook('message.publish', {?MODULE, on_message_publish, [Context]}),
    hook('message.acked', {?MODULE, on_message_acked, [Context]}).

hook(HookPoint, MFA) ->
    %% use highest hook priority so this module's callbacks
    %% are evaluated before the default hooks in EMQX
    emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

unhook(HookPoint, MFA) ->
    emqx_hooks:del(HookPoint, MFA).

%% Common helpers

binary_to_bool(<<"true">>) -> true;
binary_to_bool(<<"false">>) -> false.

table(KeyPrefix, Value) -> iolist_to_binary([KeyPrefix, ":", Value]).

msg_table(#{message_key_prefix := KeyPrefix}, Value) ->
    table(KeyPrefix, Value).

sub_table(#{subscription_key_prefix := KeyPrefix}, Value) ->
    table(KeyPrefix, Value).

%% NOTE
%% The double encoding and decoding of the MsgId is kept from v4

to_b62(<<I:128>>) ->
    emqx_base62:encode(integer_to_binary(I)).
from_b62(S) ->
    I = binary_to_integer(emqx_base62:decode(S)),
    <<I:128>>.
