%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_utils).

-include_lib("emqx/include/logger.hrl").

-export([
    fix_ssl_config/1,
    make_resource_opts/1,
    check_config/2,
    deliver_messages/2,
    induce_subscriptions/1,
    need_persist_message/2,
    topic_filters/1,
    resource_health_status/2
]).

fix_ssl_config(#{<<"ssl">> := SslConfig0} = RawConfig) ->
    SslConfig = maps:filter(
        fun
            (_K, <<>>) ->
                false;
            (_K, _V) ->
                true
        end,
        SslConfig0
    ),
    RawConfig#{<<"ssl">> => SslConfig};
fix_ssl_config(RawConfig) ->
    RawConfig#{<<"ssl">> => #{<<"enable">> => false}}.

make_resource_opts(RawConfig) ->
    #{
        start_after_created => true,
        batch_size => maps:get(<<"batch_size">>, RawConfig, 1),
        batch_time => maps:get(<<"batch_time">>, RawConfig, 100),
        %% NOTE
        %% There seems to be no point in supporting async query mode
        %% (althought it is supported in v4).
        %%
        %% If we use async, then we save messages with a delay, but a client
        %% acks messages immediately. On ack, an offline message is removed but there is
        %% nothing to remove yet.
        %% After some time, the message will be persisted in the database, and the client
        %% will receive it once more on reconnect.
        query_mode => sync,
        owner_id => <<"omp">>
    }.

check_config(Schema, ConfigRaw) ->
    case
        emqx_hocon:check(
            Schema,
            #{<<"config">> => ConfigRaw},
            #{atom_key => true}
        )
    of
        {ok, #{config := Config}} ->
            Config;
        {error, Reason} ->
            error({invalid_config, Reason})
    end.

deliver_messages(Topic, Messages) ->
    lists:foreach(
        fun(Message) ->
            erlang:send(self(), {deliver, Topic, Message})
        end,
        Messages
    ).

induce_subscriptions([]) ->
    ok;
induce_subscriptions(Subscriptions) ->
    erlang:send(self(), {subscribe, Subscriptions}),
    ok.

topic_filters(ConfigRaw) ->
    TopicFiltersRaw = maps:get(<<"topics">>, ConfigRaw, []),
    [emqx_topic:words(TopicFilterRaw) || TopicFilterRaw <- TopicFiltersRaw].

need_persist_message(Message, TopicFilters) ->
    ?SLOG(debug, #{
        msg => offline_messages_utils_need_persist_message,
        message => emqx_message:to_map(Message),
        topic_filters => TopicFilters,
        topic => emqx_message:topic(Message)
    }),
    is_message_qos_nonzero(Message) andalso does_message_topic_match(Message, TopicFilters).

is_message_qos_nonzero(Message) ->
    emqx_message:qos(Message) =/= 0.

does_message_topic_match(Message, TopicFilters) ->
    Topic = emqx_message:topic(Message),
    lists:any(fun(Filter) -> emqx_topic:match(Topic, Filter) end, TopicFilters).

resource_health_status(Name, ResourceId) ->
    case emqx_resource:health_check(ResourceId) of
        {ok, connected} ->
            ok;
        {ok, OtherStatus} ->
            {error,
                iolist_to_binary(
                    io_lib:format("Resource ~s is not connected, status: ~p", [Name, OtherStatus])
                )};
        {error, Reason} ->
            {error,
                iolist_to_binary(
                    io_lib:format("Resource ~s health check failed: ~p", [Name, Reason])
                )}
    end.
