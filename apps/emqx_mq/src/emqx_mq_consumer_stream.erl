%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_stream).

-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/2
]).

-moduledoc """
The module represents a consumer of a single stream of the Message Queue data.
""".

new(Stream, MQTopic) ->
    {ok, Iterator} = emqx_ds:make_iterator(
        ?MQ_PAYLOAD_DB, Stream, ?MQ_PAYLOAD_DB_TOPIC(MQTopic, '#'), 0
    ),
    {ok, SubHandle, SubRef} = emqx_ds:subscribe(?MQ_PAYLOAD_DB, Iterator, #{
        max_unacked => ?MQ_CONSUMER_MAX_WINDOW
    }),
    {ok, SubRef, #{
        stream => Stream,
        mq_topic => MQTopic,
        sub_handle => SubHandle,
        sub_ref => SubRef
    }}.
