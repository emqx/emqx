%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams).

-moduledoc """
The module is responsible for integrating the Streams application into the EMQX core.

The write part is integrated via registering `message.publish` hook.
The read part is integrated via registering an ExtSub handler.
""".

-include("emqx_streams_internal.hrl").

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_message_publish_stream/1
]).

%%

-spec register_hooks() -> ok.
register_hooks() ->
    ok = register_stream_hooks(),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = unregister_stream_hooks(),
    ok.

-spec register_stream_hooks() -> ok.
register_stream_hooks() ->
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish_stream, []}, ?HP_HIGHEST),
    ok = emqx_extsub_handler_registry:register(emqx_streams_extsub_handler, #{
        handle_generic_messages => true,
        multi_topic => true
    }).
-spec unregister_stream_hooks() -> ok.
unregister_stream_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish_stream}),
    emqx_extsub_handler_registry:unregister(emqx_streams_extsub_handler).

%%

on_message_publish_stream(#message{topic = Topic} = Message) ->
    ?tp_debug(streams_on_message_publish_stream, #{topic => Topic}),
    Streams = emqx_streams_registry:match(Topic),
    ok = lists:foreach(
        fun(Stream) ->
            {Time, Result} = timer:tc(fun() -> publish_to_stream(Stream, Message) end),
            case Result of
                ok ->
                    emqx_streams_metrics:inc(ds, inserted_messages),
                    ?tp_debug(streams_on_message_publish_to_queue, #{
                        topic_filter => emqx_streams_prop:topic_filter(Stream),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        result => ok
                    });
                {error, Reason} ->
                    ?tp(error, streams_on_message_publish_queue_error, #{
                        topic_filter => emqx_streams_prop:topic_filter(Stream),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        reason => Reason
                    })
            end
        end,
        Streams
    ),
    {ok, Message}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

publish_to_stream(Stream, #message{} = Message) ->
    emqx_streams_message_db:insert(Stream, Message).
