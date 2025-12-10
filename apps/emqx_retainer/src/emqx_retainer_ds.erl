%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_retainer_ds).

-include("emqx_retainer.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

%% API:
-export([
    ensure_tables/0,
    store_retained/2,
    delete_message/2,
    consume/2,
    dispatch_message/2,
    test_consume/1
]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([consume_handle/0]).

%%================================================================================
%% Type declarations
%%================================================================================

%% FIXME
-type replay_opts() :: map().

-record(handle, {
    options :: replay_opts(),
    tf :: emqx_ds:topic_filter(),
    remaining_streams :: [emqx_ds:stream()],
    sub_ref :: emqx_ds:sub_ref() | undefined,
    sub_handle :: emqx_ds:sub_handle() | undefined
}).

-type consume_handle() :: #handle{}.

%%================================================================================
%% API functions
%%================================================================================

ensure_tables() ->
    DBConf =
        emqx_utils_maps:deep_merge(
            emqx_ds_schema:db_config_retained_messages(),
            #{
                subscriptions => #{n_rt_workers => 0},
                payload_type => ?ds_pt_mqtt
            }
        ),
    ok = emqx_ds:open_db(?DS, DBConf),
    %% We want to receive `end_of_stream' for iteration, so we create
    %% a fictitious generation to act as the "latest".
    ensure_generation().

store_retained(_State, Msg = #message{topic = Topic}) ->
    TTV = {emqx_ds:topic_words(Topic), 1, Msg#message{topic = <<>>, id = <<>>}},
    Ret = emqx_ds:trans(
        #{db => ?DS, shard => {auto, Topic}, generation => active_gen(), sync => true},
        fun() ->
            emqx_ds:tx_write(TTV)
        end
    ),
    case Ret of
        {atomic, _, _} ->
            ok;
        Error ->
            Error
    end.

delete_message(_State, #message{topic = Topic}) ->
    TopicWords = emqx_ds:topic_words(Topic),
    Ret = emqx_ds:trans(
        #{db => ?DS, shard => {auto, Topic}, generation => active_gen(), sync => true},
        fun() ->
            emqx_ds:tx_del_topic(TopicWords)
        end
    ),
    case Ret of
        {atomic, _, _} ->
            ok;
        Error ->
            Error
    end.

consume(Topic, Options) ->
    TopicWords = emqx_ds:topic_words(Topic),
    %% FIXME: currently streams from shards that are temporarily down are ignored.
    {AllStreams, _Errors} = emqx_ds:get_streams(?DS, TopicWords, 0, #{}),
    Streams = lists:filtermap(
        fun({{_Shard, Gen}, Stream}) ->
            Gen =:= active_gen() andalso
                {true, Stream}
        end,
        AllStreams
    ),
    switch_stream(#handle{
        tf = TopicWords,
        options = maps:merge(#{max_unacked => 1000}, Options),
        remaining_streams = Streams
    }).

switch_stream(#handle{remaining_streams = []}) ->
    done;
switch_stream(
    H = #handle{remaining_streams = [Stream | Streams], tf = TopicFilter, options = Opts}
) ->
    %% FIXME: error handling
    {ok, It} = emqx_ds:make_iterator(?DS, Stream, TopicFilter, 0),
    {ok, SubHandle, SubRef} = emqx_ds:subscribe(?DS, It, Opts),
    {more, H#handle{
        remaining_streams = Streams,
        sub_handle = SubHandle,
        sub_ref = SubRef
    }}.

dispatch_message(H = #handle{sub_ref = SubRef, sub_handle = SubHandle}, Message) ->
    %% FIXME: error handling
    case Message of
        #ds_sub_reply{ref = SubRef, payload = Payload} ->
            case Payload of
                {error, _, _} ->
                    error(fixme);
                {ok, _It, Messages} ->
                    {data, Messages};
                {ok, end_of_stream} ->
                    emqx_ds:unsubscribe(?DS, SubHandle),
                    switch_stream(H)
            end;
        {'DOWN', SubRef, _, _, Reason} ->
            error(#{msg => subscription_down, reason => Reason});
        _ ->
            ignore
    end.

%% For testing:
test_consume(H0) ->
    receive
        Msg ->
            case dispatch_message(H0, Msg) of
                ignore ->
                    test_consume(H0);
                {more, H} ->
                    test_consume(H);
                {data, Messages} ->
                    io:format("Got messages: ~p", [Messages]),
                    test_consume(H0);
                done ->
                    ok
            end
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

ensure_generation() ->
    ok = emqx_ds:wait_db(?DS, all, infinity),
    case list_generations() of
        [_, _] ->
            ok;
        [_] ->
            emqx_ds:add_generation(?DS)
    end.

list_generations() ->
    maps:keys(
        maps:fold(
            fun({_Shard, Gen}, _Info, Acc) ->
                Acc#{Gen => true}
            end,
            #{},
            emqx_ds:list_slabs(?DS)
        )
    ).

active_gen() ->
    1.
