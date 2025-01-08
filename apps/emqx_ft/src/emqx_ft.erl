%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_channel.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-include_lib("snabbkaffe/include/trace.hrl").

-export([
    hook/0,
    unhook/0
]).

-export([
    on_message_publish/1,
    on_message_puback/4
]).

-export([
    decode_filemeta/1,
    encode_filemeta/1
]).

-export_type([
    clientid/0,
    transfer/0,
    bytes/0,
    offset/0,
    filemeta/0,
    segment/0,
    checksum/0,
    finopts/0
]).

%% Number of bytes
-type bytes() :: non_neg_integer().

%% MQTT Client ID
-type clientid() :: binary().

-type fileid() :: binary().
-type transfer() :: {clientid(), fileid()}.
-type offset() :: bytes().
-type checksum() :: {_Algo :: atom(), _Digest :: binary()}.

-type filemeta() :: #{
    %% Display name
    name := string(),
    %% Size in bytes, as advertised by the client.
    %% Client is free to specify here whatever it wants, which means we can end
    %% up with a file of different size after assembly. It's not clear from
    %% specification what that means (e.g. what are clients' expectations), we
    %% currently do not condider that an error (or, specifically, a signal that
    %% the resulting file is corrupted during transmission).
    size => _Bytes :: non_neg_integer(),
    checksum => checksum(),
    expire_at := emqx_utils_calendar:epoch_second(),
    %% TTL of individual segments
    %% Somewhat confusing that we won't know it on the nodes where the filemeta
    %% is missing.
    segments_ttl => _Seconds :: pos_integer(),
    user_data => emqx_ft_schema:json_value()
}.

-type segment() :: {offset(), _Content :: binary()}.

-type finopts() :: #{
    checksum => checksum()
}.

-define(ACK_AND_PUBLISH(Result), {true, Result}).
-define(ACK(Result), {false, Result}).
-define(DELAY_ACK, delay).

%%--------------------------------------------------------------------
%% API for app
%%--------------------------------------------------------------------

hook() ->
    ok = emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok = emqx_hooks:put('message.puback', {?MODULE, on_message_puback, []}, ?HP_LOWEST).

unhook() ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok = emqx_hooks:del('message.puback', {?MODULE, on_message_puback}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

decode_filemeta(Payload) ->
    emqx_ft_schema:decode(filemeta, Payload).

encode_filemeta(Meta = #{}) ->
    emqx_ft_schema:encode(filemeta, Meta).

encode_response(Response) ->
    emqx_ft_schema:encode(command_response, Response).

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

on_message_publish(Msg = #message{topic = <<"$file-async/", _/binary>>}) ->
    Headers = Msg#message.headers,
    {stop, Msg#message{headers = Headers#{allow_publish => false}}};
on_message_publish(Msg = #message{topic = <<"$file/", _/binary>>}) ->
    Headers = Msg#message.headers,
    {stop, Msg#message{headers = Headers#{allow_publish => false}}};
on_message_publish(Msg) ->
    {ok, Msg}.

on_message_puback(PacketId, #message{from = From, topic = Topic} = Msg, _PubRes, _RC) ->
    case Topic of
        <<"$file/", _/binary>> ->
            {stop, on_file_command(sync, From, PacketId, Msg, Topic)};
        <<"$file-async/", _/binary>> ->
            {stop, on_file_command(async, From, PacketId, Msg, Topic)};
        _ ->
            ignore
    end.

%%--------------------------------------------------------------------
%% Handlers for transfer messages
%%--------------------------------------------------------------------

%% TODO Move to emqx_ft_mqtt?

on_file_command(Mode, From, PacketId, Msg, Topic) ->
    TopicReplyData = topic_reply_data(Mode, From, PacketId, Msg),
    Result =
        case emqx_topic:tokens(Topic) of
            [_FTPrefix, FileIdIn | Rest] ->
                validate([{fileid, FileIdIn}], fun([FileId]) ->
                    do_on_file_command(TopicReplyData, FileId, Msg, Rest)
                end);
            [] ->
                ?ACK_AND_PUBLISH({error, {invalid_topic, Topic}})
        end,
    maybe_publish_response(Result, TopicReplyData).

do_on_file_command(TopicReplyData, FileId, Msg, FileCommand) ->
    Transfer = transfer(Msg, FileId),
    case FileCommand of
        [<<"init">>] ->
            validate(
                [{filemeta, Msg#message.payload}],
                fun([Meta]) ->
                    on_init(TopicReplyData, Msg, Transfer, Meta)
                end
            );
        [<<"fin">>, FinalSizeBin | MaybeChecksum] when length(MaybeChecksum) =< 1 ->
            ChecksumBin = emqx_maybe:from_list(MaybeChecksum),
            validate(
                [{size, FinalSizeBin}, {{option, checksum}, ChecksumBin}],
                fun([FinalSize, FinalChecksum]) ->
                    on_fin(TopicReplyData, Msg, Transfer, FinalSize, FinalChecksum)
                end
            );
        [<<"abort">>] ->
            on_abort(TopicReplyData, Msg, Transfer);
        [OffsetBin] ->
            validate([{offset, OffsetBin}], fun([Offset]) ->
                on_segment(TopicReplyData, Msg, Transfer, Offset, undefined)
            end);
        [OffsetBin, ChecksumBin] ->
            validate(
                [{offset, OffsetBin}, {checksum, ChecksumBin}],
                fun([Offset, Checksum]) ->
                    validate(
                        [{integrity, Msg#message.payload, Checksum}],
                        fun(_) ->
                            on_segment(TopicReplyData, Msg, Transfer, Offset, Checksum)
                        end
                    )
                end
            );
        _ ->
            ?ACK_AND_PUBLISH({error, {invalid_file_command, FileCommand}})
    end.

on_init(#{packet_id := PacketId}, Msg, Transfer, Meta) ->
    ?tp(info, "file_transfer_init", #{
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer,
        filemeta => Meta
    }),
    %% Currently synchronous.
    %% If we want to make it async, we need to use `with_responder`,
    %% like in `on_fin`.
    ?ACK_AND_PUBLISH(store_filemeta(Transfer, Meta)).

on_abort(_TopicReplyData, _Msg, _FileId) ->
    %% TODO
    ?ACK_AND_PUBLISH(ok).

on_segment(#{packet_id := PacketId}, Msg, Transfer, Offset, Checksum) ->
    ?tp(info, "file_transfer_segment", #{
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer,
        offset => Offset,
        checksum => Checksum
    }),
    Segment = {Offset, Msg#message.payload},
    %% Currently synchronous.
    %% If we want to make it async, we need to use `with_responder`,
    %% like in `on_fin`.
    ?ACK_AND_PUBLISH(store_segment(Transfer, Segment)).

on_fin(
    #{packet_id := PacketId, mode := Mode} = TopicReplyData, Msg, Transfer, FinalSize, FinalChecksum
) ->
    ?tp(info, "file_transfer_fin", #{
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer,
        final_size => FinalSize,
        checksum => FinalChecksum
    }),
    FinPacketKey = {self(), PacketId},
    Callback = fun(Result) ->
        on_complete("assemble", TopicReplyData, FinPacketKey, Transfer, Result)
    end,
    with_responder(FinPacketKey, Callback, emqx_ft_conf:assemble_timeout(), fun() ->
        case assemble(Transfer, FinalSize, FinalChecksum) of
            %% Assembling completed, ack through the responder right away
            ok ->
                emqx_ft_responder:ack(FinPacketKey, ok),
                ?DELAY_ACK;
            %% Assembling started, packet will be acked by the responder
            {async, Pid} ->
                ok = emqx_ft_responder:kickoff(FinPacketKey, Pid),
                ack_if_async(Mode);
            %% Assembling failed, ack through the responder
            {error, _} = Error ->
                emqx_ft_responder:ack(FinPacketKey, Error),
                ?DELAY_ACK
        end
    end).

with_responder(Key, Callback, Timeout, CriticalSection) ->
    case emqx_ft_responder:start(Key, Callback, Timeout) of
        %% We have new packet
        {ok, _} ->
            CriticalSection();
        %% Packet already received.
        %% Since we are still handling the previous one,
        %% we probably have retransmit here
        {error, {already_started, _}} ->
            ?DELAY_ACK
    end.

ack_if_async(sync) ->
    ?DELAY_ACK;
ack_if_async(async) ->
    ?ACK(ok).

on_complete(Op, #{mode := Mode} = TopicReplyData, {ChanPid, PacketId}, Transfer, ResponderResult) ->
    ?tp(debug, "on_complete", #{
        operation => Op,
        packet_id => PacketId,
        transfer => Transfer
    }),
    Result =
        case ResponderResult of
            {RespMode, ok} when RespMode == ack orelse RespMode == down ->
                ok;
            {RespMode, {error, _} = Reason} when RespMode == ack orelse RespMode == down ->
                ?tp(error, Op ++ "_failed", #{
                    transfer => Transfer,
                    reason => Reason
                }),
                Reason;
            timeout ->
                ?tp(error, Op ++ "_timed_out", #{
                    transfer => Transfer
                }),
                {error, timeout}
        end,
    NeedAck =
        case {ResponderResult, Mode} of
            {{down, _}, async} -> false;
            {timeout, async} -> false;
            _ -> true
        end,
    NeedAck andalso ack_packet(ChanPid, PacketId, Result),
    _ = publish_response(Result, TopicReplyData).

ack_packet(ChanPid, PacketId, Result) ->
    erlang:send(ChanPid, {puback, PacketId, [], result_to_rc(Result)}).

topic_reply_data(Mode, From, PacketId, #message{topic = Topic, headers = Headers}) ->
    Props = maps:get(properties, Headers, #{}),
    #{
        mode => Mode,
        clientid => From,
        command_topic => Topic,
        correlation_data => maps:get('Correlation-Data', Props, undefined),
        response_topic => maps:get('Response-Topic', Props, undefined),
        packet_id => PacketId
    }.

maybe_publish_response(?DELAY_ACK, _TopicReplyData) ->
    undefined;
maybe_publish_response(?ACK(Result), _TopicReplyData) ->
    result_to_rc(Result);
maybe_publish_response(?ACK_AND_PUBLISH(Result), TopicReplyData) ->
    publish_response(Result, TopicReplyData).

publish_response(Result, #{
    clientid := ClientId,
    command_topic := CommandTopic,
    correlation_data := CorrelationData,
    response_topic := ResponseTopic,
    packet_id := PacketId
}) ->
    ResultCode = result_to_rc(Result),
    Response = encode_response(#{
        topic => CommandTopic,
        packet_id => PacketId,
        reason_code => ResultCode,
        reason_description => emqx_ft_error:format(Result)
    }),
    Payload = emqx_utils_json:encode(Response),
    Topic = emqx_maybe:define(ResponseTopic, response_topic(ClientId)),
    Msg = emqx_message:make(
        emqx_guid:gen(),
        undefined,
        ?QOS_1,
        Topic,
        Payload,
        #{},
        #{properties => response_properties(CorrelationData)}
    ),
    _ = emqx_broker:publish(Msg),
    ResultCode.

response_properties(undefined) -> #{};
response_properties(CorrelationData) -> #{'Correlation-Data' => CorrelationData}.

response_topic(ClientId) ->
    <<"$file-response/", (clientid_to_binary(ClientId))/binary>>.

result_to_rc(ok) ->
    ?RC_SUCCESS;
result_to_rc({error, _}) ->
    ?RC_UNSPECIFIED_ERROR.

store_filemeta(Transfer, Segment) ->
    try
        emqx_ft_storage:store_filemeta(Transfer, Segment)
    catch
        C:E:S ->
            ?tp(error, "start_store_filemeta_failed", #{
                class => C, reason => E, stacktrace => S
            }),
            {error, {internal_error, E}}
    end.

store_segment(Transfer, Segment) ->
    try
        emqx_ft_storage:store_segment(Transfer, Segment)
    catch
        C:E:S ->
            ?tp(error, "start_store_segment_failed", #{
                class => C, reason => E, stacktrace => S
            }),
            {error, {internal_error, E}}
    end.

assemble(Transfer, FinalSize, FinalChecksum) ->
    try
        FinOpts = [{checksum, FinalChecksum} || FinalChecksum /= undefined],
        emqx_ft_storage:assemble(Transfer, FinalSize, maps:from_list(FinOpts))
    catch
        C:E:S ->
            ?tp(error, "start_assemble_failed", #{
                class => C, reason => E, stacktrace => S
            }),
            {error, {internal_error, E}}
    end.

transfer(Msg, FileId) ->
    ClientId = Msg#message.from,
    {clientid_to_binary(ClientId), FileId}.

validate(Validations, Fun) ->
    case do_validate(Validations, []) of
        {ok, Parsed} ->
            Fun(Parsed);
        {error, Reason} = Error ->
            ?tp(info, "client_violated_protocol", #{reason => Reason}),
            ?ACK_AND_PUBLISH(Error)
    end.

do_validate([], Parsed) ->
    {ok, lists:reverse(Parsed)};
do_validate([{fileid, FileId} | Rest], Parsed) ->
    case byte_size(FileId) of
        S when S > 0 ->
            do_validate(Rest, [FileId | Parsed]);
        0 ->
            {error, {invalid_fileid, FileId}}
    end;
do_validate([{filemeta, Payload} | Rest], Parsed) ->
    case decode_filemeta(Payload) of
        {ok, Meta} ->
            do_validate(Rest, [Meta | Parsed]);
        {error, Reason} ->
            {error, Reason}
    end;
do_validate([{offset, Offset} | Rest], Parsed) ->
    case string:to_integer(Offset) of
        {Int, <<>>} ->
            do_validate(Rest, [Int | Parsed]);
        _ ->
            {error, {invalid_offset, Offset}}
    end;
do_validate([{size, Size} | Rest], Parsed) ->
    case string:to_integer(Size) of
        {Int, <<>>} ->
            do_validate(Rest, [Int | Parsed]);
        _ ->
            {error, {invalid_size, Size}}
    end;
do_validate([{checksum, Checksum} | Rest], Parsed) ->
    case parse_checksum(Checksum) of
        {ok, Bin} ->
            do_validate(Rest, [Bin | Parsed]);
        {error, _Reason} ->
            {error, {invalid_checksum, Checksum}}
    end;
do_validate([{integrity, Payload, {Algo, Checksum}} | Rest], Parsed) ->
    case crypto:hash(Algo, Payload) of
        Checksum ->
            do_validate(Rest, [Payload | Parsed]);
        Mismatch ->
            {error, {checksum_mismatch, binary:encode_hex(Mismatch)}}
    end;
do_validate([{{option, _}, undefined} | Rest], Parsed) ->
    do_validate(Rest, [undefined | Parsed]);
do_validate([{{option, T}, Value} | Rest], Parsed) ->
    do_validate([{T, Value} | Rest], Parsed).

parse_checksum(Checksum) when is_binary(Checksum) andalso byte_size(Checksum) =:= 64 ->
    try
        {ok, {sha256, binary:decode_hex(Checksum)}}
    catch
        error:badarg ->
            {error, invalid_checksum}
    end;
parse_checksum(_Checksum) ->
    {error, invalid_checksum}.

clientid_to_binary(A) when is_atom(A) ->
    atom_to_binary(A);
clientid_to_binary(B) when is_binary(B) ->
    B.
