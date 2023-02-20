%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-export([
    hook/0,
    unhook/0
]).

-export([
    on_message_publish/1,
    on_message_puback/4
]).

-export([
    decode_filemeta/1
]).

-export([on_complete/4]).

-export_type([
    clientid/0,
    transfer/0,
    bytes/0,
    offset/0,
    filemeta/0,
    segment/0
]).

%% Number of bytes
-type bytes() :: non_neg_integer().

%% MQTT Client ID
-type clientid() :: emqx_types:clientid().

-type fileid() :: binary().
-type transfer() :: {clientid(), fileid()}.
-type offset() :: bytes().

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
    checksum => {sha256, <<_:256>>},
    expire_at := emqx_datetime:epoch_second(),
    %% TTL of individual segments
    %% Somewhat confusing that we won't know it on the nodes where the filemeta
    %% is missing.
    segments_ttl => _Seconds :: pos_integer(),
    user_data => emqx_ft_schema:json_value()
}.

-type segment() :: {offset(), _Content :: binary()}.

-define(STORE_SEGMENT_TIMEOUT, 10000).
-define(ASSEMBLE_TIMEOUT, 60000).

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

decode_filemeta(Payload) when is_binary(Payload) ->
    case emqx_json:safe_decode(Payload, [return_maps]) of
        {ok, Map} ->
            decode_filemeta(Map);
        {error, Error} ->
            {error, {invalid_filemeta_json, Error}}
    end;
decode_filemeta(Map) when is_map(Map) ->
    Schema = emqx_ft_schema:schema(filemeta),
    try
        Meta = hocon_tconf:check_plain(Schema, Map, #{atom_key => true, required => false}),
        {ok, Meta}
    catch
        throw:Error ->
            {error, {invalid_filemeta, Error}}
    end.

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

on_message_publish(
    Msg = #message{
        id = _Id,
        topic = <<"$file/", _/binary>>
    }
) ->
    Headers = Msg#message.headers,
    {stop, Msg#message{headers = Headers#{allow_publish => false}}};
on_message_publish(Msg) ->
    {ok, Msg}.

on_message_puback(PacketId, #message{topic = Topic} = Msg, _PubRes, _RC) ->
    case Topic of
        <<"$file/", FileCommand/binary>> ->
            {stop, on_file_command(PacketId, Msg, FileCommand)};
        _ ->
            ignore
    end.

%%--------------------------------------------------------------------
%% Handlers for transfer messages
%%--------------------------------------------------------------------

%% TODO Move to emqx_ft_mqtt?

on_file_command(PacketId, Msg, FileCommand) ->
    case string:split(FileCommand, <<"/">>, all) of
        [FileId, <<"init">>] ->
            on_init(PacketId, Msg, transfer(Msg, FileId));
        [FileId, <<"fin">>, FinalSizeBin | MaybeChecksum] when length(MaybeChecksum) =< 1 ->
            ChecksumBin = emqx_maybe:from_list(MaybeChecksum),
            validate(
                [{size, FinalSizeBin}, {{maybe, checksum}, ChecksumBin}],
                fun([FinalSize, Checksum]) ->
                    on_fin(PacketId, Msg, transfer(Msg, FileId), FinalSize, Checksum)
                end
            );
        [FileId, <<"abort">>] ->
            on_abort(Msg, transfer(Msg, FileId));
        [FileId, OffsetBin] ->
            validate([{offset, OffsetBin}], fun([Offset]) ->
                on_segment(PacketId, Msg, transfer(Msg, FileId), Offset, undefined)
            end);
        [FileId, OffsetBin, ChecksumBin] ->
            validate([{offset, OffsetBin}, {checksum, ChecksumBin}], fun([Offset, Checksum]) ->
                on_segment(PacketId, Msg, transfer(Msg, FileId), Offset, Checksum)
            end);
        _ ->
            ?RC_UNSPECIFIED_ERROR
    end.

on_init(PacketId, Msg, Transfer) ->
    ?SLOG(info, #{
        msg => "on_init",
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer
    }),
    Payload = Msg#message.payload,
    PacketKey = {self(), PacketId},
    % %% Add validations here
    case decode_filemeta(Payload) of
        {ok, Meta} ->
            Callback = fun(Result) ->
                ?MODULE:on_complete("store_filemeta", PacketKey, Transfer, Result)
            end,
            with_responder(PacketKey, Callback, ?STORE_SEGMENT_TIMEOUT, fun() ->
                case store_filemeta(Transfer, Meta) of
                    % Stored, ack through the responder right away
                    ok ->
                        emqx_ft_responder:ack(PacketKey, ok);
                    % Storage operation started, packet will be acked by the responder
                    % {async, Pid} ->
                    %     ok = emqx_ft_responder:kickoff(PacketKey, Pid),
                    %     ok;
                    %% Storage operation failed, ack through the responder
                    {error, _} = Error ->
                        emqx_ft_responder:ack(PacketKey, Error)
                end
            end);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "on_init: invalid filemeta",
                mqtt_msg => Msg,
                transfer => Transfer,
                reason => Reason
            }),
            ?RC_UNSPECIFIED_ERROR
    end.

on_abort(_Msg, _FileId) ->
    %% TODO
    ?RC_SUCCESS.

on_segment(PacketId, Msg, Transfer, Offset, Checksum) ->
    ?SLOG(info, #{
        msg => "on_segment",
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer,
        offset => Offset,
        checksum => Checksum
    }),
    %% TODO: handle checksum
    Payload = Msg#message.payload,
    Segment = {Offset, Payload},
    PacketKey = {self(), PacketId},
    Callback = fun(Result) ->
        ?MODULE:on_complete("store_segment", PacketKey, Transfer, Result)
    end,
    %% Add offset/checksum validations
    with_responder(PacketKey, Callback, ?STORE_SEGMENT_TIMEOUT, fun() ->
        case store_segment(Transfer, Segment) of
            ok ->
                emqx_ft_responder:ack(PacketKey, ok);
            % {async, Pid} ->
            %     ok = emqx_ft_responder:kickoff(PacketKey, Pid),
            %     ok;
            {error, _} = Error ->
                emqx_ft_responder:ack(PacketKey, Error)
        end
    end).

on_fin(PacketId, Msg, Transfer, FinalSize, Checksum) ->
    ?SLOG(info, #{
        msg => "on_fin",
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer,
        final_size => FinalSize,
        checksum => Checksum
    }),
    %% TODO: handle checksum? Do we need it?
    FinPacketKey = {self(), PacketId},
    Callback = fun(Result) ->
        ?MODULE:on_complete("assemble", FinPacketKey, Transfer, Result)
    end,
    with_responder(FinPacketKey, Callback, ?ASSEMBLE_TIMEOUT, fun() ->
        case assemble(Transfer, FinalSize) of
            %% Assembling completed, ack through the responder right away
            % ok ->
            %     emqx_ft_responder:ack(FinPacketKey, ok);
            %% Assembling started, packet will be acked by the responder
            {async, Pid} ->
                ok = emqx_ft_responder:kickoff(FinPacketKey, Pid),
                ok;
            %% Assembling failed, ack through the responder
            {error, _} = Error ->
                emqx_ft_responder:ack(FinPacketKey, Error)
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
            ok
    end,
    undefined.

store_filemeta(Transfer, Segment) ->
    try
        emqx_ft_storage:store_filemeta(Transfer, Segment)
    catch
        C:E:S ->
            ?SLOG(error, #{
                msg => "start_store_filemeta_failed", class => C, reason => E, stacktrace => S
            }),
            {error, {internal_error, E}}
    end.

store_segment(Transfer, Segment) ->
    try
        emqx_ft_storage:store_segment(Transfer, Segment)
    catch
        C:E:S ->
            ?SLOG(error, #{
                msg => "start_store_segment_failed", class => C, reason => E, stacktrace => S
            }),
            {error, {internal_error, E}}
    end.

assemble(Transfer, FinalSize) ->
    try
        emqx_ft_storage:assemble(Transfer, FinalSize)
    catch
        C:E:S ->
            ?SLOG(error, #{
                msg => "start_assemble_failed", class => C, reason => E, stacktrace => S
            }),
            {error, {internal_error, E}}
    end.

transfer(Msg, FileId) ->
    ClientId = Msg#message.from,
    {ClientId, FileId}.

on_complete(Op, {ChanPid, PacketId}, Transfer, Result) ->
    ?SLOG(debug, #{
        msg => "on_complete",
        operation => Op,
        packet_id => PacketId,
        transfer => Transfer
    }),
    case Result of
        {Mode, ok} when Mode == ack orelse Mode == down ->
            erlang:send(ChanPid, {puback, PacketId, [], ?RC_SUCCESS});
        {Mode, {error, _} = Reason} when Mode == ack orelse Mode == down ->
            ?SLOG(error, #{
                msg => Op ++ "_failed",
                transfer => Transfer,
                reason => Reason
            }),
            erlang:send(ChanPid, {puback, PacketId, [], ?RC_UNSPECIFIED_ERROR});
        timeout ->
            ?SLOG(error, #{
                msg => Op ++ "_timed_out",
                transfer => Transfer
            }),
            erlang:send(ChanPid, {puback, PacketId, [], ?RC_UNSPECIFIED_ERROR})
    end.

validate(Validations, Fun) ->
    case do_validate(Validations, []) of
        {ok, Parsed} ->
            Fun(Parsed);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "validate: invalid $file command",
                reason => Reason
            }),
            ?RC_UNSPECIFIED_ERROR
    end.

do_validate([], Parsed) ->
    {ok, lists:reverse(Parsed)};
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
do_validate([{{maybe, _}, undefined} | Rest], Parsed) ->
    do_validate(Rest, [undefined | Parsed]);
do_validate([{{maybe, T}, Value} | Rest], Parsed) ->
    do_validate([{T, Value} | Rest], Parsed).

parse_checksum(Checksum) when is_binary(Checksum) andalso byte_size(Checksum) =:= 64 ->
    try
        {ok, binary:decode_hex(Checksum)}
    catch
        error:badarg ->
            {error, invalid_checksum}
    end;
parse_checksum(_Checksum) ->
    {error, invalid_checksum}.
