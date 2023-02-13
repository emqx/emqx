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

-export([on_assemble_timeout/1]).

-export_type([
    clientid/0,
    transfer/0,
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

-define(ASSEMBLE_TIMEOUT, 5000).

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
            on_init(Msg, FileId);
        [FileId, <<"fin">>] ->
            on_fin(PacketId, Msg, FileId, undefined);
        [FileId, <<"fin">>, Checksum] ->
            on_fin(PacketId, Msg, FileId, Checksum);
        [FileId, <<"abort">>] ->
            on_abort(Msg, FileId);
        [FileId, OffsetBin] ->
            validate([{offset, OffsetBin}], fun([Offset]) ->
                on_segment(Msg, FileId, Offset, undefined)
            end);
        [FileId, OffsetBin, ChecksumBin] ->
            validate([{offset, OffsetBin}, {checksum, ChecksumBin}], fun([Offset, Checksum]) ->
                on_segment(Msg, FileId, Offset, Checksum)
            end);
        _ ->
            ?RC_UNSPECIFIED_ERROR
    end.

on_init(Msg, FileId) ->
    ?SLOG(info, #{
        msg => "on_init",
        mqtt_msg => Msg,
        file_id => FileId
    }),
    Payload = Msg#message.payload,
    % %% Add validations here
    case decode_filemeta(Payload) of
        {ok, Meta} ->
            case emqx_ft_storage:store_filemeta(transfer(Msg, FileId), Meta) of
                ok ->
                    ?RC_SUCCESS;
                {error, _Reason} ->
                    ?RC_UNSPECIFIED_ERROR
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "on_init: invalid filemeta",
                mqtt_msg => Msg,
                file_id => FileId,
                reason => Reason
            }),
            ?RC_UNSPECIFIED_ERROR
    end.

on_abort(_Msg, _FileId) ->
    %% TODO
    ?RC_SUCCESS.

on_segment(Msg, FileId, Offset, Checksum) ->
    ?SLOG(info, #{
        msg => "on_segment",
        mqtt_msg => Msg,
        file_id => FileId,
        offset => Offset,
        checksum => Checksum
    }),
    %% TODO: handle checksum
    Payload = Msg#message.payload,
    Segment = {Offset, Payload},
    %% Add offset/checksum validations
    case emqx_ft_storage:store_segment(transfer(Msg, FileId), Segment) of
        ok ->
            ?RC_SUCCESS;
        {error, _Reason} ->
            ?RC_UNSPECIFIED_ERROR
    end.

on_fin(PacketId, Msg, FileId, Checksum) ->
    ?SLOG(info, #{
        msg => "on_fin",
        mqtt_msg => Msg,
        file_id => FileId,
        checksum => Checksum,
        packet_id => PacketId
    }),
    %% TODO: handle checksum? Do we need it?
    FinPacketKey = {self(), PacketId},
    _ =
        case
            emqx_ft_responder:register(
                FinPacketKey, fun ?MODULE:on_assemble_timeout/1, ?ASSEMBLE_TIMEOUT
            )
        of
            %% We have new fin packet
            ok ->
                Callback = callback(FinPacketKey, FileId),
                case assemble(transfer(Msg, FileId), Callback) of
                    %% Assembling started, packet will be acked by the callback or the responder
                    {ok, _} ->
                        undefined;
                    %% Assembling failed, unregister the packet key
                    {error, _} ->
                        case emqx_ft_responder:unregister(FinPacketKey) of
                            %% We successfully unregistered the packet key,
                            %% so we can send the error code at once
                            ok ->
                                ?RC_UNSPECIFIED_ERROR;
                            %% Someone else already unregistered the key,
                            %% that is, either responder or someone else acked the packet,
                            %% we do not have to ack
                            {error, not_found} ->
                                undefined
                        end
                end;
            %% Fin packet already received.
            %% Since we are still handling the previous one,
            %% we probably have retransmit here
            {error, already_registered} ->
                undefined
        end.

assemble(Transfer, Callback) ->
    try
        emqx_ft_storage:assemble(Transfer, Callback)
    catch
        C:E:S ->
            ?SLOG(warning, #{
                msg => "file_assemble_failed", class => C, reason => E, stacktrace => S
            }),
            {error, {internal_error, E}}
    end.

callback({ChanPid, PacketId} = Key, _FileId) ->
    fun(Result) ->
        case emqx_ft_responder:unregister(Key) of
            ok ->
                case Result of
                    ok ->
                        erlang:send(ChanPid, {puback, PacketId, [], ?RC_SUCCESS});
                    {error, _} ->
                        erlang:send(ChanPid, {puback, PacketId, [], ?RC_UNSPECIFIED_ERROR})
                end;
            {error, not_found} ->
                ok
        end
    end.

transfer(Msg, FileId) ->
    ClientId = Msg#message.from,
    {ClientId, FileId}.

on_assemble_timeout({ChanPid, PacketId}) ->
    ?SLOG(warning, #{msg => "on_assemble_timeout", packet_id => PacketId}),
    erlang:send(ChanPid, {puback, PacketId, [], ?RC_UNSPECIFIED_ERROR}).

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
do_validate([{checksum, Checksum} | Rest], Parsed) ->
    case parse_checksum(Checksum) of
        {ok, Bin} ->
            do_validate(Rest, [Bin | Parsed]);
        {error, _Reason} ->
            {error, {invalid_checksum, Checksum}}
    end.

parse_checksum(Checksum) when is_binary(Checksum) andalso byte_size(Checksum) =:= 64 ->
    try
        {ok, binary:decode_hex(Checksum)}
    catch
        error:badarg ->
            {error, invalid_checksum}
    end;
parse_checksum(_Checksum) ->
    {error, invalid_checksum}.
