%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    on_message_puback/4,
    on_client_timeout/3,
    on_process_down/4,
    on_channel_unregistered/1
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

-define(FT_EVENT(EVENT), {?MODULE, EVENT}).

%%--------------------------------------------------------------------
%% API for app
%%--------------------------------------------------------------------

hook() ->
    ok = emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok = emqx_hooks:put('message.puback', {?MODULE, on_message_puback, []}, ?HP_LOWEST),
    ok = emqx_hooks:put('client.timeout', {?MODULE, on_client_timeout, []}, ?HP_LOWEST),
    ok = emqx_hooks:put(
        'client.monitored_process_down', {?MODULE, on_process_down, []}, ?HP_LOWEST
    ),
    ok = emqx_hooks:put(
        'cm.channel.unregistered', {?MODULE, on_channel_unregistered, []}, ?HP_LOWEST
    ).

unhook() ->
    ok = emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok = emqx_hooks:del('message.puback', {?MODULE, on_message_puback}),
    ok = emqx_hooks:del('client.timeout', {?MODULE, on_client_timeout}),
    ok = emqx_hooks:del('client.monitored_process_down', {?MODULE, on_process_down}),
    ok = emqx_hooks:del('cm.channel.unregistered', {?MODULE, on_channel_unregistered}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

decode_filemeta(Payload) when is_binary(Payload) ->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
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
        throw:{_Schema, Errors} ->
            {error, {invalid_filemeta, Errors}}
    end.

encode_filemeta(Meta = #{}) ->
    Schema = emqx_ft_schema:schema(filemeta),
    hocon_tconf:make_serializable(Schema, emqx_utils_maps:binary_key_map(Meta), #{}).

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

on_channel_unregistered(ChannelPid) ->
    ok = emqx_ft_async_reply:deregister_all(ChannelPid).

on_client_timeout(_TRef, ?FT_EVENT({MRef, PacketId}), Acc) ->
    _ = erlang:demonitor(MRef, [flush]),
    _ = emqx_ft_async_reply:take_by_mref(MRef),
    {stop, [?REPLY_OUTGOING(?PUBACK_PACKET(PacketId, ?RC_UNSPECIFIED_ERROR)) | Acc]};
on_client_timeout(_TRef, _Event, Acc) ->
    {ok, Acc}.

on_process_down(MRef, _Pid, Reason, Acc) ->
    case emqx_ft_async_reply:take_by_mref(MRef) of
        {ok, PacketId, TRef} ->
            _ = emqx_utils:cancel_timer(TRef),
            {stop, [?REPLY_OUTGOING(?PUBACK_PACKET(PacketId, reason_to_rc(Reason))) | Acc]};
        not_found ->
            {ok, Acc}
    end.

%%--------------------------------------------------------------------
%% Handlers for transfer messages
%%--------------------------------------------------------------------

%% TODO Move to emqx_ft_mqtt?

on_file_command(PacketId, Msg, FileCommand) ->
    case emqx_topic:tokens(FileCommand) of
        [FileIdIn | Rest] ->
            validate([{fileid, FileIdIn}], fun([FileId]) ->
                on_file_command(PacketId, FileId, Msg, Rest)
            end);
        [] ->
            ?RC_UNSPECIFIED_ERROR
    end.

on_file_command(PacketId, FileId, Msg, FileCommand) ->
    Transfer = transfer(Msg, FileId),
    case FileCommand of
        [<<"init">>] ->
            validate(
                [{filemeta, Msg#message.payload}],
                fun([Meta]) ->
                    on_init(PacketId, Msg, Transfer, Meta)
                end
            );
        [<<"fin">>, FinalSizeBin | MaybeChecksum] when length(MaybeChecksum) =< 1 ->
            ChecksumBin = emqx_maybe:from_list(MaybeChecksum),
            validate(
                [{size, FinalSizeBin}, {{maybe, checksum}, ChecksumBin}],
                fun([FinalSize, FinalChecksum]) ->
                    on_fin(PacketId, Msg, Transfer, FinalSize, FinalChecksum)
                end
            );
        [<<"abort">>] ->
            on_abort(Msg, Transfer);
        [OffsetBin] ->
            validate([{offset, OffsetBin}], fun([Offset]) ->
                on_segment(PacketId, Msg, Transfer, Offset, undefined)
            end);
        [OffsetBin, ChecksumBin] ->
            validate(
                [{offset, OffsetBin}, {checksum, ChecksumBin}],
                fun([Offset, Checksum]) ->
                    validate(
                        [{integrity, Msg#message.payload, Checksum}],
                        fun(_) ->
                            on_segment(PacketId, Msg, Transfer, Offset, Checksum)
                        end
                    )
                end
            );
        _ ->
            ?RC_UNSPECIFIED_ERROR
    end.

on_init(PacketId, Msg, Transfer, Meta) ->
    ?tp(info, "file_transfer_init", #{
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer,
        filemeta => Meta
    }),
    %% Currently synchronous.
    %% If we want to make it async, we need to use `emqx_ft_async_reply`,
    %% like in `on_fin`.
    case store_filemeta(Transfer, Meta) of
        ok -> ?RC_SUCCESS;
        {error, _} -> ?RC_UNSPECIFIED_ERROR
    end.

on_abort(_Msg, _FileId) ->
    %% TODO
    ?RC_SUCCESS.

on_segment(PacketId, Msg, Transfer, Offset, Checksum) ->
    ?tp(info, "file_transfer_segment", #{
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer,
        offset => Offset,
        checksum => Checksum
    }),
    Segment = {Offset, Msg#message.payload},
    %% Currently synchronous.
    %% If we want to make it async, we need to use `emqx_ft_async_reply`,
    %% like in `on_fin`.
    case store_segment(Transfer, Segment) of
        ok -> ?RC_SUCCESS;
        {error, _} -> ?RC_UNSPECIFIED_ERROR
    end.

on_fin(PacketId, Msg, Transfer, FinalSize, FinalChecksum) ->
    ?tp(info, "file_transfer_fin", #{
        mqtt_msg => Msg,
        packet_id => PacketId,
        transfer => Transfer,
        final_size => FinalSize,
        checksum => FinalChecksum
    }),
    %% TODO: handle checksum? Do we need it?
    emqx_ft_async_reply:with_new_packet(
        PacketId,
        fun() ->
            case assemble(Transfer, FinalSize, FinalChecksum) of
                ok ->
                    ?RC_SUCCESS;
                %% Assembling started, packet will be acked by monitor or timeout
                {async, Pid} ->
                    ok = register_async_reply(Pid, PacketId),
                    ok = emqx_ft_storage:kickoff(Pid),
                    undefined;
                {error, _} ->
                    ?RC_UNSPECIFIED_ERROR
            end
        end,
        undefined
    ).

register_async_reply(Pid, PacketId) ->
    MRef = erlang:monitor(process, Pid),
    TRef = erlang:start_timer(
        emqx_ft_conf:assemble_timeout(), self(), ?FT_EVENT({MRef, PacketId})
    ),
    ok = emqx_ft_async_reply:register(PacketId, MRef, TRef).

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
        {error, Reason} ->
            ?tp(info, "client_violated_protocol", #{reason => Reason}),
            ?RC_UNSPECIFIED_ERROR
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
do_validate([{{maybe, _}, undefined} | Rest], Parsed) ->
    do_validate(Rest, [undefined | Parsed]);
do_validate([{{maybe, T}, Value} | Rest], Parsed) ->
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

reason_to_rc(Reason) ->
    case map_down_reason(Reason) of
        ok -> ?RC_SUCCESS;
        {error, _} -> ?RC_UNSPECIFIED_ERROR
    end.

map_down_reason(normal) ->
    ok;
map_down_reason(shutdown) ->
    ok;
map_down_reason({shutdown, Result}) ->
    Result;
map_down_reason(noproc) ->
    {error, noproc};
map_down_reason(Error) ->
    {error, {internal_error, Error}}.
