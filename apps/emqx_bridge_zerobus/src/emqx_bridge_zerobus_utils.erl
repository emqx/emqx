%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_utils).

%% API
-export([
    get_serde/1,
    list_all_stream_writer_pids/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_bridge_zerobus.hrl").
-include_lib("emqx_schema_registry/include/emqx_schema_registry.hrl").

-type record_config() :: #{
    schema_name := binary(),
    message_type := binary(),
    any() => term()
}.
-type proto_record() :: emqx_bridge_zerobus_impl:proto_record().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec list_all_stream_writer_pids() -> [pid()].
list_all_stream_writer_pids() ->
    Key = emqx_bridge_zerobus_stream_writer_worker:gproc_name('_', '_'),
    MS = [{{Key, '$1', '_'}, [], ['$1']}],
    gproc:select({local, names}, MS).

-spec get_serde(record_config()) ->
    {ok, proto_record()}
    | {error, schema_not_found | bad_type | message_type_not_found}.
get_serde(Record) ->
    #{
        schema_name := SerdeName,
        message_type := MessageType
    } = Record,
    case emqx_schema_registry:get_serde(SerdeName) of
        {error, not_found} ->
            {error, schema_not_found};
        {ok, #serde{type = Type}} when Type /= ?protobuf ->
            {error, bad_type};
        {ok, #serde{type = ?protobuf, eval_context = SerdeMod}} ->
            FileDescriptorSetBin = apply(SerdeMod, descriptor, []),
            FileDescriptorSet = emqx_bridge_zerobus_gen_descriptor_pb:decode_msg(
                FileDescriptorSetBin, 'FileDescriptorSet'
            ),
            maybe
                {ok, DescriptorProtoBin} ?= find_descriptor_proto(FileDescriptorSet, MessageType),
                MessageTypeAtom = binary_to_existing_atom(MessageType, utf8),
                Proto = #{
                    ?serde_name => SerdeName,
                    ?message_type => MessageTypeAtom,
                    ?descriptor => DescriptorProtoBin
                },
                {ok, Proto}
            end
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

find_descriptor_proto(FileDescriptorSet, MessageType) ->
    #{file := Files} = FileDescriptorSet,
    case do_find_descriptor_proto_file(Files, MessageType) of
        error ->
            {error, message_type_not_found};
        {ok, DescriptorProto} ->
            DescriptorProtoBin = emqx_bridge_zerobus_gen_descriptor_pb:encode_msg(
                DescriptorProto, 'DescriptorProto'
            ),
            {ok, DescriptorProtoBin}
    end.

do_find_descriptor_proto_file([], _MessageType) ->
    error;
do_find_descriptor_proto_file([#{message_type := Types} | Rest], MessageType) ->
    case do_find_descriptor_proto_message_type(Types, MessageType) of
        error ->
            do_find_descriptor_proto_file(Rest, MessageType);
        {ok, DescriptorProto} ->
            {ok, DescriptorProto}
    end.

do_find_descriptor_proto_message_type([], _MessageType) ->
    error;
do_find_descriptor_proto_message_type([#{name := MessageType} = DescriptorProto | _], MessageType) ->
    {ok, DescriptorProto};
do_find_descriptor_proto_message_type([_ | Rest], MessageType) ->
    do_find_descriptor_proto_message_type(Rest, MessageType).
