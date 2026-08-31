%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_id).

-export([
    generate_message_id/0,
    generate_message_id_from_hash/1,
    resolve_message_id/1
]).

-include("emqx_bcast.hrl").

-spec generate_message_id() -> {binary(), binary()}.
generate_message_id() ->
    MsgId = emqx_bcast_utils:gen_guid(),
    ApiMsgId = emqx_bcast_utils:gen_api_uuid(),
    {ApiMsgId, MsgId}.

-spec generate_message_id_from_hash(binary()) -> {binary(), binary()}.
generate_message_id_from_hash(Hash) ->
    MsgId = emqx_bcast_utils:gen_guid(),
    ApiMsgId = emqx_bcast_utils:gen_api_uuid_from_hash(Hash),
    {ApiMsgId, MsgId}.

-spec resolve_message_id(binary()) -> {ok, binary()} | {error, not_found}.
resolve_message_id(ApiMsgId) when is_binary(ApiMsgId) ->
    case mnesia:dirty_read(bcast_message_api_id, ApiMsgId) of
        [#bcast_message_api_id{msg_id = MsgId}] ->
            {ok, MsgId};
        [] ->
            {error, not_found}
    end;
resolve_message_id(_) ->
    {error, not_found}.
