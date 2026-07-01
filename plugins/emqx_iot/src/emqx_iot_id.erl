%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_id).

-export([
    generate_message_id/0,
    resolve_message_id/1
]).

-include("emqx_iot.hrl").

generate_message_id() ->
    MsgId = emqx_iot_utils:gen_guid(),
    ApiMsgId = emqx_iot_utils:gen_api_uuid(),
    {ApiMsgId, MsgId}.

resolve_message_id(ApiMsgId) when is_binary(ApiMsgId) ->
    case mnesia:dirty_read(iot_mq_message_api_id, ApiMsgId) of
        [#iot_mq_message_api_id{msg_id = MsgId}] ->
            {ok, MsgId};
        [] ->
            {error, not_found}
    end.
