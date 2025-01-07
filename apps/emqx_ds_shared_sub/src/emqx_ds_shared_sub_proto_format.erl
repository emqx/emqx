%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto_format).

-include("emqx_ds_shared_sub_proto.hrl").

-export([format_agent_msg/1, format_leader_msg/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

format_agent_msg(Msg) ->
    maps:from_list(
        lists:map(
            fun({K, V}) ->
                FormattedKey = agent_msg_key(K),
                {FormattedKey, format_agent_msg_value(FormattedKey, V)}
            end,
            maps:to_list(Msg)
        )
    ).

format_leader_msg(Msg) ->
    maps:from_list(
        lists:map(
            fun({K, V}) ->
                FormattedKey = leader_msg_key(K),
                {FormattedKey, format_leader_msg_value(FormattedKey, V)}
            end,
            maps:to_list(Msg)
        )
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

format_agent_msg_value(agent_msg_type, Type) ->
    agent_msg_type(Type);
format_agent_msg_value(_, Value) ->
    Value.

format_leader_msg_value(leader_msg_type, Type) ->
    leader_msg_type(Type);
format_leader_msg_value(_, Value) ->
    Value.

agent_msg_type(?agent_connect_leader_msg) -> agent_connect_leader_msg;
agent_msg_type(?agent_update_stream_states_msg) -> agent_update_stream_states_msg;
agent_msg_type(?agent_connect_leader_timeout_msg) -> agent_connect_leader_timeout_msg;
agent_msg_type(?agent_renew_stream_lease_timeout_msg) -> agent_renew_stream_lease_timeout_msg;
agent_msg_type(?agent_disconnect_msg) -> agent_disconnect_msg.

agent_msg_key(?agent_msg_type) -> agent_msg_type;
agent_msg_key(?agent_msg_agent) -> agent_msg_agent;
agent_msg_key(?agent_msg_share_topic_filter) -> agent_msg_share_topic_filter;
agent_msg_key(?agent_msg_agent_metadata) -> agent_msg_agent_metadata;
agent_msg_key(?agent_msg_stream_states) -> agent_msg_stream_states;
agent_msg_key(?agent_msg_version) -> agent_msg_version;
agent_msg_key(?agent_msg_version_old) -> agent_msg_version_old;
agent_msg_key(?agent_msg_version_new) -> agent_msg_version_new.

leader_msg_type(?leader_lease_streams_msg) -> leader_lease_streams_msg;
leader_msg_type(?leader_renew_stream_lease_msg) -> leader_renew_stream_lease_msg;
leader_msg_type(?leader_update_streams) -> leader_update_streams;
leader_msg_type(?leader_invalidate) -> leader_invalidate.

leader_msg_key(?leader_msg_type) -> leader_msg_type;
leader_msg_key(?leader_msg_streams) -> leader_msg_streams;
leader_msg_key(?leader_msg_version) -> leader_msg_version;
leader_msg_key(?leader_msg_version_old) -> leader_msg_version_old;
leader_msg_key(?leader_msg_version_new) -> leader_msg_version_new;
leader_msg_key(?leader_msg_streams_new) -> leader_msg_streams_new;
leader_msg_key(?leader_msg_leader) -> leader_msg_leader;
leader_msg_key(?leader_msg_group_id) -> leader_msg_group_id.
