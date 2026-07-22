%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_packet_data_logger).

-export([add_packet_data/5]).

-define(REDACTED, <<"******">>).

-spec add_packet_data(
    map(), atom(), binary(), emqx_channel:channel(), raw | hex
) -> map().
add_packet_data(Log, Key, Data, Channel, Format) ->
    case is_allowed(Channel) of
        true ->
            add_raw_packet_data(Log, Key, Data, Format);
        false ->
            add_redacted_packet_data(Log, Key, Format)
    end.

add_raw_packet_data(Log, Key, Data, raw) ->
    Log#{Key => Data};
add_raw_packet_data(Log, Key, Data, hex) ->
    Log#{Key => binary_to_list(binary:encode_hex(Data)), type => "hex"}.

add_redacted_packet_data(Log, Key, raw) ->
    Log#{Key => ?REDACTED};
add_redacted_packet_data(Log, Key, hex) ->
    Log#{Key => ?REDACTED, type => "hidden"}.

is_allowed(Channel) ->
    ClientInfo = emqx_channel:info(clientinfo, Channel),
    PeerHost = maps:get(peerhost, ClientInfo),
    {Type, Name} = listener(maps:get(listener, ClientInfo)),
    IPMasks = emqx_config:get_listener_conf(Type, Name, [allow_log_packet_data_from], []),
    lists:any(
        fun(IPMask) -> esockd_cidr:match(PeerHost, IPMask) end,
        IPMasks
    ).

listener({Type, Name}) ->
    {Type, Name};
listener(ListenerId) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(ListenerId),
    {Type, Name}.
