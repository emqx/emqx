-record(session, { node, pid, connect_time}).
-record(tcp_state, { transport, socket, buff = <<>>, state, mod, addr, product_id = 0, children = [], ip = <<>>, session}).
-record(device_state, {ops, product_id, addr, protocol, children}).

%% See 'Application Message' in MQTT Version 5.0
-record(message, {
    %% Global unique message ID
    id :: binary(),
    %% Message QoS
    qos = 0,
    %% Message from
    from :: atom() | binary(),
    %% Message flags
    flags = #{} :: emqx_types:flags(),
    %% Message headers. May contain any metadata. e.g. the
    %% protocol version number, username, peerhost or
    %% the PUBLISH properties (MQTT 5.0).
    headers = #{} :: emqx_types:headers(),
    %% Topic that the message is published to
    topic :: emqx_types:topic(),
    %% Message Payload
    payload :: emqx_types:payload(),
    %% Timestamp (Unit: millisecond)
    timestamp :: integer()
}).
