%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_NATS_HRL).
-define(EMQX_NATS_HRL, true).

%%--------------------------------------------------------------------
%% NATS Protocol Operations
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Control Operations
%%--------------------------------------------------------------------

%% Server to Client
-define(OP_INFO, info).
-define(OP_RAW_INFO, <<"INFO">>).
-define(OP_RAW_INFO_L, <<"info">>).
%% Client to Server
-define(OP_CONNECT, connect).
-define(OP_RAW_CONNECT, <<"CONNECT">>).
-define(OP_RAW_CONNECT_L, <<"connect">>).
%% Bidirectional
%% Implement the keep-alive mechanism. Servers
%% periodically send PING to clients; clients must respond with PONG within a configured
%% timeout to avoid disconnection. Clients can also send PING to check server liveness
-define(OP_PING, ping).
-define(OP_RAW_PING, <<"PING">>).
-define(OP_RAW_PING_L, <<"ping">>).
%% Bidirectional
-define(OP_PONG, pong).
-define(OP_RAW_PONG, <<"PONG">>).
-define(OP_RAW_PONG_L, <<"pong">>).
%% Server to Client
-define(OP_OK, ok).
-define(OP_RAW_OK, <<"+OK">>).
-define(OP_RAW_OK_L, <<"+ok">>).
%% Server to Client
-define(OP_ERR, err).
-define(OP_RAW_ERR, <<"-ERR">>).
-define(OP_RAW_ERR_L, <<"-err">>).

%%--------------------------------------------------------------------
%% Data Message Operations
%%--------------------------------------------------------------------

%% Client to Server
-define(OP_PUB, pub).
-define(OP_RAW_PUB, <<"PUB">>).
-define(OP_RAW_PUB_L, <<"pub">>).
%% Client to Server
-define(OP_HPUB, hpub).
-define(OP_RAW_HPUB, <<"HPUB">>).
-define(OP_RAW_HPUB_L, <<"hpub">>).
%% Client to Server
-define(OP_SUB, sub).
-define(OP_RAW_SUB, <<"SUB">>).
-define(OP_RAW_SUB_L, <<"sub">>).
%% Client to Server
-define(OP_UNSUB, unsub).
-define(OP_RAW_UNSUB, <<"UNSUB">>).
-define(OP_RAW_UNSUB_L, <<"unsub">>).
%% Client to Server
-define(OP_MSG, msg).
-define(OP_RAW_MSG, <<"MSG">>).
-define(OP_RAW_MSG_L, <<"msg">>).
%% Client to Server
-define(OP_HMSG, hmsg).
-define(OP_RAW_HMSG, <<"HMSG">>).
-define(OP_RAW_HMSG_L, <<"hmsg">>).

-type bidirectional_op() ::
    ?OP_PING
    | ?OP_PONG.

-type server_to_client_op() ::
    ?OP_INFO
    | ?OP_OK
    | ?OP_ERR
    | ?OP_MSG
    | ?OP_HMSG
    | bidirectional_op().

-type client_to_server_op() ::
    ?OP_CONNECT
    | ?OP_PUB
    | ?OP_HPUB
    | ?OP_SUB
    | ?OP_UNSUB
    | bidirectional_op().

-type operation() ::
    bidirectional_op()
    | server_to_client_op()
    | client_to_server_op().

-define(ALL_OPS, [
    ?OP_RAW_INFO,
    ?OP_RAW_INFO_L,
    ?OP_RAW_CONNECT,
    ?OP_RAW_CONNECT_L,
    ?OP_RAW_PING,
    ?OP_RAW_PING_L,
    ?OP_RAW_PONG,
    ?OP_RAW_PONG_L,
    ?OP_RAW_OK,
    ?OP_RAW_OK_L,
    ?OP_RAW_ERR,
    ?OP_RAW_ERR_L,
    ?OP_RAW_PUB,
    ?OP_RAW_PUB_L,
    ?OP_RAW_HPUB,
    ?OP_RAW_HPUB_L,
    ?OP_RAW_SUB,
    ?OP_RAW_SUB_L,
    ?OP_RAW_UNSUB,
    ?OP_RAW_UNSUB_L,
    ?OP_RAW_MSG,
    ?OP_RAW_MSG_L,
    ?OP_RAW_HMSG,
    ?OP_RAW_HMSG_L
]).

%%--------------------------------------------------------------------
%% Message structure
%%--------------------------------------------------------------------

%% Sent immediately after a client establishes a TCP/IP
%% connection. It provides the client with crucial server information,
%% configuration details, and security requirements necessary for the client
%% to proceed with the connection.
-type nats_message_info() :: #{
    %% Server identification and versioning.
    server_id => binary(),
    server_name => binary(),
    version => binary(),
    %% Go version, no needed in an Erlang Gateway
    %go => binary(),
    %% Server listening address.
    host => binary(),
    port => non_neg_integer(),
    %% Maximum message size the server accepts.
    max_payload => non_neg_integer(),
    %% Server protocol version (e.g., 1 indicates support for features like echo suppression)
    proto => non_neg_integer(),
    %% Boolean indicating if the server supports message headers.
    headers => boolean(),
    %% authentication needed
    auth_required => boolean(),
    %% TLS mandatory,
    tls_required => boolean(),
    %% client certificate verification required
    tls_verify => boolean(),
    %% List of other servers in the cluster the client can connect to.
    connect_urls => list(),
    %% A random string used for NKey challenge-response authentication.
    nonce => binary(),
    %% Boolean indicating if the server has JetStream enabled.
    jetstream => boolean(),
    client_id => binary(),
    tls_available => boolean(),
    ws_connect_urls => list(),
    ldm => binary(),
    git_commit => binary(),
    ip => binary(),
    client_ip => binary(),
    cluster => binary(),
    domain => binary()
}.

%% Sent by the client after receiving the INFO message. It
%% provides client details, requests specific connection options, and includes
%% authentication credentials if required by the server
-type nats_message_connect() :: #{
    %% Request protocol acknowledgements (+OK)
    verbose => boolean(),
    %% Stricter server-side checks
    pedantic => boolean(),
    %% Client library language and version.
    lang => binary(),
    version => binary(),
    %% Client protocol version support (e.g., 1 for async INFO updates).
    protocol => non_neg_integer(),
    %% Credentials for username/password or token authentication.
    user => binary(),
    pass => binary(),
    auth_token => binary(),
    %% Indicates client requires TLS.
    tls_required => boolean(),
    %% Optional client name
    name => binary(),
    %% Boolean indicating client support for headers.
    headers => boolean(),
    %% Public NKey and signed nonce for NKey authentication.
    nkey => binary(),
    sig => binary(),
    %% User JWT for JWT authentication.
    jwt => binary(),
    %% Request suppression of messages originating from this client (requires server proto >= 1)
    echo => boolean(),
    %% Enable faster failure for requests with no listeners.
    no_responders => boolean()
}.

%% Publishes a message to a specified subject. Can optionally
%% include a reply subject for request-reply patterns.
-type nats_message_pub() :: #{
    %% Subject to publish to.
    subject => binary(),
    %% Optional reply-to subject.
    reply_to => binary(),
    %% Optional headers, only apprence if the message type is hpub
    headers => list(),
    %% Message payload.
    payload => binary(),
    %% Headers size, only used for parsing
    headers_size => non_neg_integer(),
    %% Payload size, only used for parsing
    payload_size => non_neg_integer()
}.

%% Subscribes the client to a specific subject or subject pattern
%% (using wildcards). Can optionally join a queue group.
-type nats_message_sub() :: #{
    %% <subject> to subscribe to
    subject => binary(),
    %% optional <queue group> name
    queue_group => binary(),
    %% a unique client-generated Subscription ID (<sid>) used for message delivery and
    %% unsubscribing.
    sid => binary()
}.

%% Unsubscribes the client from a previously established
%% subscription, identified by its SID. Can optionally specify a maximum number of
%% messages to receive before automatically unsubscribing.
-type nats_message_unsub() :: #{
    %% Subscription ID <sid>
    sid => binary(),
    %%  optional <max_msgs> count
    max_msgs => non_neg_integer()
}.

%% Delivers a message published on a subject to a client that has
%% a matching subscription.
-type nats_message_msg() :: #{
    %% <subject> the message was published to
    subject => binary(),
    %% the client's <sid> for the matching subscription,
    sid => binary(),
    %% the optional <reply-to> subject from the publisher
    reply_to => binary(),
    %% Optional headers, only apprence if the message type is hmsg
    headers => map(),
    %% Message payload
    payload => binary(),
    %% Headers size, only used for parsing
    headers_size => non_neg_integer(),
    %% Payload size, only used for parsing
    payload_size => non_neg_integer()
}.

-type nats_message_error() :: binary().

-type message() ::
    nats_message_info()
    | nats_message_connect()
    | nats_message_pub()
    | nats_message_sub()
    | nats_message_unsub()
    | nats_message_msg()
    | nats_message_error().

-record(nats_frame, {
    operation :: operation(),
    message :: undefined | message()
}).

-type nats_frame() :: #nats_frame{}.

%%--------------------------------------------------------------------
%% Helper marocs

-define(HAS_SUBJECT_OP(Op),
    (Op =:= ?OP_PUB orelse
        Op =:= ?OP_HPUB orelse
        Op =:= ?OP_SUB orelse
        Op =:= ?OP_MSG orelse
        Op =:= ?OP_HMSG)
).

-define(HAS_SID_OP(Op),
    (Op =:= ?OP_SUB orelse
        Op =:= ?OP_UNSUB orelse
        Op =:= ?OP_MSG orelse
        Op =:= ?OP_HMSG)
).

-define(HAS_QUEUE_GROUP_OP(Op),
    (Op =:= ?OP_SUB)
).

-define(HAS_REPLY_TO_OP(Op),
    (Op =:= ?OP_PUB orelse
        Op =:= ?OP_HPUB orelse
        Op =:= ?OP_MSG orelse
        Op =:= ?OP_HMSG)
).

-define(HAS_HEADERS_OP(Op),
    (Op =:= ?OP_HPUB orelse
        Op =:= ?OP_HMSG)
).

-define(HAS_PAYLOAD_OP(Op),
    (Op =:= ?OP_PUB orelse
        Op =:= ?OP_HPUB orelse
        Op =:= ?OP_MSG orelse
        Op =:= ?OP_HMSG)
).

-define(PACKET(Op), #nats_frame{operation = Op}).
-define(PACKET(Op, Message), #nats_frame{operation = Op, message = Message}).

%% Default max payload size

% 1MB
-define(DEFAULT_MAX_PAYLOAD, 1048576).

-endif.
