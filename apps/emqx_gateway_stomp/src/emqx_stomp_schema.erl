%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_stomp_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

%% config schema provides
-export([namespace/0, fields/1, desc/1]).

namespace() -> "gateway".

fields(stomp) ->
    [
        {frame, sc(ref(stomp_frame))},
        {transaction, sc(ref(stomp_transaction))},
        {mountpoint, emqx_gateway_schema:mountpoint()},
        {listeners, sc(ref(emqx_gateway_schema, tcp_listeners), #{desc => ?DESC(tcp_listeners)})}
    ] ++ emqx_gateway_schema:gateway_common_options();
fields(stomp_frame) ->
    [
        {max_headers,
            sc(
                non_neg_integer(),
                #{
                    default => 10,
                    desc => ?DESC(stomp_frame_max_headers)
                }
            )},
        {max_headers_length,
            sc(
                non_neg_integer(),
                #{
                    default => 1024,
                    desc => ?DESC(stomp_frame_max_headers_length)
                }
            )},
        {max_body_length,
            sc(
                integer(),
                #{
                    default => 65536,
                    desc => ?DESC(stomp_frame_max_body_length)
                }
            )}
    ];
fields(stomp_transaction) ->
    [
        {max_transactions,
            sc(
                non_neg_integer(),
                #{
                    default => 100,
                    desc => ?DESC(stomp_transaction_max_transactions)
                }
            )},
        {max_actions_per_transaction,
            sc(
                non_neg_integer(),
                #{
                    default => 1000,
                    desc => ?DESC(stomp_transaction_max_actions_per_transaction)
                }
            )},
        {max_retained_bytes,
            sc(
                non_neg_integer(),
                #{
                    default => 16777216,
                    desc => ?DESC(stomp_transaction_max_retained_bytes)
                }
            )},
        {timeout,
            sc(
                emqx_schema:duration_ms(),
                #{
                    default => <<"60s">>,
                    desc => ?DESC(stomp_transaction_timeout)
                }
            )}
    ].

desc(stomp) ->
    "The STOMP protocol gateway provides EMQX with the ability to access STOMP\n"
    "(Simple (or Streaming) Text Orientated Messaging Protocol) protocol.";
desc(stomp_frame) ->
    "Size limits for the STOMP frames.";
desc(stomp_transaction) ->
    "Limits for STOMP transactions.";
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% internal functions

sc(Type) ->
    sc(Type, #{}).

sc(Type, Meta) ->
    hoconsc:mk(Type, Meta).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
