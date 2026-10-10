%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_nats_action_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-include("emqx_bridge_nats.hrl").

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    bridge_v2_examples/1
]).

namespace() ->
    "action_nats".

roots() ->
    [].

%%--------------------------------------------------------------------
%% API and action configuration
%%--------------------------------------------------------------------

fields("get_bridge_v2") ->
    emqx_bridge_v2_schema:api_fields(
        "get_bridge_v2", ?ACTION_TYPE, fields(?ACTION_TYPE)
    );
fields("put_bridge_v2") ->
    emqx_bridge_v2_schema:api_fields(
        "put_bridge_v2", ?ACTION_TYPE, fields(?ACTION_TYPE)
    );
fields("post_bridge_v2") ->
    emqx_bridge_v2_schema:api_fields(
        "post_bridge_v2", ?ACTION_TYPE, fields(?ACTION_TYPE)
    );
fields(action) ->
    {
        ?ACTION_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, ?ACTION_TYPE)),
            #{
                desc => <<"NATS Action Config">>,
                required => false,
                validator => fun validate_actions/1
            }
        )
    };
fields(?ACTION_TYPE) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("parameters")
            }
        )
    );
fields(action_parameters) ->
    [
        {subject,
            hoconsc:mk(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC("subject")
                }
            )},
        {payload_template,
            hoconsc:mk(
                emqx_schema:template(),
                #{
                    default => <<"${.payload}">>,
                    desc => ?DESC("payload_template")
                }
            )},
        {headers,
            hoconsc:mk(
                hoconsc:array(hoconsc:ref(?MODULE, header)),
                #{
                    default => [],
                    desc => ?DESC("headers")
                }
            )},
        {delivery_mode,
            hoconsc:mk(
                hoconsc:enum([core, jetstream]),
                #{
                    default => core,
                    desc => ?DESC("delivery_mode")
                }
            )},
        {msg_id_template,
            hoconsc:mk(
                emqx_schema:template(),
                #{
                    default => <<>>,
                    desc => ?DESC("msg_id_template")
                }
            )}
    ];
fields(header) ->
    [
        {key,
            hoconsc:mk(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC("header_key")
                }
            )},
        {value,
            hoconsc:mk(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC("header_value")
                }
            )}
    ].

desc(?ACTION_TYPE) ->
    ?DESC(?ACTION_TYPE);
desc(action_parameters) ->
    ?DESC("parameters");
desc(header) ->
    ?DESC("header");
desc(_) ->
    undefined.

validate_actions(#{
    <<"parameters">> := #{<<"delivery_mode">> := DeliveryMode},
    <<"resource_opts">> := #{<<"batch_size">> := BatchSize}
}) ->
    validate_batch_size(DeliveryMode, BatchSize);
validate_actions(Actions) ->
    maps:fold(
        fun
            (_Name, _Action, {error, _} = Error) -> Error;
            (_Name, Action, ok) -> validate_actions(Action)
        end,
        ok,
        Actions
    ).

%% The client currently exposes only synchronous JetStream publishing.
%% NATS Server 2.12+ supports atomic batches via Nats-Batch-* headers (JetStream API level 2).
%% That requires stream allow_atomic and server capability checks to retain NATS 2.10 compatibility.
%% See https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-50.md.
%% Keep batching disabled until the client supports pipelined PubAcks or version-gated atomic batches.
validate_batch_size(jetstream, BatchSize) when BatchSize > 1 ->
    {error, <<"JetStream publishing requires resource_opts.batch_size = 1">>};
validate_batch_size(_DeliveryMode, _BatchSize) ->
    ok.

%%--------------------------------------------------------------------
%% Action examples
%%--------------------------------------------------------------------

bridge_v2_examples(Method) ->
    [
        #{
            ?ACTION_TYPE_BIN => #{
                summary => <<"NATS Action">>,
                value => example(Method)
            }
        }
    ].

example(post) ->
    maps:merge(
        example(put),
        #{
            type => ?ACTION_TYPE_BIN,
            name => <<"nats_action">>
        }
    );
example(get) ->
    maps:merge(
        example(put),
        #{
            status => <<"connected">>,
            node_status => []
        }
    );
example(put) ->
    #{
        enable => true,
        description => <<"NATS action">>,
        connector => <<"nats_connector">>,
        parameters => #{
            subject => <<"events">>,
            payload_template => <<"${.payload}">>,
            headers => [],
            delivery_mode => core,
            msg_id_template => <<>>
        },
        resource_opts => #{
            query_mode => <<"sync">>,
            batch_size => 1,
            batch_time => <<"0ms">>,
            request_ttl => <<"5s">>
        }
    }.
