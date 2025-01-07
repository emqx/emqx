%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_pubsub_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([roots/0, fields/1, desc/1, namespace/0]).

-export([bridge_v2_examples/1]).

-define(ACTION_TYPE, pulsar).

namespace() -> "pulsar".

roots() -> [].

fields(action) ->
    {pulsar,
        ?HOCON(
            ?MAP(name, ?R_REF(publisher_action)),
            #{
                desc => <<"Pulsar Action Config">>,
                required => false
            }
        )};
fields(publisher_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        ?HOCON(
            ?R_REF(action_parameters),
            #{
                required => true,
                desc => ?DESC(action_parameters),
                validator => fun emqx_bridge_pulsar:producer_strategy_key_validator/1
            }
        ),
        #{resource_opts_ref => ?R_REF(action_resource_opts)}
    );
fields(action_parameters) ->
    [
        {message,
            ?HOCON(?R_REF(producer_pulsar_message), #{
                required => false, desc => ?DESC("producer_message_opts")
            })},
        {sync_timeout,
            ?HOCON(emqx_schema:timeout_duration_ms(), #{
                default => <<"3s">>, desc => ?DESC("producer_sync_timeout")
            })}
    ] ++ emqx_bridge_pulsar:fields(producer_opts);
fields(producer_pulsar_message) ->
    [
        {key,
            ?HOCON(emqx_schema:template(), #{
                default => <<"${.clientid}">>,
                desc => ?DESC("producer_key_template")
            })},
        {value,
            ?HOCON(emqx_schema:template(), #{
                default => <<"${.}">>,
                desc => ?DESC("producer_value_template")
            })}
    ];
fields(action_resource_opts) ->
    UnsupportedOpts = [
        batch_size,
        batch_time,
        worker_pool_size,
        inflight_window,
        max_buffer_bytes
    ],
    Fields = lists:filter(
        fun({K, _V}) -> not lists:member(K, UnsupportedOpts) end,
        emqx_bridge_v2_schema:action_resource_opts_fields()
    ),
    Overrides = #{request_ttl => #{deprecated => {since, "5.8.1"}}},
    lists:map(
        fun({K, Sc}) ->
            case maps:find(K, Overrides) of
                {ok, Override} ->
                    {K, hocon_schema:override(Sc, Override)};
                error ->
                    {K, Sc}
            end
        end,
        Fields
    );
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(publisher_action));
fields(What) ->
    error({?MODULE, missing_field_handler, What}).

desc("config") ->
    ?DESC("desc_config");
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(action_parameters) ->
    ?DESC(action_parameters);
desc(producer_pulsar_message) ->
    ?DESC("producer_message_opts");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Pulsar Producer using `", string:to_upper(Method), "` method."];
desc(publisher_action) ->
    ?DESC(publisher_action);
desc(_) ->
    undefined.

bridge_v2_examples(Method) ->
    [
        #{
            <<"pulsar">> => #{
                summary => <<"Pulsar Producer Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method,
                    _ActionType = ?ACTION_TYPE,
                    _ConnectorType = pulsar,
                    #{
                        parameters => #{
                            sync_timeout => <<"5s">>,
                            message => #{
                                key => <<"${.clientid}">>,
                                value => <<"${.}">>
                            },
                            pulsar_topic => <<"test_topic">>
                        }
                    }
                )
            }
        }
    ].
