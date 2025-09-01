%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([namespace/0, roots/0, fields/1, desc/1, tags/0]).

-export([db_mq_state/0]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec db_mq_state() -> emqx_ds:create_db_opts().
db_mq_state() ->
    emqx_ds_schema:db_config([mq, state_db]).

%%------------------------------------------------------------------------------
%% `hocon_schema' APIs
%%------------------------------------------------------------------------------

namespace() ->
    mq.

roots() ->
    [mq].

tags() ->
    [<<"Message Queue">>].

fields(mq) ->
    [
        {state_db,
            emqx_ds_schema:db_schema(
                [builtin_raft_ttv, builtin_local_ttv],
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(state_db)
                }
            )},
        {gc_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"1h">>, required => true, desc => ?DESC(gc_interval)
            })},
        {regular_queue_retention_period,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"1d">>, required => true, desc => ?DESC(regular_queue_retention_period)
            })}
    ];
fields(message_queue) ->
    [
        {topic_filter, mk(binary(), #{desc => ?DESC(topic_filter), required => true})},
        {is_lastvalue,
            mk(
                boolean(),
                #{
                    desc => ?DESC(is_lastvalue),
                    required => false,
                    default => true
                }
            )},
        {consumer_max_inactive,
            mk(emqx_schema:duration_ms(), #{
                desc => ?DESC(consumer_max_inactive),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => <<"30s">>
            })},
        {ping_interval,
            mk(emqx_schema:duration_ms(), #{
                desc => ?DESC(ping_interval),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => <<"10s">>
            })},
        {redispatch_interval,
            mk(emqx_schema:duration_ms(), #{
                desc => ?DESC(redispatch_interval),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => <<"100ms">>
            })},
        {local_max_inflight,
            mk(pos_integer(), #{
                desc => ?DESC(local_max_inflight),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => 10
            })},
        {busy_session_retry_interval,
            mk(emqx_schema:duration_ms(), #{
                desc => ?DESC(busy_session_retry_interval),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => <<"100ms">>
            })},
        {stream_max_buffer_size,
            mk(pos_integer(), #{
                desc => ?DESC(stream_max_buffer_size),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => 2000
            })},
        {stream_max_unacked,
            mk(pos_integer(), #{
                desc => ?DESC(stream_max_unacked),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => 1000
            })},
        {consumer_persistence_interval,
            mk(emqx_schema:duration_ms(), #{
                desc => ?DESC(consumer_persistence_interval),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => <<"10s">>
            })},
        {data_retention_period,
            mk(emqx_schema:duration_ms(), #{
                desc => ?DESC(data_retention_period),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => <<"7d">>
            })},
        {dispatch_strategy,
            mk(enum([random, least_inflight, round_robin]), #{
                desc => ?DESC(dispatch_strategy),
                required => false,
                default => random
            })}
    ];
fields(message_queue_api_put) ->
    without_fields([topic_filter, is_lastvalue], fields(message_queue)).

desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) ->
    hoconsc:mk(Type, Meta).

enum(Values) -> hoconsc:enum(Values).

without_fields(FieldNames, Fields) ->
    lists:filter(
        fun({Name, _}) ->
            not lists:member(Name, FieldNames)
        end,
        Fields
    ).
