%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([namespace/0, roots/0, fields/1, desc/1, tags/0]).

-export([mq_sctype_api_get/0, mq_sctype_api_put/0, mq_sctype_api_post/0]).

%%------------------------------------------------------------------------------
%% `hocon_schema' APIs
%%------------------------------------------------------------------------------

namespace() ->
    mq.

roots() ->
    [mq].

tags() ->
    [<<"Message Queue">>].

%%
%% MQ config
%%
fields(mq) ->
    [
        {enable,
            mk(boolean(), #{
                default => true,
                required => true,
                desc => ?DESC(enable)
            })},
        {gc_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"1h">>, required => true, desc => ?DESC(gc_interval)
            })},
        {regular_queue_retention_period,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"7d">>, required => true, desc => ?DESC(regular_queue_retention_period)
            })},
        {max_queue_count,
            mk(pos_integer(), #{
                default => 100,
                required => true,
                desc => ?DESC(max_queue_count)
            })},
        {find_queue_retry_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"10s">>,
                required => true,
                desc => ?DESC(find_queue_retry_interval),
                importance => ?IMPORTANCE_MEDIUM
            })},
        {auto_create,
            mk(ref(auto_create), #{
                required => true,
                desc => ?DESC(auto_create),
                default => #{
                    <<"regular">> => #{<<"enable">> => false},
                    <<"lastvalue">> => #{<<"enable">> => true}
                }
            })}
    ];
fields(auto_create) ->
    [
        {regular,
            mk(ref(auto_create_regular), #{required => true, desc => ?DESC(auto_create_regular)})},
        {lastvalue,
            mk(ref(auto_create_lastvalue), #{required => true, desc => ?DESC(auto_create_lastvalue)})}
    ];
fields(auto_create_regular) ->
    RegularMQFields = message_queue_fields(false),
    [
        {enable,
            mk(boolean(), #{
                default => false,
                required => true,
                desc => ?DESC(auto_create_regular_enable)
            })}
    ] ++ without_fields([is_lastvalue, topic_filter], RegularMQFields);
fields(auto_create_lastvalue) ->
    LastvalueMQFields = message_queue_fields(true) ++ message_queue_lastvalue_fields(),
    [
        {enable,
            mk(boolean(), #{
                default => true,
                required => true,
                desc => ?DESC(auto_create_lastvalue_enable)
            })}
    ] ++ without_fields([is_lastvalue, topic_filter], LastvalueMQFields);
%%
%% Lastvalue structs
%%
fields(message_queue_api_lastvalue_put) ->
    without_fields([topic_filter], message_queue_fields(true)) ++ message_queue_lastvalue_fields();
fields(message_queue_lastvalue_api_get) ->
    message_queue_fields(true) ++ message_queue_lastvalue_fields();
fields(message_queue_lastvalue_api_post) ->
    message_queue_fields(true) ++ message_queue_lastvalue_fields();
%%
%% Regular structs
%%
fields(message_queue_api_regular_put) ->
    without_fields([topic_filter], message_queue_fields(false));
fields(message_queue_regular_api_get) ->
    message_queue_fields(false);
fields(message_queue_regular_api_post) ->
    message_queue_fields(false);
%%
%% Queue listing
%%
fields(message_queues_api_get) ->
    [
        {data, mk(array(mq_sctype_api_get()), #{})},
        {meta, mk(ref(emqx_dashboard_swagger, meta_with_cursor), #{})}
    ];
%%
%% Config structs
%%
fields(api_config_get) ->
    fields(mq);
fields(api_config_put) ->
    fields(api_config_get).

desc(mq) ->
    ?DESC(mq);
desc(auto_create) ->
    ?DESC(auto_create);
desc(auto_create_regular) ->
    ?DESC(auto_create_regular);
desc(auto_create_lastvalue) ->
    ?DESC(auto_create_lastvalue);
desc(_) ->
    undefined.

mq_sctype_api_get() ->
    mq_sctype(ref(message_queue_lastvalue_api_get), ref(message_queue_regular_api_get)).

mq_sctype_api_put() ->
    mq_sctype(ref(message_queue_api_lastvalue_put), ref(message_queue_api_regular_put)).

mq_sctype_api_post() ->
    mq_sctype(ref(message_queue_lastvalue_api_post), ref(message_queue_regular_api_post)).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

message_queue_fields(IsLastvalue) ->
    [
        {topic_filter, mk(binary(), #{desc => ?DESC(topic_filter), required => true})},
        {is_lastvalue,
            mk(
                IsLastvalue,
                #{
                    desc => ?DESC(is_lastvalue),
                    required => true,
                    default => IsLastvalue
                }
            )},
        {data_retention_period,
            mk(emqx_schema:duration_ms(), #{
                desc => ?DESC(data_retention_period),
                required => false,
                default => <<"7d">>
            })},
        {dispatch_strategy,
            mk(enum([random, least_inflight, round_robin]), #{
                desc => ?DESC(dispatch_strategy),
                required => false,
                default => random
            })},
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
            })}
    ].

message_queue_lastvalue_fields() ->
    [
        {key_expression,
            mk(typerefl:alias("string", any()), #{
                desc => ?DESC(key_expression),
                required => true,
                converter => fun compile_variform/2,
                default => <<"message.from">>
            })}
    ].

mq_sctype(LastvalueType, RegularType) ->
    hoconsc:union([LastvalueType, RegularType]).

mk(Type, Meta) ->
    hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
ref(Module, Struct) -> hoconsc:ref(Module, Struct).
array(Type) -> hoconsc:array(Type).

enum(Values) -> hoconsc:enum(Values).

without_fields(FieldNames, Fields) ->
    lists:filter(
        fun({Name, _}) ->
            not lists:member(Name, FieldNames)
        end,
        Fields
    ).

compile_variform(Expression, #{make_serializable := true}) ->
    case is_binary(Expression) of
        true ->
            Expression;
        false ->
            emqx_variform:decompile(Expression)
    end;
compile_variform(Expression, _Opts) ->
    case emqx_variform:compile(Expression) of
        {ok, Compiled} ->
            Compiled;
        {error, Reason} ->
            throw(#{expression => Expression, reason => Reason})
    end.
