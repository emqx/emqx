%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_schema).

-include("emqx_streams_internal.hrl").

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% `hocon_schema' API
-export([namespace/0, roots/0, fields/1, desc/1, tags/0]).

-export([
    stream_sctype_api_get/0, stream_sctype_api_put/0, stream_sctype_api_post/0, validate_name/1
]).

%%------------------------------------------------------------------------------
%% `hocon_schema' APIs
%%------------------------------------------------------------------------------

namespace() ->
    ?SCHEMA_ROOT.

roots() ->
    [?SCHEMA_ROOT].

tags() ->
    [<<"Message Streams">>].

fields(?SCHEMA_ROOT) ->
    [
        {enable,
            mk(hoconsc:union([boolean(), auto]), #{
                default => false,
                required => true,
                desc => ?DESC(enable)
            })},
        {max_stream_count,
            mk(non_neg_integer(), #{
                default => 1000,
                desc => ?DESC(max_stream_count)
            })},
        {gc_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"1h">>, required => true, desc => ?DESC(gc_interval)
            })},
        {regular_stream_retention_period,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"7d">>,
                required => true,
                desc => ?DESC(regular_stream_retention_period)
            })},
        {check_stream_status_interval,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"10s">>,
                required => true,
                desc => ?DESC(check_stream_status_interval),
                importance => ?IMPORTANCE_HIDDEN
            })},
        {auto_create,
            mk(ref(auto_create), #{
                required => true,
                desc => ?DESC(auto_create),
                default => #{
                    <<"regular">> => false,
                    <<"lastvalue">> => #{}
                }
            })},
        {quota,
            mk(ref(quota), #{
                required => true,
                desc => ?DESC(quota),
                default => #{},
                importance => ?IMPORTANCE_HIDDEN
            })}
    ];
%% NOTE
%% We do not want to expose the quota settings to the users for now.
%% We benchmarked the default values and believe that they are reasonable for the most users.
%% Exposing these settings should be done with guidance from the documentation
%% and after we gain more use cases for the quota feature.
fields(quota) ->
    emqx_mq_quota_schema:quota_fields();
fields(auto_create) ->
    [
        {regular,
            mk(hoconsc:union([false, ref(auto_create_regular)]), #{
                required => true,
                default => false,
                converter => serialize_converter(
                    fun
                        (false) ->
                            false;
                        (#{} = Val) ->
                            emqx_schema:fill_defaults_for_type(ref(auto_create_regular), Val)
                    end
                ),
                desc => ?DESC(auto_create_regular)
            })},
        {lastvalue,
            mk(hoconsc:union([false, ref(auto_create_lastvalue)]), #{
                required => true,
                default => #{},
                converter => serialize_converter(
                    fun
                        (false) ->
                            false;
                        (#{} = Val) ->
                            emqx_schema:fill_defaults_for_type(ref(auto_create_lastvalue), Val)
                    end
                ),
                desc => ?DESC(auto_create_lastvalue)
            })}
    ];
fields(auto_create_regular) ->
    RegularMQFields = stream_fields(false),
    without_fields([is_lastvalue, topic_filter, name], RegularMQFields);
fields(auto_create_lastvalue) ->
    LastvalueMQFields = stream_fields(true),
    without_fields([is_lastvalue, topic_filter, name], LastvalueMQFields);
%% Stream structs
fields(stream_individual_limits) ->
    [
        {max_shard_message_count,
            mk(hoconsc:union([infinity, pos_integer()]), #{
                desc => ?DESC(max_shard_message_count), required => true, default => <<"infinity">>
            })},
        {max_shard_message_bytes,
            mk(hoconsc:union([infinity, emqx_schema:bytesize()]), #{
                desc => ?DESC(max_shard_message_bytes), required => true, default => <<"infinity">>
            })}
    ];
%%
%% Lastvalue structs
%%
fields(stream_lastvalue_api_put) ->
    without_fields([name, topic_filter], stream_fields(true));
fields(stream_lastvalue_api_get) ->
    stream_fields(true);
fields(stream_lastvalue_api_post) ->
    stream_fields(true);
%%
%% Regular structs
%%
fields(stream_regular_api_put) ->
    without_fields([name, topic_filter], stream_fields(false));
fields(stream_regular_api_get) ->
    stream_fields(false);
fields(stream_regular_api_post) ->
    stream_fields(false);
%%
%% Queue listing
%%
fields(stream_api_get) ->
    [
        {data, mk(array(stream_sctype_api_get()), #{})},
        {meta, mk(ref(emqx_dashboard_swagger, meta_with_cursor), #{})}
    ];
%%
%% Config structs
%%
fields(api_config_get) ->
    fields(?SCHEMA_ROOT);
fields(api_config_put) ->
    fields(api_config_get).

desc(?SCHEMA_ROOT) ->
    ?DESC(streams);
desc(auto_create) ->
    ?DESC(auto_create);
desc(auto_create_regular) ->
    ?DESC(auto_create_regular);
desc(auto_create_lastvalue) ->
    ?DESC(auto_create_lastvalue);
desc(stream_individual_limits) ->
    ?DESC(stream_individual_limits);
desc(_) ->
    undefined.

stream_sctype_api_get() ->
    stream_sctype(ref(stream_lastvalue_api_get), ref(stream_regular_api_get)).

stream_sctype_api_put() ->
    stream_sctype(ref(stream_lastvalue_api_put), ref(stream_regular_api_put)).

stream_sctype_api_post() ->
    stream_sctype(ref(stream_lastvalue_api_post), ref(stream_regular_api_post)).

validate_name(Name) ->
    RE = "^[0-9a-zA-Z][\\-\\.0-9a-zA-Z_]*$",
    case re:run(Name, RE, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            {error, invalid_name}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

stream_fields(IsLastValue) ->
    [
        {name,
            mk(binary(), #{desc => ?DESC(name), required => true, validator => fun validate_name/1})},
        {topic_filter, mk(binary(), #{desc => ?DESC(topic_filter), required => true})},
        {is_lastvalue,
            mk(
                IsLastValue,
                #{
                    desc => ?DESC(is_lastvalue),
                    required => true,
                    default => IsLastValue
                }
            )},
        {key_expression,
            mk(typerefl:alias("string", any()), #{
                desc => ?DESC(key_expression),
                required => true,
                converter => fun compile_variform/2,
                default => <<"message.from">>
            })},
        {data_retention_period,
            mk(emqx_schema:duration_ms(), #{
                desc => ?DESC(data_retention_period),
                required => false,
                default => <<"7d">>
            })},
        {limits,
            mk(ref(stream_individual_limits), #{
                desc => ?DESC(stream_individual_limits),
                required => true,
                default => #{
                    <<"max_shard_message_count">> => infinity,
                    <<"max_shard_message_bytes">> => infinity
                }
            })},
        {read_max_unacked,
            mk(pos_integer(), #{
                desc => ?DESC(read_max_unacked),
                required => false,
                importance => ?IMPORTANCE_HIDDEN,
                default => 1000
            })}
    ].

stream_sctype(LastvalueType, RegularType) ->
    hoconsc:union([LastvalueType, RegularType]).

mk(Type, Meta) ->
    hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
ref(Module, Struct) -> hoconsc:ref(Module, Struct).
array(Type) -> hoconsc:array(Type).

without_fields(FieldNames, Fields) ->
    lists:filter(
        fun({Name, _}) ->
            not lists:member(Name, FieldNames)
        end,
        Fields
    ).

serialize_converter(Fun) ->
    fun
        (Val, #{make_serializable := true}) ->
            Fun(Val);
        (Val, _Opts) ->
            Val
    end.

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
