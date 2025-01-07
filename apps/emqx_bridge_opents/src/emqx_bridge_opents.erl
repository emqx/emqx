%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_opents).

-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2, array/1]).

-export([
    conn_bridge_examples/1,
    bridge_v2_examples/1,
    default_data_template/0
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, opents).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

%% -------------------------------------------------------------------------------------------------
%% v1 examples
conn_bridge_examples(Method) ->
    [
        #{
            <<"opents">> => #{
                summary => <<"OpenTSDB Bridge">>,
                value => values(Method)
            }
        }
    ].

values(_Method) ->
    #{
        enabledb => true,
        type => opents,
        name => <<"foo">>,
        server => <<"http://127.0.0.1:4242">>,
        pool_size => 8,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => async,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% v2 examples
bridge_v2_examples(Method) ->
    [
        #{
            <<"opents">> => #{
                summary => <<"OpenTSDB Action">>,
                value => emqx_bridge_v2_schema:action_values(
                    Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                )
            }
        }
    ].

action_values() ->
    #{
        parameters => #{
            data => default_data_template()
        }
    }.

default_data_template() ->
    [
        #{
            metric => <<"${metric}">>,
            tags => <<"${tags}">>,
            value => <<"${value}">>
        }
    ].

%% -------------------------------------------------------------------------------------------------
%% V1 Schema Definitions
namespace() -> "bridge_opents".

roots() -> [].

fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        emqx_bridge_opents_connector:fields(config);
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post");
%% -------------------------------------------------------------------------------------------------
%% V2 Schema Definitions

fields(action) ->
    {opents,
        mk(
            hoconsc:map(name, ref(?MODULE, action_config)),
            #{
                desc => <<"OpenTSDB Action Config">>,
                required => false
            }
        )};
fields(action_config) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(?MODULE, action_parameters),
            #{
                required => true, desc => ?DESC("action_parameters")
            }
        )
    );
fields(action_parameters) ->
    [
        {data,
            mk(
                array(ref(?MODULE, action_parameters_data)),
                #{
                    desc => ?DESC("action_parameters_data"),
                    default => []
                }
            )}
    ];
fields(action_parameters_data) ->
    TagsError = fun(Data) ->
        ?SLOG(warning, #{
            msg => "invalid_tags_template",
            path => "opents.parameters.data.tags",
            data => Data
        }),
        false
    end,
    [
        {timestamp,
            mk(
                emqx_schema:template(),
                #{
                    desc => ?DESC("config_parameters_timestamp"),
                    required => false
                }
            )},
        {metric,
            mk(
                emqx_schema:template(),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_metric")
                }
            )},
        {tags,
            mk(
                hoconsc:union([map(), emqx_schema:template()]),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_tags"),
                    validator => fun
                        (Tmpl) when is_binary(Tmpl) ->
                            case emqx_placeholder:preproc_tmpl(Tmpl) of
                                [{var, _}] ->
                                    true;
                                _ ->
                                    TagsError(Tmpl)
                            end;
                        (Map) when is_map(Map) ->
                            case maps:size(Map) >= 1 of
                                true ->
                                    true;
                                _ ->
                                    TagsError(Map)
                            end;
                        (Any) ->
                            TagsError(Any)
                    end
                }
            )},
        {value,
            mk(
                hoconsc:union([integer(), float(), emqx_schema:template()]),
                #{
                    required => true,
                    desc => ?DESC("config_parameters_value")
                }
            )}
    ];
fields("post_bridge_v2") ->
    emqx_bridge_schema:type_and_name_fields(enum([opents])) ++ fields(action_config);
fields("put_bridge_v2") ->
    fields(action_config);
fields("get_bridge_v2") ->
    emqx_bridge_schema:status_fields() ++ fields("post_bridge_v2").

desc("config") ->
    ?DESC("desc_config");
desc(action_config) ->
    ?DESC("desc_config");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(action_parameters_data) ->
    ?DESC("action_parameters_data");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for OpenTSDB using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------
%% internal

type_field() ->
    {type, mk(enum([opents]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
