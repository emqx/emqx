%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_bridge_http_schema).

-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/1, ref/2]).

-export([roots/0, fields/1, namespace/0, desc/1]).

-export([
    bridge_v2_examples/1,
    connector_examples/1
]).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge_http".

roots() -> [].

%%--------------------------------------------------------------------
%% v1 bridges http api
%% see: emqx_bridge_schema:get_response/0, put_request/0, post_request/0
fields("post") ->
    [
        old_type_field(),
        name_field()
    ] ++ fields("config");
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post");
%%--- v1 bridges config file
%% see: emqx_bridge_schema:fields(bridges)
fields("config") ->
    basic_config() ++
        request_config() ++
        emqx_connector_schema:resource_opts_ref(?MODULE, "v1_resource_opts");
fields("v1_resource_opts") ->
    UnsupportedOpts = [enable_batch, batch_size, batch_time],
    lists:filter(
        fun({K, _V}) -> not lists:member(K, UnsupportedOpts) end,
        emqx_resource_schema:fields("creation_opts")
    );
%%--------------------------------------------------------------------
%% v2: configuration
fields(action) ->
    {http,
        mk(
            hoconsc:map(name, ref(?MODULE, "http_action")),
            #{
                aliases => [webhook],
                desc => <<"HTTP Action Config">>,
                required => false
            }
        )};
fields("http_action") ->
    emqx_bridge_v2_schema:common_fields() ++
        [
            %% Note: there's an implicit convention in `emqx_bridge' that,
            %% for egress bridges with this config, the published messages
            %% will be forwarded to such bridges.
            {local_topic,
                mk(
                    binary(),
                    #{
                        required => false,
                        desc => ?DESC("config_local_topic"),
                        importance => ?IMPORTANCE_HIDDEN
                    }
                )},
            %% Since e5.3.2, we split the http bridge to two parts: a) connector. b) actions.
            %% some fields are moved to connector, some fields are moved to actions and composed into the
            %% `parameters` field.
            {parameters,
                mk(ref("parameters_opts"), #{
                    required => true,
                    desc => ?DESC("config_parameters_opts")
                })}
        ] ++
        emqx_connector_schema:resource_opts_ref(
            ?MODULE, action_resource_opts, fun legacy_action_resource_opts_converter/2
        );
fields(action_resource_opts) ->
    UnsupportedOpts = [batch_size, batch_time],
    lists:filter(
        fun({K, _V}) -> not lists:member(K, UnsupportedOpts) end,
        emqx_bridge_v2_schema:action_resource_opts_fields()
    );
fields("parameters_opts") ->
    [
        {path,
            mk(
                emqx_schema:template(),
                #{
                    desc => ?DESC("config_path"),
                    required => false
                }
            )},
        method_field(),
        headers_field(),
        body_field(),
        max_retries_field(),
        request_timeout_field()
    ];
%% v2: api schema
%% The parameter equls to
%%   `get_bridge_v2`, `post_bridge_v2`, `put_bridge_v2` from emqx_bridge_v2_schema:api_schema/1
%%   `get_connector`, `post_connector`, `put_connector` from emqx_connector_schema:api_schema/1
fields("post_" ++ Type) ->
    [type_field(), name_field() | fields("config_" ++ Type)];
fields("put_" ++ Type) ->
    fields("config_" ++ Type);
fields("get_" ++ Type) ->
    emqx_bridge_schema:status_fields() ++ fields("post_" ++ Type);
fields("config_bridge_v2") ->
    fields("http_action");
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        connector_url_headers() ++
        connector_opts() ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields().

desc("config") ->
    ?DESC("desc_config");
desc("v1_resource_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(action_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for WebHook using `", string:to_upper(Method), "` method."];
desc("config_connector") ->
    ?DESC("desc_config");
desc("http_action") ->
    ?DESC("desc_config");
desc("parameters_opts") ->
    ?DESC("config_parameters_opts");
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% helpers for v1 only

basic_config() ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC("config_enable_bridge"),
                    default => true
                }
            )},
        {tags, emqx_schema:tags_schema()},
        {description, emqx_schema:description_schema()}
    ] ++ connector_opts().

request_config() ->
    [
        url_field(),
        {direction,
            mk(
                egress,
                #{
                    required => {false, recursively},
                    deprecated => {since, "5.0.12"}
                }
            )},
        {local_topic,
            mk(
                binary(),
                #{
                    desc => ?DESC("config_local_topic"),
                    required => false
                }
            )},
        method_field(),
        headers_field(),
        body_field(),
        max_retries_field(),
        request_timeout_field()
    ].

%%--------------------------------------------------------------------
%% helpers for v2 only

connector_url_headers() ->
    [url_field(), headers_field()].

%%--------------------------------------------------------------------
%% common funcs

%% `webhook` is kept for backward compatibility.
old_type_field() ->
    {type,
        mk(
            enum([webhook, http]),
            #{
                required => true,
                desc => ?DESC("desc_type")
            }
        )}.

type_field() ->
    {type,
        mk(
            http,
            #{
                required => true,
                desc => ?DESC("desc_type")
            }
        )}.

name_field() ->
    {name,
        mk(
            binary(),
            #{
                required => true,
                desc => ?DESC("desc_name")
            }
        )}.

url_field() ->
    {url,
        mk(
            binary(),
            #{
                required => true,
                desc => ?DESC("config_url")
            }
        )}.

headers_field() ->
    {headers,
        mk(
            map(),
            #{
                default => #{
                    <<"accept">> => <<"application/json">>,
                    <<"cache-control">> => <<"no-cache">>,
                    <<"connection">> => <<"keep-alive">>,
                    <<"content-type">> => <<"application/json">>,
                    <<"keep-alive">> => <<"timeout=5">>
                },
                desc => ?DESC("config_headers"),
                is_template => true
            }
        )}.

method_field() ->
    {method,
        mk(
            enum([post, put, get, delete]),
            #{
                default => post,
                desc => ?DESC("config_method")
            }
        )}.

body_field() ->
    {body,
        mk(
            emqx_schema:template(),
            #{
                default => undefined,
                desc => ?DESC("config_body")
            }
        )}.

max_retries_field() ->
    {max_retries,
        mk(
            non_neg_integer(),
            #{
                default => 2,
                desc => ?DESC("config_max_retries")
            }
        )}.

request_timeout_field() ->
    {request_timeout,
        mk(
            emqx_schema:duration_ms(),
            #{
                default => <<"15s">>,
                deprecated => {since, "v5.0.26"},
                desc => ?DESC("config_request_timeout")
            }
        )}.

connector_opts() ->
    mark_request_field_deperecated(
        proplists:delete(max_retries, emqx_bridge_http_connector:fields(config))
    ).

mark_request_field_deperecated(Fields) ->
    lists:map(
        fun({K, V}) ->
            case K of
                request ->
                    {K, V#{
                        %% Note: if we want to deprecate a reference type, we have to change
                        %% it to a direct type first.
                        type => typerefl:map(),
                        deprecated => {since, "5.3.2"},
                        desc => <<"This field is never used, so we deprecated it since 5.3.2.">>
                    }};
                _ ->
                    {K, V}
            end
        end,
        Fields
    ).

legacy_action_resource_opts_converter(Conf, _Opts) when is_map(Conf) ->
    %% In e5.3.0, we accidentally added `start_after_created` and `start_timeout` to the action resource opts.
    %% Since e5.4.0, we have removed them. This function is used to convert the old config to the new one.
    maps:without([<<"start_after_created">>, <<"start_timeout">>], Conf);
legacy_action_resource_opts_converter(Conf, _Opts) ->
    Conf.

%%--------------------------------------------------------------------
%% Examples

bridge_v2_examples(Method) ->
    [
        #{
            <<"http">> => #{
                summary => <<"HTTP Action">>,
                value => values({Method, bridge_v2})
            }
        }
    ].

connector_examples(Method) ->
    [
        #{
            <<"http">> => #{
                summary => <<"HTTP Connector">>,
                value => values({Method, connector})
            }
        }
    ].

values({get, Type}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        },
        values({post, Type})
    );
values({post, bridge_v2}) ->
    maps:merge(
        #{
            name => <<"my_http_action">>,
            type => <<"http">>
        },
        values({put, bridge_v2})
    );
values({post, connector}) ->
    maps:merge(
        #{
            name => <<"my_http_connector">>,
            type => <<"http">>
        },
        values({put, connector})
    );
values({put, bridge_v2}) ->
    values(bridge_v2);
values({put, connector}) ->
    values(connector);
values(bridge_v2) ->
    #{
        enable => true,
        connector => <<"my_http_connector">>,
        parameters => #{
            path => <<"/room/${room_no}">>,
            method => <<"post">>,
            headers => #{},
            body => <<"${.}">>
        },
        resource_opts => #{
            worker_pool_size => 16,
            health_check_interval => <<"15s">>,
            query_mode => <<"async">>
        }
    };
values(connector) ->
    #{
        enable => true,
        url => <<"http://localhost:8080/api/v1">>,
        headers => #{<<"content-type">> => <<"application/json">>},
        connect_timeout => <<"15s">>,
        pool_type => <<"hash">>,
        pool_size => 1,
        enable_pipelining => 100
    }.
