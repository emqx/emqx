%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_s3.hrl").

-behaviour(hocon_schema).
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    bridge_v2_examples/1,
    connector_examples/1
]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_s3".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR, fields(s3_connector_config));
fields(Field) when
    Field == "get_bridge_v2";
    Field == "put_bridge_v2";
    Field == "post_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION, fields(?ACTION));
fields(action) ->
    {?ACTION,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, ?ACTION)),
            #{
                desc => <<"S3 Action Config">>,
                required => false
            }
        )};
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields(s3_connector_config);
fields(?ACTION) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            ?R_REF(s3_upload_parameters),
            #{
                required => true,
                desc => ?DESC(s3_upload)
            }
        ),
        #{
            resource_opts_ref => ?R_REF(s3_action_resource_opts)
        }
    );
fields(s3_connector_config) ->
    emqx_s3_schema:fields(s3_client) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, s3_connector_resource_opts);
fields(s3_upload_parameters) ->
    emqx_s3_schema:fields(s3_upload) ++
        [
            {content,
                hoconsc:mk(
                    string(),
                    #{
                        required => false,
                        default => <<"${.}">>,
                        desc => ?DESC(s3_object_content)
                    }
                )}
        ];
fields(s3_action_resource_opts) ->
    UnsupportedOpts = [batch_size, batch_time],
    lists:filter(
        fun({N, _}) -> not lists:member(N, UnsupportedOpts) end,
        emqx_bridge_v2_schema:action_resource_opts_fields()
    );
fields(s3_connector_resource_opts) ->
    CommonOpts = emqx_connector_schema:common_resource_opts_subfields(),
    lists:filter(
        fun({N, _}) -> lists:member(N, CommonOpts) end,
        emqx_connector_schema:resource_opts_fields()
    ).

desc("config_connector") ->
    ?DESC(config_connector);
desc(?ACTION) ->
    ?DESC(s3_upload);
desc(s3_upload) ->
    ?DESC(s3_upload);
desc(s3_upload_parameters) ->
    ?DESC(s3_upload_parameters);
desc(s3_action_resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(s3_connector_resource_opts) ->
    ?DESC(emqx_resource_schema, resource_opts);
desc(_Name) ->
    undefined.

%% Examples

bridge_v2_examples(Method) ->
    [
        #{
            <<"s3">> => #{
                summary => <<"S3 Simple Upload">>,
                value => action_example(Method)
            }
        }
    ].

action_example(post) ->
    maps:merge(
        action_example(put),
        #{
            type => atom_to_binary(?ACTION),
            name => <<"my_s3_action">>
        }
    );
action_example(get) ->
    maps:merge(
        action_example(put),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
action_example(put) ->
    #{
        enable => true,
        connector => <<"my_s3_connector">>,
        description => <<"My action">>,
        parameters => #{
            bucket => <<"${clientid}">>,
            key => <<"${topic}">>,
            content => <<"${payload}">>,
            acl => <<"public_read">>
        },
        resource_opts => #{
            query_mode => <<"sync">>,
            inflight_window => 10
        }
    }.

connector_examples(Method) ->
    [
        #{
            <<"s3_aws">> => #{
                summary => <<"S3 Connector">>,
                value => connector_example(Method)
            }
        }
    ].

connector_example(get) ->
    maps:merge(
        connector_example(put),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        }
    );
connector_example(post) ->
    maps:merge(
        connector_example(put),
        #{
            type => atom_to_binary(?CONNECTOR),
            name => <<"my_s3_connector">>
        }
    );
connector_example(put) ->
    #{
        enable => true,
        description => <<"My S3 connector">>,
        host => <<"s3.eu-east-1.amazonaws.com">>,
        port => 443,
        access_key_id => <<"ACCESS">>,
        secret_access_key => <<"SECRET">>,
        transport_options => #{
            ssl => #{
                enable => true,
                verify => <<"verify_peer">>
            },
            connect_timeout => <<"1s">>,
            request_timeout => <<"60s">>,
            pool_size => 4,
            max_retries => 1,
            enable_pipelining => 1
        }
    }.
