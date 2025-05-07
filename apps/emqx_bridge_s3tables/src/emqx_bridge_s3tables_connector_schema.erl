%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_s3tables_connector_schema).

-feature(maybe_expr, enable).

-behaviour(hocon_schema).
-behaviour(emqx_connector_examples).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_s3tables.hrl").

%% `hocon_schema' API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% `emqx_connector_examples' API
-export([
    connector_examples/1
]).

%% API
-export([
    parse_base_endpoint/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% `hocon_schema' API
%%------------------------------------------------------------------------------

namespace() ->
    "connector_s3tables".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, fields(connector_config));
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++ fields(connector_config);
fields(connector_config) ->
    [
        {parameters,
            mk(
                emqx_schema:mkunion(location_type, #{
                    <<"s3tables">> => ref(s3tables_connector_params)
                }),
                #{required => true, desc => ?DESC("parameters")}
            )}
    ] ++
        emqx_connector_schema:resource_opts();
fields(s3tables_connector_params) ->
    [
        {location_type, mk(s3tables, #{required => true, desc => ?DESC("location_type_s3t")})},
        {account_id, mk(binary(), #{required => false, importance => ?IMPORTANCE_HIDDEN})},
        {access_key_id,
            mk(binary(), #{required => true, desc => ?DESC("location_s3t_access_key_id")})},
        {secret_access_key,
            emqx_schema_secret:mk(
                #{
                    desc => ?DESC("location_s3t_secret_access_key")
                }
            )},
        {base_endpoint,
            mk(binary(), #{required => true, desc => ?DESC("location_s3t_base_endpoint")})},
        {bucket, mk(binary(), #{required => true, desc => ?DESC("location_s3t_bucket")})},
        {request_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"30s">>,
                desc => ?DESC(emqx_bridge_http_connector, "request_timeout")
            })},
        {s3_client,
            mk(hoconsc:ref(emqx_s3_schema, s3_client), #{
                required => true, desc => ?DESC("s3_client")
            })}
    ].

desc(Name) when
    Name =:= "config_connector";
    Name =:= s3tables_connector_params
->
    ?DESC(Name);
desc(_Name) ->
    undefined.

%%------------------------------------------------------------------------------
%% `emqx_connector_examples' API
%%------------------------------------------------------------------------------

connector_examples(Method) ->
    [
        #{
            <<"s3t">> => #{
                summary => <<"S3Tables Connector">>,
                value => connector_example(Method, s3tables)
            }
        }
    ].

connector_example(get, LocationProvider) ->
    maps:merge(
        connector_example(put, LocationProvider),
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
connector_example(post, LocationProvider) ->
    maps:merge(
        connector_example(put, LocationProvider),
        #{
            type => atom_to_binary(?CONNECTOR_TYPE),
            name => <<"my_connector">>
        }
    );
connector_example(put, s3tables = _LocationProvider) ->
    #{
        enable => true,
        description => <<"My connector">>,
        parameters => #{
            location_type => <<"s3tables">>,
            access_key_id => <<"12345">>,
            secret_access_key => <<"******">>,
            base_endpoint => <<"https://s3tables.sa-east-1.amazonaws.com/iceberg/v1">>,
            bucket => <<"my-s3tables-bucket">>,
            request_timeout => <<"10s">>,
            s3_client => #{
                access_key_id => <<"12345">>,
                secret_access_key => <<"******">>,
                host => <<"s3.sa-east-1.amazonaws.com">>,
                port => 443,
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
            }
        },
        resource_opts => #{
            health_check_interval => <<"45s">>,
            start_after_created => true,
            start_timeout => <<"5s">>
        }
    }.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

parse_base_endpoint(Endpoint) ->
    maybe
        #{
            path := BasePath0,
            scheme := Scheme,
            authority := #{
                host := Host
            }
        } = Parsed ?= parse_uri(Endpoint),
        true ?= lists:member(Scheme, [<<"https">>, <<"http">>]) orelse
            {error, {bad_scheme, Scheme}},
        HostStr = to_str(Host),
        BaseURI = to_str(emqx_utils_uri:base_url(Parsed)),
        BasePath1 = binary:split(BasePath0, <<"/">>, [global, trim_all]),
        BasePath = lists:map(fun to_str/1, BasePath1),
        {ok, #{
            host => HostStr,
            base_uri => BaseURI,
            base_path => BasePath
        }}
    else
        {error, Reason} ->
            {error, Reason};
        _ ->
            {error, {bad_endpoint, Endpoint}}
    end.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).

to_str(X) -> emqx_utils_conv:str(iolist_to_binary(X)).

parse_uri(Endpoint) ->
    %% When the URI has no scheme, the hostname/authority might be interpret as path...
    maybe
        #{scheme := undefined, authority := undefined} ?= emqx_utils_uri:parse(Endpoint),
        parse_uri(<<"https://", Endpoint/binary>>)
    end.
