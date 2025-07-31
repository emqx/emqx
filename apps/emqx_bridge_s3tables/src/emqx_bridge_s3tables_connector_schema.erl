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

%% `emqx_connector' API
-export([
    pre_config_update/4
]).

%% API
-export([
    parse_base_endpoint/1,
    parse_arn/1
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
        {account_id, mk(binary(), #{required => false, importance => ?IMPORTANCE_HIDDEN})},
        {access_key_id,
            mk(binary(), #{required => false, desc => ?DESC("location_s3t_access_key_id")})},
        {secret_access_key,
            emqx_schema_secret:mk(
                #{
                    desc => ?DESC("location_s3t_secret_access_key")
                }
            )},
        {base_endpoint, mk(binary(), #{required => false, importance => ?IMPORTANCE_HIDDEN})},
        {bucket, mk(binary(), #{required => false, importance => ?IMPORTANCE_HIDDEN})},
        {s3tables_arn,
            mk(binary(), #{
                required => true,
                desc => ?DESC("location_s3t_arn"),
                validator => fun s3tables_arn_validator/1
            })},
        {request_timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"30s">>,
                desc => ?DESC(emqx_bridge_http_connector, "request_timeout")
            })},
        {s3_client,
            mk(ref(s3_client_params), #{
                required => true, desc => ?DESC("s3_client")
            })}
    ] ++
        emqx_connector_schema:resource_opts();
fields(s3_client_params) ->
    Fields = emqx_s3_schema:fields(s3_client),
    %% Access key and secret are the same as parent struct.
    FieldsToRemove = [access_key_id, secret_access_key],
    lists:filtermap(
        fun
            ({K, Sc}) when K == host; K == port ->
                Override = #{
                    required => false,
                    importance => ?IMPORTANCE_HIDDEN
                },
                {true, {K, hocon_schema:override(Sc, Override)}};
            ({K, _Sc}) ->
                not lists:member(K, FieldsToRemove)
        end,
        Fields
    ).

desc("config_connector") ->
    ?DESC("config_connector");
desc(s3_client_params) ->
    ?DESC("s3_client");
desc(s3_client_transport_options) ->
    ?DESC("s3_client_transport_options");
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
        access_key_id => <<"12345">>,
        secret_access_key => <<"******">>,
        s3tables_arn => <<"arn:aws:s3tables:sa-east-1:123456789012:bucket/mybucket">>,
        request_timeout => <<"10s">>,
        s3_client => #{
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

-spec parse_arn(binary()) ->
    {ok, #{region := binary(), account_id := binary(), bucket := binary()}} | error.
parse_arn(ARN) ->
    RE = <<
        "^arn:aws:s3tables:(?<region>[-a-z0-9]+):(?<account_id>[0-9]+):"
        "bucket/(?<bucket>[-a-z0-9]+)$"
    >>,
    Res = re:run(
        ARN,
        RE,
        [anchored, {capture, ["region", "account_id", "bucket"], binary}]
    ),
    case Res of
        {match, [Region, AccountId, Bucket]} ->
            {ok, #{region => Region, account_id => AccountId, bucket => Bucket}};
        _ ->
            error
    end.

%%------------------------------------------------------------------------------
%% `emqx_connector' API
%%------------------------------------------------------------------------------

pre_config_update(
    Path,
    _Name,
    #{<<"s3_client">> := #{<<"transport_options">> := TransportOpts} = S3Conf} = Conf,
    _ConfOld
) ->
    case emqx_connector_ssl:convert_certs(filename:join(Path), TransportOpts) of
        {ok, NTransportOpts} ->
            {ok, Conf#{<<"s3_client">> := S3Conf#{<<"transport_options">> := NTransportOpts}}};
        {error, Error} ->
            {error, Error}
    end;
pre_config_update(_Path, _Name, Conf, _ConfOld) ->
    {ok, Conf}.

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

%% https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html
s3tables_arn_validator(ARN) ->
    case parse_arn(ARN) of
        {ok, _} ->
            ok;
        error ->
            {error, <<
                "Invalid ARN; must be of form "
                "`arn:aws:s3tables:<REGION>:<ACCOUNTID>:bucket/<BUCKET>`"
            >>}
    end.
