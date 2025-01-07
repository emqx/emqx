%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, namespace/0, tags/0, desc/1]).
-export([validators/1]).

-export([translate/1]).
-export([translate/2]).

roots() ->
    [].

namespace() -> "s3".

tags() ->
    [<<"S3">>].

fields(s3) ->
    lists:append([
        fields(s3_client),
        fields(s3_uploader),
        fields(s3_url_options),
        props_with([bucket, acl], fields(s3_upload))
    ]);
fields(s3_client) ->
    [
        {access_key_id,
            mk(
                string(),
                #{
                    desc => ?DESC("access_key_id"),
                    required => false
                }
            )},
        {secret_access_key,
            emqx_schema_secret:mk(
                #{
                    desc => ?DESC("secret_access_key")
                }
            )},
        {host,
            mk(
                string(),
                #{
                    desc => ?DESC("host"),
                    required => true
                }
            )},
        {port,
            mk(
                pos_integer(),
                #{
                    desc => ?DESC("port"),
                    required => true
                }
            )},
        {access_method,
            mk(
                hoconsc:enum([path, vhost]),
                #{
                    default => path,
                    desc => ?DESC("bucket_access_method")
                }
            )},
        {transport_options,
            mk(
                ref(?MODULE, transport_options),
                #{
                    desc => ?DESC("transport_options"),
                    required => false
                }
            )}
    ];
fields(s3_upload) ->
    [
        {bucket,
            mk(
                emqx_schema:template_str(),
                #{
                    desc => ?DESC("bucket"),
                    required => true
                }
            )},
        {key,
            mk(
                emqx_schema:template_str(),
                #{
                    desc => ?DESC("key"),
                    required => true
                }
            )},
        {acl,
            mk(
                hoconsc:enum([
                    private,
                    public_read,
                    public_read_write,
                    authenticated_read,
                    bucket_owner_read,
                    bucket_owner_full_control
                ]),
                #{
                    desc => ?DESC("acl"),
                    required => false
                }
            )},
        {headers,
            hoconsc:mk(
                map(),
                #{
                    required => false,
                    desc => ?DESC("upload_headers")
                }
            )}
    ];
fields(s3_uploader) ->
    [
        {min_part_size,
            mk(
                emqx_schema:bytesize(),
                #{
                    default => <<"5mb">>,
                    desc => ?DESC("min_part_size"),
                    required => false,
                    validator => fun part_size_validator/1
                }
            )},
        {max_part_size,
            mk(
                emqx_schema:bytesize(),
                #{
                    default => <<"5gb">>,
                    desc => ?DESC("max_part_size"),
                    required => false,
                    validator => fun part_size_validator/1
                }
            )}
    ];
fields(s3_url_options) ->
    [
        {url_expire_time,
            mk(
                %% not used in a `receive ... after' block, just timestamp comparison
                emqx_schema:duration_s(),
                #{
                    default => <<"1h">>,
                    desc => ?DESC("url_expire_time"),
                    required => false
                }
            )}
    ];
fields(transport_options) ->
    [
        {ipv6_probe,
            mk(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC("ipv6_probe"),
                    required => false
                }
            )},
        {connect_timeout,
            mk(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC(emqx_bridge_http_connector, "connect_timeout")
                }
            )},
        {pool_type,
            mk(
                hoconsc:enum([random, hash]),
                #{
                    default => random,
                    desc => ?DESC(emqx_bridge_http_connector, "pool_type"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {pool_size,
            mk(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC(emqx_bridge_http_connector, "pool_size")
                }
            )},
        {enable_pipelining,
            mk(
                pos_integer(),
                #{
                    default => 0,
                    desc => ?DESC(emqx_bridge_http_connector, "enable_pipelining"),
                    deprecated => {since, "5.8.2"}
                }
            )}
    ] ++
        emqx_connector_schema_lib:ssl_fields() ++
        props_with(
            [headers, max_retries, request_timeout], emqx_bridge_http_connector:fields("request")
        ).

desc(s3) ->
    "S3 connection options";
desc(s3_client) ->
    "S3 connection options";
desc(s3_upload) ->
    "S3 upload options";
desc(transport_options) ->
    "Options for the HTTP transport layer used by the S3 client".

validators(s3_uploader) ->
    [fun validate_part_size/1].

validate_part_size(Conf) ->
    Min = hocon_maps:get(<<"min_part_size">>, Conf),
    Max = hocon_maps:get(<<"max_part_size">>, Conf),
    Min =< Max orelse {error, <<"Inconsistent 'min_part_size': cannot exceed 'max_part_size'">>}.

translate(Conf) ->
    translate(Conf, #{}).

translate(Conf, OptionsIn) ->
    Options = maps:merge(#{atom_key => true}, OptionsIn),
    #{s3 := TranslatedConf} = hocon_tconf:check_plain(
        emqx_s3_schema, #{<<"s3">> => Conf}, Options, [s3]
    ),
    TranslatedConf.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

props_with(Keys, Proplist) ->
    lists:filter(fun({K, _}) -> lists:member(K, Keys) end, Proplist).

part_size_validator(PartSizeLimit) ->
    case
        PartSizeLimit >= 5 * 1024 * 1024 andalso
            PartSizeLimit =< 5 * 1024 * 1024 * 1024
    of
        true -> ok;
        false -> {error, "must be at least 5mb and less than 5gb"}
    end.
