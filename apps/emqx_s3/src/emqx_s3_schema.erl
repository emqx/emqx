%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([roots/0, fields/1, namespace/0, tags/0, desc/1]).

-export([translate/1]).
-export([translate/2]).

-type secret_access_key() :: string() | function().
-reflect_type([secret_access_key/0]).

roots() ->
    [s3].

namespace() -> "s3".

tags() ->
    [<<"S3">>].

fields(s3) ->
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
            mk(
                typerefl:alias("string", secret_access_key()),
                #{
                    desc => ?DESC("secret_access_key"),
                    required => false,
                    sensitive => true,
                    converter => fun secret/2
                }
            )},
        {bucket,
            mk(
                string(),
                #{
                    desc => ?DESC("bucket"),
                    required => true
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
        {url_expire_time,
            mk(
                %% not used in a `receive ... after' block, just timestamp comparison
                emqx_schema:duration_s(),
                #{
                    default => <<"1h">>,
                    desc => ?DESC("url_expire_time"),
                    required => false
                }
            )},
        {min_part_size,
            mk(
                emqx_schema:bytesize(),
                #{
                    default => <<"5mb">>,
                    desc => ?DESC("min_part_size"),
                    required => true,
                    validator => fun part_size_validator/1
                }
            )},
        {max_part_size,
            mk(
                emqx_schema:bytesize(),
                #{
                    default => <<"5gb">>,
                    desc => ?DESC("max_part_size"),
                    required => true,
                    validator => fun part_size_validator/1
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
        {transport_options,
            mk(
                ref(?MODULE, transport_options),
                #{
                    desc => ?DESC("transport_options"),
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
            )}
    ] ++
        props_without(
            [base_url, max_retries, retry_interval, request],
            emqx_bridge_http_connector:fields(config)
        ) ++
        props_with(
            [headers, max_retries, request_timeout], emqx_bridge_http_connector:fields("request")
        ).

desc(s3) ->
    "S3 connection options";
desc(transport_options) ->
    "Options for the HTTP transport layer used by the S3 client".

secret(undefined, #{}) ->
    undefined;
secret(Secret, #{make_serializable := true}) ->
    unicode:characters_to_binary(emqx_secret:unwrap(Secret));
secret(Secret, #{}) ->
    _ = is_binary(Secret) orelse throw({expected_type, string}),
    emqx_secret:wrap(unicode:characters_to_list(Secret)).

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

props_without(Keys, Proplist) ->
    lists:filter(fun({K, _}) -> not lists:member(K, Keys) end, Proplist).

part_size_validator(PartSizeLimit) ->
    case
        PartSizeLimit >= 5 * 1024 * 1024 andalso
            PartSizeLimit =< 5 * 1024 * 1024 * 1024
    of
        true -> ok;
        false -> {error, "must be at least 5mb and less than 5gb"}
    end.
