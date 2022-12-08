-module(emqx_bridge_webhook_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([roots/0, fields/1, namespace/0, desc/1]).

%%======================================================================================
%% Hocon Schema Definitions
namespace() -> "bridge_webhook".

roots() -> [].

fields("config") ->
    basic_config() ++ request_config();
fields("post") ->
    [
        type_field(),
        name_field()
    ] ++ fields("config");
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post");
fields("creation_opts") ->
    lists:filter(
        fun({K, _V}) ->
            not lists:member(K, unsupported_opts())
        end,
        emqx_resource_schema:fields("creation_opts")
    ).

desc("config") ->
    ?DESC("desc_config");
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for WebHook using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

basic_config() ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    desc => ?DESC("config_enable"),
                    default => true
                }
            )}
    ] ++ webhook_creation_opts() ++
        proplists:delete(
            max_retries, proplists:delete(base_url, emqx_connector_http:fields(config))
        ).

request_config() ->
    [
        {url,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("config_url")
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
        {method,
            mk(
                method(),
                #{
                    default => post,
                    desc => ?DESC("config_method")
                }
            )},
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
                    desc => ?DESC("config_headers")
                }
            )},
        {body,
            mk(
                binary(),
                #{
                    default => <<"${payload}">>,
                    desc => ?DESC("config_body")
                }
            )},
        {max_retries,
            mk(
                non_neg_integer(),
                #{
                    default => 2,
                    desc => ?DESC("config_max_retries")
                }
            )},
        {request_timeout,
            mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("config_request_timeout")
                }
            )}
    ].

webhook_creation_opts() ->
    [
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, <<"resource_opts">>)
                }
            )}
    ].

unsupported_opts() ->
    [
        enable_batch,
        batch_size,
        batch_time
    ].

%%======================================================================================

type_field() ->
    {type,
        mk(
            webhook,
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

method() ->
    enum([post, put, get, delete]).
