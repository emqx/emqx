%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_syskeeper).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    bridge_v2_examples/1,
    values/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% -------------------------------------------------------------------------------------------------
%% api
bridge_v2_examples(Method) ->
    [
        #{
            <<"syskeeper_forwarder">> => #{
                summary => <<"Syskeeper Forwarder Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
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
        values(post)
    );
values(post) ->
    maps:merge(
        #{
            name => <<"syskeeper_forwarder">>,
            type => <<"syskeeper_forwarder">>
        },
        values(put)
    );
values(put) ->
    #{
        enable => true,
        connector => <<"syskeeper_forwarder">>,
        parameters => #{
            target_topic => <<"${topic}">>,
            template => <<"${payload}">>
        },
        resource_opts => #{
            worker_pool_size => 16
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "syskeeper".

roots() -> [].

fields(action) ->
    {syskeeper_forwarder,
        mk(
            hoconsc:map(name, ref(?MODULE, config)),
            #{
                desc => <<"Syskeeper Forwarder Action Config">>,
                required => false
            }
        )};
fields(config) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        mk(
            ref(?MODULE, "parameters"),
            #{
                required => true,
                desc => ?DESC("parameters")
            }
        ),
        #{resource_opts_ref => ref(?MODULE, "creation_opts")}
    );
fields("parameters") ->
    [
        {target_topic,
            mk(
                emqx_schema:template(),
                #{desc => ?DESC("target_topic"), default => <<"${topic}">>}
            )},
        {target_qos,
            mk(
                range(0, 2),
                #{desc => ?DESC("target_qos"), required => false}
            )},
        {template,
            mk(
                emqx_schema:template(),
                #{desc => ?DESC("template"), default => <<"${payload}">>}
            )}
    ];
fields("creation_opts") ->
    emqx_resource_schema:create_opts([{request_ttl, #{default => infinity}}]);
fields("post") ->
    [type_field(), name_field() | fields(config)];
fields("post_bridge_v2") ->
    fields("post");
fields("put") ->
    fields(config);
fields("put_bridge_v2") ->
    fields("put");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post");
fields("get_bridge_v2") ->
    fields("get").

desc(config) ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Syskeeper using `", string:to_upper(Method), "` method."];
desc("parameters") ->
    ?DESC("parameters");
desc("creation_opts" = Name) ->
    emqx_resource_schema:desc(Name);
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field() ->
    {type, mk(enum([syskeeper_forwarder]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
