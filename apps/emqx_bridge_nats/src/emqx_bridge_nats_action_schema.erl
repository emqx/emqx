-module(emqx_bridge_nats_action_schema).
-behaviour(hocon_schema).
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_bridge_nats.hrl").
-export([namespace/0, roots/0, fields/1, desc/1, bridge_v2_examples/1]).

namespace() -> "action_nats".
roots() -> [].
fields("get_bridge_v2") ->
    emqx_bridge_v2_schema:api_fields("get_bridge_v2", ?ACTION_TYPE, fields(?ACTION_TYPE));
fields("put_bridge_v2") ->
    emqx_bridge_v2_schema:api_fields("put_bridge_v2", ?ACTION_TYPE, fields(?ACTION_TYPE));
fields("post_bridge_v2") ->
    emqx_bridge_v2_schema:api_fields("post_bridge_v2", ?ACTION_TYPE, fields(?ACTION_TYPE));
fields(action) ->
    {?ACTION_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, ?ACTION_TYPE)),
            #{desc => <<"NATS Action Config">>, required => false}
        )};
fields(?ACTION_TYPE) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{required => true, desc => ?DESC("parameters")}
        ),
        #{resource_opts_ref => hoconsc:ref(?MODULE, action_resource_opts)}
    );
fields(action_parameters) ->
    [
        {subject,
            hoconsc:mk(emqx_schema:template(), #{required => true, desc => ?DESC("subject")})},
        {payload_template,
            hoconsc:mk(
                emqx_schema:template(),
                #{default => <<"$", "{.payload}">>, desc => ?DESC("payload_template")}
            )},
        {headers,
            hoconsc:mk(
                hoconsc:array(hoconsc:ref(?MODULE, header)),
                #{default => [], desc => ?DESC("headers")}
            )},
        {delivery_mode,
            hoconsc:mk(
                hoconsc:enum([core, jetstream]),
                #{default => core, desc => ?DESC("delivery_mode")}
            )},
        {msg_id_template,
            hoconsc:mk(
                emqx_schema:template(),
                #{default => <<>>, desc => ?DESC("msg_id_template")}
            )}
    ];
fields(header) ->
    [
        {key, hoconsc:mk(emqx_schema:template(), #{required => true, desc => ?DESC("header_key")})},
        {value,
            hoconsc:mk(emqx_schema:template(), #{required => true, desc => ?DESC("header_value")})}
    ];
fields(action_resource_opts) ->
    emqx_bridge_v2_schema:action_resource_opts_fields([
        {batch_size, #{default => 1}},
        {batch_time, #{default => <<"0ms">>}}
    ]).

desc(?ACTION_TYPE) -> ?DESC(?ACTION_TYPE);
desc(action_parameters) -> ?DESC("parameters");
desc(action_resource_opts) -> emqx_bridge_v2_schema:desc(action_resource_opts);
desc(_) -> undefined.

bridge_v2_examples(Method) ->
    [#{?ACTION_TYPE_BIN => #{summary => <<"NATS Action">>, value => example(Method)}}].
example(post) ->
    maps:merge(example(put), #{type => ?ACTION_TYPE_BIN, name => <<"nats_action">>});
example(get) ->
    maps:merge(example(put), #{status => <<"connected">>, node_status => []});
example(put) ->
    #{
        enable => true,
        description => <<"NATS action">>,
        connector => <<"nats_connector">>,
        parameters => #{
            subject => <<"events">>,
            payload_template => <<"$", "{.payload}">>,
            headers => [],
            delivery_mode => core,
            msg_id_template => <<>>
        },
        resource_opts => #{
            query_mode => <<"sync">>,
            batch_size => 1,
            batch_time => <<"0ms">>,
            request_ttl => <<"5s">>
        }
    }.
