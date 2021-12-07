-module(emqx_bridge_mqtt_schema).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2]).

-export([roots/0, fields/1]).

%%======================================================================================
%% Hocon Schema Definitions
roots() -> [].

fields("ingress") ->
    [ direction(ingress, emqx_connector_mqtt_schema:ingress_desc())
    , emqx_bridge_schema:connector_name()
    ] ++ proplists:delete(hookpoint, emqx_connector_mqtt_schema:fields("ingress"));

fields("egress") ->
    [ direction(egress, emqx_connector_mqtt_schema:egress_desc())
    , emqx_bridge_schema:connector_name()
    ] ++ emqx_connector_mqtt_schema:fields("egress");

fields("post_ingress") ->
    [ type_field()
    , name_field()
    ] ++ fields("ingress");
fields("post_egress") ->
    [ type_field()
    , name_field()
    ] ++ fields("egress");

fields("put_ingress") ->
    fields("ingress");
fields("put_egress") ->
    fields("egress");

fields("get_ingress") ->
    [ id_field()
    ] ++ fields("post_ingress");
fields("get_egress") ->
    [ id_field()
    ] ++ fields("post_egress").

%%======================================================================================
direction(Dir, Desc) ->
    {direction, mk(Dir,
        #{ nullable => false
         , desc => "The direction of the bridge. Can be one of 'ingress' or 'egress'.<br>"
            ++ Desc
         })}.

id_field() ->
    {id, mk(binary(), #{desc => "The Bridge Id", example => "mqtt:my_mqtt_bridge"})}.

type_field() ->
    {type, mk(mqtt, #{desc => "The Bridge Type"})}.

name_field() ->
    {name, mk(binary(),
        #{ desc => "The Bridge Name"
         , example => "some_bridge_name"
         })}.
