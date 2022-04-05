-module(emqx_bridge_mqtt_schema).

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2]).

-export([roots/0, fields/1, desc/1]).

%%======================================================================================
%% Hocon Schema Definitions
roots() -> [].

fields("ingress") ->
    [ emqx_bridge_schema:direction_field(ingress, emqx_connector_mqtt_schema:ingress_desc())
    ]
    ++ emqx_bridge_schema:common_bridge_fields()
    ++ proplists:delete(hookpoint, emqx_connector_mqtt_schema:fields("ingress"));

fields("egress") ->
    [ emqx_bridge_schema:direction_field(egress, emqx_connector_mqtt_schema:egress_desc())
    ]
    ++ emqx_bridge_schema:common_bridge_fields()
    ++ emqx_connector_mqtt_schema:fields("egress");

fields("post_ingress") ->
    [ type_field()
    , name_field()
    ] ++ proplists:delete(enable, fields("ingress"));
fields("post_egress") ->
    [ type_field()
    , name_field()
    ] ++ proplists:delete(enable, fields("egress"));

fields("put_ingress") ->
    proplists:delete(enable, fields("ingress"));
fields("put_egress") ->
    proplists:delete(enable, fields("egress"));

fields("get_ingress") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post_ingress");
fields("get_egress") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post_egress").

desc(Rec) when Rec =:= "ingress"; Rec =:= "egress" ->
    "Configuration for MQTT bridge.";
desc(_) ->
    undefined.

%%======================================================================================
type_field() ->
    {type, mk(mqtt,
        #{ required => true
         , desc => "The bridge type."
         })}.

name_field() ->
    {name, mk(binary(),
        #{ required => true
         , desc => "Bridge name, used as a human-readable description of the bridge."
         })}.
