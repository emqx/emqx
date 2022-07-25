%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge_hstream).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, enum/1]).

-export([
    roots/0,
    fields/1,
    desc/1
]).

%%======================================================================================
%% Hocon Schema Definitions
% namespace() -> "bridge".

roots() -> [].

fields("config") ->
    ExtConfig = [
        {local_topic, mk(binary(), #{desc => ?DESC("local_topic")})},
        {payload, mk(binary(), #{default => <<"${payload}">>, desc => ?DESC("payload")})}
    ],
    basic_config() ++ ExtConfig;
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:metrics_status_fields() ++ fields("post").

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for HStream using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

basic_config() ->
    Basic = [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {direction, mk(egress, #{desc => ?DESC("config_direction"), default => egress})}
    ],
    emqx_ee_connector_hstream:fields(config) ++ Basic.

%%======================================================================================

type_field() ->
    % {type, mk(hstream, #{required => true, desc => ?DESC("desc_type")})}.
    {type, mk(hstream, #{required => true, desc => <<"desc_type">>})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
