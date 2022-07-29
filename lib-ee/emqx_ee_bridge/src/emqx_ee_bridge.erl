%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_bridge).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    schema_modules/0,
    conn_bridge_examples/1,
    resource_type/1,
    fields/1
]).

schema_modules() ->
    [emqx_ee_bridge_hstream, emqx_ee_bridge_influxdb].

conn_bridge_examples(Method) ->
    Fun =
        fun(Module, Examples) ->
            Example = erlang:apply(Module, conn_bridge_example, [Method]),
            maps:merge(Examples, Example)
        end,
    lists:foldl(Fun, #{}, schema_modules()).

resource_type(Type) when is_binary(Type) -> resource_type(binary_to_atom(Type, utf8));
resource_type(hstreamdb) -> emqx_ee_connector_hstream;
resource_type(influxdb) -> emqx_ee_connector_influxdb.

fields(bridges) ->
    [
        {hstreamdb,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_hstream, "config")),
                #{desc => <<"EMQX Enterprise Config">>}
            )},
        {influxdb,
            mk(
                hoconsc:map(name, ref(emqx_ee_bridge_influxdb, "config")),
                #{desc => <<"EMQX Enterprise Config">>}
            )}
    ].
