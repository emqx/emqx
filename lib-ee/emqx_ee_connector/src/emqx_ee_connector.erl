%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_connector).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    schema_modules/0,
    fields/1,
    connector_examples/1
]).

schema_modules() ->
    [emqx_ee_connector_hstream].

fields(connectors) ->
    [
        {hstreamdb,
            mk(
                hoconsc:map(name, ref(emqx_ee_connector_hstream, config)),
                #{desc => <<"EMQX Enterprise Config">>}
            )}
    ].

connector_examples(Method) ->
    Fun =
        fun(Module, Examples) ->
            Example = erlang:apply(Module, connector_example, [Method]),
            maps:merge(Examples, Example)
        end,
    lists:foldl(Fun, #{}, schema_modules()).
