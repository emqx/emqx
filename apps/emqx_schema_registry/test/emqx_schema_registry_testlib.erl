%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry_testlib).

-export([
    emqx_schema_registry_app_spec/0,
    emqx_schema_registry_app_spec/1,
    wait_for_sparkplug_schema_registered/0
]).

-include("emqx_schema_registry.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

emqx_schema_registry_app_spec() ->
    emqx_schema_registry_app_spec(_Overrides = #{}).

emqx_schema_registry_app_spec(Overrides) ->
    {emqx_schema_registry,
        maps:merge(
            #{
                config => #{},
                after_start => fun wait_for_sparkplug_schema_registered/0
            },
            Overrides
        )}.

wait_for_sparkplug_schema_registered() ->
    ?retry(
        100,
        100,
        [_] = ets:lookup(?SERDE_TAB, ?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME)
    ).
