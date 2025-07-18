%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_engine_config).

%% API
-export([
    create_or_update_rule/3,
    delete_rule/2
]).

-include("rule_engine.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

create_or_update_rule(Namespace, Id, Params) ->
    emqx_conf:update(
        ?RULE_PATH(Id),
        Params,
        with_namespace(#{override_to => cluster}, Namespace)
    ).

delete_rule(Namespace, Id) ->
    emqx_conf:remove(
        ?RULE_PATH(Id),
        with_namespace(#{override_to => cluster}, Namespace)
    ).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

with_namespace(UpdateOpts, ?global_ns) ->
    UpdateOpts;
with_namespace(UpdateOpts, Namespace) when is_binary(Namespace) ->
    UpdateOpts#{namespace => Namespace}.
