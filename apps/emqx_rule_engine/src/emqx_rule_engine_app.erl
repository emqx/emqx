%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_app).

-include("rule_engine.hrl").

-behaviour(application).

-export([start/2]).

-export([stop/1]).

start(_Type, _Args) ->
    SupRet = emqx_rule_engine_sup:start_link(),
    ok = emqx_rule_engine:load_rules(),
    ok = emqx_rule_events:reload(),
    RulePath = [RuleEngine | _] = ?KEY_PATH,
    ok = emqx_conf:add_handler(RulePath ++ ['?'], emqx_rule_engine_config),
    ok = emqx_conf:add_handler([RuleEngine], emqx_rule_engine_config),
    ok = emqx_conf:add_handler([rule_engine, jq_implementation_module], emqx_rule_engine_schema),
    ok = emqx_config_dep_registry:register_dependencies(emqx_rule_engine_config),
    emqx_rule_engine_cli:load(),
    SupRet.

stop(_State) ->
    emqx_rule_engine_cli:unload(),
    RulePath = [RuleEngine | _] = ?KEY_PATH,
    ok = emqx_conf:remove_handler(RulePath ++ ['?']),
    ok = emqx_conf:remove_handler([RuleEngine]),
    ok = emqx_conf:remove_handler([rule_engine, jq_implementation_module]),
    ok = emqx_rule_events:unload(),
    ok.
