%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_app).

-include("rule_engine.hrl").

-behaviour(application).

-export([start/2]).

-export([prep_stop/1, stop/1]).

start(_Type, _Args) ->
    SupRet = emqx_rule_engine_sup:start_link(),
    ok = emqx_rule_engine:load_rules(),
    ok = emqx_rule_events:reload(),
    RulePath = [RuleEngine | _] = ?KEY_PATH,
    ok = emqx_conf:add_handler(RulePath ++ ['?'], emqx_rule_engine_config),
    ok = emqx_conf:add_handler([RuleEngine], emqx_rule_engine_config),
    ok = emqx_conf:add_handler([rule_engine, jq_implementation_module], emqx_rule_engine_schema),
    ok = emqx_conf:add_handler([rule_engine, ssrf], emqx_rule_engine_schema),
    ok = emqx_utils_ssrf:refresh_cache(
        emqx:get_config(
            [rule_engine, ssrf],
            #{enable => false, allow_cidrs => [], deny_cidrs => [], deny_hosts => []}
        )
    ),
    emqx_rule_engine_cli:load(),
    SupRet.

prep_stop(State) ->
    %% Runs before the supervision tree is terminated.  `stop/1' runs after it, when
    %% the rule tables are already gone: the hooks must be unregistered while the
    %% tables still exist.
    ok = emqx_rule_events:unload(),
    State.

stop(_State) ->
    emqx_rule_engine_cli:unload(),
    RulePath = [RuleEngine | _] = ?KEY_PATH,
    ok = emqx_conf:remove_handler(RulePath ++ ['?']),
    ok = emqx_conf:remove_handler([RuleEngine]),
    ok = emqx_conf:remove_handler([rule_engine, jq_implementation_module]),
    ok = emqx_conf:remove_handler([rule_engine, ssrf]),
    ok.
