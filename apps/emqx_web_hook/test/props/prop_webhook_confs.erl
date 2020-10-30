%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(prop_webhook_confs).
-include_lib("proper/include/proper.hrl").

-import(emqx_ct_proper_types,
        [ url/0
        , nof/1
        ]).

-define(ALL(Vars, Types, Exprs),
        ?SETUP(fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
         end, ?FORALL(Vars, Types, Exprs))).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_confs() ->
    Schema = cuttlefish_schema:files(filelib:wildcard(code:priv_dir(emqx_web_hook) ++ "/*.schema")),
    ?ALL(Confs, confs(),
        begin
            Envs = cuttlefish_generator:map(Schema, cuttlefish_conf_file(Confs)),

            assert_confs(Confs, Envs),

            set_application_envs(Envs),
            {ok, _} = application:ensure_all_started(emqx_web_hook),
            application:stop(emqx_web_hook),
            unset_application_envs(Envs),
            true
        end).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

do_setup() ->
    application:set_env(kernel, logger_level, error),
    emqx_ct_helpers:start_apps([], fun set_special_cfgs/1),
    ok.

do_teardown(_) ->
    emqx_ct_helpers:stop_apps([]),
    ok.

set_special_cfgs(_) ->
    application:set_env(emqx, plugins_loaded_file, undefined),
    application:set_env(emqx, modules_loaded_file, undefined),
    ok.

assert_confs([{"web.hook.api.url", Url}|More], Envs) ->
    %% Assert!
    Url = deep_get_env("emqx_web_hook.url", Envs),
    assert_confs(More, Envs);

assert_confs([{"web.hook.rule." ++ HookName0, Spec}|More], Envs) ->
    HookName = re:replace(HookName0, "\\.[0-9]", "", [{return, list}]),
    Rules = deep_get_env("emqx_web_hook.rules", Envs),

    %% Assert!
    Spec = proplists:get_value(HookName, Rules),

    assert_confs(More, Envs);

assert_confs([_|More], Envs) ->
    assert_confs(More, Envs);

assert_confs([], _) ->
    true.

deep_get_env(Path, Envs) ->
    lists:foldl(
      fun(_K, undefiend) -> undefiend;
         (K, Acc) -> proplists:get_value(binary_to_atom(K, utf8), Acc)
    end, Envs, re:split(Path, "\\.")).

set_application_envs(Envs) ->
    application:set_env(Envs).

unset_application_envs(Envs) ->
    lists:foreach(fun({App, Es}) ->
        lists:foreach(fun({K, _}) ->
            application:unset_env(App, K)
        end, Es) end, Envs).

cuttlefish_conf_file(Ls) when is_list(Ls) ->
    [cuttlefish_conf_option(K,V) || {K, V} <- Ls].

cuttlefish_conf_option(K, V)
    when is_list(K) ->
    {re:split(K, "[.]", [{return, list}]), V}.

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

confs() ->
    nof([{"web.hook.api.url", url()},
         {"web.hook.encode_payload", oneof(["base64", "base62"])},
         {"web.hook.rule.client.connect.1", rule_spec()},
         {"web.hook.rule.client.connack.1", rule_spec()},
         {"web.hook.rule.client.connected.1", rule_spec()},
         {"web.hook.rule.client.disconnected.1", rule_spec()},
         {"web.hook.rule.client.subscribe.1", rule_spec()},
         {"web.hook.rule.client.unsubscribe.1", rule_spec()},
         {"web.hook.rule.session.subscribed.1", rule_spec()},
         {"web.hook.rule.session.unsubscribed.1", rule_spec()},
         {"web.hook.rule.session.terminated.1", rule_spec()},
         {"web.hook.rule.message.publish.1", rule_spec()},
         {"web.hook.rule.message.delivered.1", rule_spec()},
         {"web.hook.rule.message.acked.1", rule_spec()}
        ]).

rule_spec() ->
    ?LET(Action, action_names(),
         begin
            binary_to_list(emqx_json:encode(#{action => Action}))
         end).

action_names() ->
    oneof([on_client_connect, on_client_connack, on_client_connected,
           on_client_connected, on_client_disconnected, on_client_subscribe, on_client_unsubscribe,
           on_session_subscribed, on_session_unsubscribed, on_session_terminated,
           on_message_publish, on_message_delivered, on_message_acked]).

