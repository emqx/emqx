%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_test_api_helpers).

%% Connector API
-export([
    create_connector/1,
    delete_connector/2,
    list_connectors/0,
    delete_all_connectors/0
]).

%% Plugin API
-export([
    find_plugin/0,
    upload_plugin/1,
    list_plugins/0,
    get_plugin/1,
    start_plugin/1,
    stop_plugin/1,
    delete_plugin/1,
    delete_all_plugins/0,
    configure_plugin/2
]).

%% Rule API
-export([
    create_rule/1,
    list_rules/0,
    delete_rule/1,
    delete_all_rules/0
]).

%% Action API
-export([
    create_action/1,
    list_actions/0,
    delete_action/1,
    delete_all_actions/0
]).

%%--------------------------------------------------------------------
%% Connector API
%%--------------------------------------------------------------------

create_connector(Connector) ->
    case emqx_offline_messages_test_helpers:api_post(connectors, Connector) of
        {ok, _} ->
            ok;
        {error, Error} ->
            error(Error)
    end.

delete_connector(Type, Name) ->
    emqx_offline_messages_test_helpers:api_delete(
        {connectors, iolist_to_binary([Type, ":", Name])}
    ).

list_connectors() ->
    case emqx_offline_messages_test_helpers:api_get(connectors) of
        {ok, Connectors} when is_list(Connectors) ->
            Connectors;
        {error, Error} ->
            error(Error)
    end.

delete_all_connectors() ->
    Connectors = list_connectors(),
    lists:foreach(
        fun(#{<<"type">> := Type, <<"name">> := Name}) ->
            ok = delete_connector(Type, Name)
        end,
        Connectors
    ).

%%--------------------------------------------------------------------
%% Plugin API
%%--------------------------------------------------------------------

find_plugin() ->
    [Filename] = filelib:wildcard(filename:join([asset_path(), "*.tar.gz"])),
    PluginId = filename:basename(Filename, ".tar.gz"),
    {iolist_to_binary(PluginId), iolist_to_binary(Filename)}.

upload_plugin(Filename) ->
    Parts = [
        {file, Filename, <<"plugin">>, []}
    ],
    case
        emqx_offline_messages_test_helpers:api_post_raw(
            {plugins, install},
            [],
            {multipart, Parts}
        )
    of
        {ok, _} ->
            ok;
        {error, Error} ->
            error(Error)
    end.

list_plugins() ->
    case emqx_offline_messages_test_helpers:api_get(plugins) of
        {ok, Plugins} when is_list(Plugins) ->
            Plugins;
        {error, Error} ->
            error(Error)
    end.

get_plugin(PluginId) ->
    case emqx_offline_messages_test_helpers:api_get({plugins, PluginId}) of
        {ok, Plugin} ->
            Plugin;
        {error, Error} ->
            error(Error)
    end.

plugin_id(#{<<"name">> := Name, <<"rel_vsn">> := RelVsn}) ->
    <<Name/binary, "-", RelVsn/binary>>.

start_plugin(PluginId) ->
    case emqx_offline_messages_test_helpers:api_put_raw({plugins, PluginId, start}, <<>>) of
        {ok, _} ->
            ok;
        {error, Error} ->
            error(Error)
    end.

stop_plugin(PluginId) ->
    case emqx_offline_messages_test_helpers:api_put_raw({plugins, PluginId, stop}, <<>>) of
        {ok, _} ->
            ok;
        {error, Error} ->
            error(Error)
    end.

delete_plugin(PluginId) ->
    emqx_offline_messages_test_helpers:api_delete({plugins, PluginId}).

delete_all_plugins() ->
    Plugins = list_plugins(),
    lists:foreach(
        fun(Plugin) ->
            ok = stop_plugin(plugin_id(Plugin)),
            ok = delete_plugin(plugin_id(Plugin))
        end,
        Plugins
    ).

configure_plugin(PluginId, Config) ->
    case emqx_offline_messages_test_helpers:api_put({plugins, PluginId, config}, Config) of
        ok ->
            ok;
        {error, Error} ->
            error(Error)
    end.

%%--------------------------------------------------------------------
%% Rule API
%%--------------------------------------------------------------------

rule_id(#{<<"id">> := Id}) ->
    Id.

create_rule(Rule) ->
    case emqx_offline_messages_test_helpers:api_post(rules, Rule) of
        {ok, _} ->
            ok;
        {error, Error} ->
            error(Error)
    end.

list_rules() ->
    case emqx_offline_messages_test_helpers:api_get(<<"rules?limit=1000">>) of
        {ok, #{<<"data">> := Rules}} when is_list(Rules) ->
            Rules;
        {ok, Other} ->
            error({unexpected_response, Other});
        {error, Error} ->
            error(Error)
    end.

delete_rule(RuleId) ->
    emqx_offline_messages_test_helpers:api_delete({rules, RuleId}).

delete_all_rules() ->
    Rules = list_rules(),
    lists:foreach(
        fun(Rule) ->
            ok = delete_rule(rule_id(Rule))
        end,
        Rules
    ).

%%--------------------------------------------------------------------
%% Action API
%%--------------------------------------------------------------------

action_id(#{<<"type">> := Type, <<"name">> := Name}) ->
    iolist_to_binary([Type, ":", Name]).

create_action(Action) ->
    case emqx_offline_messages_test_helpers:api_post(actions, Action) of
        {ok, _} ->
            ok;
        {error, Error} ->
            error(Error)
    end.

list_actions() ->
    case emqx_offline_messages_test_helpers:api_get(actions) of
        {ok, Actions} when is_list(Actions) ->
            Actions;
        {error, Error} ->
            error(Error)
    end.

delete_action(ActionId) ->
    emqx_offline_messages_test_helpers:api_delete({actions, ActionId}).

delete_all_actions() ->
    Actions = list_actions(),
    lists:foreach(fun(Action) -> ok = delete_action(action_id(Action)) end, Actions).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

asset_path() ->
    filename:join([code:lib_dir(emqx_offline_messages), "test", "assets"]).
