%%%-------------------------------------------------------------------
%%% @author zhongwen
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. 10月 2022 11:14
%%%-------------------------------------------------------------------
-module(emqx_prometheus_config).

-behaviour(emqx_config_handler).

-include("emqx_prometheus.hrl").

-export([add_handler/0, remove_handler/0]).
-export([post_config_update/5]).
-export([update/1]).

update(Config) ->
    case
        emqx_conf:update(
            [prometheus],
            Config,
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewConfigRows}} ->
            {ok, NewConfigRows};
        {error, Reason} ->
            {error, Reason}
    end.

add_handler() ->
    ok = emqx_config_handler:add_handler(?PROMETHEUS, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?PROMETHEUS),
    ok.

post_config_update(?PROMETHEUS, _Req, New, _Old, AppEnvs) ->
    application:set_env(AppEnvs),
    update_prometheus(New),
    ok;
post_config_update(_ConfPath, _Req, _NewConf, _OldConf, _AppEnvs) ->
    ok.

update_prometheus(#{enable := true}) ->
    emqx_prometheus_sup:start_child(?APP);
update_prometheus(#{enable := false}) ->
    emqx_prometheus_sup:stop_child(?APP).
