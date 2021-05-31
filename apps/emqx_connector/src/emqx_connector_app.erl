%%%-------------------------------------------------------------------
%% @doc emqx_connector public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_connector_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    load_config(),
    emqx_connector_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

load_config() ->
    case hocon:load("etc/plugins/emqx_connector.conf", #{format => map}) of
        {ok, #{<<"emqx_connectors">> := Connectors}} ->
            lists:foreach(fun load_connector/1, Connectors);
        {error, Reason} ->
            error(Reason)
    end.

load_connector(Config) ->
    case emqx_resource:load_instance_from_config(Config) of
        {ok, _} -> ok;
        {error, already_created} -> ok;
        {error, Reason} ->
            error({load_connector, Reason})
    end.
