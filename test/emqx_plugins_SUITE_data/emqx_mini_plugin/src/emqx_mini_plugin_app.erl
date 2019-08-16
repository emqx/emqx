%%%-------------------------------------------------------------------
%% @doc emqx_mini_plugin public API
%% @end
%%%-------------------------------------------------------------------

-module(emqx_mini_plugin_app).

-behaviour(application).
-behaviour(supervisor).

-emqx_plugin(?MODULE).

%% Application APIs
-export([ start/2
        , stop/1
        ]).

%% Supervisor callback
-export([init/1]).


%% -- Application

start(_StartType, _StartArgs) ->
    {ok, Sup} = start_link(),
    {ok, Sup}.

stop(_State) ->
    ok.

%% --- Supervisor

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

