%%%-------------------------------------------------------------------
%% @doc alinkalarm top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(alinkalarm_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 10,
                 period => 10},
    Child0 = child_spec(alinkalarm_product_rules, worker),
    Child1 = child_spec(alinkalarm_cache, worker),
    Child2 = child_spec(alinkalarm_handler_sup, supervisor),
    ChildSpecs = [Child0, Child1, Child2],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
child_spec(Mod, WorkerType) ->
    child_spec(Mod, permanent, WorkerType, []).


child_spec(Mod, WorkerType, Args) ->
    child_spec(Mod, permanent, WorkerType, Args).


child_spec(Mod, Strategy, WorkerType, Args) ->
    IsLegal0 = lists:member(WorkerType, [worker, supervisor]),
    IsLegal1 = lists:member(Strategy, [permanent, transient, temporary]),
    case IsLegal0 andalso IsLegal1 of
        true ->
            #{id => Mod,
                start => {Mod, start_link, Args},
                restart => Strategy,
                shutdown => infinity,
                type => WorkerType,
                modules => [Mod]
            };
        false ->
            erlang:error(bad_arg)
    end.