%%%-------------------------------------------------------------------
%% @doc emqx_statsd top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(emqx_statsd_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    ensure_child_started/1,
    ensure_child_started/2,
    ensure_child_stopped/1
]).

-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Mod, Opts), #{
    id => Mod,
    start => {Mod, start_link, [Opts]},
    restart => permanent,
    shutdown => 5000,
    type => worker,
    modules => [Mod]
}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec ensure_child_started(supervisor:child_spec()) -> ok.
ensure_child_started(ChildSpec) when is_map(ChildSpec) ->
    assert_started(supervisor:start_child(?MODULE, ChildSpec)).

-spec ensure_child_started(atom(), map()) -> ok.
ensure_child_started(Mod, Opts) when is_atom(Mod) andalso is_map(Opts) ->
    assert_started(supervisor:start_child(?MODULE, ?CHILD(Mod, Opts))).

%% @doc Stop the child worker process.
-spec ensure_child_stopped(any()) -> ok.
ensure_child_stopped(ChildId) ->
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok ->
            %% with terminate_child/2 returned 'ok', it's not possible
            %% for supervisor:delete_child/2 to return {error, Reason}
            ok = supervisor:delete_child(?MODULE, ChildId);
        {error, not_found} ->
            ok
    end.

init([]) ->
    {ok, {{one_for_one, 10, 3600}, []}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

assert_started({ok, _Pid}) -> ok;
assert_started({ok, _Pid, _Info}) -> ok;
assert_started({error, {already_started, _Pid}}) -> ok;
assert_started({error, Reason}) -> erlang:error(Reason).
