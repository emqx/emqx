%%%-------------------------------------------------------------------
%% @doc emqx_statsd top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(emqx_statsd_sup).

-behaviour(supervisor).

-export([ start_link/0
        , start_child/1
        , start_child/2
        , stop_child/1
        ]).

-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Mod, Opts), #{id => Mod,
                            start => {Mod, start_link, [Opts]},
                            restart => permanent,
                            shutdown => 5000,
                            type => worker,
                            modules => [Mod]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(supervisor:child_spec()) -> ok.
start_child(ChildSpec) when is_map(ChildSpec) ->
    assert_started(supervisor:start_child(?MODULE, ChildSpec)).

-spec start_child(atom(), map()) -> ok.
start_child(Mod, Opts) when is_atom(Mod) andalso is_map(Opts) ->
    assert_started(supervisor:start_child(?MODULE, ?CHILD(Mod, Opts))).

-spec(stop_child(any()) -> ok | {error, term()}).
stop_child(ChildId) ->
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok -> supervisor:delete_child(?MODULE, ChildId);
        Error -> Error
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
