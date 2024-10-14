%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023
%%% @doc
%%%
%%% @end
%%% Created : 11. 3月 2023 下午3:44
%%%-------------------------------------------------------------------
-module(alinkalarm_pool_sup).
-author("yqfclid").


-behaviour(supervisor).
%% API
-export([]).

%%%===================================================================
%%% API
%%%===================================================================
-export([spec/1, spec/2]).

-export([
    start_link/0,
    start_link/3,
    start_link/4
]).

-export([init/1]).

-define(POOL, alinkalarm_pool).

-spec(spec(list()) -> supervisor:child_spec()).
spec(Args) ->
    spec(pool_sup, Args).

-spec(spec(any(), list()) -> supervisor:child_spec()).
spec(ChildId, Args) ->
    #{id       => ChildId,
        start    => {?MODULE, start_link, Args},
        restart  => transient,
        shutdown => infinity,
        type     => supervisor,
        modules  => [?MODULE]}.

%% @doc Start the default pool supervisor.
start_link() ->
    start_link(?POOL, random, {?POOL, start_link, []}).


start_link(Pool, Type, MFA) ->
    start_link(Pool, Type, erlang:system_info(schedulers), MFA).


start_link(Pool, Type, Size, MFA) ->
    supervisor:start_link(?MODULE, [Pool, Type, Size, MFA]).

init([Pool, Type, Size, {M, F, Args}]) ->
    ok = ensure_pool(Pool, Type, [{size, Size}]),
    {ok, {{one_for_one, 10, 3600}, [
        begin
            ensure_pool_worker(Pool, {Pool, I}, I),
            #{id => {M, I},
                start => {M, F, [Pool, I | Args]},
                restart => transient,
                shutdown => 5000,
                type => worker,
                modules => [M]}
        end || I <- lists:seq(1, Size)]}}.

ensure_pool(Pool, Type, Opts) ->
    try gproc_pool:new(Pool, Type, Opts)
    catch
        error:exists -> ok
    end.

ensure_pool_worker(Pool, Name, Slot) ->
    try gproc_pool:add_worker(Pool, Name, Slot)
    catch
        error:exists -> ok
    end.
%%%===================================================================
%%% Application callbacks
%%%===================================================================