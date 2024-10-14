%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2024, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 9月 2024 下午4:32
%%%-------------------------------------------------------------------
-module(alinkdata_scene_worker_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    AChild = #{id => 'alinkdata_scene_worker',
        start => {'alinkdata_scene_worker', start_link, []},
        restart => permanent,
        shutdown => 2000,
        type => worker,
        modules => ['alinkdata_scene_worker']},

    {ok, {#{strategy => simple_one_for_one,
        intensity => 5,
        period => 30},
        [AChild]}
    }.
