%%%----------------------------------------------------------------------
%%% File    : emysql_sup.erl
%%% Author  : Ery Lee
%%% Purpose : Mysql driver supervisor
%%% Created : 21 May 2009 
%%% Updated : 11 Jan 2010 
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(emysql_sup).

-author('ery.lee@gmail.com').

-behavior(supervisor).

%% API
-export([start_link/1, init/1]).

start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Opts).  

init(Opts) ->
    PoolSize = proplists:get_value(pool, Opts,
                                   erlang:system_info(schedulers)),
    {ok, {{one_for_one, 10, 10},
		  [{emysql, {emysql, start_link, [PoolSize]}, transient,
            16#ffffffff, worker, [emysql]} |
		   [{I, {emysql_conn, start_link, [I, Opts]}, transient, 16#ffffffff,
			worker, [emysql_conn, emysql_recv]} || I <- lists:seq(1, PoolSize)]]
		}
	}.
	

