%%%----------------------------------------------------------------------
%%% File    : emysql_app.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : mysql driver application
%%% Created : 21 May 2009
%%% Updated : 11 Jan 2010 
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2010, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(emysql_app).

-author('ery.lee@gmail.com').

-behavior(application).

-export([start/0, start/2, stop/1]).

start() -> 
	application:start(emysql).

start(normal, _Args) ->
	emysql_sup:start_link(application:get_all_env()).

stop(_) ->
	ok.

