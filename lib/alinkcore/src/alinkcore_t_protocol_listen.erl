%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2024, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 04. 6月 2024 下午3:44
%%%-------------------------------------------------------------------
-module(alinkcore_t_protocol_listen).
-author("yqfclid").

%% API
-export([
    start/0
]).

-define(TCP_OPTIONS, [
    {backlog, 512},
    {keepalive, true},
    {send_timeout, 15000},
    {send_timeout_close, true},
    {nodelay, true},
    {reuseaddr, true},
    binary,
    {packet, raw},
    {exit_on_close, true}
]).

%%%===================================================================
%%% API
%%%===================================================================
start() ->
    ListenOn = application:get_env(alinkcore, t_protocol_port, 10881),
    esockd:open('alinkcore_t_protocol_listen', ListenOn, [{tcp_options, ?TCP_OPTIONS}],
        {t_protocol_server, start_link, []}).
%%%===================================================================
%%% Internal functions
%%%===================================================================