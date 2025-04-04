%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ft_responder_sup).

-export([start_link/0]).
-export([start_child/3]).

-behaviour(supervisor).
-export([init/1]).

-define(SUPERVISOR, ?MODULE).

%%

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

start_child(Key, RespFun, Timeout) ->
    supervisor:start_child(?SUPERVISOR, [Key, RespFun, Timeout]).

-spec init(_) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_) ->
    Flags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 100
    },
    ChildSpec = #{
        id => responder,
        start => {emqx_ft_responder, start_link, []},
        restart => temporary
    },
    {ok, {Flags, [ChildSpec]}}.
