%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ft_storage_fs_reader_sup).

-behaviour(supervisor).

-export([
    init/1,
    start_link/0,
    start_child/2
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(CallerPid, Filename) ->
    Childspec = #{
        id => {CallerPid, Filename},
        start => {emqx_ft_storage_fs_reader, start_link, [CallerPid, Filename]},
        restart => temporary
    },
    case supervisor:start_child(?MODULE, Childspec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {Reason, _Child}} ->
            {error, Reason}
    end.

init(_) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1000
    },
    {ok, {SupFlags, []}}.
