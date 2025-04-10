%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ft_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 10
    },

    AssemblerSup = #{
        id => emqx_ft_assembler_sup,
        start => {emqx_ft_assembler_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [emqx_ft_assembler_sup]
    },

    FileReaderSup = #{
        id => emqx_ft_storage_fs_reader_sup,
        start => {emqx_ft_storage_fs_reader_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [emqx_ft_storage_fs_reader_sup]
    },

    ResponderSup = #{
        id => emqx_ft_responder_sup,
        start => {emqx_ft_responder_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [emqx_ft_responder_sup]
    },

    ChildSpecs = [ResponderSup, AssemblerSup, FileReaderSup],
    {ok, {SupFlags, ChildSpecs}}.
