%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggregator_test_helpers).

-compile(nowarn_export_all).
-compile(export_all).

%% API
-export([]).

%%------------------------------------------------------------------------------
%% File utilities
%%------------------------------------------------------------------------------

truncate_at(Filename, Pos) ->
    {ok, FD} = file:open(Filename, [read, write, binary]),
    {ok, Pos} = file:position(FD, Pos),
    ok = file:truncate(FD),
    ok = file:close(FD).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
