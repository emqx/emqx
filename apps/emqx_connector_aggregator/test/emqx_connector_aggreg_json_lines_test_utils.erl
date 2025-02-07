%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_aggreg_json_lines_test_utils).

-export([decode/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

decode(Binary) ->
    do_decode(Binary, []).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

do_decode(<<>>, Acc) ->
    Acc;
do_decode(Binary, Acc) ->
    try jiffy:decode(Binary, [return_maps, return_trailer]) of
        {has_trailer, Term, Rest} ->
            do_decode(Rest, [Term | Acc]);
        Term ->
            lists:reverse([Term | Acc])
    catch
        K:E:S ->
            ct:pal("bad json; error:\n  ~p\npayload:\n  ~p", [{K, E, S}, Binary]),
            erlang:raise(K, E, S)
    end.
