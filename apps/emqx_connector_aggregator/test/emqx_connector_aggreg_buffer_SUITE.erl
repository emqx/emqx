%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_buffer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% CT Setup

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    WorkDir = emqx_cth_suite:work_dir(Config),
    ok = filelib:ensure_path(WorkDir),
    [{work_dir, WorkDir} | Config].

end_per_suite(_Config) ->
    ok.

%% Testcases

t_write_read_cycle(Config) ->
    Filename = mk_filename(?FUNCTION_NAME, Config),
    Metadata = {?MODULE, #{tc => ?FUNCTION_NAME}},
    {ok, WFD} = file:open(Filename, [write, binary]),
    Writer = emqx_connector_aggreg_buffer:new_writer(WFD, Metadata),
    Terms = [
        [],
        [[[[[[[[]]]]]]]],
        123,
        lists:seq(1, 100),
        lists:seq(1, 1000),
        lists:seq(1, 10000),
        lists:seq(1, 100000),
        #{<<"id">> => 123456789, <<"ts">> => <<"2028-02-29T12:34:56Z">>, <<"gauge">> => 42.42},
        {<<"text/plain">>, _Huge = rand:bytes(1048576)},
        {<<"application/json">>, emqx_utils_json:encode(#{j => <<"son">>, null => null})}
    ],
    ok = lists:foreach(
        fun(T) -> ?assertEqual(ok, emqx_connector_aggreg_buffer:write(T, Writer)) end,
        Terms
    ),
    ok = file:close(WFD),
    {ok, RFD} = file:open(Filename, [read, binary, raw]),
    {MetadataRead, Reader} = emqx_connector_aggreg_buffer:new_reader(RFD),
    ?assertEqual(Metadata, MetadataRead),
    TermsRead = read_until_eof(Reader),
    ?assertEqual(Terms, TermsRead).

t_read_empty(Config) ->
    Filename = mk_filename(?FUNCTION_NAME, Config),
    {ok, WFD} = file:open(Filename, [write, binary]),
    ok = file:close(WFD),
    {ok, RFD} = file:open(Filename, [read, binary]),
    ?assertError(
        {buffer_incomplete, header},
        emqx_connector_aggreg_buffer:new_reader(RFD)
    ).

t_read_garbage(Config) ->
    Filename = mk_filename(?FUNCTION_NAME, Config),
    {ok, WFD} = file:open(Filename, [write, binary]),
    ok = file:write(WFD, rand:bytes(1048576)),
    ok = file:close(WFD),
    {ok, RFD} = file:open(Filename, [read, binary]),
    ?assertError(
        badarg,
        emqx_connector_aggreg_buffer:new_reader(RFD)
    ).

t_read_truncated(Config) ->
    Filename = mk_filename(?FUNCTION_NAME, Config),
    {ok, WFD} = file:open(Filename, [write, binary]),
    Metadata = {?MODULE, #{tc => ?FUNCTION_NAME}},
    Writer = emqx_connector_aggreg_buffer:new_writer(WFD, Metadata),
    Terms = [
        [[[[[[[[[[[]]]]]]]]]]],
        lists:seq(1, 100000),
        #{<<"id">> => 123456789, <<"ts">> => <<"2029-02-30T12:34:56Z">>, <<"gauge">> => 42.42},
        {<<"text/plain">>, _Huge = rand:bytes(1048576)}
    ],
    LastTerm =
        {<<"application/json">>, emqx_utils_json:encode(#{j => <<"son">>, null => null})},
    ok = lists:foreach(
        fun(T) -> ?assertEqual(ok, emqx_connector_aggreg_buffer:write(T, Writer)) end,
        Terms
    ),
    {ok, WPos} = file:position(WFD, cur),
    ?assertEqual(ok, emqx_connector_aggreg_buffer:write(LastTerm, Writer)),
    ok = file:close(WFD),
    ok = emqx_connector_aggregator_test_helpers:truncate_at(Filename, WPos + 1),
    {ok, RFD1} = file:open(Filename, [read, binary]),
    {Metadata, Reader0} = emqx_connector_aggreg_buffer:new_reader(RFD1),
    {ReadTerms1, Reader1} = read_terms(length(Terms), Reader0),
    ?assertEqual(Terms, ReadTerms1),
    ?assertError(
        badarg,
        emqx_connector_aggreg_buffer:read(Reader1)
    ),
    ok = emqx_connector_aggregator_test_helpers:truncate_at(Filename, WPos div 2),
    {ok, RFD2} = file:open(Filename, [read, binary]),
    {Metadata, Reader2} = emqx_connector_aggreg_buffer:new_reader(RFD2),
    {ReadTerms2, Reader3} = read_terms(_FitsInto = 3, Reader2),
    ?assertEqual(lists:sublist(Terms, 3), ReadTerms2),
    ?assertError(
        badarg,
        emqx_connector_aggreg_buffer:read(Reader3)
    ).

t_read_truncated_takeover_write(Config) ->
    Filename = mk_filename(?FUNCTION_NAME, Config),
    {ok, WFD} = file:open(Filename, [write, binary]),
    Metadata = {?MODULE, #{tc => ?FUNCTION_NAME}},
    Writer1 = emqx_connector_aggreg_buffer:new_writer(WFD, Metadata),
    Terms1 = [
        [[[[[[[[[[[]]]]]]]]]]],
        lists:seq(1, 10000),
        lists:duplicate(1000, ?FUNCTION_NAME),
        {<<"text/plain">>, _Huge = rand:bytes(1048576)}
    ],
    Terms2 = [
        {<<"application/json">>, emqx_utils_json:encode(#{j => <<"son">>, null => null})},
        {<<"application/x-octet-stream">>, rand:bytes(102400)}
    ],
    ok = lists:foreach(
        fun(T) -> ?assertEqual(ok, emqx_connector_aggreg_buffer:write(T, Writer1)) end,
        Terms1
    ),
    {ok, WPos} = file:position(WFD, cur),
    ok = file:close(WFD),
    ok = emqx_connector_aggregator_test_helpers:truncate_at(Filename, WPos div 2),
    {ok, RWFD} = file:open(Filename, [read, write, binary]),
    {Metadata, Reader0} = emqx_connector_aggreg_buffer:new_reader(RWFD),
    {ReadTerms1, Reader1} = read_terms(_Survived = 3, Reader0),
    ?assertEqual(
        lists:sublist(Terms1, 3),
        ReadTerms1
    ),
    ?assertError(
        badarg,
        emqx_connector_aggreg_buffer:read(Reader1)
    ),
    Writer2 = emqx_connector_aggreg_buffer:takeover(Reader1),
    ok = lists:foreach(
        fun(T) -> ?assertEqual(ok, emqx_connector_aggreg_buffer:write(T, Writer2)) end,
        Terms2
    ),
    ok = file:close(RWFD),
    {ok, RFD} = file:open(Filename, [read, binary]),
    {Metadata, Reader2} = emqx_connector_aggreg_buffer:new_reader(RFD),
    ReadTerms2 = read_until_eof(Reader2),
    ?assertEqual(
        lists:sublist(Terms1, 3) ++ Terms2,
        ReadTerms2
    ).

%%

mk_filename(Name, Config) ->
    filename:join(?config(work_dir, Config), Name).

read_terms(0, Reader) ->
    {[], Reader};
read_terms(N, Reader0) ->
    {Term, Reader1} = emqx_connector_aggreg_buffer:read(Reader0),
    {Terms, Reader} = read_terms(N - 1, Reader1),
    {[Term | Terms], Reader}.

read_until_eof(Reader0) ->
    case emqx_connector_aggreg_buffer:read(Reader0) of
        {Term, Reader} ->
            [Term | read_until_eof(Reader)];
        eof ->
            []
    end.
