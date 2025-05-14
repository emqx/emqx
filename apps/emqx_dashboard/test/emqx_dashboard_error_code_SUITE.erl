%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_error_code_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_management/include/emqx_mgmt_api.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SERVER, "http://127.0.0.1:18083/api/v5").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

t_all_code(_) ->
    HrlDef = ?ERROR_CODES,
    All = emqx_dashboard_error_code:all(),
    ?assertEqual(length(HrlDef), length(All)),
    ok.

t_list_code(_) ->
    List = emqx_dashboard_error_code:list(),
    [?assert(exist(CodeName, List)) || CodeName <- emqx_dashboard_error_code:all()],
    ok.

t_look_up_code(_) ->
    Fun =
        fun(CodeName) ->
            {ok, _} = emqx_dashboard_error_code:look_up(CodeName)
        end,
    lists:foreach(Fun, emqx_dashboard_error_code:all()),
    {error, not_found} = emqx_dashboard_error_code:look_up('_____NOT_EXIST_NAME'),
    ok.

t_description_code(_) ->
    {error, not_found} = emqx_dashboard_error_code:description('_____NOT_EXIST_NAME'),
    {ok, <<"Request parameters are invalid">>} =
        emqx_dashboard_error_code:description('BAD_REQUEST'),
    ok.

t_format_code(_) ->
    #{code := 'TEST_SUITE_CODE', description := <<"for test suite">>} =
        emqx_dashboard_error_code:format({'TEST_SUITE_CODE', <<"for test suite">>}),
    ok.

t_api_codes(_) ->
    Url = ?SERVER ++ "/error_codes",
    {ok, List} = request(Url),
    [
        ?assert(exist(atom_to_binary(CodeName, utf8), List))
     || CodeName <- emqx_dashboard_error_code:all()
    ],
    ok.

t_api_code(_) ->
    Url = ?SERVER ++ "/error_codes/BAD_REQUEST",
    {ok, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"description">> := <<"Request parameters are invalid">>
    }} = request(Url),
    ok.

exist(_CodeName, []) ->
    false;
exist(CodeName, [#{code := CodeName, description := _} | _List]) ->
    true;
exist(CodeName, [#{<<"code">> := CodeName, <<"description">> := _} | _List]) ->
    true;
exist(CodeName, [_ | List]) ->
    exist(CodeName, List).

request(Url) ->
    Request = {Url, []},
    case httpc:request(get, Request, [], []) of
        {error, Reason} ->
            {error, Reason};
        {ok, {{"HTTP/1.1", Code, _}, _, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, emqx_utils_json:decode(Return)};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.
