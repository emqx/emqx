%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_api_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf,
                "authorization.cache { enable = false },"
                "authorization.no_match = deny,"
                "authorization.sources = [{type = built_in_database, max_rules = 5}]"},
            emqx,
            emqx_auth,
            emqx_auth_mnesia,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{
            work_dir => filename:join(?config(priv_dir, Config), ?MODULE)
        }
    ),
    _ = emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].
end_per_suite(_Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, _Config)),
    _ = emqx_common_test_http:delete_default_app(),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_api(_) ->
    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "users"]),
            [?USERNAME_RULES_EXAMPLE]
        ),

    %% check length limit
    {ok, 400, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "users"]),
            [dup_rules_example(?USERNAME_RULES_EXAMPLE)]
        ),

    {ok, 409, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "users"]),
            [?USERNAME_RULES_EXAMPLE]
        ),

    {ok, 200, Request1} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "users"]),
            []
        ),
    #{
        <<"data">> := [#{<<"username">> := <<"user1">>, <<"rules">> := Rules1}],
        <<"meta">> := #{
            <<"count">> := 1,
            <<"limit">> := 100,
            <<"page">> := 1,
            <<"hasnext">> := false
        }
    } = emqx_utils_json:decode(Request1),
    ?assertEqual(?USERNAME_RULES_EXAMPLE_COUNT, length(Rules1)),

    {ok, 200, Request1_1} =
        request(
            get,
            uri([
                "authorization",
                "sources",
                "built_in_database",
                "rules",
                "users?page=1&limit=20&like_username=noexist"
            ]),
            []
        ),
    ?assertEqual(
        #{
            <<"data">> => [],
            <<"meta">> => #{
                <<"limit">> => 20,
                <<"page">> => 1,
                <<"hasnext">> => false
            }
        },
        emqx_utils_json:decode(Request1_1)
    ),

    {ok, 200, Request2} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "users", "user1"]),
            []
        ),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules1} = emqx_utils_json:decode(Request2),

    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database", "rules", "users", "user1"]),
            ?USERNAME_RULES_EXAMPLE(#{rules => []})
        ),

    %% check length limit

    {ok, 400, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database", "rules", "users", "user1"]),
            dup_rules_example2(?USERNAME_RULES_EXAMPLE)
        ),

    {ok, 200, Request3} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "users", "user1"]),
            []
        ),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules2} = emqx_utils_json:decode(Request3),
    ?assertEqual(0, length(Rules2)),

    {ok, 204, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "rules", "users", "user1"]),
            []
        ),
    {ok, 404, _} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "users", "user1"]),
            []
        ),
    {ok, 404, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "rules", "users", "user1"]),
            []
        ),

    % ensure that db contain a mix of records
    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "users"]),
            [?USERNAME_RULES_EXAMPLE]
        ),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "clients"]),
            [?CLIENTID_RULES_EXAMPLE]
        ),

    {ok, 400, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "clients"]),
            [dup_rules_example(?CLIENTID_RULES_EXAMPLE)]
        ),

    {ok, 409, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "clients"]),
            [?CLIENTID_RULES_EXAMPLE]
        ),

    {ok, 200, Request4} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "clients"]),
            []
        ),
    {ok, 200, Request5} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "clients", "client1"]),
            []
        ),
    #{
        <<"data">> := [#{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3}],
        <<"meta">> := #{<<"count">> := 1, <<"limit">> := 100, <<"page">> := 1}
    } =
        emqx_utils_json:decode(Request4),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3} = emqx_utils_json:decode(Request5),
    ?assertEqual(?CLIENTID_RULES_EXAMPLE_COUNT, length(Rules3)),

    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database", "rules", "clients", "client1"]),
            ?CLIENTID_RULES_EXAMPLE(#{rules => []})
        ),

    {ok, 400, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database", "rules", "clients", "client1"]),
            dup_rules_example2(
                ?CLIENTID_RULES_EXAMPLE
            )
        ),

    {ok, 200, Request6} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "clients", "client1"]),
            []
        ),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules4} = emqx_utils_json:decode(Request6),
    ?assertEqual(0, length(Rules4)),

    {ok, 204, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "rules", "clients", "client1"]),
            []
        ),
    {ok, 404, _} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "clients", "client1"]),
            []
        ),
    {ok, 404, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "rules", "clients", "client1"]),
            []
        ),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "all"]),
            ?ALL_RULES_EXAMPLE
        ),

    {ok, 400, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "all"]),
            dup_rules_example(?ALL_RULES_EXAMPLE)
        ),

    {ok, 200, Request7} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "all"]),
            []
        ),
    #{<<"rules">> := Rules5} = emqx_utils_json:decode(Request7),
    ?assertEqual(?ALL_RULES_EXAMPLE_COUNT, length(Rules5)),

    {ok, 204, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "rules", "all"]),
            []
        ),
    {ok, 200, Request8} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "all"]),
            []
        ),
    #{<<"rules">> := Rules6} = emqx_utils_json:decode(Request8),
    ?assertEqual(0, length(Rules6)),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "users"]),
            [
                #{username => erlang:integer_to_binary(N), rules => []}
             || N <- lists:seq(1, 20)
            ]
        ),
    {ok, 200, Request9} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "users?page=2&limit=5"]),
            []
        ),
    #{<<"data">> := Data1} = emqx_utils_json:decode(Request9),
    ?assertEqual(5, length(Data1)),

    {ok, 204, _} =
        request(
            post,
            uri(["authorization", "sources", "built_in_database", "rules", "clients"]),
            [
                #{clientid => erlang:integer_to_binary(N), rules => []}
             || N <- lists:seq(1, 20)
            ]
        ),
    {ok, 200, Request10} =
        request(
            get,
            uri(["authorization", "sources", "built_in_database", "rules", "clients?limit=5"]),
            []
        ),
    #{<<"data">> := Data2} = emqx_utils_json:decode(Request10),
    ?assertEqual(5, length(Data2)),

    {ok, 400, Msg1} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "rules"]),
            []
        ),
    ?assertMatch({match, _}, re:run(Msg1, "must\sbe\sdisabled\sbefore")),
    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database"]),
            #{<<"enable">> => true, <<"type">> => <<"built_in_database">>}
        ),
    %% test idempotence
    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database"]),
            #{<<"enable">> => true, <<"type">> => <<"built_in_database">>}
        ),
    {ok, 204, _} =
        request(
            put,
            uri(["authorization", "sources", "built_in_database"]),
            #{<<"enable">> => false, <<"type">> => <<"built_in_database">>}
        ),
    {ok, 204, _} =
        request(
            delete,
            uri(["authorization", "sources", "built_in_database", "rules"]),
            []
        ),
    ?assertEqual(0, emqx_authz_mnesia:record_count()),

    Examples = make_examples(emqx_authz_api_mnesia),
    ?assertEqual(
        14,
        length(Examples)
    ),

    Fixtures1 = fun() ->
        {ok, _, _} =
            request(
                delete,
                uri(["authorization", "sources", "built_in_database", "rules", "all"]),
                []
            ),
        {ok, _, _} =
            request(
                delete,
                uri(["authorization", "sources", "built_in_database", "rules", "users"]),
                []
            ),
        {ok, _, _} =
            request(
                delete,
                uri(["authorization", "sources", "built_in_database", "rules", "clients"]),
                []
            )
    end,
    run_examples(Examples, Fixtures1),

    Fixtures2 = fun() ->
        %% disable/remove built_in_database
        {ok, 204, _} =
            request(
                delete,
                uri(["authorization", "sources", "built_in_database"]),
                []
            )
    end,

    run_examples(404, Examples, Fixtures2),

    ok.

%% test helpers
-define(REPLACEMENTS, #{
    ":clientid" => <<"client1">>,
    ":username" => <<"user1">>
}).

run_examples(Examples) ->
    %% assume all ok
    run_examples(
        fun
            ({ok, Code, _}) when
                Code >= 200,
                Code =< 299
            ->
                true;
            (_Res) ->
                ct:pal("check failed: ~p", [_Res]),
                false
        end,
        Examples
    ).

run_examples(Examples, Fixtures) when is_function(Fixtures) ->
    Fixtures(),
    run_examples(Examples);
run_examples(Check, Examples) when is_function(Check) ->
    lists:foreach(
        fun({Path, Op, Body} = _Req) ->
            ct:pal("req: ~p", [_Req]),
            ?assert(
                Check(
                    request(Op, uri(Path), Body)
                )
            )
        end,
        Examples
    );
run_examples(Code, Examples) when is_number(Code) ->
    run_examples(
        fun
            ({ok, ResCode, _}) when Code =:= ResCode -> true;
            (_Res) ->
                ct:pal("check failed: ~p", [_Res]),
                false
        end,
        Examples
    ).

run_examples(CodeOrCheck, Examples, Fixtures) when is_function(Fixtures) ->
    Fixtures(),
    run_examples(CodeOrCheck, Examples).

make_examples(ApiMod) ->
    make_examples(ApiMod, ?REPLACEMENTS).

-spec make_examples(Mod :: atom()) -> [{Path :: list(), [{Op :: atom(), Body :: term()}]}].
make_examples(ApiMod, Replacements) ->
    Paths = ApiMod:paths(),
    lists:flatten(
        lists:map(
            fun(Path) ->
                Schema = ApiMod:schema(Path),
                lists:map(
                    fun({Op, OpSchema}) ->
                        Body =
                            case maps:get('requestBody', OpSchema, undefined) of
                                undefined ->
                                    [];
                                HoconWithExamples ->
                                    maps:get(
                                        value,
                                        hd(
                                            maps:values(
                                                maps:get(
                                                    <<"examples">>,
                                                    maps:get(examples, HoconWithExamples)
                                                )
                                            )
                                        )
                                    )
                            end,
                        {replace_parts(to_parts(Path), Replacements), Op, Body}
                    end,
                    lists:sort(
                        fun op_sort/2, maps:to_list(maps:with([get, put, post, delete], Schema))
                    )
                )
            end,
            Paths
        )
    ).

op_sort({post, _}, {_, _}) ->
    true;
op_sort({put, _}, {_, _}) ->
    true;
op_sort({get, _}, {delete, _}) ->
    true;
op_sort(_, _) ->
    false.

to_parts(Path) ->
    string:tokens(Path, "/").

replace_parts(Parts, Replacements) ->
    lists:map(
        fun(Part) ->
            %% that's the fun part
            case maps:is_key(Part, Replacements) of
                true ->
                    maps:get(Part, Replacements);
                false ->
                    Part
            end
        end,
        Parts
    ).

dup_rules_example(#{username := _, rules := Rules}) ->
    #{username => user2, rules => Rules ++ Rules};
dup_rules_example(#{clientid := _, rules := Rules}) ->
    #{clientid => client2, rules => Rules ++ Rules};
dup_rules_example(#{rules := Rules}) ->
    #{rules => Rules ++ Rules}.

dup_rules_example2(#{rules := Rules} = Example) ->
    Example#{rules := Rules ++ Rules}.
