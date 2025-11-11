%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_api_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf,
                "authorization.cache { enable = false },"
                "authorization.no_match = deny,"
                "authorization.sources = [{type = built_in_database, max_rules = 7}]"},
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
%% Helper fns
%%------------------------------------------------------------------------------

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

create_username_rules(Params) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => post,
        url => uri(["authorization", "sources", "built_in_database", "rules", "users"]),
        body => Params
    }).

get_username_rules(QueryParams) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => uri(["authorization", "sources", "built_in_database", "rules", "users"]),
        query_params => QueryParams
    }).

get_one_username_rules(Username, QueryParams) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => uri(["authorization", "sources", "built_in_database", "rules", "users", Username]),
        query_params => QueryParams
    }).

delete_one_username_rules(Username) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => delete,
        url => uri(["authorization", "sources", "built_in_database", "rules", "users", Username])
    }).

update_one_username(Username, Params) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => put,
        url => uri(["authorization", "sources", "built_in_database", "rules", "users", Username]),
        body => Params
    }).

create_clientid_rules(Params) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => post,
        url => uri(["authorization", "sources", "built_in_database", "rules", "clients"]),
        body => Params
    }).

get_clientid_rules(QueryParams) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => uri(["authorization", "sources", "built_in_database", "rules", "clients"]),
        query_params => QueryParams
    }).

get_one_clientid_rules(ClientId, QueryParams) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => uri(["authorization", "sources", "built_in_database", "rules", "clients", ClientId]),
        query_params => QueryParams
    }).

update_one_clientid(ClientId, Params) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => put,
        url => uri(["authorization", "sources", "built_in_database", "rules", "clients", ClientId]),
        body => Params
    }).

delete_one_clientid_rules(ClientId) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => delete,
        url => uri(["authorization", "sources", "built_in_database", "rules", "clients", ClientId])
    }).

create_all_rules(Params) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => post,
        url => uri(["authorization", "sources", "built_in_database", "rules", "all"]),
        body => Params
    }).

get_all_rules(QueryParams) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => uri(["authorization", "sources", "built_in_database", "rules", "all"]),
        query_params => QueryParams
    }).

delete_all_rules() ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => delete,
        url => uri(["authorization", "sources", "built_in_database", "rules", "all"])
    }).

delete_username_rules() ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => delete,
        url => uri(["authorization", "sources", "built_in_database", "rules", "users"])
    }).

delete_clientid_rules() ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => delete,
        url => uri(["authorization", "sources", "built_in_database", "rules", "clients"])
    }).

delete_root_rules() ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => delete,
        url => uri(["authorization", "sources", "built_in_database", "rules"])
    }).

update_config(Params) ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => put,
        url => uri(["authorization", "sources", "built_in_database"]),
        body => Params
    }).

delete_authz() ->
    emqx_mgmt_api_test_util:simple_request(#{
        method => delete,
        url => uri(["authorization", "sources", "built_in_database"])
    }).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_api(_) ->
    {204, _} = create_username_rules([?USERNAME_RULES_EXAMPLE]),

    %% check length limit
    {400, _} = create_username_rules([dup_rules_example(?USERNAME_RULES_EXAMPLE)]),

    {409, _} = create_username_rules([?USERNAME_RULES_EXAMPLE]),

    {200, Request1} = get_username_rules(#{}),
    #{
        <<"data">> := [#{<<"username">> := <<"user1">>, <<"rules">> := Rules1}],
        <<"meta">> := #{
            <<"count">> := 1,
            <<"limit">> := 100,
            <<"page">> := 1,
            <<"hasnext">> := false
        }
    } = Request1,
    ?assertEqual(?USERNAME_RULES_EXAMPLE_COUNT, length(Rules1)),

    {200, Request1_1} = get_username_rules(#{
        <<"page">> => <<"1">>,
        <<"limit">> => <<"20">>,
        <<"like_username">> => <<"noexist">>
    }),
    ?assertEqual(
        #{
            <<"data">> => [],
            <<"meta">> => #{
                <<"limit">> => 20,
                <<"page">> => 1,
                <<"hasnext">> => false
            }
        },
        Request1_1
    ),

    Username1 = <<"user1">>,
    {200, Request2} = get_one_username_rules(Username1, #{}),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules1} = Request2,

    {204, _} = update_one_username(Username1, maps:merge(?USERNAME_RULES_EXAMPLE, #{rules => []})),

    %% check length limit

    {400, _} = update_one_username(Username1, dup_rules_example2(?USERNAME_RULES_EXAMPLE)),

    {200, Request3} = get_one_username_rules(Username1, #{}),
    #{<<"username">> := <<"user1">>, <<"rules">> := Rules2} = Request3,
    ?assertEqual(0, length(Rules2)),

    {204, _} = delete_one_username_rules(Username1),
    {404, _} = get_one_username_rules(Username1, #{}),
    {404, _} = delete_one_username_rules(Username1),

    % ensure that db contain a mix of records
    {204, _} = create_username_rules([?USERNAME_RULES_EXAMPLE]),
    {204, _} = create_clientid_rules([?CLIENTID_RULES_EXAMPLE]),

    {400, _} = create_clientid_rules([dup_rules_example(?CLIENTID_RULES_EXAMPLE)]),

    {409, _} = create_clientid_rules([?CLIENTID_RULES_EXAMPLE]),

    {200, Request4} = get_clientid_rules(#{}),
    ClientId1 = <<"client1">>,
    {200, Request5} = get_one_clientid_rules(ClientId1, #{}),
    #{
        <<"data">> := [#{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3}],
        <<"meta">> := #{<<"count">> := 1, <<"limit">> := 100, <<"page">> := 1}
    } = Request4,
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules3} = Request5,
    ?assertEqual(?CLIENTID_RULES_EXAMPLE_COUNT, length(Rules3)),

    {204, _} = update_one_clientid(ClientId1, maps:merge(?CLIENTID_RULES_EXAMPLE, #{rules => []})),

    {400, _} = update_one_clientid(ClientId1, dup_rules_example2(?CLIENTID_RULES_EXAMPLE)),

    {200, Request6} = get_one_clientid_rules(ClientId1, #{}),
    #{<<"clientid">> := <<"client1">>, <<"rules">> := Rules4} = Request6,
    ?assertEqual(0, length(Rules4)),

    {204, _} = delete_one_clientid_rules(ClientId1),
    {404, _} = get_one_clientid_rules(ClientId1, #{}),
    {404, _} = delete_one_clientid_rules(ClientId1),

    {204, _} = create_all_rules(?ALL_RULES_EXAMPLE),

    {400, _} = create_all_rules(dup_rules_example(?ALL_RULES_EXAMPLE)),

    {200, Request7} = get_all_rules(#{}),
    #{<<"rules">> := Rules5} = Request7,
    ?assertEqual(?ALL_RULES_EXAMPLE_COUNT, length(Rules5)),

    {204, _} = delete_all_rules(),
    {200, Request8} = get_all_rules(#{}),
    #{<<"rules">> := Rules6} = Request8,
    ?assertEqual(0, length(Rules6)),

    {204, _} = create_username_rules([
        #{username => erlang:integer_to_binary(N), rules => []}
     || N <- lists:seq(1, 20)
    ]),
    {200, Request9} = get_username_rules(#{
        <<"page">> => <<"2">>,
        <<"limit">> => <<"5">>
    }),
    #{<<"data">> := Data1} = Request9,
    ?assertEqual(5, length(Data1)),

    {204, _} = create_clientid_rules([
        #{clientid => erlang:integer_to_binary(N), rules => []}
     || N <- lists:seq(1, 20)
    ]),
    {200, Request10} = get_clientid_rules(#{<<"limit">> => <<"5">>}),
    #{<<"data">> := Data2} = Request10,
    ?assertEqual(5, length(Data2)),

    {400, #{<<"message">> := Msg1}} = delete_root_rules(),
    ?assertMatch({match, _}, re:run(Msg1, "must\sbe\sdisabled\sbefore")),
    {204, _} = update_config(#{<<"enable">> => true, <<"type">> => <<"built_in_database">>}),
    %% test idempotence
    ?assertEqual(0, emqx_authz_mnesia:record_count()),
    {204, _} = update_config(#{<<"enable">> => true, <<"type">> => <<"built_in_database">>}),
    {204, _} = update_config(#{<<"enable">> => false, <<"type">> => <<"built_in_database">>}),
    {204, _} = delete_root_rules(),

    Examples = make_examples(emqx_authz_api_mnesia),
    ?assertEqual(
        14,
        length(Examples)
    ),

    Fixtures1 = fun() ->
        _ = delete_all_rules(),
        _ = delete_username_rules(),
        _ = delete_clientid_rules()
    end,
    run_examples(Examples, Fixtures1),

    Fixtures2 = fun() ->
        %% disable/remove built_in_database
        {204, _} = delete_authz()
    end,

    run_examples(404, Examples, Fixtures2),

    ok.
