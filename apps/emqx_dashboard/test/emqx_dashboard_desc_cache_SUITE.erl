%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_desc_cache_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(NS, <<"emqx_test_schema">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Tab = ets:new(desc_cache_test, [public, ordered_set]),
    %% en has every text, zh and zh-TW have only some of them
    ok = insert(Tab, <<"en">>, <<"only_en">>, <<"en only">>),
    ok = insert(Tab, <<"en">>, <<"zh_only">>, <<"en fallback">>),
    ok = insert(Tab, <<"zh">>, <<"zh_only">>, <<"简体">>),
    ok = insert(Tab, <<"en">>, <<"all_langs">>, <<"english">>),
    ok = insert(Tab, <<"zh">>, <<"all_langs">>, <<"简体中文">>),
    ok = insert(Tab, <<"zh-TW">>, <<"all_langs">>, <<"繁體中文">>),
    [{tab, Tab} | Config].

end_per_testcase(_TestCase, Config) ->
    true = ets:delete(?config(tab, Config)),
    ok.

insert(Tab, Lang, Id, Text) ->
    true = ets:insert(Tab, {{Lang, ?NS, Id, <<"desc">>}, Text}),
    ok.

lookup(Config, Lang, Id) ->
    emqx_dashboard_desc_cache:lookup(?config(tab, Config), Lang, ?NS, Id, <<"desc">>).

%% A text present in the requested language is returned as is. zh-TW carries a
%% hyphen, which the file name parsing and the cache key have to keep intact.
t_exact_match(Config) ->
    ?assertEqual(<<"繁體中文">>, lookup(Config, <<"zh-TW">>, <<"all_langs">>)),
    ?assertEqual(<<"简体中文">>, lookup(Config, <<"zh">>, <<"all_langs">>)),
    ?assertEqual(<<"english">>, lookup(Config, <<"en">>, <<"all_langs">>)).

%% The languages are independent: a text missing from zh-TW shows the English
%% one, never the Simplified Chinese one.
t_falls_back_to_en(Config) ->
    ?assertEqual(<<"en fallback">>, lookup(Config, <<"zh-TW">>, <<"zh_only">>)),
    ?assertEqual(<<"en only">>, lookup(Config, <<"zh-TW">>, <<"only_en">>)),
    ?assertEqual(<<"en only">>, lookup(Config, <<"zh">>, <<"only_en">>)).

t_unknown_lang_falls_back_to_en(Config) ->
    ?assertEqual(<<"english">>, lookup(Config, <<"ja">>, <<"all_langs">>)).

t_missing_everywhere(Config) ->
    ?assertEqual(undefined, lookup(Config, <<"zh-TW">>, <<"no_such_id">>)),
    ?assertEqual(undefined, lookup(Config, <<"en">>, <<"no_such_id">>)).

%% The language may be given as an atom or a string, like the config value is.
t_lang_is_normalised(Config) ->
    ?assertEqual(<<"繁體中文">>, lookup(Config, 'zh-TW', <<"all_langs">>)),
    ?assertEqual(<<"繁體中文">>, lookup(Config, "zh-TW", <<"all_langs">>)),
    ?assertEqual(<<"en fallback">>, lookup(Config, 'zh-TW', <<"zh_only">>)).

%% An uninitialised cache must not crash the caller.
t_no_table(_Config) ->
    ?assertEqual(
        undefined,
        emqx_dashboard_desc_cache:lookup(
            no_such_ets_table, <<"zh-TW">>, ?NS, <<"all_langs">>, <<"desc">>
        )
    ).
