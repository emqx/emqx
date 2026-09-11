%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_swagger_literal_docs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("hocon/include/hoconsc.hrl").

all() ->
    [t_literal_method_docs_option_preserves_summary].

t_literal_method_docs_option_preserves_summary(_Config) ->
    {Apis, _Components} = emqx_dashboard_swagger:spec(
        ?MODULE,
        #{allow_literal_method_docs => true}
    ),
    [{"/literal_docs", #{post := Operation}, literal_docs, _}] = Apis,
    ?assertEqual(<<"Literal operation summary">>, maps:get(summary, Operation)),
    ?assertEqual(<<"Literal operation description">>, maps:get(description, Operation)).

paths() ->
    ["/literal_docs"].

schema("/literal_docs") ->
    #{
        'operationId' => literal_docs,
        post => #{
            summary => <<"Literal operation summary">>,
            description => <<"Literal operation description">>,
            tags => [<<"Literal">>],
            responses => #{200 => <<"OK">>}
        }
    }.
