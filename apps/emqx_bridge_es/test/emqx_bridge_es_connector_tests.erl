%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_es_connector_tests).

-include_lib("eunit/include/eunit.hrl").

-define(MSG, #{
    <<"payload">> => #{
        <<"index">> => <<"a#b/c d">>,
        <<"id">> => <<"i#1/2">>,
        <<"routing">> => <<"r/1">>,
        <<"doc">> => <<"{\"a\":\"x#y/z ?\"}">>
    }
}).

%% Interpolated `index'/`id'/`routing' values are URL-encoded when the
%% request path is rendered, so URL-significant characters stay within
%% their path segment.
path_render_urlencodes_values_test_() ->
    [
        ?_assertEqual(
            <<"/a%23b%2Fc%20d/_doc/i%231%2F2?routing=r%2F1">>,
            render_path(
                #{
                    action => delete,
                    index => <<"${payload.index}">>,
                    id => <<"${payload.id}">>,
                    routing => <<"${payload.routing}">>
                },
                ?MSG
            )
        ),
        ?_assertEqual(
            <<"/a%23b%2Fc%20d/_update/i%231%2F2">>,
            render_path(
                #{
                    action => update,
                    index => <<"${payload.index}">>,
                    id => <<"${payload.id}">>
                },
                ?MSG
            )
        ),
        ?_assertEqual(
            <<"/a%23b%2Fc%20d/_doc/i%231%2F2">>,
            render_path(
                #{
                    action => create,
                    index => <<"${payload.index}">>,
                    id => <<"${payload.id}">>
                },
                ?MSG
            )
        ),
        ?_assertEqual(
            <<"/a%23b%2Fc%20d/_doc/">>,
            render_path(
                #{action => create, index => <<"${payload.index}">>},
                ?MSG
            )
        )
    ].

%% Ordinary index/id values render exactly as before, whether templated
%% or configured as literals.
path_render_plain_values_unchanged_test_() ->
    Msg = #{<<"payload">> => #{<<"index">> => <<"devices">>, <<"id">> => <<"42">>}},
    [
        ?_assertEqual(
            <<"/devices/_doc/42">>,
            render_path(
                #{
                    action => create,
                    index => <<"${payload.index}">>,
                    id => <<"${payload.id}">>
                },
                Msg
            )
        ),
        ?_assertEqual(
            <<"/devices/_doc/42">>,
            render_path(
                #{action => create, index => <<"devices">>, id => <<"42">>},
                Msg
            )
        )
    ].

%% Non-ASCII values are percent-encoded as UTF-8 within the path.
path_render_unicode_test() ->
    Msg = #{<<"payload">> => #{<<"index">> => <<"日志"/utf8>>}},
    ?assertEqual(
        <<"/%E6%97%A5%E5%BF%97/_doc/">>,
        render_path(#{action => create, index => <<"${payload.index}">>}, Msg)
    ).

%% A path value that is not valid unicode fails the render with a clear
%% error instead of composing a corrupt path.
path_render_invalid_unicode_test() ->
    Msg = #{<<"payload">> => #{<<"index">> => <<255, 254, 1>>}},
    ?assertError(
        {failed_to_urlencode_path_value, "payload.index", _},
        render_path(#{action => create, index => <<"${payload.index}">>}, Msg)
    ).

%% The request body is rendered by the same function; it must remain
%% byte-identical, without URL-encoding.
body_render_unchanged_test_() ->
    [
        ?_assertEqual(
            <<"{\"a\":\"x#y/z ?\"}">>,
            render(body, emqx_template:parse(<<"${payload.doc}">>), ?MSG)
        ),
        ?_assertEqual(
            <<"{\"doc\":{\"a\":\"x#y/z ?\"}}">>,
            render(body, emqx_template:parse(<<"{\"doc\":${payload.doc}}">>), ?MSG)
        ),
        ?_assertEqual(
            emqx_utils_json:encode(#{<<"doc">> => ?MSG}),
            render(body, [<<"update_without_doc_template">>], ?MSG)
        )
    ].

%% Renders a path exactly as `emqx_bridge_http_connector' does: the
%% template built at channel creation is parsed with
%% `emqx_template:parse/1' and rendered with `render_template/3' in
%% the `path' render context.
render_path(Parameter, Msg) ->
    PathTemplate = emqx_bridge_es_connector:path(Parameter),
    render(path, emqx_template:parse(PathTemplate), Msg).

render(RenderContext, Template, Msg) ->
    unicode:characters_to_binary(
        emqx_bridge_es_connector:render_template(RenderContext, Template, Msg)
    ).
