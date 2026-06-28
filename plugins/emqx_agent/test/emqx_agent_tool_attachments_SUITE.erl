%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_attachments_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_json_autodiscovers_data_image_url(_Config) ->
    Data = #{
        <<"image_url">> => <<"data:image/png;base64,abc123">>,
        <<"label">> => <<"box">>
    },
    ?assertMatch(
        {ok, #{<<"image_url">> := <<"Image .image_url">>, <<"label">> := <<"box">>}, [
            #{<<"id">> := <<".image_url">>, <<"mime_type">> := <<"image/png">>}
        ]},
        process(Data)
    ).

t_json_extracts_explicit_path(_Config) ->
    Data = #{
        <<"nested">> => #{<<"photo">> => <<"data:image/jpeg;base64,xyz">>}
    },
    {ok, Payload, Attachments} = process(Data, #{
        autodiscover_images => false,
        images => [<<".nested.photo">>]
    }),
    ?assertMatch(#{<<"nested">> := #{<<"photo">> := <<"Image .nested.photo">>}}, Payload),
    ?assertMatch(
        [#{<<"id">> := <<".nested.photo">>, <<"mime_type">> := <<"image/jpeg">>}], Attachments
    ).

t_missing_explicit_path_is_skipped(_Config) ->
    Data = #{<<"value">> => 1},
    ?assertEqual(
        {ok, #{<<"value">> => 1}, []},
        process(Data, #{images => [<<".missing">>]})
    ).

t_binary_root_image_by_content_type(_Config) ->
    Body = <<"not really png">>,
    {ok, Payload, Attachments} = process(Body, #{
        content_type => <<"image/png">>
    }),
    ?assertEqual(<<"Image .">>, Payload),
    ?assertMatch([#{<<"id">> := <<".">>, <<"mime_type">> := <<"image/png">>}], Attachments).

t_binary_root_explicit_image_path(_Config) ->
    Png = <<137, 80, 78, 71, 13, 10, 26, 10, 0, 0>>,
    {ok, Payload, Attachments} = process(Png, #{
        autodiscover_images => false,
        images => [<<".">>]
    }),
    ?assertEqual(<<"Image .">>, Payload),
    ?assertMatch([#{<<"id">> := <<".">>, <<"mime_type">> := <<"image/png">>}], Attachments).

process(Body) ->
    process(Body, #{}).

process(Body, Opts) ->
    emqx_agent_tool_attachments:process(
        Body,
        maps:merge(
            #{
                autodiscover_images => true,
                images => [],
                content_type => undefined
            },
            Opts
        )
    ).
