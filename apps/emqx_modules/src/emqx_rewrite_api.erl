-module(emqx_rewrite_api).

-behaviour(minirest_api).

-export([api_spec/0]).

-export([topic_rewrite/2]).

api_spec() ->
    {[rewrite_api()], []}.

topic_rewrite_schema() ->
    #{
        type => object,
        properties => #{
            action => #{
                type => string,
                description => <<"Node">>,
                enum => [subscribe, publish]},
            source_topic => #{
                type => string,
                description => <<"Topic">>},
            re => #{
                type => string,
                description => <<"Regular expressions">>},
            dest_topic => #{
                type => string,
                description => <<"Destination topic">>}
        }
    }.

rewrite_api() ->
    Path = "/topic_rewrite",
    Metadata = #{
        get => #{
            description => <<"List topic rewrite">>,
            responses => #{
                <<"200">> =>
                    emqx_mgmt_util:response_array_schema(<<"List all rewrite rules">>, topic_rewrite_schema())}},
        post => #{
            description => <<"New topic rewrite">>,
            'requestBody' => emqx_mgmt_util:request_body_array_schema(topic_rewrite_schema()),
            response => #{
                <<"200">> =>
                    emqx_mgmt_util:response_schema(<<"Update topic rewrite success">>, topic_rewrite_schema())}}},
    {Path, Metadata, topic_rewrite}.

topic_rewrite(get, _Request) ->
    {200, emqx_rewrite:list()};

topic_rewrite(post, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    ok = emqx_rewrite:update(Params),
    {200, emqx_rewrite:list()}.
