%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_client).

-export([
    api_get/1,
    api_get_raw/1,
    api_post/2,
    api_post_raw/3,
    api_put/2,
    api_put_raw/2,
    api_delete/1
]).


api_get(Path) ->
    Result = make_request({get, Path}),
    handle_result(Result).

api_get_raw(Path) ->
    Result = hackney:request(get, api_url(Path), []),
    handle_result_raw(Result).

api_post(Path, Body) ->
    Result = make_request({post, Path, Body}),
    handle_result(Result).

api_post_raw(Path, Headers0, Body) ->
    Headers = [auth_header() | Headers0],
    Result = hackney:request(post, api_url(Path), Headers, Body),
    handle_result_raw(Result).

api_put(Path, Body) ->
    Result = make_request({put, Path, Body}),
    handle_result(Result).

api_put_raw(Path, Body) ->
    Result = hackney:request(put, api_url(Path), [auth_header()], Body),
    handle_result_raw(Result).

api_delete(Path) ->
    Result = make_request({delete, Path}),
    handle_result(Result).

make_request({get, Path}) ->
    hackney:request(get, api_url(Path), headers());
make_request({post, Path, Body}) ->
    hackney:request(post, api_url(Path), headers(), encode_json(Body));
make_request({put, Path, Body}) ->
    hackney:request(put, api_url(Path), headers(), encode_json(Body));
make_request({delete, Path}) ->
    hackney:request(delete, api_url(Path), headers()).

handle_result({ok, Code, _Headers, ClientRef}) when Code >= 200 andalso Code < 300 ->
    {ok, Json} = hackney:body(ClientRef),
    case Json of
        <<>> ->
            ok;
        _ ->
            {ok, decode_json(Json)}
    end;
handle_result({ok, Code, _Headers, ClientRef}) ->
    {ok, Json} = hackney:body(ClientRef),
    {error, {http, {Code, Json}}};
handle_result({error, Reason}) ->
    {error, Reason}.

handle_result_raw({ok, Code, _Headers, ClientRef}) when Code >= 200 andalso Code < 300 ->
    hackney:body(ClientRef);
handle_result_raw({ok, Code, _Headers, _Body}) ->
    {error, {http_status, Code}};
handle_result_raw({error, Reason}) ->
    {error, Reason}.

api_url(Path) when is_binary(Path) ->
    <<(api_url_base())/binary, Path/binary>>;
api_url(Path) when is_list(Path) ->
    api_url(iolist_to_binary(Path));
api_url(Path) when is_atom(Path) ->
    api_url(atom_to_binary(Path, utf8));
api_url(Path0) when is_tuple(Path0) ->
    Path1 = [bin(Segment) || Segment <- tuple_to_list(Path0)],
    Path = lists:join("/", Path1),
    api_url(iolist_to_binary(Path)).

bin(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
bin(B) when is_binary(B) ->
    B;
bin(I) when is_integer(I) ->
    integer_to_binary(I);
bin(L) when is_list(L) ->
    iolist_to_binary(L).

api_host() ->
    <<"api.openai.com">>.

api_url_base() ->
    <<"https://", (api_host())/binary, "/v1/">>.

headers() ->
    [auth_header(), {<<"Content-Type">>, <<"application/json">>}].

auth_header() ->
    {<<"Authorization">>, <<"Bearer ", (api_auth())/binary>>}.

api_auth() ->
    iolist_to_binary(os:getenv("OPENAI_API_KEY")).

decode_json(Body) ->
    jiffy:decode(Body, [return_maps]).

encode_json(Data) ->
    jiffy:encode(Data).


