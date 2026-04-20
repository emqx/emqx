%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_test_helpers).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_offline_messages.hrl").

all(Suite) ->
    lists:usort([
        F
     || {F, 1} <- Suite:module_info(exports),
        string:substr(atom_to_list(F), 1, 2) == "t_"
    ]).

nested_groups([GroupsNames, LeafItems]) ->
    [
        {Group, [], LeafItems}
     || Group <- GroupsNames
    ];
nested_groups([ParentNames, ChildNames | Names]) ->
    ChildGroups = [{group, GroupName} || GroupName <- ChildNames],
    [
        {ParentName, [], ChildGroups}
     || ParentName <- ParentNames
    ] ++ nested_groups([ChildNames | Names]).

start() ->
    {ok, _} = application:ensure_all_started(hackney),
    ok.

stop() ->
    ok.

allow_plugin_install() ->
    Command =
        "docker compose exec emqx /opt/emqx/bin/emqx ctl plugins allow " ++
            binary_to_list(?PLUGIN_NAME_VSN),
    ct:print("Command: ~s~n", [Command]),
    os:cmd(Command),
    timer:sleep(1000),
    ok.

api_get(Path) ->
    Result = make_request({get, Path}),
    handle_result(Result).

api_get_raw(Path) ->
    ct:pal("GET ~s~n~n", [api_url(Path)]),
    Result = hackney:request(get, api_url(Path), []),
    handle_result_raw(Result).

api_post(Path, Body) ->
    Result = make_request({post, Path, Body}),
    handle_result(Result).

api_post_raw(Path, Headers0, Body) ->
    HeadersS = [io_lib:format("~s: ~s~n", [Key, Value]) || {Key, Value} <- Headers0],
    ct:pal("POST ~s~n~s~n~n...", [api_url(Path), iolist_to_binary(HeadersS)]),
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
    ct:pal("GET ~s~n~n", [api_url(Path)]),
    hackney:request(get, api_url(Path), headers(), <<>>, hackney_options());
make_request({post, Path, Body}) ->
    ct:pal("POST ~s~n~n~s~n~n", [api_url(Path), encode_json(Body)]),
    hackney:request(post, api_url(Path), headers(), encode_json(Body), hackney_options());
make_request({put, Path, Body}) ->
    ct:pal("PUT ~s~n~n~s~n~n", [api_url(Path), encode_json(Body)]),
    hackney:request(put, api_url(Path), headers(), encode_json(Body), hackney_options());
make_request({delete, Path}) ->
    ct:pal("DELETE ~s~n~n", [api_url(Path)]),
    hackney:request(delete, api_url(Path), headers(), <<>>, hackney_options()).

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
handle_result_raw({ok, Code, _Headers, ClientRef}) ->
    {ok, Body} = hackney:body(ClientRef),
    ct:pal("Response body:~n~s~n", [Body]),
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
    <<"localhost">>.

api_url_base() ->
    <<"http://", (api_host())/binary, ":18083/api/v5/">>.

headers() ->
    [auth_header(), {<<"Content-Type">>, <<"application/json">>}].

auth_header() ->
    {<<"Authorization">>, <<"Basic ", (api_auth())/binary>>}.

api_auth() ->
    base64:encode(<<"key:secret">>).

decode_json(Body) ->
    jiffy:decode(Body, [return_maps]).

encode_json(Data) ->
    jiffy:encode(Data).

hackney_options() ->
    [{recv_timeout, 10000}, {connect_timeout, 5000}, {checkout_timeout, 5000}].
