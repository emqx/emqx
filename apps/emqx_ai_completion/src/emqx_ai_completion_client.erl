%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ai_completion_client).

-include_lib("emqx/include/logger.hrl").

-export([
    new/1,
    api_get/2,
    api_post/3,
    api_put/3,
    api_delete/2
]).

-type transport_options() :: emqx_ai_completion_config:transport_options().

-type options() :: #{
    base_url := binary(),
    headers := [{binary(), emqx_secret:t(binary())}],
    transport_options := transport_options()
}.

-export_type([t/0, options/0]).

-record(state, {
    headers :: [{binary(), emqx_secret:t(binary())}],
    base_url :: binary(),
    transport_options :: transport_options()
}).

-type t() :: #state{}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec new(options()) -> t().
new(#{
    base_url := BaseUrl,
    headers := Headers,
    transport_options := TransportOptions
}) ->
    #state{
        base_url = BaseUrl,
        headers = Headers,
        transport_options = TransportOptions
    }.

api_get(State, Path) ->
    Result = make_request(State, {get, Path}),
    handle_result(Result).

api_post(State, Path, Body) ->
    Result = make_request(State, {post, Path, Body}),
    handle_result(Result).

api_put(State, Path, Body) ->
    Result = make_request(State, {put, Path, Body}),
    handle_result(Result).

api_delete(State, Path) ->
    Result = make_request(State, {delete, Path}),
    handle_result(Result).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

make_request(State, {get, Path}) ->
    hackney:request(get, api_url(State, Path), headers(State), <<>>, req_options(State));
make_request(State, {post, Path, Body}) ->
    hackney:request(
        post, api_url(State, Path), headers(State), emqx_utils_json:encode(Body), req_options(State)
    );
make_request(State, {put, Path, Body}) ->
    hackney:request(
        put, api_url(State, Path), headers(State), emqx_utils_json:encode(Body), req_options(State)
    );
make_request(State, {delete, Path}) ->
    hackney:request(delete, api_url(State, Path), headers(State), <<>>, req_options(State)).

handle_result({ok, Code, _Headers, ClientRef}) when Code >= 200 andalso Code < 300 ->
    {ok, Json} = hackney:body(ClientRef),
    case Json of
        <<>> ->
            ok;
        _ ->
            {ok, emqx_utils_json:decode(Json)}
    end;
handle_result({ok, Code, _Headers, ClientRef}) ->
    {ok, Json} = hackney:body(ClientRef),
    {error, {http, {Code, Json}}};
handle_result({error, Reason}) ->
    {error, Reason}.

api_url(#state{base_url = BaseUrl}, Path) when is_binary(Path) ->
    iolist_to_binary(emqx_utils_uri:join_path(BaseUrl, Path));
api_url(State, Path) when is_list(Path) ->
    api_url(State, iolist_to_binary(Path));
api_url(State, Path) when is_atom(Path) ->
    api_url(State, atom_to_binary(Path, utf8));
api_url(State, Path0) when is_tuple(Path0) ->
    Path1 = [bin(Segment) || Segment <- tuple_to_list(Path0)],
    Path = lists:join("/", Path1),
    api_url(State, iolist_to_binary(Path)).

headers(#state{headers = Headers}) ->
    [eval_header(Header) || Header <- Headers].

eval_header({Name, Value}) ->
    {Name, emqx_secret:unwrap(Value)}.

bin(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
bin(B) when is_binary(B) ->
    B;
bin(I) when is_integer(I) ->
    integer_to_binary(I);
bin(L) when is_list(L) ->
    iolist_to_binary(L).

req_options(#state{transport_options = TransportOptions}) ->
    maps:to_list(TransportOptions).
