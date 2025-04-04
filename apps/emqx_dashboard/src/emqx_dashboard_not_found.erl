%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_not_found).

-include_lib("emqx/include/logger.hrl").

-export([init/2]).

init(Req0, State) ->
    RedactedReq = emqx_utils:redact(Req0),
    ?SLOG(notice, #{
        msg => "api_path_not_found",
        path => cowboy_req:path(RedactedReq),
        scheme => cowboy_req:scheme(RedactedReq),
        method => cowboy_req:method(RedactedReq),
        headers => cowboy_req:headers(RedactedReq),
        query_string => cowboy_req:qs(RedactedReq),
        peer => cowboy_req:peer(RedactedReq)
    }),
    CT = ct(cowboy_req:header(<<"accept">>, Req0, <<"text/html">>)),
    Req = cowboy_req:reply(
        404,
        #{<<"content-type">> => CT},
        ct_body(CT),
        RedactedReq
    ),
    {ok, Req, State}.

ct(<<"text/plain", _/binary>>) -> <<"text/plain">>;
ct(<<"application/json", _/binary>>) -> <<"application/json">>;
ct(_AnyOther) -> <<"text/html">>.

ct_body(<<"text/html">>) ->
    <<"<html><head><title>404 - NOT FOUND</title></head><body><h1>404 - NOT FOUND</h1></body></html>">>;
ct_body(<<"text/plain">>) ->
    <<"404 - NOT FOUND">>;
ct_body(<<"application/json">>) ->
    <<"{\"code\": \"NOT_FOUND\", \"message\": \"Request Path Not Found\"}">>.
