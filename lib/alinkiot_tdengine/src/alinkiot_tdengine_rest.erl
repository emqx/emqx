%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022
%%% @doc
%%%
%%% @end
%%% Created : 19. 9月 2022 下午7:20
%%%-------------------------------------------------------------------
-module(alinkiot_tdengine_rest).
-author("yqfclid").


-export([
    start/0,
    simple_query/5,
    simple_query/6,
    query/2,
    query/3,
    try_http_query/6,
    do_http_query/4
]).

-export([
    build_url/3
]).

-define(DEFAULT_POOL, alinkiot_td_default).


start() ->
    hackney_pool:start_pool(?DEFAULT_POOL, [{max_connections, 100}]).

simple_query(Host, Port, UserName, Passsword, Sql) ->
    simple_query(Host, Port, UserName, Passsword, <<>>, Sql).

simple_query(Host, Port, UserName, Passsword, Database, Sql) ->
    HostB = alinkutil_type:to_binary(Host),
    PortB = alinkutil_type:to_binary(Port),
    UserNameB = alinkutil_type:to_binary(UserName),
    PassswordB = alinkutil_type:to_binary(Passsword),
    DatabaseB = alinkutil_type:to_binary(Database),
    Url = build_url(HostB, PortB, DatabaseB),
    query(Url, alinkutil_type:to_binary(Sql), #{username => UserNameB, password => PassswordB}).


query(Url, Sql) ->
    query(Url, Sql, #{username => <<"root">>, password => <<"taosdata">>}).



query(Url, Sql, #{authorization := Authorization} = QueryOpts) ->
    HttpOpts = [
        {pool, maps:get(pool, QueryOpts, ?DEFAULT_POOL)},
        {connect_timeout, 10000},
        {recv_timeout, 30000},
        {follow_redirectm, true},
        {max_redirect, 5},
        with_body
    ],
    try_http_query(Url, Sql, Authorization, HttpOpts, undefined, maps:get(retry, QueryOpts, 1));
query(Url, Sql, #{username := Username, password := Password} = QueryOpts) ->
    Token = base64:encode(<<Username/binary, ":", Password/binary>>),
    query(Url, Sql, QueryOpts#{authorization => <<"Basic ", Token/binary>>}).


try_http_query(_Url, _Sql, _Authorization, _HttpOpts, Err, 0) ->
    Err;
try_http_query(Url, Sql, Authorization, HttpOpts, _Err, Retry) ->
    case do_http_query(Url, Authorization, Sql, HttpOpts) of
        {ok, Row, Result} ->
            {ok, Row, Result};
        {error, Reason} ->
            try_http_query(Url, Sql, Authorization, HttpOpts, {error, Reason}, Retry - 1)
    end.





do_http_query(Url, Authorization, Sql, HttpOpts) ->
    Headers = [{<<"Authorization">>, Authorization}],
    case hackney:request(post, Url, Headers, Sql, HttpOpts) of
        {ok, StatusCode, _Headers, ResponseBody}
            when StatusCode =:= 200 orelse StatusCode =:= 204 ->
            case jiffy:decode(ResponseBody, [return_maps]) of
                #{<<"code">> := 0} = ResponseMap ->
                    {Row, RespFromat} = format_td_return(ResponseMap),
                    {ok, Row, RespFromat};
                ResponseMap -> {error, ResponseMap}
            end;
        {ok, StatusCode, _Headers, ResponseBody} ->
            {error,
                {StatusCode, catch jiffy:decode(ResponseBody, [return_maps])}};
        {error, Reason} -> {error, Reason}
    end.



format_td_return(#{<<"column_meta">> := ColumnMetas, <<"data">> := Datas, <<"rows">> := Row}) ->
    ColumnNames = lists:map(fun(ColumnMeta) -> hd(ColumnMeta) end, ColumnMetas),
    Result =
        lists:map(
            fun(Data) ->
                maps:from_list(lists:zip(ColumnNames, Data))
        end, Datas),
    {Row, Result};
format_td_return(ResponseMap) ->
    {0, ResponseMap}.


build_url(HostB, PortB, <<>>) ->
    <<"http://", HostB/binary, ":", PortB/binary, "/rest/sql">>;
build_url(HostB, PortB, DatabaseB) ->
    <<"http://", HostB/binary, ":", PortB/binary, "/rest/sql/", DatabaseB/binary>>.



