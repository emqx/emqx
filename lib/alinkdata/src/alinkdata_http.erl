%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 03. 7月 2022 下午11:18
%%%-------------------------------------------------------------------
-module(alinkdata_http).
-author("yqfclid").

%% API
-export([
    start_pool/2,
    stop_pool/1,
    request/4,
    request/5,
    escape/1
]).

-type(pool_opts() :: [pool_opt()]).

-type(pool_opt() :: {max_connections, integer()} | {timeout, integer()}).

%%%===================================================================
%%% API
%%%===================================================================
-spec(start_pool(PoolName :: atom(), PoolOpts :: pool_opts()) -> ok).
start_pool(PoolName, PoolOpts) ->
    case hackney_pool:start_pool(PoolName, PoolOpts) of
        ok ->
            logger:info("[HTTP]Started ~p pool with options ~p", [PoolName, PoolOpts]),
            ok;
        Error ->
            logger:error("[HTTP]Start ~p pool failed: ~p",
                [PoolName, Error])
    end.

-spec(stop_pool(PoolName :: atom()) -> ok).
stop_pool(PoolName) ->
    hackney_pool:stop_pool(PoolName).

request(Method, Url, Headers, Body) ->
    request(Method, Url, Headers, Body, []).


request(Method, Url, Headers, Body, Options) ->
    RetryTimes = proplists:get_value(retry_time, Options, 1),
    do_request(Method, Url, Headers, Body, Options, 1, RetryTimes).


-spec(escape(binary()) -> binary()).
escape(Bin) ->
    list_to_binary(edoc_lib:escape_uri(binary_to_list(Bin))).
%%%===================================================================
%%% Internal functions
%%%===================================================================
do_request(Method, Url, Headers, Body, Options, Cur, Max) when Cur < Max ->
    case do_hackney_request(Method, Url, Headers, Body, Options) of
        {ok, StatusCode, RepHeaders, RtnBody} ->
            case proplists:get_value(right_status_codes, Options) of
                undefined ->
                    {ok, StatusCode, RepHeaders, RtnBody};
                RightCodes ->
                    case lists:member(StatusCode, RightCodes) of
                        true ->
                            {ok, StatusCode, RepHeaders, RtnBody};
                        false ->
                            do_request(Method, Url, Headers, Body, Options, Cur + 1, Max)
                    end
            end;
        {error, _Reason} ->
            do_request(Method, Url, Headers, Body, Options, Cur + 1, Max)
    end;
do_request(Method, Url, Headers, Body, Options, _Cur, _Max) ->
    do_hackney_request(Method, Url, Headers, Body, Options).


do_hackney_request(Method, Url, Headers, Body, Options) ->
    case hackney:request(Method, Url, Headers, Body, Options) of
        {ok, StatusCode, RepHeaders, Ref} ->
            case hackney:body(Ref) of
                {ok, RtnBody} ->
                    {ok, StatusCode, RepHeaders, RtnBody};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.