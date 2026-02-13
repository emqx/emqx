%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_api).

-export([handle/3]).

-include("emqx_username_quota.hrl").

handle(get, [<<"quota">>, <<"usernames">>], Request) ->
    Query = maps:get(query_string, Request, #{}),
    Limit = get_pos_int(Query, <<"limit">>, ?DEFAULT_LIMIT),
    Cursor = get_cursor(Query),
    TimeoutMs = emqx_username_quota_config:snapshot_request_timeout_ms(),
    DeadlineMs = now_ms() + TimeoutMs,
    case emqx_username_quota_state:list_usernames(self(), DeadlineMs, Cursor, Limit) of
        {ok, PageResult} ->
            Data = maps:get(data, PageResult, []),
            {ok, 200, #{}, #{
                data => Data,
                meta => build_meta(Limit, Data, PageResult)
            }};
        {error, {busy, RetryCursor}} ->
            {error, 503, #{}, #{
                code => <<"SERVICE_UNAVAILABLE">>,
                message => <<"Snapshot owner is busy handling another request">>,
                retry_cursor => RetryCursor
            }};
        {error, {rebuilding_snapshot, RetryCursor}} ->
            {error, 503, #{}, #{
                code => <<"SERVICE_UNAVAILABLE">>,
                message => <<"Snapshot owner is rebuilding snapshot">>,
                retry_cursor => RetryCursor
            }}
    end;
handle(get, [<<"quota">>, <<"usernames">>, Username0], _Request) ->
    Username = uri_string:percent_decode(Username0),
    case emqx_username_quota_state:get_username(Username) of
        {ok, Info} ->
            {ok, 200, #{}, Info};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}}
    end;
handle(delete, [<<"quota">>, <<"usernames">>, Username0], _Request) ->
    Username = uri_string:percent_decode(Username0),
    case emqx_username_quota_state:kick_username(Username) of
        {ok, N} ->
            {ok, 200, #{}, #{kicked => N}};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}}
    end;
handle(_Method, _Path, _Request) ->
    {error, not_found}.

get_pos_int(Map, Key, Default) ->
    Value = get_q(Map, Key, Default),
    Int =
        case Value of
            V when is_integer(V) -> V;
            V when is_binary(V) -> to_integer(V, Default);
            V when is_list(V) -> to_integer(iolist_to_binary(V), Default);
            _ -> Default
        end,
    case Int > 0 andalso Int =< ?MAX_LIMIT of
        true -> Int;
        false -> Default
    end.

get_q(Map, Key, Default) ->
    case maps:find(Key, Map) of
        {ok, V} -> V;
        error -> Default
    end.

to_integer(Bin, Default) ->
    try binary_to_integer(Bin) of
        Int -> Int
    catch
        _:_ -> Default
    end.

now_ms() ->
    erlang:system_time(millisecond).

build_meta(Limit, Data, PageResult) ->
    Base = #{
        limit => Limit,
        count => length(Data),
        total => maps:get(total, PageResult, 0),
        snapshot => maps:get(snapshot, PageResult, #{})
    },
    case maps:get(next_cursor, PageResult, undefined) of
        undefined -> Base;
        NextCursor -> Base#{next_cursor => NextCursor}
    end.

get_cursor(Query) ->
    case maps:find(<<"cursor">>, Query) of
        {ok, Cursor} when is_binary(Cursor) ->
            Cursor;
        {ok, Cursor} when is_list(Cursor) ->
            iolist_to_binary(Cursor);
        _ ->
            undefined
    end.
