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
    case emqx_username_quota_state:get_username(Username0) of
        {ok, Info} ->
            {ok, 200, #{}, Info};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}}
    end;
handle(post, [<<"kick">>, Username0], _Request) ->
    case emqx_username_quota_state:kick_username(Username0) of
        {ok, N} ->
            {ok, 200, #{}, #{kicked => N}};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Not Found">>}}
    end;
handle(post, [<<"quota">>, <<"overrides">>], Request) ->
    Body = maps:get(body, Request, []),
    case validate_override_list(Body) of
        ok ->
            {ok, N} = emqx_username_quota_state:set_overrides(Body),
            {ok, 200, #{}, #{set => N}};
        {error, Reason} ->
            {error, 400, #{}, #{code => <<"BAD_REQUEST">>, message => Reason}}
    end;
handle(delete, [<<"quota">>, <<"overrides">>], Request) ->
    Body = maps:get(body, Request, []),
    case validate_username_list(Body) of
        ok ->
            {ok, N} = emqx_username_quota_state:delete_overrides(Body),
            {ok, 200, #{}, #{deleted => N}};
        {error, Reason} ->
            {error, 400, #{}, #{code => <<"BAD_REQUEST">>, message => Reason}}
    end;
handle(get, [<<"quota">>, <<"overrides">>], _Request) ->
    Data = emqx_username_quota_state:list_overrides(),
    {ok, 200, #{}, #{data => Data}};
handle(_Method, _Path, _Request) ->
    {error, not_found}.

get_pos_int(Map, Key, Default) ->
    Value = maps:get(Key, Map, Default),
    Int = to_integer(Value, Default),
    case Int > 0 andalso Int =< ?MAX_LIMIT of
        true -> Int;
        false -> Default
    end.

to_integer(Int, _Default) when is_integer(Int) ->
    Int;
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
        error ->
            undefined
    end.

validate_override_list(List) when is_list(List) ->
    case lists:all(fun is_valid_override_entry/1, List) of
        true -> ok;
        false -> {error, <<"Each entry must have non-empty 'username' and valid 'quota'">>}
    end;
validate_override_list(_) ->
    {error, <<"Expected a JSON array">>}.

is_valid_override_entry(#{<<"username">> := U, <<"quota">> := <<"nolimit">>}) when
    is_binary(U), U =/= <<>>
->
    true;
is_valid_override_entry(#{<<"username">> := U, <<"quota">> := Q}) when
    is_binary(U), U =/= <<>>, is_integer(Q), Q >= 0
->
    true;
is_valid_override_entry(_) ->
    false.

validate_username_list(List) when is_list(List) ->
    case lists:all(fun(U) -> is_binary(U) andalso U =/= <<>> end, List) of
        true -> ok;
        false -> {error, <<"Expected a list of non-empty username strings">>}
    end;
validate_username_list(_) ->
    {error, <<"Expected a JSON array">>}.
