%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_api).

-export([handle/3]).

-include("emqx_username_quota.hrl").

handle(get, [<<"quota">>, <<"usernames">>], Request) ->
    Query = maps:get(query_string, Request, #{}),
    Page = get_page(Query, <<"page">>, ?DEFAULT_PAGE),
    Limit = get_pos_int(Query, <<"limit">>, ?DEFAULT_LIMIT),
    Sort = get_bool(Query, <<"sort">>, false),
    All = emqx_username_quota_state:list_usernames(Sort),
    Total = length(All),
    Data = paginate(All, Page, Limit),
    {ok, 200, #{}, #{
        data => Data,
        meta => #{page => Page, limit => Limit, count => length(Data), total => Total}
    }};
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

get_page(Map, Key, Default) ->
    Value = get_q(Map, Key, Default),
    Int =
        case Value of
            V when is_integer(V) -> V;
            V when is_binary(V) -> to_integer(V, Default);
            V when is_list(V) -> to_integer(iolist_to_binary(V), Default);
            _ -> Default
        end,
    case Int > 0 of
        true -> Int;
        false -> Default
    end.

get_bool(Map, Key, Default) ->
    Value = get_q(Map, Key, Default),
    case Value of
        true -> true;
        false -> false;
        <<"true">> -> true;
        <<"false">> -> false;
        "true" -> true;
        "false" -> false;
        _ -> Default
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

paginate(List, Page, Limit) ->
    Start = (Page - 1) * Limit,
    lists:sublist(lists:nthtail(min(Start, length(List)), List), Limit).
