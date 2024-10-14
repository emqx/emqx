%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022
%%% @doc
%%%
%%% @end
%%% Created : 25. 6月 2022 下午2:02
%%%-------------------------------------------------------------------
-module(alinkutil_tables).
-author("yqfclid").

%% API
-export([
    new/1,
    new/2,
    lookup_value/2,
    lookup_value/3,
    delete/1
]).

%%%===================================================================
%%% API
%%%===================================================================
%% Create an ets table.
-spec(new(atom()) -> ok).
new(Tab) ->
    new(Tab, []).

%% Create a named_table ets.
-spec(new(atom(), list()) -> ok).
new(Tab, Opts) ->
    case ets:info(Tab, name) of
        undefined ->
            _ = ets:new(Tab, lists:usort([named_table | Opts])),
            ok;
        Tab -> ok
    end.

%% KV lookup
-spec(lookup_value(ets:tab(), term()) -> any()).
lookup_value(Tab, Key) ->
    lookup_value(Tab, Key, undefined).

-spec(lookup_value(ets:tab(), term(), any()) -> any()).
lookup_value(Tab, Key, Def) ->
    try ets:lookup_element(Tab, Key, 2)
    catch
        error:badarg -> Def
    end.

%% Delete the ets table.
-spec(delete(ets:tab()) -> ok).
delete(Tab) ->
    case ets:info(Tab, name) of
        undefined -> ok;
        Tab ->
            ets:delete(Tab),
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================