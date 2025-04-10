%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sequence).

-export([
    create/1,
    nextval/2,
    currval/2,
    reclaim/2,
    delete/1
]).

-export_type([seqid/0]).

-type key() :: term().

-type name() :: atom().

-type seqid() :: non_neg_integer().

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Create a sequence.
-spec create(name()) -> ok.
create(Name) ->
    emqx_utils_ets:new(Name, [public, set, {write_concurrency, true}]).

%% @doc Next value of the sequence.
-spec nextval(name(), key()) -> seqid().
nextval(Name, Key) ->
    ets:update_counter(Name, Key, {2, 1}, {Key, 0}).

%% @doc Current value of the sequence.
-spec currval(name(), key()) -> seqid().
currval(Name, Key) ->
    try
        ets:lookup_element(Name, Key, 2)
    catch
        error:badarg -> 0
    end.

%% @doc Reclaim a sequence id.
-spec reclaim(name(), key()) -> seqid().
reclaim(Name, Key) ->
    try ets:update_counter(Name, Key, {2, -1, 0, 0}) of
        0 ->
            ets:delete_object(Name, {Key, 0}),
            0;
        I ->
            I
    catch
        error:badarg -> 0
    end.

%% @doc Delete the sequence.
-spec delete(name()) -> boolean().
delete(Name) ->
    case ets:info(Name, name) of
        Name -> ets:delete(Name);
        undefined -> false
    end.
