%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Pipeline context operations — JSONPath-style read/write/interpolation
%% on plain binary-keyed maps.
%%
%% Path DSL
%%   "$.foo"         — top-level key
%%   "$.foo.bar.baz" — nested traversal
%%
%% Read  (resolve/2):     deep traversal, returns `null` for missing paths
%% Write (write/3):       deep nested write, creates intermediate maps
%% Delete (delete/2):     deep nested delete, no-op on missing paths
%% Resolve map (resolve_map/2): recursively substitute "$."-prefixed values

-module(emqx_agent_pipeline_ctx).

-export([
    resolve/2,
    write/3,
    delete/2,
    resolve_map/2,
    init/2,
    deep_get/2,
    deep_put/3,
    deep_delete/2
]).

-spec init(map(), binary()) -> map().
init(Event, Key) ->
    KeyBase62 = emqx_base62:encode(Key),
    #{
        <<"event">> => Event,
        <<"key">> => Key,
        <<"key_base62">> => KeyBase62
    }.

-spec resolve(binary(), map()) -> term().
resolve(<<"$.", Path/binary>>, Context) ->
    Parts = binary:split(Path, <<".">>, [global]),
    deep_get(Parts, Context);
resolve(Value, _Context) ->
    Value.

-spec write(undefined | binary(), term(), map()) -> map().
write(undefined, _Value, Context) ->
    Context;
write(<<"$.", Key/binary>>, Value, Context) ->
    Parts = binary:split(Key, <<".">>, [global]),
    deep_put(Parts, Value, Context);
write(_Other, _Value, Context) ->
    Context.

-spec delete(binary(), map()) -> map().
delete(<<"$.", Key/binary>>, Context) ->
    Parts = binary:split(Key, <<".">>, [global]),
    deep_delete(Parts, Context);
delete(_Other, Context) ->
    Context.

-spec resolve_map(term(), map()) -> term().
resolve_map(Map, Context) when is_map(Map) ->
    maps:map(
        fun
            (_K, V) when is_binary(V) -> resolve_top_value(V, Context);
            (_K, V) when is_map(V) -> resolve_nested_map(V, Context);
            (_K, V) -> V
        end,
        Map
    );
resolve_map(Other, _Context) ->
    Other.

resolve_top_value(Value, Context) ->
    case decode_json_map(Value) of
        {ok, Map} -> resolve_nested_map(Map, Context);
        error -> resolve(Value, Context)
    end.

resolve_nested_map(Map, Context) when is_map(Map) ->
    maps:map(
        fun
            (_K, V) when is_binary(V) -> resolve(V, Context);
            (_K, V) when is_map(V) -> resolve_nested_map(V, Context);
            (_K, V) -> V
        end,
        Map
    ).

decode_json_map(Bin) ->
    try emqx_utils_json:decode(Bin) of
        Map when is_map(Map) -> {ok, Map};
        _ -> error
    catch
        _:_ -> error
    end.

-spec deep_get([binary()], map()) -> term().
deep_get([], Value) ->
    Value;
deep_get([Key | Rest], Map) when is_map(Map) ->
    case maps:get(Key, Map, undefined) of
        undefined -> null;
        V -> deep_get(Rest, V)
    end;
deep_get(_, _) ->
    null.

-spec deep_put([binary()], term(), map()) -> map().
deep_put([Key], Value, Map) ->
    maps:put(Key, Value, Map);
deep_put([Key | Rest], Value, Map) ->
    SubMap = maps:get(Key, Map, #{}),
    maps:put(Key, deep_put(Rest, Value, SubMap), Map).

-spec deep_delete([binary()], map()) -> map().
deep_delete([Key], Map) ->
    maps:remove(Key, Map);
deep_delete([Key | Rest], Map) ->
    case maps:get(Key, Map, undefined) of
        SubMap when is_map(SubMap) ->
            maps:put(Key, deep_delete(Rest, SubMap), Map);
        _ ->
            Map
    end.
