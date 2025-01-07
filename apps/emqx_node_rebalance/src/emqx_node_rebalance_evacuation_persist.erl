%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_evacuation_persist).

-export([
    save/1,
    clear/0,
    read/1
]).

-ifdef(TEST).
-export([evacuation_filepath/0]).
-endif.

-include("emqx_node_rebalance.hrl").
-include_lib("emqx/include/types.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-type start_opts() :: #{
    server_reference => emqx_eviction_agent:server_reference(),
    conn_evict_rate => pos_integer(),
    sess_evict_rate => pos_integer(),
    wait_takeover => number(),
    migrate_to => emqx_node_rebalance_evacuation:migrate_to(),
    wait_health_check => number()
}.

-spec save(start_opts()) -> ok_or_error(term()).
save(
    #{
        server_reference := ServerReference,
        conn_evict_rate := ConnEvictRate,
        sess_evict_rate := SessEvictRate,
        wait_takeover := WaitTakeover
    } = Data
) when
    (is_binary(ServerReference) orelse ServerReference =:= undefined) andalso
        is_integer(ConnEvictRate) andalso ConnEvictRate > 0 andalso
        is_integer(SessEvictRate) andalso SessEvictRate > 0 andalso
        is_number(WaitTakeover) andalso WaitTakeover >= 0
->
    Filepath = evacuation_filepath(),
    case filelib:ensure_dir(Filepath) of
        ok ->
            JsonData = emqx_utils_json:encode(
                prepare_for_encode(maps:with(persist_keys(), Data)),
                [pretty]
            ),
            file:write_file(Filepath, JsonData);
        {error, _} = Error ->
            Error
    end.

-spec clear() -> ok.
clear() ->
    file:delete(evacuation_filepath()).

-spec read(start_opts()) -> {ok, start_opts()} | none.
read(DefaultOpts) ->
    case file:read_file(evacuation_filepath()) of
        {ok, Data} ->
            case emqx_utils_json:safe_decode(Data, [return_maps]) of
                {ok, Map} when is_map(Map) ->
                    {ok, map_to_opts(DefaultOpts, Map)};
                _NotAMap ->
                    {ok, DefaultOpts}
            end;
        {error, _} ->
            none
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

persist_keys() ->
    [
        server_reference,
        conn_evict_rate,
        sess_evict_rate,
        wait_takeover
    ].

prepare_for_encode(#{server_reference := undefined} = Data) ->
    Data#{server_reference => null};
prepare_for_encode(Data) ->
    Data.

format_after_decode(#{server_reference := null} = Data) ->
    Data#{server_reference => undefined};
format_after_decode(Data) ->
    Data.

map_to_opts(DefaultOpts, Map) ->
    format_after_decode(
        map_to_opts(
            maps:to_list(DefaultOpts), Map, #{}
        )
    ).

map_to_opts([], _Map, Opts) ->
    Opts;
map_to_opts([{Key, DefaultVal} | Rest], Map, Opts) ->
    map_to_opts(Rest, Map, Opts#{Key => maps:get(atom_to_binary(Key), Map, DefaultVal)}).

evacuation_filepath() ->
    filename:join([emqx:data_dir(), ?EVACUATION_FILENAME]).
