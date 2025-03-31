%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_config_zones).

-behaviour(emqx_config_handler).

%% API
-export([add_handler/0, remove_handler/0, pre_config_update/3]).
-export([is_olp_enabled/0]).
-export([assert_zone_exists/1]).

-define(ZONES, [zones]).

add_handler() ->
    ok = emqx_config_handler:add_handler(?ZONES, ?MODULE),
    ok.

remove_handler() ->
    ok = emqx_config_handler:remove_handler(?ZONES),
    ok.

%% replace the old config with the new config
pre_config_update(?ZONES, NewRaw, _OldRaw) ->
    {ok, NewRaw}.

is_olp_enabled() ->
    maps:fold(
        fun
            (_, #{overload_protection := #{enable := true}}, _Acc) -> true;
            (_, _, Acc) -> Acc
        end,
        false,
        emqx_config:get([zones], #{})
    ).

-spec assert_zone_exists(binary() | atom()) -> ok.
assert_zone_exists(Name0) when is_binary(Name0) ->
    %% an existing zone must have already an atom-name
    Name =
        try
            binary_to_existing_atom(Name0)
        catch
            _:_ ->
                throw({unknown_zone, Name0})
        end,
    assert_zone_exists(Name);
assert_zone_exists(default) ->
    %% there is always a 'default' zone
    ok;
assert_zone_exists(Name) when is_atom(Name) ->
    try
        _ = emqx_config:get([zones, Name]),
        ok
    catch
        error:{config_not_found, _} ->
            throw({unknown_zone, Name})
    end.
