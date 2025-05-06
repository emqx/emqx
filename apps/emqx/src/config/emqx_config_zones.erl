%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_config_zones).

-export([post_update/2]).

%% API
-export([is_olp_enabled/0]).
-export([assert_zone_exists/1]).

%% NOTE
%% Zone configs are implicitly updated with the global zone configuration
%% when the global zone configuration changes.
%% Therefore, zone config updates cannot be handled by `emqx_config_handler' callbacks.
%%
%% So we introduce a special callback and call it directly.
%%
%% NOTE
%% This callback is called first time with initial values
%% _before_ most of the EMQX applications are started.
post_update(OldZones, NewZones) ->
    ok = emqx_flapping:update_config(),
    ok = emqx_limiter:post_zone_config_update(OldZones, NewZones),
    ok = run_update_hook(OldZones, NewZones).

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

run_update_hook(OldZones, NewZones) ->
    try
        emqx_hooks:run('config.zones_updated', [OldZones, NewZones])
    catch
        error:{invalid_hookpoint, _} ->
            %% hooks are not registered yet
            ok
    end.
