%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_boot).

-export([is_enabled/1]).

-define(BOOT_MODULES, [broker, listeners]).

-spec is_enabled(all | broker | listeners) -> boolean().
is_enabled(Mod) ->
    (BootMods = boot_modules()) =:= all orelse lists:member(Mod, BootMods).

boot_modules() ->
    application:get_env(emqx, boot_modules, ?BOOT_MODULES).
