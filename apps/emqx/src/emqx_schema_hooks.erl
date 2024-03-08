%%--------------------------------------------------------------------
%% Copyright (c) 2017-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_schema_hooks).

-type hookpoint() :: atom().

-callback injected_fields() ->
    #{
        hookpoint() => [hocon_schema:field()]
    }.
-callback injected_fields(term()) ->
    #{
        hookpoint() => [hocon_schema:field()]
    }.
-callback union_option_injections() ->
    #{
        hookpoint() => [hocon_schema:type()]
    }.
-optional_callbacks([
    injected_fields/0,
    injected_fields/1,
    union_option_injections/0
]).

-export_type([hookpoint/0]).

-define(HOOKPOINT_PT_KEY(POINT_NAME), {?MODULE, fields, POINT_NAME}).
-define(UNION_OPTION_PT_KEY(POINT_NAME), {?MODULE, union_options, POINT_NAME}).

-export([
    injection_point/1,
    injection_point/2,
    union_option_injections/1,
    union_option_injections/2,
    inject_from_modules/1
]).

%% for tests
-export([
    erase_injections/0,
    any_injections/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec injection_point(hookpoint()) -> [hocon_schema:field()].
injection_point(PointName) ->
    injection_point(PointName, []).

-spec injection_point(hookpoint(), [hocon_schema:field()]) -> [hocon_schema:field()].
injection_point(PointName, Default) ->
    persistent_term:get(?HOOKPOINT_PT_KEY(PointName), Default).

-spec union_option_injections(hookpoint()) -> [hocon_schema:field()].
union_option_injections(PointName) ->
    union_option_injections(PointName, []).

-spec union_option_injections(hookpoint(), [hocon_schema:type()]) -> [hocon_schema:type()].
union_option_injections(PointName, Default) ->
    persistent_term:get(?UNION_OPTION_PT_KEY(PointName), Default).

-spec erase_injections() -> ok.
erase_injections() ->
    lists:foreach(
        fun
            ({?HOOKPOINT_PT_KEY(_) = Key, _}) ->
                persistent_term:erase(Key);
            ({?UNION_OPTION_PT_KEY(_) = Key, _}) ->
                persistent_term:erase(Key);
            (_) ->
                ok
        end,
        persistent_term:get()
    ).

-spec any_injections() -> boolean().
any_injections() ->
    lists:any(
        fun
            ({?HOOKPOINT_PT_KEY(_), _}) ->
                true;
            ({?UNION_OPTION_PT_KEY(_), _}) ->
                true;
            (_) ->
                false
        end,
        persistent_term:get()
    ).

-spec inject_from_modules([module() | {module(), term()}]) -> ok.
inject_from_modules(Modules) ->
    ok = register_union_options(Modules),
    Injections =
        lists:foldl(
            fun append_module_injections/2,
            #{},
            Modules
        ),
    ok = inject_fields(maps:to_list(Injections)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

append_module_injections(Module, AllInjections) when is_atom(Module) ->
    ModuleInjections = call_if_implemented(
        Module,
        injected_fields,
        [],
        #{}
    ),
    append_module_injections(ModuleInjections, AllInjections);
append_module_injections({Module, Options}, AllInjections) when is_atom(Module) ->
    ModuleInjections = call_if_implemented(
        Module,
        injected_fields,
        [Options],
        #{}
    ),
    append_module_injections(ModuleInjections, AllInjections);
append_module_injections(ModuleInjections, AllInjections) when is_map(ModuleInjections) ->
    maps:fold(
        fun(PointName, Fields, Acc) ->
            maps:update_with(
                PointName,
                fun(Fields0) ->
                    Fields0 ++ Fields
                end,
                Fields,
                Acc
            )
        end,
        AllInjections,
        ModuleInjections
    ).

inject_fields([]) ->
    ok;
inject_fields([{PointName, Fields} | Rest]) ->
    case any_field_injections(PointName) of
        true ->
            inject_fields(Rest);
        false ->
            ok = inject_fields(PointName, Fields),
            inject_fields(Rest)
    end.

inject_fields(PointName, Fields) ->
    Key = ?HOOKPOINT_PT_KEY(PointName),
    persistent_term:put(Key, Fields).

any_field_injections(PointName) ->
    persistent_term:get(?HOOKPOINT_PT_KEY(PointName), undefined) =/= undefined.

register_union_options(Modules) ->
    AllInjections =
        lists:foldl(
            fun append_union_option_injections/2,
            #{},
            Modules
        ),
    ok = inject_union_options(maps:to_list(AllInjections)).

inject_union_options([]) ->
    ok;
inject_union_options([{PointName, Types} | Rest]) ->
    case any_union_option_injections(PointName) of
        true ->
            inject_union_options(Rest);
        false ->
            ok = inject_union_options(PointName, Types),
            inject_union_options(Rest)
    end.

inject_union_options(PointName, Types) ->
    Key = ?UNION_OPTION_PT_KEY(PointName),
    persistent_term:put(Key, Types).

append_union_option_injections(Module, AllInjections) when is_atom(Module) ->
    ModuleInjections = call_if_implemented(
        Module,
        union_option_injections,
        [],
        #{}
    ),
    append_union_option_injections(ModuleInjections, AllInjections);
append_union_option_injections({Module, _Args}, AllInjections) when is_atom(Module) ->
    AllInjections;
append_union_option_injections(ModuleInjections, AllInjections) when is_map(ModuleInjections) ->
    maps:fold(
        fun(PointName, Types, Acc) ->
            maps:update_with(
                PointName,
                fun(Types0) ->
                    Types0 ++ Types
                end,
                Types,
                Acc
            )
        end,
        AllInjections,
        ModuleInjections
    ).

any_union_option_injections(PointName) ->
    persistent_term:get(?UNION_OPTION_PT_KEY(PointName), undefined) =/= undefined.

call_if_implemented(Mod, Fun, Args, Default) ->
    %% ensure modeule is loaded
    _ = Mod:module_info(),
    case erlang:function_exported(Mod, Fun, length(Args)) of
        true ->
            apply(Mod, Fun, Args);
        false ->
            Default
    end.
