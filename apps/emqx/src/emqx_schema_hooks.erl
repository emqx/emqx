%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-optional_callbacks([injected_fields/0, injected_fields/1]).

-export_type([hookpoint/0]).

-define(HOOKPOINT_PT_KEY(POINT_NAME), {?MODULE, fields, POINT_NAME}).

-export([
    injection_point/1,
    injection_point/2,
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

-spec erase_injections() -> ok.
erase_injections() ->
    lists:foreach(
        fun
            ({?HOOKPOINT_PT_KEY(_) = Key, _}) ->
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
            (_) ->
                false
        end,
        persistent_term:get()
    ).

-spec inject_from_modules([module() | {module(), term()}]) -> ok.
inject_from_modules(Modules) ->
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
    append_module_injections(Module:injected_fields(), AllInjections);
append_module_injections({Module, Options}, AllInjections) when is_atom(Module) ->
    append_module_injections(Module:injected_fields(Options), AllInjections);
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
    case any_injections(PointName) of
        true ->
            inject_fields(Rest);
        false ->
            ok = inject_fields(PointName, Fields),
            inject_fields(Rest)
    end.

inject_fields(PointName, Fields) ->
    Key = ?HOOKPOINT_PT_KEY(PointName),
    persistent_term:put(Key, Fields).

any_injections(PointName) ->
    persistent_term:get(?HOOKPOINT_PT_KEY(PointName), undefined) =/= undefined.
