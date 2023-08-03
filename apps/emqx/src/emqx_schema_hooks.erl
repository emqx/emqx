%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-optional_callbacks([injected_fields/0]).

-define(HOOKPOINT_PT_KEY(POINT_NAME), {?MODULE, fields, POINT_NAME}).
-define(MODULE_PT_KEY(MOD_NAME), {?MODULE, mod, MOD_NAME}).

-export([
    inject_fields/3,
    injection_point/1,

    inject_fields_from_mod/1
]).

%% for tests
-export([
    erase_injections/0,
    any_injections/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

injection_point(PointName) ->
    InjectedFields = persistent_term:get(?HOOKPOINT_PT_KEY(PointName), #{}),
    lists:concat(maps:values(InjectedFields)).

inject_fields(PointName, Name, Fields) ->
    Key = ?HOOKPOINT_PT_KEY(PointName),
    InjectedFields = persistent_term:get(Key, #{}),
    persistent_term:put(Key, InjectedFields#{Name => Fields}).

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

inject_fields_from_mod(Module) ->
    case persistent_term:get(?MODULE_PT_KEY(Module), false) of
        false ->
            persistent_term:put(?MODULE_PT_KEY(Module), true),
            do_inject_fields_from_mod(Module);
        true ->
            ok
    end.

erase_injections() ->
    lists:foreach(
        fun
            ({?HOOKPOINT_PT_KEY(_) = Key, _}) ->
                persistent_term:erase(Key);
            ({?MODULE_PT_KEY(_) = Key, _}) ->
                persistent_term:erase(Key);
            (_) ->
                ok
        end,
        persistent_term:get()
    ).

any_injections() ->
    lists:any(
        fun
            ({?HOOKPOINT_PT_KEY(_), _}) ->
                true;
            ({?MODULE_PT_KEY(_), _}) ->
                true;
            (_) ->
                false
        end,
        persistent_term:get()
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_inject_fields_from_mod(Module) ->
    _ = Module:module_info(),
    case erlang:function_exported(Module, injected_fields, 0) of
        true ->
            do_inject_fields_from_mod(Module, Module:injected_fields());
        false ->
            ok
    end.

do_inject_fields_from_mod(Module, HookFields) ->
    maps:foreach(
        fun(PointName, Fields) ->
            inject_fields(PointName, Module, Fields)
        end,
        HookFields
    ).
