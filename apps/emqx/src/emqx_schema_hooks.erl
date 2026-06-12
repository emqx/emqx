%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_schema_hooks).

-type hookpoint() :: atom().

-callback injected_fields() ->
    #{hookpoint() => [hocon_schema:field() | binary()]}.
-callback injected_fields(term()) ->
    #{hookpoint() => [hocon_schema:field() | binary()]}.
-optional_callbacks([injected_fields/0, injected_fields/1]).

-export_type([hookpoint/0]).

-define(HOOKPOINT_APPEND_PT_KEY(POINT_NAME), {?MODULE, fields, POINT_NAME}).

-export([
    list_injection_point/1,
    list_injection_point/2,
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

-spec list_injection_point(hookpoint()) -> [hocon_schema:field()].
list_injection_point(PointName) ->
    list_injection_point(PointName, []).

-spec list_injection_point(hookpoint(), [hocon_schema:field()]) -> [hocon_schema:field()].
list_injection_point(PointName, Default) ->
    persistent_term:get(?HOOKPOINT_APPEND_PT_KEY(PointName), Default).

-spec erase_injections() -> ok.
erase_injections() ->
    lists:foreach(
        fun
            ({?HOOKPOINT_APPEND_PT_KEY(_) = Key, _}) ->
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
            ({?HOOKPOINT_APPEND_PT_KEY(_), _}) ->
                true;
            (_) ->
                false
        end,
        persistent_term:get()
    ).

-spec inject_from_modules([module() | {module(), term()}]) -> ok.
inject_from_modules(Modules) ->
    ListInjections =
        lists:foldl(
            fun append_module_list_injections/2,
            #{},
            Modules
        ),
    ok = inject_list_fields(maps:to_list(ListInjections)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

append_module_list_injections(Module, AllInjections) when is_atom(Module) ->
    Injections = call_if_defined(Module, injected_fields, [], #{}),
    append_module_list_injections(Injections, AllInjections);
append_module_list_injections({Module, Options}, AllInjections) when is_atom(Module) ->
    Injections = call_if_defined(Module, injected_fields, [Options], #{}),
    append_module_list_injections(Injections, AllInjections);
append_module_list_injections(ModuleInjections, AllInjections) when is_map(ModuleInjections) ->
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

inject_list_fields([]) ->
    ok;
inject_list_fields([{PointName, Fields} | Rest]) ->
    case any_list_injections(PointName) of
        true ->
            inject_list_fields(Rest);
        false ->
            ok = inject_list_fields(PointName, Fields),
            inject_list_fields(Rest)
    end.

inject_list_fields(PointName, Fields) ->
    Key = ?HOOKPOINT_APPEND_PT_KEY(PointName),
    persistent_term:put(Key, Fields).

any_list_injections(PointName) ->
    persistent_term:get(?HOOKPOINT_APPEND_PT_KEY(PointName), undefined) =/= undefined.

call_if_defined(Module, Function, Args, Default) ->
    %% Ensure module is loaded, especially when called from nodetool
    ok = emqx_utils:interactive_load(Module),
    case erlang:function_exported(Module, Function, length(Args)) of
        true ->
            apply(Module, Function, Args);
        false ->
            Default
    end.
