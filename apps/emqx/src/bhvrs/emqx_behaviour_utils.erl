%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_behaviour_utils).

%% API
-export([find_behaviours/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

find_behaviours(Behaviour) ->
    find_behaviours(Behaviour, apps(), []).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% Based on minirest_api:find_api_modules/1
find_behaviours(_Behaviour, [] = _Apps, Acc) ->
    Acc;
find_behaviours(Behaviour, [App | Apps], Acc) ->
    case application:get_key(App, modules) of
        undefined ->
            Acc;
        {ok, Modules} ->
            NewAcc = lists:filter(
                fun(Module) ->
                    Info = Module:module_info(attributes),
                    Bhvrs = lists:flatten(
                        proplists:get_all_values(behavior, Info) ++
                            proplists:get_all_values(behaviour, Info)
                    ),
                    lists:member(Behaviour, Bhvrs)
                end,
                Modules
            ),
            find_behaviours(Behaviour, Apps, NewAcc ++ Acc)
    end.

apps() ->
    [
        App
     || {App, _, _} <- application:loaded_applications(),
        case re:run(atom_to_list(App), "^emqx") of
            {match, [{0, 4}]} -> true;
            _ -> false
        end
    ].
