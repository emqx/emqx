%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_extension_hook).

-include("emqx_extension_hook.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[ExHook]").

%% Mgmt APIs
-export([ enable/2
        , disable/1
        , disable_all/0
        , list/0
        ]).

-export([ cast/2
        , call_fold/4
        ]).

%%--------------------------------------------------------------------
%% Mgmt APIs
%%--------------------------------------------------------------------

-spec list() -> [emqx_extension_hook_driver:driver()].
list() ->
    [state(Name) || Name <- running()].

-spec enable(atom(), list()) -> ok | {error, term()}.
enable(Name, Opts) ->
    case lists:member(Name, running()) of
        true ->
            {error, already_started};
        _ ->
            case emqx_extension_hook_driver:load(Name, Opts) of
                {ok, DriverState} ->
                    save(Name, DriverState);
                {error, Reason} ->
                    ?LOG(error, "Load driver ~p failed: ~p", [Name, Reason]),
                    {error, Reason}
            end
    end.

-spec disable(atom()) -> ok | {error, term()}.
disable(Name) ->
    case state(Name) of
        undefined -> {error, not_running};
        Driver ->
            ok = emqx_extension_hook_driver:unload(Driver),
            unsave(Name)
    end.

-spec disable_all() -> [atom()].
disable_all() ->
    [begin disable(Name), Name end || Name <- running()].

%%----------------------------------------------------------
%% Dispatch APIs
%%----------------------------------------------------------

-spec cast(atom(), list()) -> ok.
cast(Name, Args) ->
    cast(Name, Args, running()).

cast(_, _, []) ->
    ok;
cast(Name, Args, [DriverName|More]) ->
    emqx_extension_hook_driver:run_hook(Name, Args, state(DriverName)),
    cast(Name, Args, More).

-spec call_fold(atom(), list(), term(), function()) -> ok | {stop, term()}.
call_fold(Name, InfoArgs, AccArg, Validator) ->
    call_fold(Name, InfoArgs, AccArg, Validator, running()).

call_fold(_, _, _, _, []) ->
    ok;
call_fold(Name, InfoArgs, AccArg, Validator, [NameDriver|More]) ->
    Driver = state(NameDriver),
    case emqx_extension_hook_driver:run_hook_fold(Name, InfoArgs, AccArg, Driver) of
        ok         -> call_fold(Name, InfoArgs, AccArg, Validator, More);
        {error, _} -> call_fold(Name, InfoArgs, AccArg, Validator, More);
        {ok, NAcc} ->
            case Validator(NAcc) of
                true ->
                    {stop, NAcc};
                _ ->
                    ?LOG(error, "Got invalid return type for calling ~p on ~p",
                         [Name, emqx_extension_hook_driver:name(Driver)]),
                    call_fold(Name, InfoArgs, AccArg, Validator, More)
            end
    end.

%%----------------------------------------------------------
%% Storage

-compile({inline, [save/2]}).
save(Name, DriverState) ->
    Saved = persistent_term:get(?APP, []),
    persistent_term:put(?APP, lists:reverse([Name | Saved])),
    persistent_term:put({?APP, Name}, DriverState).

-compile({inline, [unsave/1]}).
unsave(Name) ->
    case persistent_term:get(?APP, []) of
        [] ->
            persistent_term:erase(?APP);
        Saved ->
            persistent_term:put(?APP, lists:delete(Name, Saved))
    end,
    persistent_term:erase({?APP, Name}),
    ok.

-compile({inline, [running/0]}).
running() ->
    persistent_term:get(?APP, []).

-compile({inline, [state/1]}).
state(Name) ->
    case catch persistent_term:get({?APP, Name}) of
        {'EXIT', {badarg,_}} -> undefined;
        State -> State
    end.
