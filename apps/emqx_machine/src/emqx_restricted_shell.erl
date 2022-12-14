%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_restricted_shell).

-export([local_allowed/3, non_local_allowed/3]).
-export([set_prompt_func/0, prompt_func/1]).
-export([lock/0, unlock/0, is_locked/0]).

-include_lib("emqx/include/logger.hrl").

-define(APP, 'emqx_machine').
-define(IS_LOCKED, 'restricted.is_locked').
-define(MAX_HEAP_SIZE, 1024 * 1024 * 1).
-define(MAX_ARGS_SIZE, 1024 * 10).

-define(RED_BG, "\e[48;2;184;0;0m").
-define(RESET, "\e[0m").

-define(LOCAL_PROHIBITED, [halt, q]).
-define(REMOTE_PROHIBITED, [{erlang, halt}, {c, q}, {init, stop}, {init, restart}, {init, reboot}]).

is_locked() ->
    {ok, false} =/= application:get_env(?APP, ?IS_LOCKED).

lock() -> application:set_env(?APP, ?IS_LOCKED, true).
unlock() -> application:set_env(?APP, ?IS_LOCKED, false).

set_prompt_func() ->
    shell:prompt_func({?MODULE, prompt_func}).

prompt_func(PropList) ->
    Line = proplists:get_value(history, PropList, 1),
    Version = emqx_release:version(),
    Prefix =
        case emqx_release:edition() of
            ce -> "v";
            ee -> "e"
        end,
    case is_alive() of
        true -> io_lib:format(<<"~ts~ts(~s)~w> ">>, [Prefix, Version, node(), Line]);
        false -> io_lib:format(<<"~ts~ts ~w> ">>, [Prefix, Version, Line])
    end.

local_allowed(MF, Args, State) ->
    Allowed = check_allowed(MF, ?LOCAL_PROHIBITED),
    log(Allowed, MF, Args),
    {is_allowed(Allowed), State}.

non_local_allowed(MF, Args, State) ->
    Allow = check_allowed(MF, ?REMOTE_PROHIBITED),
    log(Allow, MF, Args),
    {is_allowed(Allow), State}.

check_allowed(MF, NotAllowed) ->
    case {lists:member(MF, NotAllowed), is_locked()} of
        {true, false} -> exempted;
        {true, true} -> prohibited;
        {false, _} -> ignore
    end.

is_allowed(prohibited) -> false;
is_allowed(_) -> true.

limit_warning(MF, Args) ->
    max_heap_size_warning(MF, Args),
    max_args_warning(MF, Args).

max_args_warning(MF, Args) ->
    ArgsSize = erts_debug:flat_size(Args),
    case ArgsSize < ?MAX_ARGS_SIZE of
        true ->
            ok;
        false ->
            warning("[WARNING] current_args_size:~w, max_args_size:~w", [ArgsSize, ?MAX_ARGS_SIZE]),
            ?SLOG(warning, #{
                msg => "execute_function_in_shell_max_args_size",
                function => MF,
                %%args => Args,
                args_size => ArgsSize,
                max_heap_size => ?MAX_ARGS_SIZE
            })
    end.

max_heap_size_warning(MF, Args) ->
    {heap_size, HeapSize} = erlang:process_info(self(), heap_size),
    case HeapSize < ?MAX_HEAP_SIZE of
        true ->
            ok;
        false ->
            warning("[WARNING] current_heap_size:~w, max_heap_size_warning:~w", [
                HeapSize, ?MAX_HEAP_SIZE
            ]),
            ?SLOG(warning, #{
                msg => "shell_process_exceed_max_heap_size",
                current_heap_size => HeapSize,
                function => MF,
                args => Args,
                max_heap_size => ?MAX_HEAP_SIZE
            })
    end.

log(prohibited, MF, Args) ->
    warning("DANGEROUS FUNCTION: FORBIDDEN IN SHELL!!!!!", []),
    ?SLOG(error, #{msg => "execute_function_in_shell_prohibited", function => MF, args => Args});
log(exempted, MF, Args) ->
    limit_warning(MF, Args),
    ?SLOG(error, #{
        msg => "execute_dangerous_function_in_shell_exempted", function => MF, args => Args
    });
log(ignore, MF, Args) ->
    limit_warning(MF, Args).

warning(Format, Args) ->
    io:format(?RED_BG ++ Format ++ ?RESET ++ "~n", Args).
