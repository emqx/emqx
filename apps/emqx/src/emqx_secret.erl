%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Note: this module CAN'T be hot-patched to avoid invalidating the
%% closures, so it must not be changed.
-module(emqx_secret).

%% API:
-export([wrap/1, wrap_load/1, unwrap/1, term/1]).

-export_type([t/1]).

-opaque t(T) :: T | fun(() -> t(T)).

%% Secret loader module.
%% Any changes related to processing of secrets should be made there.
-define(LOADER, emqx_secret_loader).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Wrap a term in a secret closure.
%% This effectively hides the term from any term formatting / printing code.
-spec wrap(T) -> t(T).
wrap(Term) ->
    fun() ->
        Term
    end.

%% @doc Wrap a loader function call over a term in a secret closure.
%% This is slightly more flexible form of `wrap/1` with the same basic purpose.
-spec wrap_load(emqx_secret_loader:source()) -> t(_).
wrap_load(Source) ->
    fun() ->
        apply(?LOADER, load, [Source])
    end.

%% @doc Unwrap a secret closure, revealing the secret.
%% This is either `Term` or `Module:Function(Term)` depending on how it was wrapped.
-spec unwrap(t(T)) -> T.
unwrap(Term) when is_function(Term, 0) ->
    %% Handle potentially nested funs
    unwrap(Term());
unwrap(Term) ->
    Term.

%% @doc Inspect the term wrapped in a secret closure.
-spec term(t(_)) -> _Term.
term(Wrap) when is_function(Wrap, 0) ->
    case erlang:fun_info(Wrap, module) of
        {module, ?MODULE} ->
            {env, Env} = erlang:fun_info(Wrap, env),
            lists:last(Env);
        _ ->
            error(badarg, [Wrap])
    end.
