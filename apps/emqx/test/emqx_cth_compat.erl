%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Facilities to test backward / forward compatibility in EMQX clusters
-module(emqx_cth_compat).

-include_lib("typerefl/include/types.hrl").

-export([
    profiles/0,
    profile/1
]).

-export_type([profiles/0]).

%% @doc Compatibility testing profile.
%% It is essentially a limited subset of `emqx_cth_cluster:nodespec()` options.
%% The most common use case is to specify `erl_libs_path` to point to the directory
%% where the code of the former version of EMQX is located, e.g.:
%% ```
%% #{
%%     description => "EMQX v5.3.0",
%%     erl_libs_path => "/workspace/_compat/emqx-5.3.0/lib"
%% }
%% ```
-type profile() :: #{
    description => string(),
    erl_libs_path => string()
}.

-type name() :: atom().
-type profiles() :: [{name(), profile()}].

-reflect_type([profile/0]).

-define(ENV_COMPAT_PROFILES_FILENAME, "EMQX_CI_CT_COMPAT_PROFILES_FILENAME").

%%

%% @doc Get the list of defined compatibility profiles.
%% The list is read from the file specified by the `EMQX_CI_CT_COMPAT_PROFILES_FILENAME`
%% environment variable. If the variable is not set, an empty list is returned.
-spec profiles() -> [profile()].
profiles() ->
    profiles(file).

profiles(_Source = file) ->
    case os:getenv(?ENV_COMPAT_PROFILES_FILENAME) of
        Filename when length(Filename) > 0 ->
            case file:consult(Filename) of
                {ok, Profiles} ->
                    validate_profiles(Profiles);
                {error, Reason} ->
                    ct:pal(
                        error,
                        "Compat profiles cannot be read from ~s: ~p",
                        [Filename, Reason]
                    ),
                    error({file, Reason})
            end;
        "" ->
            [];
        false ->
            []
    end.

validate_profiles(Profiles) ->
    case typerefl:typecheck(list({atom(), profile()}), Profiles) of
        ok ->
            Profiles;
        {error, Reason} ->
            ct:pal(
                error,
                "Compat profiles are invalid: ~p, must follow `~p:profiles()` type",
                [Reason, ?MODULE]
            ),
            error({invalid, Reason})
    end.

-spec profile(name()) -> profile() | undefined.
profile(Name) ->
    proplists:get_value(Name, profiles(), undefined).
