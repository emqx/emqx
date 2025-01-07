%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc HOCON schema that defines _secret_ concept.
-module(emqx_schema_secret).

-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-export([mk/1]).

%% HOCON Schema API
-export([convert_secret/2, source/1]).

%% @doc Secret value.
-type t() :: binary().

%% @doc Source of the secret value.
%% * "file://...": file path to a file containing secret value.
%% * other binaries: secret value itself.
-type source() :: iodata().

-type secret() :: binary() | function().
-reflect_type([secret/0]).

-define(SCHEMA, #{
    required => false,
    format => <<"password">>,
    sensitive => true,
    converter => fun ?MODULE:convert_secret/2
}).

-dialyzer({nowarn_function, source/1}).

%%

-spec mk(#{atom() => _}) -> hocon_schema:field_schema().
mk(Overrides = #{}) ->
    hoconsc:mk(secret(), maps:merge(?SCHEMA, Overrides)).

convert_secret(undefined, #{}) ->
    undefined;
convert_secret(Secret, #{make_serializable := true}) ->
    unicode:characters_to_binary(source(Secret));
convert_secret(Secret, #{}) when is_function(Secret, 0) ->
    Secret;
convert_secret(Secret, #{}) when is_integer(Secret) ->
    wrap(integer_to_binary(Secret));
convert_secret(Secret, #{}) ->
    try unicode:characters_to_binary(Secret) of
        String when is_binary(String) ->
            wrap(String);
        {error, _, _} ->
            throw(invalid_string)
    catch
        error:_ ->
            throw(invalid_type)
    end.

-spec wrap(source()) -> emqx_secret:t(t()).
wrap(<<"file://", _Filename/binary>> = Secret) ->
    emqx_secret:wrap_load({file, Secret});
wrap(Secret) ->
    emqx_secret:wrap(Secret).

-spec source(emqx_secret:t(t())) -> source().
source(Secret) when is_function(Secret) ->
    source(emqx_secret:term(Secret));
source({file, Filename}) ->
    <<"file://", Filename/binary>>;
source(Secret) ->
    Secret.
