%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_file).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(emqx_authz).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% APIs
-export([
    description/0,
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

description() ->
    "AuthZ with static rules".

create(#{path := Path} = Source) ->
    Rules =
        case file:consult(Path) of
            {ok, Terms} ->
                [emqx_authz_rule:compile(Term) || Term <- Terms];
            {error, Reason} when is_atom(Reason) ->
                ?SLOG(alert, #{
                    msg => failed_to_read_acl_file,
                    path => Path,
                    explain => emqx_misc:explain_posix(Reason)
                }),
                throw(failed_to_read_acl_file);
            {error, Reason} ->
                ?SLOG(alert, #{msg => bad_acl_file_content, path => Path, reason => Reason}),
                throw(bad_acl_file_content)
        end,
    Source#{annotations => #{rules => Rules}}.

update(#{path := _Path} = Source) ->
    create(Source).

destroy(_Source) -> ok.

authorize(Client, PubSub, Topic, #{annotations := #{rules := Rules}}) ->
    emqx_authz_rule:matches(Client, PubSub, Topic, Rules).
