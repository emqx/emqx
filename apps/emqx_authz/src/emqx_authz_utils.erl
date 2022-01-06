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

-module(emqx_authz_utils).

-include_lib("emqx/include/emqx_placeholder.hrl").

-export([ cleanup_resources/0
        , make_resource_id/1
        , create_resource/2
        , update_config/2
        ]).

-define(RESOURCE_GROUP, <<"emqx_authz">>).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create_resource(Module, Config) ->
    ResourceID = make_resource_id(Module),
    case emqx_resource:create_local(ResourceID, Module, Config) of
        {ok, already_created} -> {ok, ResourceID};
        {ok, _} -> {ok, ResourceID};
        {error, Reason} -> {error, Reason}
    end.

cleanup_resources() ->
    lists:foreach(
      fun emqx_resource:remove_local/1,
      emqx_resource:list_group_instances(?RESOURCE_GROUP)).

make_resource_id(Name) ->
    NameBin = bin(Name),
    emqx_resource:generate_id(?RESOURCE_GROUP, NameBin).

update_config(Path, ConfigRequest) ->
    emqx_conf:update(Path, ConfigRequest, #{rawconf_with_defaults => true,
                                            override_to => cluster}).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
