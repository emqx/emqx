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
-include_lib("emqx_authz.hrl").

-export([
    cleanup_resources/0,
    make_resource_id/1,
    create_resource/2,
    update_config/2,
    parse_deep/2,
    parse_sql/3,
    render_deep/2,
    render_sql_params/2
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create_resource(Module, Config) ->
    ResourceID = make_resource_id(Module),
    case
        emqx_resource:create_local(
            ResourceID,
            ?RESOURCE_GROUP,
            Module,
            Config,
            #{}
        )
    of
        {ok, already_created} -> {ok, ResourceID};
        {ok, _} -> {ok, ResourceID};
        {error, Reason} -> {error, Reason}
    end.

cleanup_resources() ->
    lists:foreach(
        fun emqx_resource:remove_local/1,
        emqx_resource:list_group_instances(?RESOURCE_GROUP)
    ).

make_resource_id(Name) ->
    NameBin = bin(Name),
    emqx_resource:generate_id(NameBin).

update_config(Path, ConfigRequest) ->
    emqx_conf:update(Path, ConfigRequest, #{
        rawconf_with_defaults => true,
        override_to => cluster
    }).

parse_deep(Template, PlaceHolders) ->
    emqx_placeholder:preproc_tmpl_deep(Template, #{placeholders => PlaceHolders}).

parse_sql(Template, ReplaceWith, PlaceHolders) ->
    emqx_placeholder:preproc_sql(
        Template,
        #{
            replace_with => ReplaceWith,
            placeholders => PlaceHolders
        }
    ).

render_deep(Template, Values) ->
    emqx_placeholder:proc_tmpl_deep(
        Template,
        client_vars(Values),
        #{return => full_binary, var_trans => fun handle_var/2}
    ).

render_sql_params(ParamList, Values) ->
    emqx_placeholder:proc_tmpl(
        ParamList,
        client_vars(Values),
        #{return => rawlist, var_trans => fun handle_sql_var/2}
    ).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

client_vars(ClientInfo) ->
    maps:from_list(
        lists:map(
            fun convert_client_var/1,
            maps:to_list(ClientInfo)
        )
    ).

convert_client_var({cn, CN}) -> {cert_common_name, CN};
convert_client_var({dn, DN}) -> {cert_subject, DN};
convert_client_var({protocol, Proto}) -> {proto_name, Proto};
convert_client_var(Other) -> Other.

handle_var({var, _Name}, undefined) ->
    "undefined";
handle_var({var, <<"peerhost">>}, IpAddr) ->
    inet_parse:ntoa(IpAddr);
handle_var(_Name, Value) ->
    emqx_placeholder:bin(Value).

handle_sql_var({var, _Name}, undefined) ->
    "undefined";
handle_sql_var({var, <<"peerhost">>}, IpAddr) ->
    inet_parse:ntoa(IpAddr);
handle_sql_var(_Name, Value) ->
    emqx_placeholder:sql_data(Value).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
