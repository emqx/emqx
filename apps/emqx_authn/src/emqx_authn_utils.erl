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

-module(emqx_authn_utils).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx_authn.hrl").

-export([
    create_resource/3,
    update_resource/3,
    check_password_from_selected_map/3,
    parse_deep/1,
    parse_str/1,
    parse_sql/2,
    render_deep/2,
    render_str/2,
    render_sql_params/2,
    is_superuser/1,
    bin/1,
    ensure_apps_started/1,
    cleanup_resources/0,
    make_resource_id/1
]).

-define(AUTHN_PLACEHOLDERS, [
    ?PH_USERNAME,
    ?PH_CLIENTID,
    ?PH_PASSWORD,
    ?PH_PEERHOST,
    ?PH_CERT_SUBJECT,
    ?PH_CERT_CN_NAME
]).

-define(DEFAULT_RESOURCE_OPTS, #{
    auto_retry_interval => 6000,
    start_after_created => false
}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

create_resource(ResourceId, Module, Config) ->
    Result = emqx_resource:create_local(
        ResourceId,
        ?RESOURCE_GROUP,
        Module,
        Config,
        ?DEFAULT_RESOURCE_OPTS
    ),
    start_resource_if_enabled(Result, ResourceId, Config).

update_resource(Module, Config, ResourceId) ->
    Result = emqx_resource:recreate_local(
        ResourceId, Module, Config, ?DEFAULT_RESOURCE_OPTS
    ),
    start_resource_if_enabled(Result, ResourceId, Config).

start_resource_if_enabled({ok, _} = Result, ResourceId, #{enable := true}) ->
    _ = emqx_resource:start(ResourceId),
    Result;
start_resource_if_enabled(Result, _ResourceId, _Config) ->
    Result.

check_password_from_selected_map(_Algorithm, _Selected, undefined) ->
    {error, bad_username_or_password};
check_password_from_selected_map(Algorithm, Selected, Password) ->
    Hash = maps:get(
        <<"password_hash">>,
        Selected,
        maps:get(<<"password">>, Selected, undefined)
    ),
    case Hash of
        undefined ->
            {error, not_authorized};
        _ ->
            Salt = maps:get(<<"salt">>, Selected, <<>>),
            case
                emqx_authn_password_hashing:check_password(
                    Algorithm, Salt, Hash, Password
                )
            of
                true -> ok;
                false -> {error, bad_username_or_password}
            end
    end.

parse_deep(Template) ->
    emqx_placeholder:preproc_tmpl_deep(Template, #{placeholders => ?AUTHN_PLACEHOLDERS}).

parse_str(Template) ->
    emqx_placeholder:preproc_tmpl(Template, #{placeholders => ?AUTHN_PLACEHOLDERS}).

parse_sql(Template, ReplaceWith) ->
    emqx_placeholder:preproc_sql(
        Template,
        #{
            replace_with => ReplaceWith,
            placeholders => ?AUTHN_PLACEHOLDERS
        }
    ).

render_deep(Template, Credential) ->
    emqx_placeholder:proc_tmpl_deep(
        Template,
        Credential,
        #{return => full_binary, var_trans => fun handle_var/2}
    ).

render_str(Template, Credential) ->
    emqx_placeholder:proc_tmpl(
        Template,
        Credential,
        #{return => full_binary, var_trans => fun handle_var/2}
    ).

render_sql_params(ParamList, Credential) ->
    emqx_placeholder:proc_tmpl(
        ParamList,
        Credential,
        #{return => rawlist, var_trans => fun handle_sql_var/2}
    ).

%% true
is_superuser(#{<<"is_superuser">> := <<"true">>}) ->
    #{is_superuser => true};
is_superuser(#{<<"is_superuser">> := true}) ->
    #{is_superuser => true};
is_superuser(#{<<"is_superuser">> := <<"1">>}) ->
    #{is_superuser => true};
is_superuser(#{<<"is_superuser">> := I}) when
    is_integer(I) andalso I >= 1
->
    #{is_superuser => true};
%% false
is_superuser(#{<<"is_superuser">> := <<"">>}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := <<"0">>}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := 0}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := null}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := undefined}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := <<"false">>}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := false}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := MaybeBinInt}) when
    is_binary(MaybeBinInt)
->
    try binary_to_integer(MaybeBinInt) of
        Int when Int >= 1 ->
            #{is_superuser => true};
        Int when Int =< 0 ->
            #{is_superuser => false}
    catch
        error:badarg ->
            #{is_superuser => false}
    end;
%% fallback to default
is_superuser(#{<<"is_superuser">> := _}) ->
    #{is_superuser => false};
is_superuser(#{}) ->
    #{is_superuser => false}.

ensure_apps_started(bcrypt) ->
    {ok, _} = application:ensure_all_started(bcrypt),
    ok;
ensure_apps_started(_) ->
    ok.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.

cleanup_resources() ->
    lists:foreach(
        fun emqx_resource:remove_local/1,
        emqx_resource:list_group_instances(?RESOURCE_GROUP)
    ).

make_resource_id(Name) ->
    NameBin = bin(Name),
    emqx_resource:generate_id(NameBin).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_var({var, Name}, undefined) ->
    error({cannot_get_variable, Name});
handle_var({var, <<"peerhost">>}, PeerHost) ->
    emqx_placeholder:bin(inet:ntoa(PeerHost));
handle_var(_, Value) ->
    emqx_placeholder:bin(Value).

handle_sql_var({var, Name}, undefined) ->
    error({cannot_get_variable, Name});
handle_sql_var({var, <<"peerhost">>}, PeerHost) ->
    emqx_placeholder:bin(inet:ntoa(PeerHost));
handle_sql_var(_, Value) ->
    emqx_placeholder:sql_data(Value).
