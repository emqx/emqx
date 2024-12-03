%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx_authn.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    create_resource/3,
    update_resource/3,
    check_password_from_selected_map/3,
    parse_deep/1,
    parse_str/1,
    parse_sql/2,
    is_superuser/1,
    client_attrs/1,
    bin/1,
    ensure_apps_started/1,
    cleanup_resources/0,
    make_resource_id/1,
    without_password/1,
    to_bool/1
]).

-define(DEFAULT_RESOURCE_OPTS, #{
    start_after_created => false
}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

create_resource(ResourceId, Module, Config) ->
    Result = emqx_resource:create_local(
        ResourceId,
        ?AUTHN_RESOURCE_GROUP,
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

parse_deep(Template) -> emqx_auth_template:parse_deep(Template, ?AUTHN_DEFAULT_ALLOWED_VARS).

parse_str(Template) -> emqx_auth_template:parse_str(Template, ?AUTHN_DEFAULT_ALLOWED_VARS).

parse_sql(Template, ReplaceWith) ->
    emqx_auth_template:parse_sql(Template, ReplaceWith, ?AUTHN_DEFAULT_ALLOWED_VARS).

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

is_superuser(#{<<"is_superuser">> := Value}) ->
    #{is_superuser => to_bool(Value)};
is_superuser(#{}) ->
    #{is_superuser => false}.

client_attrs(#{<<"client_attrs">> := Attrs}) ->
    #{client_attrs => drop_invalid_attr(Attrs)};
client_attrs(_) ->
    #{client_attrs => #{}}.

drop_invalid_attr(Map) when is_map(Map) ->
    maps:from_list(do_drop_invalid_attr(maps:to_list(Map))).

do_drop_invalid_attr([]) ->
    [];
do_drop_invalid_attr([{K, V} | More]) ->
    case emqx_utils:is_restricted_str(K) of
        true ->
            [{iolist_to_binary(K), iolist_to_binary(V)} | do_drop_invalid_attr(More)];
        false ->
            ?SLOG(debug, #{msg => "invalid_client_attr_dropped", attr_name => K}, #{
                tag => "AUTHN"
            }),
            do_drop_invalid_attr(More)
    end.

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
        emqx_resource:list_group_instances(?AUTHN_RESOURCE_GROUP)
    ).

make_resource_id(Name) ->
    NameBin = bin(Name),
    emqx_resource:generate_id(NameBin).

without_password(Credential) ->
    without_password(Credential, [password, <<"password">>]).

to_bool(<<"true">>) ->
    true;
to_bool(true) ->
    true;
to_bool(<<"1">>) ->
    true;
to_bool(I) when is_integer(I) andalso I >= 1 ->
    true;
%% false
to_bool(<<"">>) ->
    false;
to_bool(<<"0">>) ->
    false;
to_bool(0) ->
    false;
to_bool(null) ->
    false;
to_bool(undefined) ->
    false;
to_bool(<<"false">>) ->
    false;
to_bool(false) ->
    false;
to_bool(MaybeBinInt) when is_binary(MaybeBinInt) ->
    try
        binary_to_integer(MaybeBinInt) >= 1
    catch
        error:badarg ->
            false
    end;
%% fallback to default
to_bool(_) ->
    false.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

without_password(Credential, []) ->
    Credential;
without_password(Credential, [Name | Rest]) ->
    case maps:is_key(Name, Credential) of
        true ->
            without_password(Credential#{Name => <<"[password]">>}, Rest);
        false ->
            without_password(Credential, Rest)
    end.
