%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    create_resource/3,
    update_resource/3,
    check_password_from_selected_map/3,
    parse_deep/1,
    parse_str/1,
    parse_sql/2,
    render_deep/2,
    render_str/2,
    render_urlencoded_str/2,
    render_sql_params/2,
    is_superuser/1,
    bin/1,
    ensure_apps_started/1,
    cleanup_resources/0,
    make_resource_id/1,
    without_password/1,
    to_bool/1,
    parse_url/1,
    convert_headers/1,
    convert_headers_no_content_type/1,
    default_headers/0,
    default_headers_no_content_type/0
]).

-define(ALLOWED_VARS, [
    ?VAR_USERNAME,
    ?VAR_CLIENTID,
    ?VAR_PASSWORD,
    ?VAR_PEERHOST,
    ?VAR_CERT_SUBJECT,
    ?VAR_CERT_CN_NAME
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
    Result = emqx_template:parse_deep(Template),
    handle_disallowed_placeholders(Result, {deep, Template}).

parse_str(Template) ->
    Result = emqx_template:parse(Template),
    handle_disallowed_placeholders(Result, {string, Template}).

parse_sql(Template, ReplaceWith) ->
    {Statement, Result} = emqx_template_sql:parse_prepstmt(
        Template,
        #{parameters => ReplaceWith, strip_double_quote => true}
    ),
    {Statement, handle_disallowed_placeholders(Result, {string, Template})}.

handle_disallowed_placeholders(Template, Source) ->
    case emqx_template:validate(?ALLOWED_VARS, Template) of
        ok ->
            Template;
        {error, Disallowed} ->
            ?tp(warning, "authn_template_invalid", #{
                template => Source,
                reason => Disallowed,
                allowed => #{placeholders => ?ALLOWED_VARS},
                notice =>
                    "Disallowed placeholders will be rendered as is."
                    " However, consider using `${$}` escaping for literal `$` where"
                    " needed to avoid unexpected results."
            }),
            Result = prerender_disallowed_placeholders(Template),
            case Source of
                {string, _} ->
                    emqx_template:parse(Result);
                {deep, _} ->
                    emqx_template:parse_deep(Result)
            end
    end.

prerender_disallowed_placeholders(Template) ->
    {Result, _} = emqx_template:render(Template, #{}, #{
        var_trans => fun(Name, _) ->
            % NOTE
            % Rendering disallowed placeholders in escaped form, which will then
            % parse as a literal string.
            case lists:member(Name, ?ALLOWED_VARS) of
                true -> "${" ++ Name ++ "}";
                false -> "${$}{" ++ Name ++ "}"
            end
        end
    }),
    Result.

render_deep(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Term, _Errors} = emqx_template:render(
        Template,
        mapping_credential(Credential),
        #{var_trans => fun to_string/2}
    ),
    Term.

render_str(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {String, _Errors} = emqx_template:render(
        Template,
        mapping_credential(Credential),
        #{var_trans => fun to_string/2}
    ),
    unicode:characters_to_binary(String).

render_urlencoded_str(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {String, _Errors} = emqx_template:render(
        Template,
        mapping_credential(Credential),
        #{var_trans => fun to_urlencoded_string/2}
    ),
    unicode:characters_to_binary(String).

render_sql_params(ParamList, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Row, _Errors} = emqx_template:render(
        ParamList,
        mapping_credential(Credential),
        #{var_trans => fun to_sql_valaue/2}
    ),
    Row.

is_superuser(#{<<"is_superuser">> := Value}) ->
    #{is_superuser => to_bool(Value)};
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

parse_url(Url) ->
    case string:split(Url, "//", leading) of
        [Scheme, UrlRem] ->
            case string:split(UrlRem, "/", leading) of
                [HostPort, Remaining] ->
                    BaseUrl = iolist_to_binary([Scheme, "//", HostPort]),
                    case string:split(Remaining, "?", leading) of
                        [Path, QueryString] ->
                            {BaseUrl, <<"/", Path/binary>>, QueryString};
                        [Path] ->
                            {BaseUrl, <<"/", Path/binary>>, <<>>}
                    end;
                [HostPort] ->
                    {iolist_to_binary([Scheme, "//", HostPort]), <<>>, <<>>}
            end;
        [Url] ->
            throw({invalid_url, Url})
    end.

convert_headers(Headers) ->
    maps:merge(default_headers(), transform_header_name(Headers)).

convert_headers_no_content_type(Headers) ->
    maps:without(
        [<<"content-type">>],
        maps:merge(default_headers_no_content_type(), transform_header_name(Headers))
    ).

default_headers() ->
    maps:put(
        <<"content-type">>,
        <<"application/json">>,
        default_headers_no_content_type()
    ).

default_headers_no_content_type() ->
    #{
        <<"accept">> => <<"application/json">>,
        <<"cache-control">> => <<"no-cache">>,
        <<"connection">> => <<"keep-alive">>,
        <<"keep-alive">> => <<"timeout=30, max=1000">>
    }.

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

to_urlencoded_string(Name, Value) ->
    emqx_http_lib:uri_encode(to_string(Name, Value)).

to_string(Name, Value) ->
    emqx_template:to_string(render_var(Name, Value)).

to_sql_valaue(Name, Value) ->
    emqx_utils_sql:to_sql_value(render_var(Name, Value)).

render_var(_, undefined) ->
    % NOTE
    % Any allowed but undefined binding will be replaced with empty string, even when
    % rendering SQL values.
    <<>>;
render_var(?VAR_PEERHOST, Value) ->
    inet:ntoa(Value);
render_var(_Name, Value) ->
    Value.

mapping_credential(C = #{cn := CN, dn := DN}) ->
    C#{cert_common_name => CN, cert_subject => DN};
mapping_credential(C) ->
    C.

transform_header_name(Headers) ->
    maps:fold(
        fun(K0, V, Acc) ->
            K = list_to_binary(string:to_lower(to_list(K0))),
            maps:put(K, V, Acc)
        end,
        #{},
        Headers
    ).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.
