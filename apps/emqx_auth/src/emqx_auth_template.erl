%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_template).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% Template parsing/rendering
-export([
    parse_deep/2,
    parse_str/2,
    parse_sql/3,
    cache_key_template/1,
    cache_key/2,
    cache_key/3,
    placeholder_vars_from_str/1,
    render_deep_for_json/2,
    render_deep_for_url/2,
    render_deep_for_raw/2,
    render_str/2,
    render_urlencoded_str/2,
    render_sql_params/2,
    render_strict/2,
    escape_disallowed_placeholders_str/2,
    rename_client_info_vars/1
]).

-record(cache_key_template, {
    id :: binary(),
    vars :: emqx_template:t()
}).

-type var() :: emqx_template:varname() | {var_namespace, emqx_template:varname()}.
-type allowed_vars() :: [var()].
-type used_vars() :: [var()].
-type cache_key_template() :: #cache_key_template{}.
-type cache_key() :: fun(() -> term()).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec parse_deep(term(), allowed_vars()) -> {used_vars(), emqx_template:t()}.
parse_deep(Template, AllowedVars) ->
    Result = emqx_template:parse_deep(Template),
    handle_disallowed_placeholders(Result, AllowedVars, {deep, Template}).

-spec parse_str(unicode:chardata(), allowed_vars()) -> {used_vars(), emqx_template:t()}.
parse_str(Template, AllowedVars) ->
    Result = emqx_template:parse(Template),
    handle_disallowed_placeholders(Result, AllowedVars, {string, Template}).

-spec parse_sql(
    emqx_template_sql:raw_statement_template(), emqx_template_sql:sql_parameters(), allowed_vars()
) ->
    {
        used_vars(),
        emqx_template_sql:statement(),
        emqx_template_sql:row_template()
    }.
parse_sql(Template, ReplaceWith, AllowedVars) ->
    {Statement, Result} = emqx_template_sql:parse_prepstmt(
        Template,
        #{parameters => ReplaceWith, strip_double_quote => true}
    ),
    {UsedVars, TemplateWithAllowedVars} = handle_disallowed_placeholders(
        Result, AllowedVars, {string, Template}
    ),
    {UsedVars, Statement, TemplateWithAllowedVars}.

-spec cache_key_template(allowed_vars()) -> cache_key_template().
cache_key_template(Vars) ->
    #cache_key_template{
        id = list_to_binary(emqx_utils:gen_id()),
        vars = emqx_template:parse_deep(
            lists:map(
                fun(Var) ->
                    list_to_binary("${" ++ Var ++ "}")
                end,
                Vars
            )
        )
    }.

-spec cache_key(map(), cache_key_template()) -> cache_key().
cache_key(Values, CacheKeyTemplate) ->
    cache_key(Values, CacheKeyTemplate, []).

-spec cache_key(map(), cache_key_template(), list()) -> cache_key().
cache_key(Values0, #cache_key_template{id = TemplateId, vars = KeyVars}, ExtraKeyParts) when
    is_list(ExtraKeyParts)
->
    fun() ->
        %% We hash the password because we do not want it to be stored as-is in the cache
        %% for a significant amount of time.
        %% TemplateId is used as some kind of salt for the password hash.
        %%
        %% We may just hash the whole key, but we chose to hash the password only for now
        %% for better introspection.
        Values1 = hash_password(TemplateId, Values0),
        Key0 = render_deep_for_raw(KeyVars, Values1),
        Key1 = ExtraKeyParts ++ Key0,
        [TemplateId | Key1]
    end.

-spec placeholder_vars_from_str(unicode:chardata()) -> [var()].
placeholder_vars_from_str(Str) ->
    emqx_template:placeholders(emqx_template:parse(Str)).

-spec escape_disallowed_placeholders_str(unicode:chardata(), allowed_vars()) -> term().
escape_disallowed_placeholders_str(Template, AllowedVars) ->
    ParsedTemplate = emqx_template:parse(Template),
    prerender_disallowed_placeholders(ParsedTemplate, AllowedVars).

-spec rename_client_info_vars(map()) -> map().
rename_client_info_vars(ClientInfo) ->
    Renames = [
        {cn, cert_common_name},
        {dn, cert_subject},
        {protocol, proto_name}
    ],
    lists:foldl(
        fun({Old, New}, Acc) ->
            emqx_utils_maps:rename(Old, New, Acc)
        end,
        ClientInfo,
        Renames
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

hash_password(TemplateId, Values) ->
    emqx_utils_maps:update_if_present(
        password,
        fun(Password) -> crypto:hash(sha256, [TemplateId, Password]) end,
        Values
    ).

handle_disallowed_placeholders(Template, AllowedVars, Source) ->
    {UsedAllowedVars, UsedDisallowedVars} = emqx_template:placeholders(AllowedVars, Template),
    TemplateWithAllowedVars =
        case UsedDisallowedVars of
            [] ->
                Template;
            Disallowed ->
                ?tp(warning, "auth_template_invalid", #{
                    template => Source,
                    reason => Disallowed,
                    allowed => #{placeholders => AllowedVars},
                    notice =>
                        "Disallowed placeholders will be rendered as is."
                        " However, consider using `${$}` escaping for literal `$` where"
                        " needed to avoid unexpected results."
                }),
                Result = prerender_disallowed_placeholders(Template, AllowedVars),
                case Source of
                    {string, _} ->
                        emqx_template:parse(Result);
                    {deep, _} ->
                        emqx_template:parse_deep(Result)
                end
        end,
    {UsedAllowedVars, TemplateWithAllowedVars}.

prerender_disallowed_placeholders(Template, AllowedVars) ->
    {Result, _} = emqx_template:render(Template, #{}, #{
        var_trans => fun(Name, _) ->
            % NOTE
            % Rendering disallowed placeholders in escaped form, which will then
            % parse as a literal string.
            case lists:member(Name, AllowedVars) of
                true -> "${" ++ Name ++ "}";
                false -> "${$}{" ++ Name ++ "}"
            end
        end
    }),
    Result.

render_deep_for_json(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Term, _Errors} = emqx_template:render(
        Template,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_string_for_json/2}
    ),
    Term.

render_deep_for_raw(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Term, _Errors} = emqx_template:render(
        Template,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_string_for_raw/2}
    ),
    Term.

render_deep_for_url(Template, Credential) ->
    render_deep_for_raw(Template, Credential).

render_str(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {String, _Errors} = emqx_template:render(
        Template,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_string/2}
    ),
    unicode:characters_to_binary(String).

render_urlencoded_str(Template, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {String, _Errors} = emqx_template:render(
        Template,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_urlencoded_string/2}
    ),
    unicode:characters_to_binary(String).

render_sql_params(ParamList, Credential) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Row, _Errors} = emqx_template:render(
        ParamList,
        rename_client_info_vars(Credential),
        #{var_trans => fun to_sql_value/2}
    ),
    Row.

to_urlencoded_string(Name, Value) ->
    case uri_string:compose_query([{<<"q">>, to_string(Name, Value)}]) of
        <<"q=", EncodedBin/binary>> ->
            EncodedBin;
        "q=" ++ EncodedStr ->
            list_to_binary(EncodedStr)
    end.

to_string(Name, Value) ->
    emqx_template:to_string(render_var(Name, Value)).

%% This converter is to generate data structure possibly with non-utf8 strings.
%% It converts to unicode only strings (character lists).

to_string_for_raw(Name, Value) ->
    strings_to_unicode(Name, render_var(Name, Value)).

%% This converter is to generate data structure suitable for JSON serialization.
%% JSON strings are sequences of unicode characters, not bytes.
%% So we force all rendered data to be unicode, not only character lists.

to_string_for_json(Name, Value) ->
    all_to_unicode(Name, render_var(Name, Value)).

strings_to_unicode(_Name, Value) when is_binary(Value) ->
    Value;
strings_to_unicode(Name, Value) when is_list(Value) ->
    to_unicode_binary(Name, Value);
strings_to_unicode(_Name, Value) ->
    emqx_template:to_string(Value).

all_to_unicode(Name, Value) when is_list(Value) orelse is_binary(Value) ->
    to_unicode_binary(Name, Value);
all_to_unicode(_Name, Value) ->
    emqx_template:to_string(Value).

to_unicode_binary(Name, Value) when is_list(Value) orelse is_binary(Value) ->
    try unicode:characters_to_binary(Value) of
        Encoded when is_binary(Encoded) ->
            Encoded;
        _ ->
            error({encode_error, {non_unicode_data, Name}})
    catch
        error:badarg ->
            error({encode_error, {non_unicode_data, Name}})
    end.

to_sql_value(Name, Value) ->
    emqx_utils_sql:to_sql_value(render_var(Name, Value)).

render_var(_, undefined) ->
    % NOTE
    % Any allowed but undefined binding will be replaced with empty string, even when
    % rendering SQL values.
    <<>>;
render_var(?VAR_CERT_PEM, Value) ->
    base64:encode(Value);
render_var(?VAR_PEERHOST, Value) ->
    inet:ntoa(Value);
render_var(?VAR_PASSWORD, Value) ->
    iolist_to_binary(Value);
render_var(?VAR_PEERPORT, Value) ->
    integer_to_binary(Value);
render_var(_Name, Value) ->
    Value.

render_strict(Topic, ClientInfo) ->
    emqx_template:render_strict(Topic, rename_client_info_vars(ClientInfo)).
