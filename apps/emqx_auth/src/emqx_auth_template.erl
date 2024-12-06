%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    render_deep_for_json/2,
    render_deep_for_url/2,
    render_deep_for_raw/2,
    render_str/2,
    render_urlencoded_str/2,
    render_sql_params/2,
    render_strict/2,
    escape_disallowed_placeholders_str/2
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

parse_deep(Template, AllowedVars) ->
    Result = emqx_template:parse_deep(Template),
    handle_disallowed_placeholders(Result, AllowedVars, {deep, Template}).

parse_str(Template, AllowedVars) ->
    Result = emqx_template:parse(Template),
    handle_disallowed_placeholders(Result, AllowedVars, {string, Template}).

parse_sql(Template, ReplaceWith, AllowedVars) ->
    {Statement, Result} = emqx_template_sql:parse_prepstmt(
        Template,
        #{parameters => ReplaceWith, strip_double_quote => true}
    ),
    {Statement, handle_disallowed_placeholders(Result, AllowedVars, {string, Template})}.

escape_disallowed_placeholders_str(Template, AllowedVars) ->
    ParsedTemplate = emqx_template:parse(Template),
    prerender_disallowed_placeholders(ParsedTemplate, AllowedVars).

handle_disallowed_placeholders(Template, AllowedVars, Source) ->
    case emqx_template:validate(AllowedVars, Template) of
        ok ->
            Template;
        {error, Disallowed} ->
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
    end.

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
