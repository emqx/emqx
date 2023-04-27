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

-module(emqx_authz_utils).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx_authz.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    cleanup_resources/0,
    make_resource_id/1,
    create_resource/2,
    create_resource/3,
    update_resource/2,
    remove_resource/1,
    update_config/2,
    parse_deep/2,
    parse_str/2,
    render_urlencoded_str/2,
    parse_sql/3,
    render_deep/2,
    render_str/2,
    render_sql_params/2,
    client_vars/1,
    vars_for_rule_query/2,
    parse_rule_from_row/2
]).

-export([
    parse_http_resp_body/2,
    content_type/1
]).

-define(DEFAULT_RESOURCE_OPTS, #{
    start_after_created => false
}).

-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

create_resource(Module, Config) ->
    ResourceId = make_resource_id(Module),
    create_resource(ResourceId, Module, Config).

create_resource(ResourceId, Module, Config) ->
    Result = emqx_resource:create_local(
        ResourceId,
        ?AUTHZ_RESOURCE_GROUP,
        Module,
        Config,
        ?DEFAULT_RESOURCE_OPTS
    ),
    start_resource_if_enabled(Result, ResourceId, Config).

update_resource(Module, #{annotations := #{id := ResourceId}} = Config) ->
    Result =
        case
            emqx_resource:recreate_local(
                ResourceId,
                Module,
                Config,
                ?DEFAULT_RESOURCE_OPTS
            )
        of
            {ok, _} -> {ok, ResourceId};
            {error, Reason} -> {error, Reason}
        end,
    start_resource_if_enabled(Result, ResourceId, Config).

remove_resource(ResourceId) ->
    emqx_resource:remove_local(ResourceId).

start_resource_if_enabled({ok, _} = Result, ResourceId, #{enable := true}) ->
    _ = emqx_resource:start(ResourceId),
    Result;
start_resource_if_enabled(Result, _ResourceId, _Config) ->
    Result.

cleanup_resources() ->
    lists:foreach(
        fun emqx_resource:remove_local/1,
        emqx_resource:list_group_instances(?AUTHZ_RESOURCE_GROUP)
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
    Result = emqx_connector_template:parse_deep(Template),
    handle_disallowed_placeholders(Result, {deep, Template}, PlaceHolders).

parse_str(Template, PlaceHolders) ->
    Result = emqx_connector_template:parse(Template),
    handle_disallowed_placeholders(Result, {string, Template}, PlaceHolders).

parse_sql(Template, ReplaceWith, PlaceHolders) ->
    {Statement, Result} = emqx_connector_template_sql:parse_prepstmt(
        Template,
        #{parameters => ReplaceWith, strip_double_quote => true}
    ),
    FResult = handle_disallowed_placeholders(Result, {string, Template}, PlaceHolders),
    {Statement, FResult}.

handle_disallowed_placeholders(Template, Source, Allowed) ->
    case emqx_connector_template:validate(Allowed, Template) of
        ok ->
            Template;
        {error, Disallowed} ->
            ?tp(warning, "authz_template_invalid", #{
                template => Source,
                reason => Disallowed,
                allowed => #{placeholders => Allowed},
                notice =>
                    "Disallowed placeholders will be rendered as is."
                    " However, consider using `$${...}` escaping for literal `${...}` where"
                    " needed to avoid unexpected results."
            }),
            Result = prerender_disallowed_placeholders(Template, Allowed),
            case Source of
                {string, _} ->
                    emqx_connector_template:parse(Result);
                {deep, _} ->
                    emqx_connector_template:parse_deep(Result)
            end
    end.

prerender_disallowed_placeholders(Template, Allowed) ->
    {Result, _} = emqx_connector_template:render(Template, #{}, #{
        var_trans => fun(Name, _) ->
            % NOTE
            % Rendering disallowed placeholders in escaped form, which will then
            % parse as a literal string.
            case lists:member(Name, Allowed) of
                true -> "${" ++ Name ++ "}";
                false -> "$${" ++ Name ++ "}"
            end
        end
    }),
    Result.

render_deep(Template, Values) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Term, _Errors} = emqx_connector_template:render(
        Template,
        client_vars(Values),
        #{var_trans => fun to_string/2}
    ),
    Term.

render_str(Template, Values) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {String, _Errors} = emqx_connector_template:render(
        Template,
        client_vars(Values),
        #{var_trans => fun to_string/2}
    ),
    unicode:characters_to_binary(String).

render_urlencoded_str(Template, Values) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {String, _Errors} = emqx_connector_template:render(
        Template,
        client_vars(Values),
        #{var_trans => fun to_urlencoded_string/2}
    ),
    unicode:characters_to_binary(String).

render_sql_params(ParamList, Values) ->
    % NOTE
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    {Row, _Errors} = emqx_connector_template:render(
        ParamList,
        client_vars(Values),
        #{var_trans => fun to_sql_value/2}
    ),
    Row.

-spec parse_http_resp_body(binary(), binary()) -> allow | deny | ignore | error.
parse_http_resp_body(<<"application/x-www-form-urlencoded", _/binary>>, Body) ->
    try
        result(maps:from_list(cow_qs:parse_qs(Body)))
    catch
        _:_ -> error
    end;
parse_http_resp_body(<<"application/json", _/binary>>, Body) ->
    try
        result(emqx_utils_json:decode(Body, [return_maps]))
    catch
        _:_ -> error
    end.

result(#{<<"result">> := <<"allow">>}) -> allow;
result(#{<<"result">> := <<"deny">>}) -> deny;
result(#{<<"result">> := <<"ignore">>}) -> ignore;
result(_) -> error.

-spec content_type(cow_http:headers()) -> binary().
content_type(Headers) when is_list(Headers) ->
    %% header name is lower case, see:
    %% https://github.com/ninenines/cowlib/blob/ce6798c6b2e95b6a34c6a76d2489eaf159827d80/src/cow_http.erl#L192
    proplists:get_value(
        <<"content-type">>,
        Headers,
        <<"application/json">>
    ).

-define(RAW_RULE_KEYS, [<<"permission">>, <<"action">>, <<"topic">>, <<"qos">>, <<"retain">>]).

parse_rule_from_row(ColumnNames, Row) ->
    RuleRaw = maps:with(?RAW_RULE_KEYS, maps:from_list(lists:zip(ColumnNames, to_list(Row)))),
    case emqx_authz_rule_raw:parse_rule(RuleRaw) of
        {ok, {Permission, Action, Topics}} ->
            emqx_authz_rule:compile({Permission, all, Action, Topics});
        {error, Reason} ->
            error(Reason)
    end.

vars_for_rule_query(Client, ?authz_action(PubSub, Qos) = Action) ->
    Client#{
        action => PubSub,
        qos => Qos,
        retain => maps:get(retain, Action, false)
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

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

to_urlencoded_string(Name, Value) ->
    emqx_http_lib:uri_encode(to_string(Name, Value)).

to_string(Name, Value) ->
    emqx_connector_template:to_string(render_var(Name, Value)).

to_sql_value(Name, Value) ->
    emqx_connector_sql:to_sql_value(render_var(Name, Value)).

render_var(_, undefined) ->
    % NOTE
    % Any allowed but undefined binding will be replaced with empty string, even when
    % rendering SQL values.
    <<>>;
render_var(?VAR_PEERHOST, Value) ->
    inet:ntoa(Value);
render_var(_Name, Value) ->
    Value.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.

to_list(Tuple) when is_tuple(Tuple) ->
    tuple_to_list(Tuple);
to_list(List) when is_list(List) ->
    List.
