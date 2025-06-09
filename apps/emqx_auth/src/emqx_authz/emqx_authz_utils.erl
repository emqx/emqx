%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_utils).

-feature(maybe_expr, enable).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx_authz.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    cleanup_resources/0,
    make_resource_id/1,
    create_resource/2,
    update_resource/2,
    remove_resource/1,
    update_config/2,
    vars_for_rule_query/2,
    authorize_with_row/6,
    init_state/2,
    resource_config/2
]).

-export([
    parse_http_resp_body/2,
    content_type/1
]).

-export([
    cached_simple_sync_query/3
]).

-define(DEFAULT_RESOURCE_OPTS(Type), #{
    start_after_created => false,
    owner_id => Type
}).

-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

create_resource(
    ConnectorModule,
    #{resource_id := ResourceId, type := Type, resource_config := ResourceConfig} = State
) ->
    maybe
        {ok, _} ?=
            emqx_resource:create_local(
                ResourceId,
                ?AUTHZ_RESOURCE_GROUP,
                ConnectorModule,
                ResourceConfig,
                ?DEFAULT_RESOURCE_OPTS(Type)
            ),
        ok = start_stop_resource(State)
    else
        {error, Reason} ->
            error({create_resource_error, Reason})
    end.

update_resource(
    ConnectorModule,
    #{resource_id := ResourceId, type := Type, resource_config := ResourceConfig} = State
) ->
    maybe
        {ok, _} ?=
            emqx_resource:recreate_local(
                ResourceId,
                ConnectorModule,
                ResourceConfig,
                ?DEFAULT_RESOURCE_OPTS(Type)
            ),
        ok = start_stop_resource(State),
        ok
    else
        {error, Reason} ->
            error({update_resource_error, Reason})
    end.

remove_resource(ResourceId) ->
    emqx_resource:remove_local(ResourceId).

resource_config(WithoutFields, Source) ->
    maps:without([enable, type] ++ WithoutFields, Source).

start_stop_resource(#{resource_id := ResourceId, enable := true}) ->
    _ = emqx_resource:start(ResourceId),
    ok;
start_stop_resource(#{resource_id := ResourceId, enable := false}) ->
    _ = emqx_resource:stop(ResourceId),
    ok.

cleanup_resources() ->
    lists:foreach(
        fun emqx_resource:remove_local/1,
        emqx_resource:list_group_instances(?AUTHZ_RESOURCE_GROUP)
    ).

make_resource_id(Name) ->
    NameBin = iolist_to_binary(["authz:", emqx_utils_conv:bin(Name)]),
    emqx_resource:generate_id(NameBin).

update_config(Path, ConfigRequest) ->
    emqx_conf:update(Path, ConfigRequest, #{
        rawconf_with_defaults => true,
        override_to => cluster
    }).

-spec parse_http_resp_body(binary(), binary()) -> allow | deny | ignore | error | {error, term()}.
parse_http_resp_body(<<"application/x-www-form-urlencoded", _/binary>>, Body) ->
    try
        result(maps:from_list(cow_qs:parse_qs(Body)))
    catch
        _:_ -> error
    end;
parse_http_resp_body(<<"application/json", _/binary>>, Body) ->
    try
        result(emqx_utils_json:decode(Body))
    catch
        _:_ -> error
    end;
parse_http_resp_body(ContentType = <<_/binary>>, _Body) ->
    {error, <<"unsupported content-type: ", ContentType/binary>>}.

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

-spec vars_for_rule_query(emqx_types:clientinfo(), emqx_types:pubsub()) -> map().
vars_for_rule_query(Client, ?authz_action(PubSub, Qos) = Action) ->
    Client#{
        action => PubSub,
        qos => Qos,
        retain => maps:get(retain, Action, false)
    }.

-spec cached_simple_sync_query(
    emqx_auth_cache:cache_key(),
    emqx_resource:resource_id(),
    _Request :: term()
) -> term().
cached_simple_sync_query(CacheKey, ResourceID, Query) ->
    emqx_auth_utils:cached_simple_sync_query(?AUTHZ_CACHE, CacheKey, ResourceID, Query).

-spec authorize_with_row(
    emqx_authz_source:source_type(),
    emqx_types:clientinfo(),
    emqx_types:pubsub(),
    emqx_types:topic(),
    [binary()] | undefined,
    [binary()] | map()
) -> nomatch | {matched, allow | deny | ignore}.
authorize_with_row(Type, Client, Action, Topic, ColumnNames, Row) ->
    try
        maybe
            {ok, Rule} ?= parse_rule_from_row(ColumnNames, Row),
            {matched, Permission} ?= emqx_authz_rule:match(Client, Action, Topic, Rule),
            {matched, Permission}
        else
            nomatch ->
                nomatch;
            {error, Reason0} ->
                log_match_rule_error(Type, Row, Reason0),
                nomatch
        end
    catch
        throw:Reason1 ->
            log_match_rule_error(Type, Row, Reason1),
            nomatch
    end.

-spec init_state(emqx_authz_source:source(), map()) -> emqx_authz_source:source_state().
init_state(#{type := Type, enable := Enable} = _Source, Values) ->
    maps:merge(
        #{
            type => Type,
            enable => Enable
        },
        Values
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

parse_rule_from_row(_ColumnNames, RuleMap = #{}) ->
    case emqx_authz_rule_raw:parse_rule(RuleMap) of
        {ok, {Permission, Who, Action, Topics}} ->
            {ok, emqx_authz_rule:compile({Permission, Who, Action, Topics})};
        {error, Reason} ->
            {error, Reason}
    end;
parse_rule_from_row(ColumnNames, Row) ->
    RuleMap = maps:from_list(lists:zip(ColumnNames, to_list(Row))),
    parse_rule_from_row(ColumnNames, RuleMap).

to_list(Tuple) when is_tuple(Tuple) ->
    tuple_to_list(Tuple);
to_list(List) when is_list(List) ->
    List.

log_match_rule_error(Type, Row, Reason0) ->
    Msg0 = #{
        msg => "match_rule_error",
        rule => Row,
        type => Type
    },
    Msg1 =
        case is_map(Reason0) of
            true -> maps:merge(Msg0, Reason0);
            false -> Msg0#{reason => Reason0}
        end,
    ?SLOG(
        error,
        Msg1,
        #{tag => "AUTHZ"}
    ).
