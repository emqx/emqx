%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_bigquery_impl).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").

%% `emqx_resource` API
-export([
    resource_type/0,
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4
]).

%% API
-export([]).

%% Internal exports
-export([insert_all_reply_delegator/2]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(PREPARED_REQUEST(METHOD, PATH, BODY, OPTS),
    {prepared_request, {METHOD, PATH, BODY}, OPTS}
).

%% Allocatable resources

%% Fields
-define(client, client).
-define(insert_all_path, insert_all_path).
-define(installed_channels, installed_channels).
-define(project_id, project_id).
-define(request_ttl, request_ttl).

-type connector_config() :: #{}.
-type connector_state() :: #{
    ?client := emqx_bridge_gcp_pubsub_client:state(),
    ?installed_channels := #{channel_id() => channel_state()},
    ?project_id := binary()
}.

-type channel_config() :: #{
    parameters := #{}
}.
-type channel_state() :: #{}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    bigquery.

-spec callback_mode() -> callback_mode().
-ifdef(TEST).
callback_mode() ->
    persistent_term:get({?MODULE, callback_mode}, async_if_possible).
-else.
callback_mode() ->
    async_if_possible.
-endif.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig0) ->
    ConnConfig1 = maps:update_with(
        service_account_json,
        fun(X) -> emqx_utils_json:decode(X) end,
        ConnConfig0
    ),
    {Transport, HostPort} = emqx_bridge_gcp_pubsub_client:get_transport(bigquery),
    #{hostname := Host, port := Port} = emqx_schema:parse_server(HostPort, #{default_port => 443}),
    ConnConfig = ConnConfig1#{
        jwt_opts => #{
            %% trailing slash is important.
            aud => <<"https://bigquery.googleapis.com/">>
        },
        transport => Transport,
        host => Host,
        port => Port
    },
    #{service_account_json := #{<<"project_id">> := ProjectId}} = ConnConfig,
    maybe
        {ok, Client} ?= emqx_bridge_gcp_pubsub_client:start(ConnResId, ConnConfig),
        ConnState = #{
            ?client => Client,
            ?installed_channels => #{},
            ?project_id => ProjectId
        },
        {ok, ConnState}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    Res = emqx_bridge_gcp_pubsub_client:stop(ConnResId),
    ?tp("bigquery_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | {?status_disconnected, term()}.
on_get_status(_ConnResId, #{?client := Client} = _ConnState) ->
    %% Should we also try to list datasets?
    emqx_bridge_gcp_pubsub_client:get_status(Client).

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), channel_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    channel_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnResId, ConnState0, ChanResId, ChanConfig) ->
    maybe
        {ok, ChanState} ?= create_channel(ChanConfig, ConnState0),
        ConnState =
            emqx_utils_maps:deep_put([?installed_channels, ChanResId], ConnState0, ChanState),
        {ok, ConnState}
    end.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    _ConnResId,
    ConnState0 = #{?installed_channels := InstalledChannels0},
    ChanResId
) when
    is_map_key(ChanResId, InstalledChannels0)
->
    ConnState = emqx_utils_maps:deep_remove([?installed_channels, ChanResId], ConnState0),
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, _ChanResId) ->
    {ok, ConnState}.

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    _ConnResId,
    ChanResId,
    _ConnState = #{?installed_channels := InstalledChannels}
) when is_map_key(ChanResId, InstalledChannels) ->
    %% Should we attempt to get the table here?
    ?status_connected;
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    _ConnResId,
    {ChanResId, #{} = Data},
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    ChanState = maps:get(ChanResId, InstalledChannels),
    do_insert_all_sync([Data], ConnState, ChanResId, ChanState);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_query_async(
    connector_resource_id(),
    {message_tag(), map()},
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) -> {ok, pid()} | {error, term()}.
on_query_async(
    _ConnResId,
    {ChanResId, #{} = Data},
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    ChanState = maps:get(ChanResId, InstalledChannels),
    do_insert_all_async([Data], ReplyFnAndArgs, ConnState, ChanResId, ChanState);
on_query_async(_ConnResId, Query, _ReplyFnAndArgs, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(
    _ConnResId,
    [{ChanResId, _Data} | _] = Queries,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    ChanState = maps:get(ChanResId, InstalledChannels),
    Batch = [Data || {_, Data} <- Queries],
    do_insert_all_sync(Batch, ConnState, ChanResId, ChanState);
on_batch_query(_ConnResId, Queries, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

-spec on_batch_query_async(
    connector_resource_id(),
    [query()],
    {ReplyFun :: function(), Args :: list()},
    connector_state()
) ->
    ok | {error, term()}.
on_batch_query_async(
    _ConnResId,
    [{ChanResId, _Data} | _] = Queries,
    ReplyFnAndArgs,
    #{?installed_channels := InstalledChannels} = ConnState
) when
    is_map_key(ChanResId, InstalledChannels)
->
    ChanState = maps:get(ChanResId, InstalledChannels),
    Batch = [Data || {_, Data} <- Queries],
    do_insert_all_async(Batch, ReplyFnAndArgs, ConnState, ChanResId, ChanState);
on_batch_query_async(_ConnResId, Queries, _ReplyFnAndArgs, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

insert_all_reply_delegator(ReplyFnAndArgs, Result0) ->
    Result = handle_insert_all_result(Result0),
    emqx_resource:apply_reply_fun(ReplyFnAndArgs, Result).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

create_channel(ChanConfig, ConnState) ->
    #{
        parameters := #{
            dataset := Dataset,
            table := Table
        },
        resource_opts := #{
            request_ttl := RequestTTL
        }
    } = ChanConfig,
    #{?client := Client, ?project_id := ProjectId} = ConnState,
    Opts = #{?request_ttl => RequestTTL},
    maybe
        {ok, _} ?= get_table(Client, ProjectId, Dataset, Table, Opts),
        InsertAllPath =
            render(
                <<
                    "/bigquery/v2/projects/${project_id}/datasets/${dataset}/"
                    "tables/${table}/insertAll"
                >>,
                #{
                    project_id => ProjectId,
                    dataset => Dataset,
                    table => Table
                }
            ),
        ActionState = #{
            ?insert_all_path => InsertAllPath,
            ?request_ttl => RequestTTL
        },
        {ok, ActionState}
    else
        {error, #{status_code := 404}} ->
            {error, {unhealthy_target, <<"Table or dataset does not exist">>}};
        {error, #{status_code := 403}} ->
            {error, {unhealthy_target, <<"Permission denied trying to access table">>}};
        {error, _} = Error ->
            Error
    end.

get_table(Client, ProjectId, Dataset, Table, Opts) ->
    Method = get,
    Path = render(<<"/bigquery/v2/projects/${project_id}/datasets/${dataset}/tables/${table}">>, #{
        project_id => ProjectId,
        dataset => Dataset,
        table => Table
    }),
    Body = <<"">>,
    emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body, Opts),
        Client
    ).

do_insert_all_sync(Batch, ConnState, ChanResId, ChanState) ->
    #{?client := Client} = ConnState,
    #{
        ?insert_all_path := Path,
        ?request_ttl := RequestTTL
    } = ChanState,
    Method = post,
    Body = render_insert_all_payload(Batch),
    ReqOpts = #{request_ttl => RequestTTL},
    Req = ?PREPARED_REQUEST(Method, Path, Body, ReqOpts),
    emqx_trace:rendered_action_template(ChanResId, #{
        method => Method,
        path => Path,
        body => Body,
        options => ReqOpts,
        is_async => false
    }),
    Result = emqx_bridge_gcp_pubsub_client:query_sync(Req, Client),
    handle_insert_all_result(Result).

do_insert_all_async(Batch, ReplyFnAndArgs0, ConnState, ChanResId, ChanState) ->
    #{?client := Client} = ConnState,
    #{
        ?insert_all_path := Path,
        ?request_ttl := RequestTTL
    } = ChanState,
    Method = post,
    Body = render_insert_all_payload(Batch),
    ReqOpts = #{request_ttl => RequestTTL},
    Req = ?PREPARED_REQUEST(Method, Path, Body, ReqOpts),
    emqx_trace:rendered_action_template(ChanResId, #{
        method => Method,
        path => Path,
        body => Body,
        options => ReqOpts,
        is_async => true
    }),
    ReplyFnAndArgs = {fun ?MODULE:insert_all_reply_delegator/2, [ReplyFnAndArgs0]},
    emqx_bridge_gcp_pubsub_client:query_async(Req, ReplyFnAndArgs, Client).

render_insert_all_payload(Batch) ->
    Body = #{
        <<"rows">> =>
            lists:map(
                fun(Record) ->
                    #{<<"json">> => Record}
                end,
                Batch
            )
    },
    emqx_utils_json:encode(Body).

handle_insert_all_result({ok, #{status_code := 200, body := BodyRaw}}) ->
    case emqx_utils_json:safe_decode(BodyRaw) of
        {ok, #{<<"insertErrors">> := [_ | _] = Errors}} ->
            ?tp("bigquery_insert_errors", #{errors => Errors}),
            Context = #{reason => <<"insert_returned_errors">>, errors => Errors},
            {error, {unrecoverable_error, Context}};
        {ok, _} ->
            ok;
        {error, _} ->
            Context = #{reason => <<"insert_returned_not_a_json">>, body => BodyRaw},
            {error, {unrecoverable_error, Context}}
    end;
handle_insert_all_result({ok, #{status_code := Status, headers := Headers} = Resp}) ->
    Context = #{
        reason => <<"unexpected_insert_response">>,
        status => Status,
        headers => Headers,
        body => maps:get(body, Resp, undefined)
    },
    {error, {unrecoverable_error, Context}};
handle_insert_all_result({error, {recoverable_error, _Reason}} = Result) ->
    Result;
handle_insert_all_result({error, {unrecoverable_error, _Reason}} = Result) ->
    Result;
handle_insert_all_result({error, #{status_code := 403 = Status} = Resp}) ->
    BodyRaw = maps:get(body, Resp, undefined),
    Body = emqx_maybe:apply(fun try_json_decode/1, BodyRaw),
    Context = #{reason => <<"access_denied_to_insert">>, status => Status, body => Body},
    ?tp("bigquery_insert_access_denied", #{context => Context}),
    {error, {unrecoverable_error, Context}};
handle_insert_all_result({error, #{status_code := Status} = Resp}) ->
    BodyRaw = maps:get(body, Resp, undefined),
    Body = emqx_maybe:apply(fun try_json_decode/1, BodyRaw),
    Context = #{
        reason => <<"error_response_trying_to_insert">>,
        status => Status,
        body => Body
    },
    {error, {unrecoverable_error, Context}};
handle_insert_all_result({error, Reason}) ->
    {error, {unrecoverable_error, Reason}}.

try_json_decode(BodyRaw) ->
    case emqx_utils_json:safe_decode(BodyRaw) of
        {ok, JSON} -> JSON;
        {error, _} -> BodyRaw
    end.

render(TemplateStr, Context) ->
    Template = emqx_template:parse(TemplateStr),
    emqx_template:render_strict(Template, Context).
