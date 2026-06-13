%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_registry).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% API
-export([start_link/0]).
-export([
    lookup/2,
    reconcile/0,
    statuses/0,
    register_type/2,
    unregister_type/1,
    resolve_type/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TAB, ?MODULE).
-define(TYPE_TAB, emqx_agent_tool_type_registry).
-define(STATUS_TAB, emqx_agent_tool_status_registry).

-type tool_id() :: binary().
-type tool_type() :: binary().

-type tool() :: #{
    tool_id := tool_id(),
    type := tool_type(),
    module := module(),
    display_name := binary(),
    description := binary(),
    context => term(),
    input_schema => map()
}.

-export_type([tool/0, tool_id/0, tool_type/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec lookup(tool_type(), tool_id()) -> {ok, tool()} | {error, not_found}.
lookup(Type, ToolId) ->
    case ets:lookup(?TAB, {Type, ToolId}) of
        [{_Key, Tool}] -> {ok, Tool};
        [] -> {error, not_found}
    end.

-spec reconcile() -> ok.
reconcile() ->
    gen_server:call(?MODULE, reconcile, infinity).

-spec statuses() -> map().
statuses() ->
    maps:from_list([
        {status_key(Type, ToolId), status_to_external(Status)}
     || {{Type, ToolId}, Status} <- ets:tab2list(?STATUS_TAB)
    ]).

-spec register_type(tool_type(), module()) -> ok.
register_type(Type, Module) when is_binary(Type), is_atom(Module) ->
    gen_server:call(?MODULE, {register_type, Type, Module}).

-spec unregister_type(tool_type()) -> ok.
unregister_type(Type) when is_binary(Type) ->
    gen_server:call(?MODULE, {unregister_type, Type}).

-spec resolve_type(tool_type()) -> module().
resolve_type(Type) when is_binary(Type) ->
    case ets:lookup(?TYPE_TAB, Type) of
        [{Type, Module}] -> Module;
        [] -> throw(unknown_type)
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?TAB, [named_table, set, public, {read_concurrency, true}]),
    _ = ets:new(?TYPE_TAB, [named_table, set, public, {read_concurrency, true}]),
    _ = ets:new(?STATUS_TAB, [named_table, set, public, {read_concurrency, true}]),
    {ok, #{}}.

handle_call(reconcile, _From, State) ->
    {reply, do_reconcile(), State};
handle_call({register_type, Type, Module}, _From, State) ->
    true = ets:insert(?TYPE_TAB, {Type, Module}),
    {reply, ok, State};
handle_call({unregister_type, Type}, _From, State) ->
    true = ets:delete(?TYPE_TAB, Type),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal operations
%%--------------------------------------------------------------------

insert_runtime(#{tool_id := ToolId, type := Type} = Tool) ->
    true = ets:insert(?TAB, {{Type, ToolId}, Tool}),
    put_status(Type, ToolId, running, null),
    ok.

drop_runtime(Type, ToolId) ->
    OldTool = ets:lookup(?TAB, {Type, ToolId}),
    true = ets:delete(?TAB, {Type, ToolId}),
    true = ets:delete(?STATUS_TAB, {Type, ToolId}),
    destroy_runtime(OldTool),
    ok.

do_reconcile() ->
    Desired = desired_tools(),
    DesiredKeys = maps:keys(Desired),
    RuntimeKeys0 = runtime_keys(),
    StatusKeys0 = status_keys(),
    ?SLOG(info, #{
        msg => "agent_tool_reconcile_started",
        desired_count => map_size(Desired),
        runtime_count => length(RuntimeKeys0),
        status_count => length(StatusKeys0)
    }),
    lists:foreach(fun(Key) -> maybe_drop_removed(Key, DesiredKeys) end, StatusKeys0),
    lists:foreach(fun(Key) -> maybe_drop_removed(Key, DesiredKeys) end, RuntimeKeys0),
    maps:foreach(fun reconcile_tool/2, Desired),
    ?SLOG(info, #{
        msg => "agent_tool_reconcile_finished",
        desired_count => map_size(Desired),
        runtime_count => length(runtime_keys()),
        status_count => length(status_keys())
    }),
    ok.

desired_tools() ->
    maps:from_list([
        {{maps:get(<<"type">>, Tool), maps:get(<<"id">>, Tool)}, Tool}
     || Tool <- emqx_agent_config:parsed_config([tools], [])
    ]).

status_keys() ->
    [Key || {Key, _Status} <- ets:tab2list(?STATUS_TAB)].

runtime_keys() ->
    [Key || {Key, _Tool} <- ets:tab2list(?TAB)].

maybe_drop_removed({Type, ToolId}, DesiredKeys) ->
    case lists:member({Type, ToolId}, DesiredKeys) of
        true -> ok;
        false -> drop_runtime(Type, ToolId)
    end.

reconcile_tool({Type, ToolId}, ToolConfig) ->
    ok = drop_runtime(Type, ToolId),
    case resolve_type_safe(Type) of
        {ok, Module} ->
            Ctx = create_context(ToolConfig),
            case Module:create(Ctx) of
                {ok, Tool} ->
                    ?SLOG(info, #{
                        msg => "agent_tool_reconcile_created",
                        type => Type,
                        tool_id => ToolId,
                        module => Module
                    }),
                    insert_runtime(Tool);
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "agent_tool_reconcile_create_failed",
                        type => Type,
                        tool_id => ToolId,
                        module => Module,
                        reason => Reason,
                        config => ToolConfig
                    }),
                    put_status(Type, ToolId, failed, Reason)
            end;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "agent_tool_reconcile_missing_type",
                type => Type,
                tool_id => ToolId,
                reason => Reason
            }),
            put_status(Type, ToolId, missing_type, Reason)
    end.

resolve_type_safe(Type) ->
    try resolve_type(Type) of
        Module -> {ok, Module}
    catch
        throw:Reason -> {error, Reason}
    end.

create_context(#{<<"id">> := ToolId} = ToolConfig) ->
    Runtime = maps:from_list([runtime_field(K, V) || {K, V} <- maps:to_list(ToolConfig)]),
    maps:put(tool_id, ToolId, maps:remove(id, Runtime)).

runtime_field(<<"type">>, V) -> {type, V};
runtime_field(<<"id">>, V) -> {id, V};
runtime_field(<<"desc">>, V) -> {desc, V};
runtime_field(<<"topic_prefix">>, V) -> {topic_prefix, V};
runtime_field(<<"payload_schema">>, <<>>) -> {payload_schema, undefined};
runtime_field(<<"payload_schema">>, V) -> {payload_schema, decode_schema(V)};
runtime_field(<<"request_payload_schema">>, <<>>) -> {request_payload_schema, undefined};
runtime_field(<<"request_payload_schema">>, V) -> {request_payload_schema, decode_schema(V)};
runtime_field(<<"method">>, V) -> {method, V};
runtime_field(<<"url">>, V) -> {url, V};
runtime_field(<<"headers">>, V) -> {headers, V};
runtime_field(<<"input_schema">>, V) -> {input_schema, decode_schema(V)};
runtime_field(<<"query">>, V) -> {query, V};
runtime_field(<<"resource">>, V) -> {resource, V};
runtime_field(K, V) -> {K, V}.

decode_schema(V) when is_binary(V) ->
    emqx_utils_json:decode(V);
decode_schema(V) ->
    V.

destroy_runtime([]) ->
    ok;
destroy_runtime([{_Key, #{module := Module} = Tool}]) ->
    _ = catch Module:destroy(Tool),
    ok.

put_status(Type, ToolId, Status, Error) ->
    true = ets:insert(
        ?STATUS_TAB,
        {{Type, ToolId}, #{
            type => Type,
            tool_id => ToolId,
            status => Status,
            error => Error
        }}
    ),
    ok.

status_key(Type, ToolId) ->
    <<Type/binary, "@", ToolId/binary>>.

status_to_external(#{status := Status, error := Error}) ->
    #{
        <<"status">> => status_to_binary(Status),
        <<"error">> => error_to_json(Error)
    }.

status_to_binary(Status) when is_atom(Status) ->
    atom_to_binary(Status, utf8);
status_to_binary(Status) when is_binary(Status) ->
    Status.

error_to_json(null) ->
    null;
error_to_json(undefined) ->
    null;
error_to_json(Error) when is_binary(Error) ->
    Error;
error_to_json(Error) ->
    iolist_to_binary(io_lib:format("~0p", [Error])).
