%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_blob_storage_connector).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).
-behaviour(emqx_connector_aggreg_delivery).
-behaviour(emqx_template).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_connector_aggregator/include/emqx_connector_aggregator.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_azure_blob_storage.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    resource_type/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3
]).

%% `emqx_connector_aggreg_delivery' API
-export([
    init_transfer_state/2,
    process_append/2,
    process_write/1,
    process_complete/1
]).

%% `emqx_template' API
-export([lookup/2]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type container() :: string().
-type blob() :: string().

-type connector_config() :: #{
    endpoint => string(),
    account_name := string(),
    account_key := emqx_secret:t(string()),
    resource_opts := map(),
    any() => term()
}.

-type connector_state() :: #{
    driver_state := driver_state(),
    installed_actions := #{action_resource_id() => action_state()}
}.

-type action_config() :: direct_action_config() | aggreg_action_config().
-type direct_action_config() :: #{
    parameters := #{
        mode := direct,
        container := template_str(),
        blob := template_str(),
        content := template_str(),
        max_block_size := pos_integer()
    }
}.
-type aggreg_action_config() :: #{
    parameters := #{
        mode := aggregated,
        aggregation := #{
            %% TODO: other containers
            container := #{type := csv},
            time_interval := pos_integer(),
            max_records := pos_integer()
        },
        container := string(),
        blob := template_str(),
        max_block_size := pos_integer(),
        min_block_size := pos_integer()
    },
    any() => term()
}.

-type template_str() :: unicode:chardata().

-type action_state() :: direct_action_state() | aggreg_action_state().
-type direct_action_state() :: #{
    mode := direct,
    container := emqx_template:t(),
    blob := emqx_template:t(),
    content := emqx_template:t(),
    max_block_size := pos_integer()
}.
-type aggreg_action_state() :: #{
    mode := aggregated,
    name := binary(),
    container := string(),
    aggreg_id := aggreg_id(),
    supervisor := pid(),
    on_stop := {module(), atom(), [term()]}
}.
-type aggreg_id() :: {binary(), binary()}.

-type query() :: {_Tag :: channel_id(), _Data :: emqx_jsonish:t()}.

-type driver_state() :: _.

-type transfer_opts() :: #{
    upload_options := #{
        action := binary(),
        blob := emqx_template:t(),
        container := string(),
        min_block_size := pos_integer(),
        max_block_size := pos_integer(),
        driver_state := driver_state()
    }
}.

-type transfer_buffer() :: iolist().

-type transfer_state() :: #{
    blob := blob(),
    buffer := transfer_buffer(),
    buffer_size := non_neg_integer(),
    container := container(),
    max_block_size := pos_integer(),
    min_block_size := pos_integer(),
    next_block := queue:queue(iolist()),
    num_blocks := non_neg_integer(),
    driver_state := driver_state(),
    started := boolean()
}.

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec callback_mode() -> callback_mode().
callback_mode() ->
    always_sync.

-spec resource_type() -> atom().
resource_type() ->
    azure_blob_storage.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(_ConnResId, ConnConfig) ->
    #{
        account_name := AccountName,
        account_key := AccountKey
    } = ConnConfig,
    maybe
        ok ?= emqx_bridge_azure_blob_storage_connector_schema:validate_account_key(AccountKey),
        Endpoint = maps:get(endpoint, ConnConfig, undefined),
        {ok, DriverState} = erlazure:new(#{
            account => AccountName,
            key => AccountKey,
            endpoint => Endpoint
        }),
        State = #{
            driver_state => DriverState,
            installed_actions => #{}
        },
        {ok, State}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(_ConnResId, _ConnState) ->
    ?tp(azure_blob_storage_stop, #{instance_id => _ConnResId}),
    ok.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | ?status_disconnected | {?status_disconnected, connector_state(), term()}.
on_get_status(_ConnResId, #{driver_state := DriverState}) ->
    health_check(DriverState).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    action_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnResId, ConnState0, ActionResId, ActionConfig) ->
    ActionState = install_action(ActionConfig, ConnState0),
    ConnState = emqx_utils_maps:deep_put([installed_actions, ActionResId], ConnState0, ActionState),
    {ok, ConnState}.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(_ConnResId, ConnState0, ActionResId) ->
    #{installed_actions := InstalledActions0} = ConnState0,
    case maps:take(ActionResId, InstalledActions0) of
        {ActionState, InstalledActions} ->
            ok = stop_action(ActionState),
            ConnState = ConnState0#{installed_actions := InstalledActions},
            {ok, ConnState};
        error ->
            {ok, ConnState0}
    end.

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), action_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    _ConnResId,
    ActionResId,
    ConnectorState = #{installed_actions := InstalledActions}
) when is_map_key(ActionResId, InstalledActions) ->
    #{ActionResId := ActionConfig} = InstalledActions,
    channel_status(ActionConfig, ConnectorState);
on_get_channel_status(_ConnResId, _ActionResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(ConnResId, {Tag, Data}, #{installed_actions := InstalledActions} = ConnState) ->
    case maps:get(Tag, InstalledActions, undefined) of
        ChannelState = #{mode := direct} ->
            ?tp(azure_blob_storage_bridge_on_query_enter, #{mode => direct}),
            run_direct_transfer(Data, ConnResId, Tag, ChannelState, ConnState);
        ChannelState = #{mode := aggregated} ->
            ?tp(azure_blob_storage_bridge_on_query_enter, #{mode => aggregated}),
            run_aggregated_transfer([Data], ChannelState);
        undefined ->
            {error, {unrecoverable_error, {invalid_message_tag, Tag}}}
    end.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_batch_query(_ConnResId, [{Tag, Data0} | Rest], #{installed_actions := InstalledActions}) ->
    case maps:get(Tag, InstalledActions, undefined) of
        ActionState = #{mode := aggregated} ->
            Records = [Data0 | [Data || {_, Data} <- Rest]],
            run_aggregated_transfer(Records, ActionState);
        undefined ->
            {error, {unrecoverable_error, {invalid_message_tag, Tag}}}
    end.

%%------------------------------------------------------------------------------
%% Driver calls
%%------------------------------------------------------------------------------

do_create_block_blob(DriverState, Container, Blob) ->
    %% TODO: check container type before setting content type
    Opts = [{content_type, "text/csv"}],
    erlazure:put_block_blob(DriverState, Container, Blob, <<>>, Opts).

do_append_data(DriverState, Container, Blob, BlockId, IOData) ->
    erlazure:put_block(DriverState, Container, Blob, BlockId, IOData, []).

do_put_block_list(DriverState, Container, Blob, BlockRefs) ->
    %% TODO: check container type before setting content type
    Opts = [{req_opts, [{headers, [{"x-ms-blob-content-type", "text/csv"}]}]}],
    erlazure:put_block_list(DriverState, Container, Blob, BlockRefs, Opts).

do_put_block_blob(DriverState, Container, Blob, IOData) ->
    erlazure:put_block_blob(DriverState, Container, Blob, IOData, []).

do_list_blobs(DriverState, Container) ->
    try erlazure:list_blobs(DriverState, Container, []) of
        {L, _} when is_list(L) ->
            ok
    catch
        _:_ ->
            error
    end.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_delivery' API
%%------------------------------------------------------------------------------

-spec init_transfer_state(buffer(), transfer_opts()) ->
    transfer_state().
init_transfer_state(Buffer, Opts) ->
    #{
        upload_options := #{
            action := ActionName,
            blob := BlobTemplate,
            container := Container,
            max_block_size := MaxBlockSize,
            min_block_size := MinBlockSize,
            driver_state := DriverState
        }
    } = Opts,
    Blob = mk_blob_name_key(Buffer, ActionName, BlobTemplate),
    #{
        blob => Blob,
        buffer => [],
        buffer_size => 0,
        container => Container,
        max_block_size => MaxBlockSize,
        min_block_size => MinBlockSize,
        next_block => queue:new(),
        num_blocks => 0,
        driver_state => DriverState,
        started => false
    }.

mk_blob_name_key(Buffer, ActionName, BlobTemplate) ->
    emqx_template:render_strict(BlobTemplate, {?MODULE, {ActionName, Buffer}}).

-spec process_append(iodata(), transfer_state()) ->
    transfer_state().
process_append(IOData, TransferState0) ->
    #{
        buffer := Buffer,
        buffer_size := BufferSize0,
        min_block_size := MinBlockSize,
        next_block := NextBlock0
    } = TransferState0,
    Size = iolist_size(IOData),
    case Size + BufferSize0 >= MinBlockSize of
        true ->
            %% Block is ready to be written.
            TransferState0#{
                buffer := [],
                buffer_size := 0,
                next_block := queue:in([Buffer, IOData], NextBlock0)
            };
        false ->
            TransferState0#{
                buffer := [Buffer, IOData],
                buffer_size := BufferSize0 + Size
            }
    end.

-spec process_write(transfer_state()) ->
    {ok, transfer_state()} | {error, term()}.
process_write(TransferState0 = #{started := false}) ->
    #{
        driver_state := DriverState,
        blob := Blob,
        container := Container
    } = TransferState0,
    %% TODO
    %% Possible optimization: if the whole buffer fits the 5000 MiB `put_block_blob'
    %% limit, we could upload the whole thing here.
    case do_create_block_blob(DriverState, Container, Blob) of
        {ok, _} ->
            TransferState = TransferState0#{started := true},
            process_write(TransferState);
        {error, Reason} ->
            {error, Reason}
    end;
process_write(TransferState0 = #{started := true}) ->
    #{
        next_block := NextBlock0
    } = TransferState0,
    case queue:out(NextBlock0) of
        {{value, Block}, NextBlock} ->
            ?tp(azure_blob_storage_will_write_chunk, #{}),
            do_process_write(Block, TransferState0#{next_block := NextBlock});
        {empty, _} ->
            {ok, TransferState0}
    end.

do_process_write(IOData, TransferState0 = #{started := true}) ->
    #{
        blob := Blob,
        container := Container,
        num_blocks := NumBlocks,
        driver_state := DriverState
    } = TransferState0,
    case do_append_data(DriverState, Container, Blob, block_id(NumBlocks), IOData) of
        {ok, _} ->
            TransferState = TransferState0#{num_blocks := NumBlocks + 1},
            process_write(TransferState);
        {error, Reason} ->
            {error, Reason}
    end.

-spec process_complete(transfer_state()) ->
    {ok, term()}.
process_complete(TransferState) ->
    #{
        blob := Blob,
        buffer := Buffer,
        buffer_size := BufferSize,
        container := Container,
        num_blocks := NumBlocks0,
        driver_state := DriverState
    } = TransferState,
    %% Flush any left-over data
    NumBlocks =
        case BufferSize > 0 of
            true ->
                {ok, #{num_blocks := NumBlocks1}} = do_process_write(Buffer, TransferState),
                NumBlocks1;
            false ->
                NumBlocks0
        end,
    BlockRefs = [{block_id(N), latest} || N <- lists:seq(0, NumBlocks - 1)],
    case do_put_block_list(DriverState, Container, Blob, BlockRefs) of
        {ok, _} ->
            {ok, #{num_blocks => NumBlocks}};
        {error, Reason} ->
            exit({upload_failed, Reason})
    end.

%%------------------------------------------------------------------------------
%% `emqx_template' API
%%------------------------------------------------------------------------------

-spec lookup(emqx_template:accessor(), {_Name, buffer()}) ->
    {ok, integer() | string()} | {error, undefined}.
lookup([<<"action">>], {ActionName, _Buffer}) ->
    {ok, mk_fs_safe_string(ActionName)};
lookup([<<"node">>], {_ActionName, _Buffer}) ->
    {ok, mk_fs_safe_string(atom_to_binary(erlang:node()))};
lookup(Accessor, {_ActionName, Buffer}) ->
    lookup_buffer_var(Accessor, Buffer);
lookup(_Accessor, _Context) ->
    {error, undefined}.

lookup_buffer_var(Accessor, Buffer) ->
    case emqx_connector_aggreg_buffer_ctx:lookup(Accessor, Buffer) of
        {ok, String} when is_list(String) ->
            {ok, mk_fs_safe_string(String)};
        {ok, Value} ->
            {ok, Value};
        {error, Reason} ->
            {error, Reason}
    end.

mk_fs_safe_string(String) ->
    unicode:characters_to_binary(string:replace(String, ":", "_", all)).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec install_action(action_config(), connector_state()) -> action_state().
install_action(#{parameters := #{mode := direct}} = ActionConfig, _ConnState) ->
    #{
        parameters := #{
            mode := Mode = direct,
            container := ContainerTemplateStr,
            blob := BlobTemplateStr,
            content := ContentTemplateStr,
            max_block_size := MaxBlockSize
        }
    } = ActionConfig,
    ContainerTemplate = emqx_template:parse(ContainerTemplateStr),
    BlobTemplate = emqx_template:parse(BlobTemplateStr),
    ContentTemplate = emqx_template:parse(ContentTemplateStr),
    #{
        mode => Mode,
        container => ContainerTemplate,
        blob => BlobTemplate,
        content => ContentTemplate,
        max_block_size => MaxBlockSize
    };
install_action(#{parameters := #{mode := aggregated}} = ActionConfig, ConnState) ->
    #{driver_state := DriverState} = ConnState,
    #{
        bridge_name := Name,
        parameters := #{
            mode := Mode = aggregated,
            aggregation := #{
                container := ContainerOpts,
                max_records := MaxRecords,
                time_interval := TimeInterval
            },
            container := ContainerName,
            blob := BlobTemplateStr,
            max_block_size := MaxBlockSize,
            min_block_size := MinBlockSize
        }
    } = ActionConfig,
    Type = ?ACTION_TYPE_BIN,
    AggregId = {Type, Name},
    Blob = mk_blob_name_template(BlobTemplateStr),
    AggregOpts = #{
        max_records => MaxRecords,
        time_interval => TimeInterval,
        work_dir => work_dir(Type, Name)
    },
    TransferOpts = #{
        action => Name,
        blob => Blob,
        container => ContainerName,
        max_block_size => MaxBlockSize,
        min_block_size => MinBlockSize,
        driver_state => DriverState
    },
    DeliveryOpts = #{
        callback_module => ?MODULE,
        container => ContainerOpts,
        upload_options => TransferOpts
    },
    _ = ?AGGREG_SUP:delete_child(AggregId),
    {ok, SupPid} = ?AGGREG_SUP:start_child(#{
        id => AggregId,
        start =>
            {emqx_connector_aggreg_upload_sup, start_link, [AggregId, AggregOpts, DeliveryOpts]},
        type => supervisor,
        restart => permanent
    }),
    #{
        mode => Mode,
        name => Name,
        container => ContainerName,
        aggreg_id => AggregId,
        supervisor => SupPid,
        on_stop => {?AGGREG_SUP, delete_child, [AggregId]}
    }.

-spec stop_action(action_config()) -> ok | {error, any()}.
stop_action(#{on_stop := {M, F, A}}) ->
    apply(M, F, A);
stop_action(_) ->
    ok.

run_direct_transfer(Data, ConnResId, ActionResId, ActionState, ConnState) ->
    #{driver_state := DriverState} = ConnState,
    #{
        container := ContainerTemplate,
        blob := BlobTemplate,
        content := ContentTemplate,
        max_block_size := MaxBlockSize
    } = ActionState,
    Container = render_container(ContainerTemplate, Data),
    Blob = render_blob(BlobTemplate, Data),
    Content = render_content(ContentTemplate, Data),
    emqx_trace:rendered_action_template(ActionResId, #{
        container => Container,
        blob => Blob,
        content => #emqx_trace_format_func_data{
            function = fun unicode:characters_to_binary/1,
            data = Content
        }
    }),
    case iolist_size(Content) > MaxBlockSize of
        true ->
            error({unrecoverable_error, payload_too_large});
        false ->
            ok
    end,
    case do_put_block_blob(DriverState, Container, Blob, Content) of
        {ok, created} ->
            ?tp(azure_blob_storage_bridge_connector_upload_ok, #{instance_id => ConnResId}),
            ok;
        {error, Reason} ->
            ?tp(
                azure_blob_storage_bridge_direct_upload_error,
                #{instance_id => ConnResId, reason => Reason}
            ),
            {error, map_error(Reason)}
    end.

run_aggregated_transfer(Records, #{aggreg_id := AggregId}) ->
    Timestamp = erlang:system_time(second),
    case emqx_connector_aggregator:push_records(AggregId, Timestamp, Records) of
        ok ->
            ok;
        {error, Reason} ->
            {error, {unrecoverable_error, Reason}}
    end.

work_dir(Type, Name) ->
    filename:join([emqx:data_dir(), bridge, Type, Name]).

-spec mk_blob_name_template(template_str()) -> emqx_template:str().
mk_blob_name_template(TemplateStr) ->
    Template = emqx_template:parse(TemplateStr),
    {_, BindingErrors} = emqx_template:render(Template, #{}),
    {UsedBindings, _} = lists:unzip(BindingErrors),
    SuffixTemplate = mk_suffix_template(UsedBindings),
    case emqx_template:is_const(SuffixTemplate) of
        true ->
            Template;
        false ->
            Template ++ SuffixTemplate
    end.

mk_suffix_template(UsedBindings) ->
    RequiredBindings = ["action", "node", "datetime.", "sequence"],
    SuffixBindings = [
        mk_default_binding(RB)
     || RB <- RequiredBindings,
        lists:all(fun(UB) -> string:prefix(UB, RB) == nomatch end, UsedBindings)
    ],
    SuffixTemplate = [["/", B] || B <- SuffixBindings],
    emqx_template:parse(SuffixTemplate).

mk_default_binding("datetime.") ->
    "${datetime.rfc3339utc}";
mk_default_binding(Binding) ->
    "${" ++ Binding ++ "}".

render_container(Template, Data) ->
    case emqx_template:render(Template, {emqx_jsonish, Data}) of
        {Result, []} ->
            iolist_to_string(Result);
        {_, Errors} ->
            error({unrecoverable_error, {container_undefined, Errors}})
    end.

render_blob(Template, Data) ->
    %% NOTE: ignoring errors here, missing variables will be rendered as `"undefined"`.
    {Result, _Errors} = emqx_template:render(Template, {emqx_jsonish, Data}),
    iolist_to_string(Result).

render_content(Template, Data) ->
    %% NOTE: ignoring errors here, missing variables will be rendered as `"undefined"`.
    {Result, _Errors} = emqx_template:render(Template, {emqx_jsonish, Data}),
    Result.

iolist_to_string(IOList) ->
    unicode:characters_to_list(IOList).

channel_status(#{mode := direct}, _ConnState) ->
    %% There's nothing in particular to check for in this mode; the connector health check
    %% already verifies that we're able to use the client to list containers.
    ?status_connected;
channel_status(#{mode := aggregated} = ActionState, ConnState) ->
    #{driver_state := DriverState} = ConnState,
    #{container := Container, aggreg_id := AggregId} = ActionState,
    %% NOTE: This will effectively trigger uploads of buffers yet to be uploaded.
    Timestamp = erlang:system_time(second),
    ok = emqx_connector_aggregator:tick(AggregId, Timestamp),
    ok = check_container_accessible(DriverState, Container),
    ok = check_aggreg_upload_errors(AggregId),
    ?status_connected.

health_check(DriverState) ->
    case erlazure:list_containers(DriverState, []) of
        {error, Reason} ->
            {?status_disconnected, Reason};
        {L, _} when is_list(L) ->
            ?status_connected
    end.

map_error({failed_connect, _} = Reason) ->
    {recoverable_error, Reason};
map_error(Reason) ->
    {unrecoverable_error, Reason}.

check_aggreg_upload_errors(AggregId) ->
    case emqx_connector_aggregator:take_error(AggregId) of
        [Error] ->
            %% TODO
            %% This approach means that, for example, 3 upload failures will cause
            %% the channel to be marked as unhealthy for 3 consecutive health checks.
            ErrorMessage = emqx_utils:format(Error),
            throw({unhealthy_target, ErrorMessage});
        [] ->
            ok
    end.

check_container_accessible(DriverState, Container) ->
    do_list_blobs(DriverState, Container).

block_id(N) ->
    NumDigits = 32,
    list_to_binary(string:pad(integer_to_list(N), NumDigits, leading, $0)).
