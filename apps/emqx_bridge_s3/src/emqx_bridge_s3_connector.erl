%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_connector).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("emqx_connector_aggregator/include/emqx_connector_aggregator.hrl").
-include("emqx_bridge_s3.hrl").

-behaviour(emqx_resource).
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_get_channel_status/3
]).

-behaviour(emqx_connector_aggreg_delivery).
-export([
    init_transfer_state/2,
    process_append/2,
    process_write/1,
    process_complete/1,
    process_format_status/1,
    process_terminate/1
]).

-behaviour(emqx_template).
-export([lookup/2]).

-type config() :: #{
    access_key_id => string(),
    secret_access_key => emqx_secret:t(string()),
    host := string(),
    port := pos_integer(),
    transport_options => emqx_s3:transport_options()
}.

-type channel_config() :: #{
    bridge_type := binary(),
    parameters := s3_upload_parameters() | s3_aggregated_upload_parameters()
}.

-type s3_upload_parameters() :: #{
    mode := direct,
    bucket := string(),
    key := string(),
    content := string(),
    acl => emqx_s3:acl()
}.

-type s3_aggregated_upload_parameters() :: #{
    mode := aggregated,
    bucket := string(),
    key := string(),
    acl => emqx_s3:acl(),
    aggregation => #{
        time_interval := emqx_schema:duration_s(),
        max_records := pos_integer()
    },
    container := #{
        type := csv,
        column_order => [string()]
    },
    min_part_size := emqx_schema:bytesize(),
    max_part_size := emqx_schema:bytesize()
}.

-type channel_state() :: #{
    bucket := emqx_template:str(),
    key := emqx_template:str(),
    upload_options => emqx_s3_client:upload_options()
}.

-type state() :: #{
    pool_name := resource_id(),
    client_config := emqx_s3_client:config(),
    channels := #{channel_id() => channel_state()}
}.

-define(AGGREG_SUP, emqx_bridge_s3_sup).

%%
-spec resource_type() -> resource_type().
resource_type() -> s3.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    always_sync.

%% Management

-spec on_start(_InstanceId :: resource_id(), config()) ->
    {ok, state()} | {error, _Reason}.
on_start(InstId, Config) ->
    PoolName = InstId,
    S3Config = Config#{url_expire_time => 0},
    State = #{
        pool_name => PoolName,
        client_config => emqx_s3_profile_conf:client_config(S3Config, PoolName),
        channels => #{}
    },
    _ = emqx_s3_client_http:stop_pool(PoolName),
    case emqx_s3_client_http:start_pool(PoolName, S3Config) of
        ok ->
            ?SLOG(info, #{msg => "s3_connector_start_http_pool_success", pool_name => PoolName}),
            {ok, State};
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "s3_connector_start_http_pool_fail",
                pool_name => PoolName,
                config => S3Config,
                reason => Reason
            }),
            Error
    end.

-spec on_stop(_InstanceId :: resource_id(), state()) ->
    ok.
on_stop(InstId, _State = #{pool_name := PoolName}) ->
    case ehttpc_sup:stop_pool(PoolName) of
        ok ->
            ?tp(s3_bridge_stopped, #{instance_id => InstId}),
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "s3_connector_http_pool_stop_fail",
                pool_name => PoolName,
                reason => Reason
            }),
            ok
    end.

-spec on_get_status(_InstanceId :: resource_id(), state()) ->
    health_check_status().
on_get_status(_InstId, #{client_config := Config}) ->
    case emqx_s3_client:aws_config(Config) of
        {error, Reason} ->
            {?status_disconnected, map_error_details(Reason)};
        AWSConfig ->
            try erlcloud_s3:list_buckets(AWSConfig) of
                Props when is_list(Props) ->
                    ?status_connected
            catch
                error:Error ->
                    {?status_disconnected, map_error_details(Error)}
            end
    end.

-spec on_add_channel(_InstanceId :: resource_id(), state(), channel_id(), channel_config()) ->
    {ok, state()} | {error, _Reason}.
on_add_channel(_InstId, State = #{channels := Channels}, ChannelId, Config) ->
    ChannelState = start_channel(State, Config),
    {ok, State#{channels => Channels#{ChannelId => ChannelState}}}.

-spec on_remove_channel(_InstanceId :: resource_id(), state(), channel_id()) ->
    {ok, state()}.
on_remove_channel(_InstId, State = #{channels := Channels}, ChannelId) ->
    ok = stop_channel(maps:get(ChannelId, Channels, undefined)),
    {ok, State#{channels => maps:remove(ChannelId, Channels)}}.

-spec on_get_channels(_InstanceId :: resource_id()) ->
    [_ChannelConfig].
on_get_channels(InstId) ->
    emqx_bridge_v2:get_channels_for_connector(InstId).

-spec on_get_channel_status(_InstanceId :: resource_id(), channel_id(), state()) ->
    channel_status().
on_get_channel_status(_InstId, ChannelId, State = #{channels := Channels}) ->
    case maps:get(ChannelId, Channels, undefined) of
        ChannelState = #{} ->
            channel_status(ChannelState, State);
        undefined ->
            ?status_disconnected
    end.

start_channel(_State, #{
    bridge_type := ?BRIDGE_TYPE_UPLOAD,
    parameters := Parameters = #{
        mode := Mode = direct,
        bucket := Bucket,
        key := Key,
        content := Content
    }
}) ->
    #{
        mode => Mode,
        bucket => emqx_template:parse(Bucket),
        key => emqx_template:parse(Key),
        content => emqx_template:parse(Content),
        upload_options => upload_options(Parameters)
    };
start_channel(State, #{
    bridge_type := Type = ?BRIDGE_TYPE_UPLOAD,
    bridge_name := Name,
    parameters := Parameters = #{
        mode := Mode = aggregated,
        aggregation := #{
            time_interval := TimeInterval,
            max_records := MaxRecords
        },
        container := Container,
        bucket := Bucket,
        key := Key
    }
}) ->
    AggregId = {Type, Name},
    AggregOpts = #{
        time_interval => TimeInterval,
        max_records => MaxRecords,
        work_dir => work_dir(Type, Name)
    },
    Template = emqx_bridge_s3_upload:mk_key_template(Key),
    DeliveryOpts = #{
        bucket => Bucket,
        key => Template,
        container => Container,
        upload_options => emqx_bridge_s3_upload:mk_upload_options(Parameters),
        callback_module => ?MODULE,
        client_config => maps:get(client_config, State),
        uploader_config => maps:with([min_part_size, max_part_size], Parameters)
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
        aggreg_id => AggregId,
        bucket => Bucket,
        supervisor => SupPid,
        on_stop => fun() -> ?AGGREG_SUP:delete_child(AggregId) end
    }.

upload_options(Parameters) ->
    #{acl => maps:get(acl, Parameters, undefined)}.

work_dir(Type, Name) ->
    filename:join([emqx:data_dir(), bridge, Type, Name]).

stop_channel(#{on_stop := OnStop}) ->
    OnStop();
stop_channel(_ChannelState) ->
    ok.

channel_status(#{mode := direct}, _State) ->
    %% TODO
    %% Since bucket name may be templated, we can't really provide any additional
    %% information regarding the channel health.
    ?status_connected;
channel_status(#{mode := aggregated, aggreg_id := AggregId, bucket := Bucket}, State) ->
    %% NOTE: This will effectively trigger uploads of buffers yet to be uploaded.
    Timestamp = erlang:system_time(second),
    ok = emqx_connector_aggregator:tick(AggregId, Timestamp),
    ok = check_bucket_accessible(Bucket, State),
    ok = check_aggreg_upload_errors(AggregId),
    ?status_connected.

check_bucket_accessible(Bucket, #{client_config := Config}) ->
    case emqx_s3_client:aws_config(Config) of
        {error, Reason} ->
            throw({unhealthy_target, map_error_details(Reason)});
        AWSConfig ->
            try erlcloud_s3:list_objects(Bucket, [{max_keys, 1}], AWSConfig) of
                Props when is_list(Props) ->
                    ok
            catch
                error:{aws_error, {http_error, 404, _, _Reason}} ->
                    throw({unhealthy_target, "Bucket does not exist"});
                error:Error ->
                    throw({unhealthy_target, map_error_details(Error)})
            end
    end.

check_aggreg_upload_errors(AggregId) ->
    case emqx_connector_aggregator:take_error(AggregId) of
        [Error] ->
            %% TODO
            %% This approach means that, for example, 3 upload failures will cause
            %% the channel to be marked as unhealthy for 3 consecutive health checks.
            throw({unhealthy_target, map_error_details(Error)});
        [] ->
            ok
    end.

%% Queries

-type query() :: {_Tag :: channel_id(), _Data :: emqx_jsonish:t()}.

-spec on_query(_InstanceId :: resource_id(), query(), state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(InstId, {Tag, Data}, #{client_config := Config, channels := Channels}) ->
    case maps:get(Tag, Channels, undefined) of
        ChannelState = #{mode := direct} ->
            run_simple_upload(InstId, Tag, Data, ChannelState, Config);
        ChannelState = #{mode := aggregated} ->
            run_aggregated_upload(InstId, Tag, [Data], ChannelState);
        undefined ->
            {error, {unrecoverable_error, {invalid_message_tag, Tag}}}
    end.

-spec on_batch_query(_InstanceId :: resource_id(), [query()], state()) ->
    {ok, _Result} | {error, _Reason}.
on_batch_query(InstId, [{Tag, Data0} | Rest], #{channels := Channels}) ->
    case maps:get(Tag, Channels, undefined) of
        ChannelState = #{mode := aggregated} ->
            Records = [Data0 | [Data || {_, Data} <- Rest]],
            run_aggregated_upload(InstId, Tag, Records, ChannelState);
        undefined ->
            {error, {unrecoverable_error, {invalid_message_tag, Tag}}}
    end.

run_simple_upload(
    InstId,
    ChannelID,
    Data,
    #{
        bucket := BucketTemplate,
        key := KeyTemplate,
        content := ContentTemplate,
        upload_options := UploadOpts
    },
    Config
) ->
    Bucket = render_bucket(BucketTemplate, Data),
    Client = emqx_s3_client:create(Bucket, Config),
    Key = render_key(KeyTemplate, Data),
    Content = render_content(ContentTemplate, Data),
    emqx_trace:rendered_action_template(ChannelID, #{
        bucket => Bucket,
        key => Key,
        content => #emqx_trace_format_func_data{
            function = fun unicode:characters_to_binary/1,
            data = Content
        }
    }),
    case emqx_s3_client:put_object(Client, Key, UploadOpts, Content) of
        ok ->
            ?tp(s3_bridge_connector_upload_ok, #{
                instance_id => InstId,
                bucket => Bucket,
                key => Key
            }),
            ok;
        {error, Reason} ->
            {error, map_error(Reason)}
    end.

run_aggregated_upload(InstId, ChannelID, Records, #{aggreg_id := AggregId}) ->
    Timestamp = erlang:system_time(second),
    emqx_trace:rendered_action_template(ChannelID, #{
        mode => aggregated,
        records => Records
    }),
    case emqx_connector_aggregator:push_records(AggregId, Timestamp, Records) of
        ok ->
            ?tp(s3_bridge_aggreg_push_ok, #{instance_id => InstId, name => AggregId}),
            ok;
        {error, Reason} ->
            {error, {unrecoverable_error, emqx_utils:explain_posix(Reason)}}
    end.

map_error(Error) ->
    {map_error_class(Error), map_error_details(Error)}.

map_error_class({s3_error, _, _}) ->
    unrecoverable_error;
map_error_class({aws_error, Error}) ->
    map_error_class(Error);
map_error_class({socket_error, _}) ->
    recoverable_error;
map_error_class({http_error, Status, _, _}) when Status >= 500 ->
    %% https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
    recoverable_error;
map_error_class(_Error) ->
    unrecoverable_error.

map_error_details({s3_error, Code, Message}) ->
    emqx_utils:format("S3 error: ~s ~s", [Code, Message]);
map_error_details({aws_error, Error}) ->
    map_error_details(Error);
map_error_details({socket_error, Reason}) ->
    emqx_utils:format("Socket error: ~s", [emqx_utils:readable_error_msg(Reason)]);
map_error_details({http_error, _, _, _} = Error) ->
    emqx_utils:format("AWS error: ~s", [map_aws_error_details(Error)]);
map_error_details({failed_to_obtain_credentials, Error}) ->
    emqx_utils:format("Unable to obtain AWS credentials: ~s", [map_error_details(Error)]);
map_error_details({upload_failed, Error}) ->
    map_error_details(Error);
map_error_details(Error) ->
    Error.

render_bucket(Template, Data) ->
    case emqx_template:render(Template, {emqx_jsonish, Data}) of
        {Result, []} ->
            iolist_to_string(Result);
        {_, Errors} ->
            erlang:error({unrecoverable_error, {bucket_undefined, Errors}})
    end.

render_key(Template, Data) ->
    %% NOTE: ignoring errors here, missing variables will be rendered as `"undefined"`.
    {Result, _Errors} = emqx_template:render(Template, {emqx_jsonish, Data}),
    iolist_to_string(Result).

render_content(Template, Data) ->
    %% NOTE: ignoring errors here, missing variables will be rendered as `"undefined"`.
    {Result, _Errors} = emqx_template:render(Template, {emqx_jsonish, Data}),
    Result.

iolist_to_string(IOList) ->
    unicode:characters_to_list(IOList).

%%

-include_lib("xmerl/include/xmerl.hrl").

-spec map_aws_error_details(_AWSError) ->
    unicode:chardata().
map_aws_error_details({http_error, _Status, _, Body}) ->
    try xmerl_scan:string(unicode:characters_to_list(Body), [{quiet, true}]) of
        {Error = #xmlElement{name = 'Error'}, _} ->
            map_aws_error_details(Error);
        _ ->
            Body
    catch
        exit:_ ->
            Body
    end;
map_aws_error_details(#xmlElement{content = Content}) ->
    Code = extract_xml_text(lists:keyfind('Code', #xmlElement.name, Content)),
    Message = extract_xml_text(lists:keyfind('Message', #xmlElement.name, Content)),
    [Code, $:, $\s | Message].

extract_xml_text(#xmlElement{content = Content}) ->
    [Fragment || #xmlText{value = Fragment} <- Content];
extract_xml_text(false) ->
    [].

%% `emqx_connector_aggreg_delivery` APIs

-spec init_transfer_state(buffer(), map()) -> emqx_s3_upload:t().
init_transfer_state(Buffer, Opts) ->
    #{
        bucket := Bucket,
        upload_options := UploadOpts,
        client_config := Config,
        uploader_config := UploaderConfig
    } = Opts,
    Client = emqx_s3_client:create(Bucket, Config),
    Key = mk_object_key(Buffer, Opts),
    emqx_s3_upload:new(Client, Key, UploadOpts, UploaderConfig).

mk_object_key(Buffer, #{action := AggregId, key := Template}) ->
    emqx_template:render_strict(Template, {?MODULE, {AggregId, Buffer}}).

process_append(Writes, Upload0) ->
    {ok, Upload} = emqx_s3_upload:append(Writes, Upload0),
    Upload.

process_write(Upload0) ->
    case emqx_s3_upload:write(Upload0) of
        {ok, Upload} ->
            {ok, Upload};
        {cont, Upload} ->
            process_write(Upload);
        {error, Reason} ->
            _ = emqx_s3_upload:abort(Upload0),
            {error, Reason}
    end.

process_complete(Upload) ->
    case emqx_s3_upload:complete(Upload) of
        {ok, Completed} ->
            {ok, Completed};
        {error, Reason} ->
            _ = emqx_s3_upload:abort(Upload),
            exit({upload_failed, Reason})
    end.

process_format_status(Upload) ->
    emqx_s3_upload:format(Upload).

process_terminate(Upload) ->
    emqx_s3_upload:abort(Upload).

%% `emqx_template` APIs

-spec lookup(emqx_template:accessor(), {_Name, buffer()}) ->
    {ok, integer() | string()} | {error, undefined}.
lookup([<<"action">>], {_AggregId = {_Type, Name}, _Buffer}) ->
    {ok, mk_fs_safe_string(Name)};
lookup([<<"node">>], {_AggregId, _Buffer}) ->
    {ok, mk_fs_safe_string(atom_to_binary(erlang:node()))};
lookup(Accessor, {_AggregId, Buffer}) ->
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
