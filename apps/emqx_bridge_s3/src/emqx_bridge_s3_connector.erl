%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_connector).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-behaviour(emqx_resource).
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_query/3,
    % on_batch_query/3,
    on_get_status/2,
    on_get_channel_status/3
]).

-type config() :: #{
    access_key_id => string(),
    secret_access_key => emqx_secret:t(string()),
    host := string(),
    port := pos_integer(),
    transport_options => emqx_s3:transport_options()
}.

-type channel_config() :: #{
    parameters := #{
        bucket := string(),
        key := string(),
        content := string(),
        acl => emqx_s3:acl()
    }
}.

-type channel_state() :: #{
    bucket := emqx_template:str(),
    key := emqx_template:str(),
    upload_options => emqx_s3_client:upload_options()
}.

-type state() :: #{
    pool_name := resource_id(),
    pool_pid => pid(),
    client_config := emqx_s3_client:config(),
    channels := #{channel_id() => channel_state()}
}.

%%

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
    HttpConfig = emqx_s3_profile_conf:http_config(Config),
    _ = ehttpc_sup:stop_pool(PoolName),
    case ehttpc_sup:start_pool(PoolName, HttpConfig) of
        {ok, Pid} ->
            ?SLOG(info, #{msg => "s3_connector_start_http_pool_success", pool_name => PoolName}),
            {ok, State#{pool_pid => Pid}};
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "s3_connector_start_http_pool_fail",
                pool_name => PoolName,
                http_config => HttpConfig,
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
on_get_status(_InstId, State = #{client_config := Config}) ->
    case emqx_s3_client:aws_config(Config) of
        {error, Reason} ->
            {?status_disconnected, State, Reason};
        AWSConfig ->
            try erlcloud_s3:list_buckets(AWSConfig) of
                Props when is_list(Props) ->
                    ?status_connected
            catch
                error:{aws_error, {http_error, _Code, _, Reason}} ->
                    {?status_disconnected, State, Reason};
                error:{aws_error, {socket_error, Reason}} ->
                    {?status_disconnected, State, Reason}
            end
    end.

-spec on_add_channel(_InstanceId :: resource_id(), state(), channel_id(), channel_config()) ->
    {ok, state()} | {error, _Reason}.
on_add_channel(_InstId, State = #{channels := Channels}, ChannelId, Config) ->
    ChannelState = init_channel_state(Config),
    {ok, State#{channels => Channels#{ChannelId => ChannelState}}}.

-spec on_remove_channel(_InstanceId :: resource_id(), state(), channel_id()) ->
    {ok, state()}.
on_remove_channel(_InstId, State = #{channels := Channels}, ChannelId) ->
    {ok, State#{channels => maps:remove(ChannelId, Channels)}}.

-spec on_get_channels(_InstanceId :: resource_id()) ->
    [_ChannelConfig].
on_get_channels(InstId) ->
    emqx_bridge_v2:get_channels_for_connector(InstId).

-spec on_get_channel_status(_InstanceId :: resource_id(), channel_id(), state()) ->
    channel_status().
on_get_channel_status(_InstId, ChannId, State) ->
    %% TODO
    %% Since bucket name may be templated, we can't really provide any
    %% additional information regarding the channel health.
    emqx_resource:channel_status(ChannId, channels, State).

init_channel_state(#{parameters := Parameters}) ->
    #{
        bucket => emqx_template:parse(maps:get(bucket, Parameters)),
        key => emqx_template:parse(maps:get(key, Parameters)),
        content => emqx_template:parse(maps:get(content, Parameters)),
        upload_options => #{
            acl => maps:get(acl, Parameters, undefined)
        }
    }.

%% Queries

-type query() :: {_Tag :: channel_id(), _Data :: emqx_jsonish:t()}.

-spec on_query(_InstanceId :: resource_id(), query(), state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(InstId, {Tag, Data}, #{client_config := Config, channels := Channels}) ->
    case maps:get(Tag, Channels, undefined) of
        ChannelState = #{} ->
            run_simple_upload(InstId, Data, ChannelState, Config);
        undefined ->
            {error, {unrecoverable_error, {invalid_message_tag, Tag}}}
    end.

run_simple_upload(
    InstId,
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

map_error({socket_error, _} = Reason) ->
    {recoverable_error, Reason};
map_error(Reason = {aws_error, Status, _, _Body}) when Status >= 500 ->
    %% https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
    {recoverable_error, Reason};
map_error(Reason) ->
    {unrecoverable_error, Reason}.

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
