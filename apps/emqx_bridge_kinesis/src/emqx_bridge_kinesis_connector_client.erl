%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis_connector_client).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-behaviour(gen_server).

-type state() :: #{
    instance_id := resource_id(),
    %% Unresolved: without static credentials, they are obtained for each request.
    aws_config := aws_config(),
    assume_role := emqx_bridge_kinesis_sts:t() | undefined,
    %% Holds the temporary credentials of the assumed role.
    assumed_aws_config := aws_config() | undefined
}.
-type record() :: {Data :: binary(), PartitionKey :: binary()}.

-define(DEFAULT_PORT, 443).
%% Assume the role again when the temporary credentials expire in less than this.
-define(ASSUMED_ROLE_REFRESH_MARGIN_S, 300).

%% API
-export([
    start_link/1,
    connection_status/1,
    connection_status/2,
    query/3,
    check_credentials/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ifdef(TEST).
-export([execute/3]).
-endif.

%% The default timeout for Kinesis API calls is 10 seconds,
%% but this value for `gen_server:call` is 5s,
%% so we should adjust timeout for `gen_server:call`
-ifdef(TEST).
-define(HEALTH_CHECK_TIMEOUT, 1_000).
-else.
-define(HEALTH_CHECK_TIMEOUT, 15_000).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

-spec connection_status(pid()) -> {ok, ?status_connected} | {error, timeout | term()}.
connection_status(Pid) ->
    try
        gen_server:call(Pid, connection_status, ?HEALTH_CHECK_TIMEOUT)
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

-spec connection_status(pid(), binary()) ->
    {ok, ?status_connected} | {error, timeout | unhealthy_target | term()}.
connection_status(Pid, StreamName) ->
    try
        gen_server:call(Pid, {connection_status, StreamName}, ?HEALTH_CHECK_TIMEOUT)
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

query(Pid, Records, StreamName) ->
    gen_server:call(Pid, {query, Records, StreamName}, infinity).

%% @doc Checks that credentials can be obtained, when they are not configured statically.
-spec check_credentials(emqx_bridge_kinesis_impl_producer:config_connector()) ->
    ok
    | {error, {failed_to_obtain_credentials, term()}}
    | {error, {failed_to_assume_role, term()}}.
check_credentials(Config) ->
    maybe
        {{ok, _}, _State} ?= resolve(new_state(Config)),
        ok
    else
        {Error, _} -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts Bridge which communicates to Amazon Kinesis Data Streams
%% @end
%%--------------------------------------------------------------------
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% Initialize kinesis connector
-spec init(emqx_bridge_kinesis_impl_producer:config_connector()) ->
    {ok, state()} | {stop, Reason :: term()}.
init(Config) ->
    process_flag(trap_exit, true),
    %% Leave checking the connection to health checks
    {ok, new_state(Config)}.

handle_call({connection_status, StreamName}, _From, State0) ->
    {Resolved, State} = resolve(State0),
    Status =
        maybe
            {ok, AWSConfig} ?= Resolved,
            get_status(StreamName, AWSConfig)
        end,
    {reply, Status, State};
handle_call(connection_status, _From, State0) ->
    {Resolved, State} = resolve(State0),
    Status =
        maybe
            {ok, AWSConfig} ?= Resolved,
            {ok, _ListStreamsResult} ?= erlcloud_kinesis:list_streams(<<".">>, 1, AWSConfig),
            {ok, ?status_connected}
        end,
    {reply, Status, State};
handle_call({query, Records, StreamName}, _From, State0) ->
    {Resolved, State} = resolve(State0),
    Result = do_query(StreamName, Records, Resolved),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, #{instance_id := InstanceId} = _State) ->
    ?tp(kinesis_stop, #{instance_id => InstanceId, reason => Reason}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_state(#{instance_id := InstanceId} = Config) ->
    #{
        instance_id => InstanceId,
        aws_config => new_aws_config(Config),
        assume_role => new_assume_role(Config),
        assumed_aws_config => undefined
    }.

new_assume_role(#{assume_role_arn := RoleArn, endpoint := Endpoint} = Config) when
    is_binary(RoleArn), RoleArn =/= <<>>
->
    StsEndpoint =
        case maps:get(sts_endpoint, Config, undefined) of
            StsEndpoint0 when is_binary(StsEndpoint0), StsEndpoint0 =/= <<>> ->
                StsEndpoint0;
            _ ->
                #{hostname := Host} =
                    emqx_schema:parse_server(Endpoint, #{
                        default_port => ?DEFAULT_PORT,
                        supported_schemes => ["http", "https"]
                    }),
                emqx_bridge_kinesis_sts:default_endpoint(Host)
        end,
    ExternalId = maps:get(assume_role_external_id, Config, undefined),
    emqx_bridge_kinesis_sts:new(RoleArn, ExternalId, StsEndpoint);
new_assume_role(_Config) ->
    undefined.

new_aws_config(#{endpoint := Endpoint, max_retries := MaxRetries} = Config) ->
    #{scheme := Scheme, hostname := Host, port := Port} =
        emqx_schema:parse_server(
            Endpoint,
            #{
                default_port => ?DEFAULT_PORT,
                supported_schemes => ["http", "https"]
            }
        ),
    {AccessKeyID, SecretAccessKey} = static_credentials(Config),
    #aws_config{
        access_key_id = AccessKeyID,
        secret_access_key = SecretAccessKey,
        kinesis_host = Host,
        kinesis_port = Port,
        kinesis_scheme = Scheme ++ "://",
        retry_num = MaxRetries
    }.

static_credentials(#{aws_access_key_id := AccessKeyID, aws_secret_access_key := Secret}) when
    is_binary(AccessKeyID), AccessKeyID =/= <<>>
->
    %% TODO: teach `erlcloud` to to accept 0-arity closures as passwords.
    {to_str(AccessKeyID), to_str(emqx_secret:unwrap(Secret))};
static_credentials(_Config) ->
    {undefined, undefined}.

%% Without static credentials, erlcloud obtains them from the ECS task role or EC2 instance
%% metadata and caches them node-wide until shortly before they expire.  The config kept in
%% the state stays unresolved so that they get refreshed, and each request gets a resolved
%% copy, which erlcloud then uses as is.  Resolving here rather than inside erlcloud tells
%% failures to obtain credentials apart from Kinesis API errors.
resolve_aws_config(AWSConfig) ->
    case erlcloud_aws:update_config(AWSConfig) of
        {ok, _ResolvedAWSConfig} = Ok ->
            Ok;
        {error, Reason} ->
            {error, {failed_to_obtain_credentials, Reason}}
    end.

%% Returns the config to sign a request with, and the state with the cached temporary
%% credentials of the assumed role, if any.
resolve(#{assume_role := undefined, aws_config := AWSConfig} = State) ->
    {resolve_aws_config(AWSConfig), State};
resolve(#{assumed_aws_config := #aws_config{expiration = Expiration} = AWSConfig} = State) when
    is_integer(Expiration)
->
    case Expiration - erlang:system_time(second) > ?ASSUMED_ROLE_REFRESH_MARGIN_S of
        true ->
            {{ok, AWSConfig}, State};
        false ->
            assume_role(State)
    end;
resolve(State) ->
    assume_role(State).

assume_role(#{instance_id := InstanceId, aws_config := AWSConfig0, assume_role := Opts} = State) ->
    maybe
        {ok, BaseAWSConfig} ?= resolve_aws_config(AWSConfig0),
        {ok, Credentials} ?= emqx_bridge_kinesis_sts:assume_role(Opts, BaseAWSConfig),
        #{
            access_key_id := AccessKeyID,
            secret_access_key := SecretAccessKey,
            session_token := SessionToken,
            expiration := Expiration
        } = Credentials,
        AWSConfig = AWSConfig0#aws_config{
            access_key_id = AccessKeyID,
            secret_access_key = SecretAccessKey,
            security_token = SessionToken,
            expiration = Expiration
        },
        ?tp(kinesis_role_assumed, #{instance_id => InstanceId, expiration => Expiration}),
        {{ok, AWSConfig}, State#{assumed_aws_config := AWSConfig}}
    else
        {error, _} = Error ->
            ?tp(kinesis_assume_role_failed, #{instance_id => InstanceId, error => Error}),
            {Error, State#{assumed_aws_config := undefined}}
    end.

get_status(StreamName, AWSConfig) ->
    case erlcloud_kinesis:describe_stream(StreamName, 1, AWSConfig) of
        {ok, _} ->
            {ok, ?status_connected};
        {error, {<<"ResourceNotFoundException">>, _}} ->
            {error, unhealthy_target};
        {error, Error} ->
            {error, Error}
    end.

-spec do_query(binary(), [record()], {ok, aws_config()} | {error, term()}) ->
    {ok, jsx:json_term() | binary()}
    | {error, {recoverable_error, term()}}
    | {error, {unrecoverable_error, term()}}
    | {error, term()}.
do_query(StreamName, Records, Resolved) ->
    case Resolved of
        {ok, AWSConfig} ->
            try
                execute(put_record, {StreamName, Records}, AWSConfig)
            catch
                _Type:Reason ->
                    {error, {unrecoverable_error, {invalid_request, Reason}}}
            end;
        {error, Reason} ->
            {error, {recoverable_error, Reason}}
    end.

-spec execute(put_record, {binary(), [record()]}, aws_config()) ->
    {ok, jsx:json_term() | binary()}
    | {error, term()}.
execute(put_record, {StreamName, [{Data, PartitionKey}] = Record}, AWSConfig) ->
    Result = erlcloud_kinesis:put_record(StreamName, PartitionKey, Data, AWSConfig),
    ?tp(kinesis_put_record, #{records => Record, result => Result}),
    Result;
execute(put_record, {StreamName, Items}, AWSConfig) when is_list(Items) ->
    Result = erlcloud_kinesis:put_records(StreamName, Items, AWSConfig),
    ?tp(kinesis_put_record, #{records => Items, result => Result}),
    Result.

-spec to_str(list() | binary()) -> list().
to_str(List) when is_list(List) ->
    List;
to_str(Bin) when is_binary(Bin) ->
    erlang:binary_to_list(Bin).
