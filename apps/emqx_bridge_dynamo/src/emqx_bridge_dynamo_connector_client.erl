%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_dynamo_connector_client).

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    is_connected/2,
    query/6
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
-export([execute/4]).
-endif.

-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-type state() :: #{aws_config := #aws_config{}}.

%%%===================================================================
%%% API
%%%===================================================================
is_connected(Pid, Timeout) ->
    try
        gen_server:call(Pid, is_connected, Timeout)
    catch
        _:{timeout, _} ->
            {error, <<"timeout_while_checking_connection_dynamo_client">>};
        _:Error ->
            {error, Error}
    end.

query(Pid, Table, Query, Templates, TraceRenderedCTX, ChannelState) ->
    gen_server:call(
        Pid, {query, Table, Query, Templates, TraceRenderedCTX, ChannelState}, infinity
    ).

%%--------------------------------------------------------------------
%% @doc
%% Starts Bridge which transfer data to DynamoDB
%% @endn
%%--------------------------------------------------------------------
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% Initialize dynamodb data bridge
-spec init(map()) -> {ok, state()} | {stop, term()}.
init(#{
    aws_access_key_id := AccessKeyID,
    aws_secret_access_key := Secret,
    host := Host,
    port := Port,
    scheme := Scheme
}) ->
    %% TODO: teach `erlcloud` to to accept 0-arity closures as passwords.
    SecretAccessKey = to_str(emqx_secret:unwrap(Secret)),
    AWSConfig = (new_aws_config(Host, Port, Scheme))#aws_config{
        access_key_id = to_str(AccessKeyID),
        secret_access_key = SecretAccessKey
    },
    {ok, #{aws_config => AWSConfig}};
init(#{host := Host, port := Port, scheme := Scheme}) ->
    AWSConfig = new_aws_config(Host, Port, Scheme),
    case check_metadata_credentials_available(AWSConfig) of
        ok ->
            {ok, #{aws_config => AWSConfig}};
        {error, Reason} ->
            {stop, {failed_to_obtain_credentials, Reason}}
    end.

handle_call(is_connected, _From, State = #{aws_config := AWSConfig}) ->
    IsConnected =
        case erlcloud_ddb2:list_tables([{limit, 1}], AWSConfig) of
            {ok, _} ->
                true;
            Error ->
                {false, Error}
        end,
    {reply, IsConnected, State};
handle_call(
    {query, Table, Query, Templates, TraceRenderedCTX, ChannelState},
    _From,
    State = #{aws_config := AWSConfig}
) ->
    Result = do_query(Table, Query, Templates, TraceRenderedCTX, ChannelState, AWSConfig),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(
    {query, Table, Query, Templates, {ReplyFun, [Context]}, ChannelState},
    State = #{aws_config := AWSConfig}
) ->
    Result = do_query(
        Table, Query, Templates, {fun(_, _) -> ok end, none}, ChannelState, AWSConfig
    ),
    ReplyFun(Context, Result),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_query(Table, Query0, Templates, TraceRenderedCTX, ChannelState, AWSConfig) ->
    try
        Query = apply_template(Query0, Templates, ChannelState),
        emqx_trace:rendered_action_template_with_ctx(TraceRenderedCTX, #{
            table => Table,
            query => #emqx_trace_format_func_data{
                function = fun trace_format_query/1,
                data = Query
            }
        }),
        %% Resolve before entering erlcloud's DDB request path so credential-fetch failures
        %% can be classified as recoverable.  Only this request receives the resolved snapshot.
        case erlcloud_aws:update_config(AWSConfig) of
            {ok, ResolvedAWSConfig} ->
                execute(Query, Table, ChannelState, ResolvedAWSConfig);
            {error, CredentialError} ->
                {error, {recoverable_error, {failed_to_obtain_credentials, CredentialError}}}
        end
    catch
        error:{unrecoverable_error, Reason} ->
            {error, {unrecoverable_error, Reason}};
        Err:Reason:ST ->
            {error, {unrecoverable_error, {invalid_request, {Err, Reason, ST}}}}
    end.

trace_format_query({Type, Data}) ->
    #{type => Type, data => Data};
trace_format_query([_ | _] = Batch) ->
    BatchData = [trace_format_query(Q) || Q <- Batch],
    #{type => batch, data => BatchData};
trace_format_query(Query) ->
    Query.

%% some simple query commands for authn/authz or test
execute({insert_item, Msg}, Table, ChannelState, AWSConfig) ->
    Item = convert_to_item(Msg, ChannelState),
    erlcloud_ddb2:put_item(Table, Item, [], AWSConfig);
execute({delete_item, Key}, Table, _, AWSConfig) ->
    erlcloud_ddb2:delete_item(Table, Key, [], AWSConfig);
execute({get_item, Key}, Table, _, AWSConfig) ->
    erlcloud_ddb2:get_item(Table, Key, [], AWSConfig);
%% commands for data bridge query or batch query
execute({send_message, Msg}, Table, ChannelState, AWSConfig) ->
    Item = convert_to_item(Msg, ChannelState),
    erlcloud_ddb2:put_item(Table, Item, [], AWSConfig);
execute([{put, _} | _] = Msgs, Table, _, AWSConfig) ->
    %% type of batch_write_item argument :: batch_write_item_request_items()
    %% batch_write_item_request_items() :: maybe_list(batch_write_item_request_item())
    %% batch_write_item_request_item() :: {table_name(), list(batch_write_item_request())}
    %% batch_write_item_request() :: {put, item()} | {delete, key()}
    erlcloud_ddb2:batch_write_item({Table, Msgs}, [], AWSConfig).

new_aws_config(Host, Port, Scheme) ->
    #aws_config{
        ddb_host = to_str(Host),
        ddb_port = Port,
        ddb_scheme = to_str(Scheme)
    }.

check_metadata_credentials_available(AWSConfig) ->
    %% Keep the AWS config unresolved in the worker state.  Every query resolves a temporary
    %% snapshot explicitly, while the unresolved config remains available for the next query.
    %%
    %% erlcloud caches ECS task-role and EC2 instance-role credentials, including their
    %% expiration, in the node-wide application environment key
    %% `{erlcloud, metadata_credentials}'.  All IAM-role clients in this BEAM node share
    %% that cache.  Calling `update_config/1' here only checks that credentials are initially
    %% available; query-time snapshots must not be retained in the worker state.
    case erlcloud_aws:update_config(AWSConfig) of
        {ok, _ResolvedAWSConfig} -> ok;
        {error, Reason} -> {error, Reason}
    end.

apply_template({Key, Msg} = Req, Templates, _) ->
    case maps:find(Key, Templates) of
        error -> Req;
        {ok, Template} -> {Key, emqx_placeholder:proc_tmpl(Template, Msg)}
    end;
%% now there is no batch delete, so
%% 1. we can simply replace the `send_message` to `put`
%% 2. convert the message to in_item() here, not at the time when calling `batch_write_items`,
%%    so we can reduce some list map cost
apply_template([{_, _Msg} | _] = Msgs, Templates, ChannelState) ->
    lists:map(
        fun(Req) ->
            {_, Msg} = apply_template(Req, Templates, ChannelState),
            {put, convert_to_item(Msg, ChannelState)}
        end,
        Msgs
    ).

convert_to_item(Msg, ChannelState) when is_map(Msg), map_size(Msg) > 0 ->
    maps:fold(
        fun
            (_K, <<>>, AccIn) ->
                AccIn;
            (K, V, AccIn) ->
                [{to_bin(K), val_to_bin(V, ChannelState)} | AccIn]
        end,
        [],
        Msg
    );
convert_to_item(MsgBin, ChannelState) when is_binary(MsgBin) ->
    Msg = emqx_utils_json:decode(MsgBin),
    convert_to_item(Msg, ChannelState);
convert_to_item(Item, _) ->
    erlang:throw({invalid_item, Item}).

val_to_bin(Null, #{undefined_vars_as_null := true}) when
    Null =:= <<"undefined">>;
    Null =:= <<"null">>;
    Null =:= undefined;
    Null =:= null
->
    {null, true};
val_to_bin(Val, _) ->
    to_bin(Val).

to_bin(Value) when is_atom(Value) ->
    erlang:atom_to_binary(Value, utf8);
to_bin(Value) when is_binary(Value); is_number(Value) ->
    Value;
to_bin(Value) when is_list(Value) ->
    unicode:characters_to_binary(Value);
to_bin(Value) when is_map(Value) ->
    emqx_utils_json:encode(Value).

to_str(List) when is_list(List) ->
    List;
to_str(Bin) when is_binary(Bin) ->
    erlang:binary_to_list(Bin).
