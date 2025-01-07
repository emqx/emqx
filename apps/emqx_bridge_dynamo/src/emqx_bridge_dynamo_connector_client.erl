%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([execute/3]).
-endif.

-include_lib("emqx/include/emqx_trace.hrl").

%%%===================================================================
%%% API
%%%===================================================================
is_connected(Pid, Timeout) ->
    try
        gen_server:call(Pid, is_connected, Timeout)
    catch
        _:{timeout, _} ->
            {false, <<"timeout_while_checking_connection_dynamo_client">>};
        _:Error ->
            {false, Error}
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
init(#{
    aws_access_key_id := AccessKeyID,
    aws_secret_access_key := Secret,
    host := Host,
    port := Port,
    scheme := Scheme
}) ->
    %% TODO: teach `erlcloud` to to accept 0-arity closures as passwords.
    SecretAccessKey = to_str(emqx_secret:unwrap(Secret)),
    erlcloud_ddb2:configure(AccessKeyID, SecretAccessKey, Host, Port, Scheme),
    {ok, #{}}.

handle_call(is_connected, _From, State) ->
    IsConnected =
        case erlcloud_ddb2:list_tables([{limit, 1}]) of
            {ok, _} ->
                true;
            Error ->
                {false, Error}
        end,
    {reply, IsConnected, State};
handle_call({query, Table, Query, Templates, TraceRenderedCTX, ChannelState}, _From, State) ->
    Result = do_query(Table, Query, Templates, TraceRenderedCTX, ChannelState),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({query, Table, Query, Templates, {ReplyFun, [Context]}, ChannelState}, State) ->
    Result = do_query(Table, Query, Templates, {fun(_, _) -> ok end, none}, ChannelState),
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
do_query(Table, Query0, Templates, TraceRenderedCTX, ChannelState) ->
    try
        Query = apply_template(Query0, Templates, ChannelState),
        emqx_trace:rendered_action_template_with_ctx(TraceRenderedCTX, #{
            table => Table,
            query => #emqx_trace_format_func_data{
                function = fun trace_format_query/1,
                data = Query
            }
        }),
        execute(Query, Table, ChannelState)
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
execute({insert_item, Msg}, Table, ChannelState) ->
    Item = convert_to_item(Msg, ChannelState),
    erlcloud_ddb2:put_item(Table, Item);
execute({delete_item, Key}, Table, _) ->
    erlcloud_ddb2:delete_item(Table, Key);
execute({get_item, Key}, Table, _) ->
    erlcloud_ddb2:get_item(Table, Key);
%% commands for data bridge query or batch query
execute({send_message, Msg}, Table, ChannelState) ->
    Item = convert_to_item(Msg, ChannelState),
    erlcloud_ddb2:put_item(Table, Item);
execute([{put, _} | _] = Msgs, Table, _) ->
    %% type of batch_write_item argument :: batch_write_item_request_items()
    %% batch_write_item_request_items() :: maybe_list(batch_write_item_request_item())
    %% batch_write_item_request_item() :: {table_name(), list(batch_write_item_request())}
    %% batch_write_item_request() :: {put, item()} | {delete, key()}
    erlcloud_ddb2:batch_write_item({Table, Msgs}).

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
