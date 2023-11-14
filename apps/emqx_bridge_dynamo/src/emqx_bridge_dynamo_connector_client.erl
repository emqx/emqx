%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_dynamo_connector_client).

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    is_connected/1,
    query/4
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
-export([execute/2]).
-endif.

%% The default timeout for DynamoDB REST API calls is 10 seconds,
%% but this value for `gen_server:call` is 5s,
%% so we should pass the timeout to `gen_server:call`
-define(HEALTH_CHECK_TIMEOUT, 10000).

%%%===================================================================
%%% API
%%%===================================================================
is_connected(Pid) ->
    try
        gen_server:call(Pid, is_connected, ?HEALTH_CHECK_TIMEOUT)
    catch
        _:_ ->
            false
    end.

query(Pid, Table, Query, Templates) ->
    gen_server:call(Pid, {query, Table, Query, Templates}, infinity).

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
    schema := Schema
}) ->
    %% TODO: teach `erlcloud` to to accept 0-arity closures as passwords.
    SecretAccessKey = to_str(emqx_secret:unwrap(Secret)),
    erlcloud_ddb2:configure(AccessKeyID, SecretAccessKey, Host, Port, Schema),
    {ok, #{}}.

handle_call(is_connected, _From, State) ->
    IsConnected =
        case erlcloud_ddb2:list_tables([{limit, 1}]) of
            {ok, _} ->
                true;
            _ ->
                false
        end,
    {reply, IsConnected, State};
handle_call({query, Table, Query, Templates}, _From, State) ->
    Result = do_query(Table, Query, Templates),
    {reply, Result, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({query, Table, Query, Templates, {ReplyFun, [Context]}}, State) ->
    Result = do_query(Table, Query, Templates),
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
do_query(Table, Query0, Templates) ->
    try
        Query = apply_template(Query0, Templates),
        execute(Query, Table)
    catch
        _Type:Reason ->
            {error, {unrecoverable_error, {invalid_request, Reason}}}
    end.

%% some simple query commands for authn/authz or test
execute({insert_item, Msg}, Table) ->
    Item = convert_to_item(Msg),
    erlcloud_ddb2:put_item(Table, Item);
execute({delete_item, Key}, Table) ->
    erlcloud_ddb2:delete_item(Table, Key);
execute({get_item, Key}, Table) ->
    erlcloud_ddb2:get_item(Table, Key);
%% commands for data bridge query or batch query
execute({send_message, Msg}, Table) ->
    Item = convert_to_item(Msg),
    erlcloud_ddb2:put_item(Table, Item);
execute([{put, _} | _] = Msgs, Table) ->
    %% type of batch_write_item argument :: batch_write_item_request_items()
    %% batch_write_item_request_items() :: maybe_list(batch_write_item_request_item())
    %% batch_write_item_request_item() :: {table_name(), list(batch_write_item_request())}
    %% batch_write_item_request() :: {put, item()} | {delete, key()}
    erlcloud_ddb2:batch_write_item({Table, Msgs}).

apply_template({Key, Msg} = Req, Templates) ->
    case maps:get(Key, Templates, undefined) of
        undefined ->
            Req;
        Template ->
            {Key, emqx_placeholder:proc_tmpl(Template, Msg)}
    end;
%% now there is no batch delete, so
%% 1. we can simply replace the `send_message` to `put`
%% 2. convert the message to in_item() here, not at the time when calling `batch_write_items`,
%%    so we can reduce some list map cost
apply_template([{send_message, _Msg} | _] = Msgs, Templates) ->
    lists:map(
        fun(Req) ->
            {_, Msg} = apply_template(Req, Templates),
            {put, convert_to_item(Msg)}
        end,
        Msgs
    ).

convert_to_item(Msg) when is_map(Msg), map_size(Msg) > 0 ->
    maps:fold(
        fun
            (_K, <<>>, AccIn) ->
                AccIn;
            (K, V, AccIn) ->
                [{convert2binary(K), convert2binary(V)} | AccIn]
        end,
        [],
        Msg
    );
convert_to_item(MsgBin) when is_binary(MsgBin) ->
    Msg = emqx_utils_json:decode(MsgBin),
    convert_to_item(Msg);
convert_to_item(Item) ->
    erlang:throw({invalid_item, Item}).

convert2binary(Value) when is_atom(Value) ->
    erlang:atom_to_binary(Value, utf8);
convert2binary(Value) when is_binary(Value); is_number(Value) ->
    Value;
convert2binary(Value) when is_list(Value) ->
    unicode:characters_to_binary(Value);
convert2binary(Value) when is_map(Value) ->
    emqx_utils_json:encode(Value).

to_str(List) when is_list(List) ->
    List;
to_str(Bin) when is_binary(Bin) ->
    erlang:binary_to_list(Bin).
