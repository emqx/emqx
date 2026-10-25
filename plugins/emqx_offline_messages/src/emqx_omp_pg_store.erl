%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_store).
-moduledoc "Fixed PostgreSQL statements, connection pools and the versioned MQTT row encoding.".

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include("emqx_omp_pg.hrl").

-export([connect/1, install/1, run/2, encode/1, decode/3, dispatch/3, query/3]).
-export([pool_spec/3, call/4, cast/3, async/5]).
-export([exec/2, exec_async/2, reply/3]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

pool_spec(Pool, Size, Config) ->
    Options = [{pool_size, Size}, {pool_type, hash}, {auto_reconnect, 1}, {config, Config}],
    ecpool:pool_spec(Pool, Pool, ?MODULE, Options).

-doc "Run an operation on the pool worker selected by `Key` and wait for the result.".
call(Pool, Key, Operation, #{query_timeout := Timeout}) ->
    try
        ecpool:pick_and_do({Pool, Key}, {?MODULE, exec, [Operation]}, {handover, Timeout + 1000})
    catch
        exit:Reason -> {error, Reason}
    end.

-doc "Run an operation without waiting for the result.".
cast(Pool, Key, Operation) ->
    _ = ecpool:pick_and_do({Pool, Key}, {?MODULE, exec, [Operation]}, handover_async),
    ok.

-doc """
Run an operation and send `{pg_result, Ref, Result}` to the handler.
When the pool is missing or empty, send the error at once.
The pool sends no result when its connection is down, so the caller needs a timeout.
""".
async(Pool, Key, Operation, Target, Ref) ->
    Callback = {?MODULE, reply, [Target, Ref]},
    case
        ecpool:pick_and_do({Pool, Key}, {?MODULE, exec_async, [Operation]}, {
            handover_async, Callback
        })
    of
        {error, _} = Error -> reply({done, Error}, Target, Ref);
        _ -> ok
    end.

connect(Options) when is_list(Options) ->
    connect(proplists:get_value(config, Options));
connect(#{connection := Options} = Config) ->
    case epgsql:connect(Options) of
        {ok, Conn} ->
            ok = set_statement_timeout(Conn, Config),
            {ok, Conn};
        Error ->
            Error
    end.

install(Config) ->
    {ok, Conn} = connect(Config),
    try
        {ok, SQL} = file:read_file(
            filename:join(code:priv_dir(emqx_offline_messages), "postgresql.sql")
        ),
        Results = epgsql:squery(Conn, SQL),
        [] = [Error || {error, _} = Error <- Results],
        ok
    after
        unlink(Conn),
        epgsql:close(Conn)
    end.

exec(Conn, Operation) ->
    try
        run(Conn, Operation)
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

%% The pool skips the callback for an `{error, _}` result, so wrap the result.
exec_async(Conn, Operation) ->
    {done, exec(Conn, Operation)}.

reply({done, Result}, Target, Ref) ->
    emqx_extsub_handler:send(Target, {pg_result, Ref, Result}).

run(Conn, health) ->
    {ok, [{1}]} = query(Conn, "SELECT version FROM mqtt_pg_schema", []),
    ok;
run(Conn, {append, Namespace, Topic, Messages}) ->
    {ok, [{Id, Offset}]} = query(Conn, "SELECT * FROM omp_append($1, $2, $3::jsonb)", [
        Namespace, Topic, emqx_utils_json:encode(Messages)
    ]),
    {ok, {Id, Offset}};
run(Conn, {subscribe, Namespace, ClientId, Topic, SubOpts, Start}) ->
    %% The share lock on the topic row blocks appends until the subscription row commits.
    %% No subscriber can complete an offset above the tip before cleanup sees this row.
    transaction(Conn, fun() ->
        {ok, _} = query(
            Conn,
            "INSERT INTO mqtt_pg_topic(namespace, topic) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            [Namespace, Topic]
        ),
        {ok, [{Id, Next}]} = query(
            Conn,
            """
            SELECT topic_id, next_offset FROM mqtt_pg_topic
            WHERE namespace = $1 AND topic = $2 FOR SHARE
            """,
            [Namespace, Topic]
        ),
        Ack =
            case Start of
                earliest -> 0;
                latest -> Next - 1
            end,
        %% Create the subscription, or keep the progress of an existing one.
        {ok, [{Cursor}]} = query(Conn, """
        INSERT INTO mqtt_pg_subscription(namespace, client_id, topic_id, subopts, ack_offset)
        VALUES ($1, $2, $3, $4::jsonb, $5)
        ON CONFLICT (namespace, client_id, topic_id) DO UPDATE
        SET subopts = EXCLUDED.subopts, updated_at = now()
        RETURNING ack_offset
        """, [Namespace, ClientId, Id, emqx_utils_json:encode(SubOpts), Ack]),
        {ok, #{topic_id => Id, cursor => Cursor}}
    end);
run(Conn, {restore, Namespace, ClientId}) ->
    %% Renew the client's subscriptions and return their topics and options.
    query(Conn, """
    WITH touched AS (
      UPDATE mqtt_pg_subscription SET updated_at = now()
      WHERE namespace = $1 AND client_id = $2 RETURNING topic_id, subopts
    ) SELECT t.topic, s.subopts FROM touched s JOIN mqtt_pg_topic t USING (topic_id)
    """, [Namespace, ClientId]);
run(Conn, {unsubscribe, Namespace, ClientId, Topic}) ->
    query(Conn, """
    DELETE FROM mqtt_pg_subscription s USING mqtt_pg_topic t
    WHERE s.topic_id = t.topic_id AND s.namespace = $1 AND s.client_id = $2 AND t.topic = $3
    """, [Namespace, ClientId, Topic]);
run(Conn, {checkpoint, Namespace, ClientId, Id, Offset}) ->
    %% Progress never moves backwards. The update also renews the subscription.
    query(Conn, """
    UPDATE mqtt_pg_subscription SET ack_offset = GREATEST(ack_offset, $4), updated_at = now()
    WHERE namespace = $1 AND client_id = $2 AND topic_id = $3
    """, [Namespace, ClientId, Id, Offset]);
run(Conn, {page, Id, Cursor, Count}) ->
    %% Return the next rows after the cursor with the topic tip in every row.
    %% The left join returns one row with null message fields when no rows follow.
    query(Conn, """
    SELECT t.next_offset - 1, m.msg_offset, m.message_id, m.sender, m.qos, m.retain,
           m.payload, m.properties, m.published_at, m.expires_at
    FROM mqtt_pg_topic t LEFT JOIN LATERAL (
      SELECT * FROM mqtt_pg_message
      WHERE topic_id = t.topic_id AND msg_offset > $2 ORDER BY msg_offset LIMIT $3
    ) m ON true
    WHERE t.topic_id = $1 ORDER BY m.msg_offset
    """, [Id, Cursor, Count]);
run(Conn, {topics, Namespace, After, Limit}) ->
    query(Conn, """
    SELECT topic_id, next_offset FROM mqtt_pg_topic
    WHERE namespace = $1 AND topic_id > $2 ORDER BY topic_id LIMIT $3
    """, [Namespace, After, Limit]);
run(Conn, {age_cutoff, Id, Next, Oldest}) ->
    %% The first message that is not too old. The scan stops at the end of the expired prefix.
    case query(Conn, """
    SELECT msg_offset FROM mqtt_pg_message
    WHERE topic_id = $1 AND published_at >= $2 ORDER BY msg_offset LIMIT 1
    """, [Id, Oldest]) of
        {ok, [{Offset}]} -> {ok, Offset};
        {ok, []} -> {ok, Next}
    end;
run(Conn, {consumed_cutoff, Id, Next}) ->
    %% A subscribe transaction blocks appends until its row commits, so no subscriber can
    %% complete an offset above the new subscription's start before this query sees it.
    case
        query(Conn, "SELECT min(ack_offset) FROM mqtt_pg_subscription WHERE topic_id = $1", [Id])
    of
        {ok, [{null}]} -> {ok, Next};
        {ok, [{Acked}]} -> {ok, Acked + 1}
    end;
run(Conn, {trim, Id, Cutoff, Batch}) ->
    %% Delete the messages below the cutoff in primary key ranges of at most `Batch` offsets.
    case query(Conn, """
    SELECT min(msg_offset), max(msg_offset) FROM mqtt_pg_message
    WHERE topic_id = $1 AND msg_offset < $2
    """, [Id, Cutoff]) of
        {ok, [{null, null}]} -> {ok, 0};
        {ok, [{Min, Max}]} -> trim(Conn, Id, Min, Max, Batch, 0)
    end;
run(Conn, {expire, Namespace, Expiry, Batch}) ->
    %% Delete subscriptions that no client renewed within the expiry time.
    query(Conn, """
    DELETE FROM mqtt_pg_subscription WHERE ctid IN (
      SELECT ctid FROM mqtt_pg_subscription
      WHERE namespace = $1 AND updated_at < now() - $2 * interval '1 millisecond'
      LIMIT $3
    )
    """, [Namespace, Expiry, Batch]).

-doc "Send the live copy of a committed message to the subscribers of its topic.".
dispatch(#message{topic = Topic} = Msg, {Namespace, Id}, Offset) ->
    emqx_broker:dispatch_info(Topic, ?OMP_LIVE(Namespace, Id, Offset, Msg)).

encode(#message{
    id = Id,
    from = From,
    qos = QoS,
    flags = Flags,
    payload = Payload,
    timestamp = Published,
    headers = Headers
}) ->
    Props = maps:get(properties, Headers, #{}),
    Expiry =
        case Props of
            #{'Message-Expiry-Interval' := Seconds} -> Published + Seconds * 1000;
            _ -> null
        end,
    #{
        message_id => base64:encode(Id),
        sender => sender(From),
        qos => QoS,
        retain => maps:get(retain, Flags, false),
        payload => base64:encode(Payload),
        properties => #{version => 1, values => encode_properties(Props)},
        published_at => Published,
        expires_at => Expiry
    }.

decode(
    Topic,
    {Namespace, TopicId},
    {_, Offset, Id, Sender, _QoS, Retain, Payload, Properties, Published, Expiry}
) ->
    try
        #{<<"version">> := 1, <<"values">> := Values} = emqx_utils_json:decode(Properties),
        Props0 = decode_properties(Values),
        Props =
            case Expiry of
                null -> Props0;
                _ -> Props0#{'Message-Expiry-Interval' => max(0, (Expiry - Published) div 1000)}
            end,
        ok = emqx_mqtt_props:validate(Props),
        MsgId =
            case Id of
                <<_:128>> -> Id;
                _ -> emqx_guid:gen()
            end,
        Msg = #message{
            id = MsgId,
            from = Sender,
            qos = ?QOS_1,
            topic = Topic,
            payload = Payload,
            timestamp = Published,
            flags = #{retain => Retain},
            headers = #{
                properties => Props,
                sub_topic => Topic,
                ?OMP_POSITION => {Namespace, TopicId, Offset}
            }
        },
        {ok, Msg}
    catch
        _:_ -> {error, invalid_row}
    end.

query(Conn, SQL, Params) ->
    case epgsql:equery(Conn, SQL, Params) of
        {ok, _Columns, Rows} -> {ok, Rows};
        {ok, Count} -> {ok, Count};
        {ok, _Count, _Columns, Rows} -> {ok, Rows};
        {error, Error} -> error({postgresql, Error})
    end.

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

transaction(Conn, Fun) ->
    epgsql:with_transaction(Conn, fun(_) -> Fun() end, [{reraise, true}]).

trim(_Conn, _Id, Min, Max, _Batch, Removed) when Min > Max ->
    {ok, Removed};
trim(Conn, Id, Min, Max, Batch, Removed0) ->
    Last = min(Max, Min + Batch - 1),
    {ok, Count} = query(Conn, """
    DELETE FROM mqtt_pg_message WHERE topic_id = $1 AND msg_offset BETWEEN $2 AND $3
    """, [Id, Min, Last]),
    trim(Conn, Id, Last + 1, Max, Batch, Removed0 + Count).

%% Without this, the database, role or server default applies.
set_statement_timeout(Conn, #{set_statement_timeout := true, query_timeout := Timeout}) ->
    {ok, _, _} = epgsql:equery(Conn, "SELECT set_config('statement_timeout', $1, false)", [
        integer_to_binary(Timeout)
    ]),
    ok;
set_statement_timeout(_Conn, #{set_statement_timeout := false}) ->
    ok.

sender(From) when is_atom(From) -> atom_to_binary(From);
sender(From) when is_binary(From) -> From.

encode_properties(Props) ->
    maps:fold(
        fun
            ('Correlation-Data', Value, Acc) ->
                Acc#{<<"Correlation-Data">> => base64:encode(Value)};
            ('User-Property', Values, Acc) ->
                Acc#{<<"User-Property">> => [[K, V] || {K, V} <- Values]};
            (Key, Value, Acc) ->
                case lists:member(Key, property_keys()) of
                    true -> Acc#{atom_to_binary(Key) => Value};
                    false -> Acc
                end
        end,
        #{},
        Props
    ).

decode_properties(Values) ->
    maps:fold(
        fun
            (<<"Correlation-Data">>, Value, Acc) ->
                Acc#{'Correlation-Data' => base64:decode(Value)};
            (<<"User-Property">>, Values0, Acc) ->
                Acc#{'User-Property' => lists:map(fun([K, V]) -> {K, V} end, Values0)};
            (Key, Value, Acc) ->
                [Atom] = [K || K <- property_keys(), atom_to_binary(K) =:= Key],
                Acc#{Atom => Value}
        end,
        #{},
        Values
    ).

property_keys() ->
    ['Payload-Format-Indicator', 'Content-Type', 'Response-Topic'].
