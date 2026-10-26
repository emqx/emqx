%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_omp_pg_sql).
-moduledoc """
Fixed PostgreSQL statements of the offline message backend.
Each function runs on the given connection and raises an error when a statement fails.
""".

-export([
    health/1,
    append/4,
    subscribe/6,
    restore/3,
    unsubscribe/4,
    checkpoint/5,
    page/4,
    topics/4,
    age_cutoff/4,
    consumed_cutoff/3,
    trim/4,
    expire/4,
    query/3
]).

-export_type([
    namespace/0,
    topic_id/0,
    offset/0,
    start_position/0,
    encoded_message/0,
    page_row/0,
    subscription_row/0
]).

%% Deployment namespace that isolates the rows of brokers sharing a database.
-type namespace() :: binary().
-type topic_id() :: pos_integer().
-type offset() :: non_neg_integer().
-type start_position() :: earliest | latest.
-type timestamp_ms() :: integer().

%% Message fields in the versioned JSON format accepted by `omp_append`.
-type encoded_message() :: #{
    message_id := binary(),
    sender := binary(),
    qos := emqx_types:qos(),
    retain := boolean(),
    payload := binary(),
    properties := #{version := 1, values := #{binary() => term()}},
    published_at := timestamp_ms(),
    expires_at := timestamp_ms() | null
}.

%% The topic tip followed by the message columns, which are `null` when no message follows.
-type page_row() :: {
    Tip :: offset(),
    Offset :: offset() | null,
    MessageId :: binary() | null,
    Sender :: binary() | null,
    QoS :: emqx_types:qos() | null,
    Retain :: boolean() | null,
    Payload :: binary() | null,
    Properties :: binary() | null,
    PublishedAt :: timestamp_ms() | null,
    ExpiresAt :: timestamp_ms() | null
}.

%% A subscribed topic and its JSON-encoded subscription options.
-type subscription_row() :: {emqx_types:topic(), binary()}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Check that the schema version table exists and contains version 1.".
-spec health(epgsql:connection()) -> ok.
health(Conn) ->
    {ok, [{1}]} = query(Conn, "SELECT version FROM mqtt_pg_schema", []),
    ok.

-doc "Append encoded messages with consecutive offsets and return the topic ID and first offset.".
-spec append(epgsql:connection(), namespace(), emqx_types:topic(), [encoded_message()]) ->
    {ok, {topic_id(), offset()}}.
append(Conn, Namespace, Topic, Messages) ->
    {ok, [{Id, Offset}]} = query(Conn, "SELECT * FROM omp_append($1, $2, $3::jsonb)", [
        Namespace, Topic, emqx_utils_json:encode(Messages)
    ]),
    {ok, {Id, Offset}}.

-doc "Create a subscription, or keep the progress of an existing one, and return its cursor.".
-spec subscribe(
    epgsql:connection(),
    namespace(),
    emqx_types:clientid(),
    emqx_types:topic(),
    emqx_types:subopts(),
    start_position()
) -> {ok, #{topic_id := topic_id(), cursor := offset()}}.
subscribe(Conn, Namespace, ClientId, Topic, SubOpts, Start) ->
    %% The share lock on the topic row blocks appends until the subscription row commits.
    %% No subscriber can acknowledge an offset above the tip before cleanup sees this row.
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
        {ok, [{Cursor}]} = query(Conn, """
        INSERT INTO mqtt_pg_subscription(namespace, client_id, topic_id, subopts, ack_offset)
        VALUES ($1, $2, $3, $4::jsonb, $5)
        ON CONFLICT (namespace, client_id, topic_id) DO UPDATE
        SET subopts = EXCLUDED.subopts, updated_at = now()
        RETURNING ack_offset
        """, [Namespace, ClientId, Id, emqx_utils_json:encode(SubOpts), Ack]),
        {ok, #{topic_id => Id, cursor => Cursor}}
    end).

-doc "Renew the client's subscriptions and return their topics and options.".
-spec restore(epgsql:connection(), namespace(), emqx_types:clientid()) ->
    {ok, [subscription_row()]}.
restore(Conn, Namespace, ClientId) ->
    query(Conn, """
    WITH touched AS (
      UPDATE mqtt_pg_subscription SET updated_at = now()
      WHERE namespace = $1 AND client_id = $2 RETURNING topic_id, subopts
    ) SELECT t.topic, s.subopts FROM touched s JOIN mqtt_pg_topic t USING (topic_id)
    """, [Namespace, ClientId]).

-doc "Delete the subscription of a client to a topic.".
-spec unsubscribe(epgsql:connection(), namespace(), emqx_types:clientid(), emqx_types:topic()) ->
    {ok, non_neg_integer()}.
unsubscribe(Conn, Namespace, ClientId, Topic) ->
    query(Conn, """
    DELETE FROM mqtt_pg_subscription s USING mqtt_pg_topic t
    WHERE s.topic_id = t.topic_id AND s.namespace = $1 AND s.client_id = $2 AND t.topic = $3
    """, [Namespace, ClientId, Topic]).

-doc "Save acknowledged progress. Progress never moves backwards, and the update renews the row.".
-spec checkpoint(
    epgsql:connection(), namespace(), emqx_types:clientid(), topic_id(), offset()
) -> {ok, non_neg_integer()}.
checkpoint(Conn, Namespace, ClientId, TopicId, Offset) ->
    query(Conn, """
    UPDATE mqtt_pg_subscription SET ack_offset = GREATEST(ack_offset, $4), updated_at = now()
    WHERE namespace = $1 AND client_id = $2 AND topic_id = $3
    """, [Namespace, ClientId, TopicId, Offset]).

-doc """
Return up to `Count` rows after `Cursor` with the topic tip in every row.
When no rows follow, return one row with null message fields.
""".
-spec page(epgsql:connection(), topic_id(), offset(), pos_integer()) -> {ok, [page_row()]}.
page(Conn, TopicId, Cursor, Count) ->
    query(Conn, """
    SELECT t.next_offset - 1, m.msg_offset, m.message_id, m.sender, m.qos, m.retain,
           m.payload, m.properties, m.published_at, m.expires_at
    FROM mqtt_pg_topic t LEFT JOIN LATERAL (
      SELECT * FROM mqtt_pg_message
      WHERE topic_id = t.topic_id AND msg_offset > $2 ORDER BY msg_offset LIMIT $3
    ) m ON true
    WHERE t.topic_id = $1 ORDER BY m.msg_offset
    """, [TopicId, Cursor, Count]).

-doc "Return up to `Limit` topics of a namespace after topic ID `After`, with their next offsets.".
-spec topics(epgsql:connection(), namespace(), topic_id() | 0, pos_integer()) ->
    {ok, [{topic_id(), Next :: offset()}]}.
topics(Conn, Namespace, After, Limit) ->
    query(Conn, """
    SELECT topic_id, next_offset FROM mqtt_pg_topic
    WHERE namespace = $1 AND topic_id > $2 ORDER BY topic_id LIMIT $3
    """, [Namespace, After, Limit]).

-doc "Return the first offset that is not older than `Oldest`, or `Next` when all are older.".
-spec age_cutoff(epgsql:connection(), topic_id(), offset(), timestamp_ms()) -> {ok, offset()}.
age_cutoff(Conn, TopicId, Next, Oldest) ->
    %% The scan stops at the end of the expired prefix.
    case query(Conn, """
    SELECT msg_offset FROM mqtt_pg_message
    WHERE topic_id = $1 AND published_at >= $2 ORDER BY msg_offset LIMIT 1
    """, [TopicId, Oldest]) of
        {ok, [{Offset}]} -> {ok, Offset};
        {ok, []} -> {ok, Next}
    end.

-doc "Return the first offset that some subscription has not acknowledged.".
-spec consumed_cutoff(epgsql:connection(), topic_id(), offset()) -> {ok, offset()}.
consumed_cutoff(Conn, TopicId, Next) ->
    %% A subscribe transaction blocks appends until its row commits, so no subscriber can
    %% acknowledge an offset above the new subscription's start before this query sees it.
    case
        query(
            Conn, "SELECT min(ack_offset) FROM mqtt_pg_subscription WHERE topic_id = $1", [TopicId]
        )
    of
        {ok, [{null}]} -> {ok, Next};
        {ok, [{Acked}]} -> {ok, Acked + 1}
    end.

-doc "Delete messages below `Cutoff` in primary key ranges of at most `Batch` offsets.".
-spec trim(epgsql:connection(), topic_id(), offset(), pos_integer()) ->
    {ok, Removed :: non_neg_integer()}.
trim(Conn, TopicId, Cutoff, Batch) ->
    case query(Conn, """
    SELECT min(msg_offset), max(msg_offset) FROM mqtt_pg_message
    WHERE topic_id = $1 AND msg_offset < $2
    """, [TopicId, Cutoff]) of
        {ok, [{null, null}]} -> {ok, 0};
        {ok, [{Min, Max}]} -> trim_range(Conn, TopicId, Min, Max, Batch, 0)
    end.

-doc "Delete up to `Batch` subscriptions that no client renewed within `Expiry` milliseconds.".
-spec expire(epgsql:connection(), namespace(), pos_integer(), pos_integer()) ->
    {ok, Removed :: non_neg_integer()}.
expire(Conn, Namespace, Expiry, Batch) ->
    query(Conn, """
    DELETE FROM mqtt_pg_subscription WHERE ctid IN (
      SELECT ctid FROM mqtt_pg_subscription
      WHERE namespace = $1 AND updated_at < now() - $2 * interval '1 millisecond'
      LIMIT $3
    )
    """, [Namespace, Expiry, Batch]).

-doc "Run a parameterized statement and raise an error when it fails.".
-spec query(epgsql:connection(), epgsql:sql_query(), [epgsql:bind_param()]) ->
    {ok, [tuple()] | non_neg_integer()}.
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

-doc "Delete a retained prefix in bounded offset ranges and count the removed rows.".
trim_range(_Conn, _TopicId, Min, Max, _Batch, Removed) when Min > Max ->
    {ok, Removed};
trim_range(Conn, TopicId, Min, Max, Batch, Removed0) ->
    Last = min(Max, Min + Batch - 1),
    {ok, Count} = query(Conn, """
    DELETE FROM mqtt_pg_message WHERE topic_id = $1 AND msg_offset BETWEEN $2 AND $3
    """, [TopicId, Min, Last]),
    trim_range(Conn, TopicId, Last + 1, Max, Batch, Removed0 + Count).
