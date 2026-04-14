%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_mysql).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-include("emqx_offline_messages.hrl").

-export([
    on_config_changed/2,
    on_health_check/1
]).

-export([
    on_client_connected/3,
    on_session_subscribed/4,
    on_session_unsubscribed/4,
    on_message_publish/2,
    on_message_acked/3
]).

-define(RESOURCE_ID, <<"offline_messages_mysql">>).
-define(RESOURCE_ID_INIT, <<"offline_messages_mysql_init">>).
-define(RESOURCE_GROUP, <<"omp">>).
-define(TIMEOUT, 1000).

-define(INIT_SQL, [
    <<
        "CREATE TABLE IF NOT EXISTS `mqtt_msg` ("
        "`id` bigint unsigned NOT NULL AUTO_INCREMENT,"
        "`msgid` varchar(64) DEFAULT NULL,"
        "`topic` varchar(180) NOT NULL,"
        "`sender` varchar(64) DEFAULT NULL,"
        "`qos` tinyint(1) NOT NULL DEFAULT '0',"
        "`retain` tinyint(1) DEFAULT NULL,"
        "`payload` blob,"
        "`arrived` datetime NOT NULL,"
        "PRIMARY KEY (`id`),"
        "INDEX topic_index(`topic`)"
        ")"
        "ENGINE=InnoDB DEFAULT CHARSET=utf8MB4;"
    >>,

    <<
        "CREATE TABLE IF NOT EXISTS `mqtt_sub` ("
        "`clientid` varchar(64) NOT NULL,"
        "`topic` varchar(180) NOT NULL,"
        "`qos` tinyint(1) NOT NULL DEFAULT '0',"
        "PRIMARY KEY (`clientid`, `topic`)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8MB4;"
    >>
]).

-type statement() :: emqx_template_sql:statement().
-type param_template() :: emqx_template_sql:row_template().

-type context() :: #{
    select_message_sql => {statement(), param_template()},
    delete_message_sql => {statement(), param_template()},
    insert_subscription_sql => {statement(), param_template()},
    select_subscriptions_sql => {statement(), param_template()},
    delete_subscription_sql => {statement(), param_template()}
}.

%%-------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------

-spec on_config_changed(map(), map()) -> ok.
on_config_changed(#{<<"enable">> := false}, #{<<"enable">> := false}) ->
    ok;
on_config_changed(#{<<"enable">> := true} = Conf, #{<<"enable">> := true} = Conf) ->
    ok;
on_config_changed(#{<<"enable">> := true} = _OldConf, #{<<"enable">> := true} = NewConf) ->
    ok = stop(),
    ok = start(NewConf);
on_config_changed(#{<<"enable">> := true} = _OldConf, #{<<"enable">> := false} = _NewConf) ->
    ok = stop(),
    ok;
on_config_changed(#{<<"enable">> := false} = _OldConf, #{<<"enable">> := true} = NewConf) ->
    ok = start(NewConf).

-spec on_health_check(map()) -> ok | {error, binary()}.
on_health_check(#{<<"enable">> := false}) ->
    ok;
on_health_check(#{<<"enable">> := true}) ->
    emqx_offline_messages_utils:resource_health_status(<<"MySQL">>, ?RESOURCE_ID).

%%-------------------------------------------------------------------
%% start/stop
%%-------------------------------------------------------------------

-spec stop() -> ok.
stop() ->
    unhook(),
    ok = stop_resource().

-spec start(map()) -> ok.
start(ConfigRaw) ->
    ok = init_default_schema(ConfigRaw),
    {MysqlConfig, ResourceOpts} = make_mysql_resource_config(ConfigRaw),
    ok = start_resource(MysqlConfig, ResourceOpts),

    Statements = parse_statements(
        [
            delete_message_sql,
            select_message_sql,
            insert_subscription_sql,
            select_subscriptions_sql,
            delete_subscription_sql
        ],
        ConfigRaw
    ),
    TopicFilters = emqx_offline_messages_utils:topic_filters(ConfigRaw),
    Context = #{
        statements => Statements,
        topic_filters => TopicFilters
    },
    hook(Context).

%%-------------------------------------------------------------------
%% Hooks
%%-------------------------------------------------------------------

on_client_connected(
    ClientInfo = #{clientid := ClientId},
    ConnInfo,
    #{statements := #{select_subscriptions_sql := {Sql, ParamTemplate}}} = _Context
) ->
    ?SLOG(info, #{
        msg => offline_messages_mysql_client_connected,
        clientid => ClientId,
        client_info => ClientInfo,
        conn_info => ConnInfo
    }),
    Params = render_row(ParamTemplate, #{clientid => ClientId}),
    _ =
        case sync_query(Sql, Params) of
            {ok, Columns, Rows} ->
                Subscriptions = to_subscriptions(Columns, Rows),
                ok = emqx_offline_messages_utils:induce_subscriptions(Subscriptions),
                ok;
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "offline_messages_mysql_client_connected_error",
                    reason => Reason
                })
        end,
    ok.

on_session_subscribed(
    #{clientid := ClientId} = _ClientInfo,
    Topic,
    SubOpts,
    Context
) ->
    ?SLOG(info, #{
        msg => offline_messages_mysql_session_subscribed,
        clientid => ClientId,
        topic => Topic,
        subopts => SubOpts
    }),
    ok = insert_subscription(ClientId, Topic, SubOpts, Context),
    ok = fetch_and_deliver_messages(ClientId, Topic, Context).

insert_subscription(
    ClientId,
    Topic,
    SubOpts,
    #{statements := #{insert_subscription_sql := {Sql, ParamTemplate}}} = _Context
) ->
    Qos = maps:get(qos, SubOpts, 0),
    Params = render_row(ParamTemplate, #{clientid => ClientId, topic => Topic, qos => Qos}),
    _ =
        case sync_query(Sql, Params) of
            ok ->
                ok;
            {error, Reason} ->
                ?SLOG(error, #{
                    msg => "offline_messages_mysql_insert_subscription_error",
                    reason => Reason
                })
        end,
    ok.

fetch_and_deliver_messages(
    ClientId, Topic, #{statements := #{select_message_sql := {Sql, ParamTemplate}}} = _Context
) ->
    Params = render_row(ParamTemplate, #{clientid => ClientId, topic => Topic}),
    _ =
        case sync_query(Sql, Params) of
            {ok, Columns, Rows} ->
                Messages = to_messages(Columns, Rows),
                ?SLOG(debug, #{
                    msg => offline_messages_mysql_fetch_and_deliver_messages,
                    topic => Topic,
                    messages => length(Messages)
                }),
                ok = emqx_offline_messages_utils:deliver_messages(Topic, Messages),
                emqx_metrics_worker:inc(?METRICS_WORKER, session_subscribed, success);
            {error, Reason} ->
                emqx_metrics_worker:inc(?METRICS_WORKER, session_subscribed, fail),
                ?SLOG(error, #{
                    msg => "offline_messages_mysql_on_session_subscribed_error",
                    reason => Reason
                })
        end,
    ok.

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, Context) ->
    ?SLOG(info, #{
        msg => offline_messages_mysql_session_unsubscribed,
        clientid => ClientId,
        topic => Topic,
        opts => Opts
    }),
    ok = delete_subscription(ClientId, Topic, Context).

delete_subscription(
    ClientId,
    Topic,
    #{statements := #{delete_subscription_sql := {Sql, ParamTemplate}}} = _Context
) ->
    Params = render_row(ParamTemplate, #{clientid => ClientId, topic => Topic}),
    case sync_query(Sql, Params) of
        ok ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "offline_messages_mysql_delete_subscription_error",
                reason => Reason
            }),
            ok
    end.

on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Context) ->
    {ok, Message};
on_message_publish(Message, #{topic_filters := TopicFilters} = _Context) ->
    _ =
        case emqx_offline_messages_utils:need_persist_message(Message, TopicFilters) of
            false ->
                ?SLOG(debug, #{
                    msg => offline_messages_mysql_message_publish_skipped,
                    message => Message
                });
            true ->
                MessageMap = message_to_map(Message),
                Res = emqx_resource:query(?RESOURCE_ID, {insert_message, MessageMap}),
                ?SLOG(info, #{
                    msg => offline_messages_mysql_message_publish,
                    message => MessageMap,
                    result => Res
                })
        end,
    {ok, Message}.

on_message_acked(
    _ClientInfo = #{clientid := ClientId},
    #message{id = MsgId} = Message,
    #{statements := #{delete_message_sql := {Sql, ParamTemplate}}} = _Context
) ->
    ?SLOG(info, #{
        msg => offline_messages_mysql_message_puback,
        message => emqx_message:to_map(Message),
        clientid => ClientId
    }),
    Params = render_row(ParamTemplate, #{id => emqx_guid:to_hexstr(MsgId)}),
    _ =
        case sync_query(Sql, Params) of
            ok ->
                emqx_metrics_worker:inc(?METRICS_WORKER, message_acked, success),
                ?SLOG(debug, #{
                    msg => "offline_messages_mysql_message_puback_success",
                    message => emqx_message:to_map(Message),
                    clientid => ClientId
                });
            {error, Reason} ->
                emqx_metrics_worker:inc(?METRICS_WORKER, message_acked, fail),
                ?SLOG(error, #{
                    msg => "offline_messages_mysql_on_message_acked_error",
                    reason => Reason
                })
        end,
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Message helpers

to_messages(Columns, Rows) ->
    [record_to_msg(lists:zip(Columns, Row)) || Row <- Rows].

record_to_msg(Record) ->
    record_to_msg(Record, new_message()).

record_to_msg([], Msg) ->
    Msg;
record_to_msg([{<<"id">>, Id} | Record], Msg) ->
    record_to_msg(Record, emqx_message:set_header(mysql_id, Id, Msg));
record_to_msg([{<<"msgid">>, MsgId} | Record], Msg) ->
    record_to_msg(Record, Msg#message{id = emqx_guid:from_hexstr(MsgId)});
record_to_msg([{<<"topic">>, Topic} | Record], Msg) ->
    record_to_msg(Record, Msg#message{topic = Topic});
record_to_msg([{<<"sender">>, Sender} | Record], Msg) ->
    record_to_msg(Record, Msg#message{from = Sender});
record_to_msg([{<<"qos">>, Qos} | Record], Msg) ->
    record_to_msg(Record, Msg#message{qos = Qos});
record_to_msg([{<<"retain">>, R} | Record], Msg) ->
    record_to_msg(Record, Msg#message{flags = #{retain => int_to_bool(R)}});
record_to_msg([{<<"payload">>, Payload} | Record], Msg) ->
    record_to_msg(Record, Msg#message{payload = Payload});
record_to_msg([{<<"arrived">>, Arrived} | Record], Msg) ->
    record_to_msg(Record, Msg#message{timestamp = Arrived});
record_to_msg([_ | Record], Msg) ->
    record_to_msg(Record, Msg).

new_message() ->
    #message{
        id = <<>>,
        qos = 0,
        flags = #{},
        topic = <<>>,
        payload = <<>>,
        timestamp = 0,
        headers = #{}
    }.

message_to_map(Message) ->
    Map0 = emqx_message:to_map(Message),
    Map1 = emqx_utils_maps:update_if_present(
        flags,
        fun(Flags) ->
            maps:map(
                fun(_K, V) ->
                    bool_to_int(V)
                end,
                Flags
            )
        end,
        Map0
    ),
    emqx_utils_maps:update_if_present(
        id,
        fun(MsgId) ->
            emqx_guid:to_hexstr(MsgId)
        end,
        Map1
    ).

%% Subscription helpers

to_subscriptions(Columns, Rows) ->
    lists:flatmap(fun(Row) -> record_to_subscription(lists:zip(Columns, Row)) end, Rows).

record_to_subscription(Record) ->
    record_to_subscription(Record, #{}).

record_to_subscription([], #{topic := Topic, qos := Qos}) ->
    [{Topic, #{qos => Qos}}];
record_to_subscription([], #{} = Record) ->
    ?SLOG(warning, #{
        msg => "offline_messages_mysql_record_to_subscription_incomplete_record",
        record => Record
    }),
    [];
record_to_subscription([{<<"topic">>, Topic} | Record], Acc) ->
    record_to_subscription(Record, Acc#{topic => Topic});
record_to_subscription([{<<"qos">>, Qos} | Record], Acc) ->
    record_to_subscription(Record, Acc#{qos => Qos});
record_to_subscription([_ | Record], Acc) ->
    record_to_subscription(Record, Acc).

%% Resource helpers
start_resource(MysqlConfig, ResourceOpts) ->
    ?SLOG(info, #{
        msg => offline_messages_mysql_resource_start,
        config => MysqlConfig,
        resource_opts => ResourceOpts,
        resource_id => ?RESOURCE_ID,
        resource_group => ?RESOURCE_GROUP
    }),
    {ok, _} = emqx_resource:create_local(
        ?RESOURCE_ID,
        ?RESOURCE_GROUP,
        emqx_bridge_mysql_connector,
        MysqlConfig,
        ResourceOpts
    ),
    ok.

stop_resource() ->
    ?SLOG(info, #{
        msg => offline_messages_mysql_resource_stop,
        resource_id => ?RESOURCE_ID
    }),
    emqx_resource:remove_local(?RESOURCE_ID).

make_mysql_resource_config(#{<<"insert_message_sql">> := InsertMessageStatement} = RawConfig0) ->
    RawMysqlConfig0 = maps:with(
        [
            <<"server">>,
            <<"database">>,
            <<"username">>,
            <<"password">>,
            <<"pool_size">>,
            <<"ssl">>
        ],
        RawConfig0
    ),
    RawMysqlConfig = emqx_offline_messages_utils:fix_ssl_config(RawMysqlConfig0),

    MysqlConfig0 = emqx_offline_messages_utils:check_config(emqx_mysql, RawMysqlConfig),

    MysqlConfig = MysqlConfig0#{
        prepare_statement => #{
            insert_message => InsertMessageStatement
        }
    },

    ResourceOpts0 = emqx_offline_messages_utils:make_resource_opts(RawConfig0),
    ResourceOpts = ResourceOpts0#{spawn_buffer_workers => true},

    {MysqlConfig, ResourceOpts}.

init_default_schema(#{<<"init_default_schema">> := true} = ConfigRaw) ->
    ?SLOG(info, #{
        msg => offline_messages_mysql_init_default_schema,
        config => ConfigRaw
    }),
    {MysqlConfig0, ResourceOpts} = make_mysql_resource_config(ConfigRaw),
    MysqlConfig = MysqlConfig0#{prepare_statement => #{}},
    {ok, _} = emqx_resource:create_local(
        ?RESOURCE_ID_INIT,
        ?RESOURCE_GROUP,
        emqx_mysql,
        MysqlConfig,
        ResourceOpts
    ),
    ok = lists:foreach(
        fun(Sql) ->
            case emqx_resource:simple_sync_query(?RESOURCE_ID_INIT, {sql, Sql, [], ?TIMEOUT}) of
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "offline_messages_mysql_init_default_schema_error",
                        sql => Sql,
                        reason => Reason
                    });
                _ ->
                    ok
            end
        end,
        ?INIT_SQL
    ),
    ok = emqx_resource:remove_local(?RESOURCE_ID_INIT);
init_default_schema(_ConfigRaw) ->
    ok.

sync_query(Sql, Params) ->
    emqx_resource:simple_sync_query(?RESOURCE_ID, {sql, Sql, Params, ?TIMEOUT}).

%% Render helpers

parse_statements(Keys, BinMap) ->
    lists:foldl(
        fun(Key, StatementsAcc) ->
            BinKey = atom_to_binary(Key, utf8),
            StatementRaw = maps:get(BinKey, BinMap),
            Parsed = parse_statement(StatementRaw),
            StatementsAcc#{Key => Parsed}
        end,
        #{},
        Keys
    ).

parse_statement(StatementRaw) ->
    {Statement, RowTamplate} = emqx_template_sql:parse_prepstmt(
        StatementRaw,
        #{parameters => '?', strip_double_quote => true}
    ),
    {Statement, RowTamplate}.

render_row(RowTemplate, Map) ->
    {Row, _Errors} = emqx_template:render(
        RowTemplate,
        Map,
        #{var_trans => fun(_Name, Value) -> emqx_utils_sql:to_sql_value(Value) end}
    ),
    Row.

%% Hook helpers

unhook() ->
    unhook('client.connected', {?MODULE, on_client_connected}),
    unhook('session.subscribed', {?MODULE, on_session_subscribed}),
    unhook('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    unhook('message.publish', {?MODULE, on_message_publish}),
    unhook('message.acked', {?MODULE, on_message_acked}).

-spec hook(context()) -> ok.
hook(Context) ->
    hook('client.connected', {?MODULE, on_client_connected, [Context]}),
    hook('session.subscribed', {?MODULE, on_session_subscribed, [Context]}),
    hook('session.unsubscribed', {?MODULE, on_session_unsubscribed, [Context]}),
    hook('message.publish', {?MODULE, on_message_publish, [Context]}),
    hook('message.acked', {?MODULE, on_message_acked, [Context]}).

hook(HookPoint, MFA) ->
    %% use highest hook priority so this module's callbacks
    %% are evaluated before the default hooks in EMQX
    emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

unhook(HookPoint, MFA) ->
    emqx_hooks:del(HookPoint, MFA).

%% Common helpers

bool_to_int(false) -> 0;
bool_to_int(true) -> 1.

int_to_bool(0) -> false;
int_to_bool(1) -> true.
