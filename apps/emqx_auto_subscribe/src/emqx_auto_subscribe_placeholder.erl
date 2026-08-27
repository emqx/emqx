%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auto_subscribe_placeholder).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx/include/logger.hrl").

-export([generate/1]).

-export([to_topic_table/3]).

%% `emqx_template' access module callback
-export([lookup/2]).

%% Legacy auto-subscribe names. Unlike the standard `${peerhost}'/`${peerport}',
%% `${host}' and `${port}' resolve from the connection peername.
-define(VAR_HOST, "host").
-define(VAR_PORT, "port").

-define(ALLOWED_VARS, [
    ?VAR_CLIENTID,
    ?VAR_USERNAME,
    ?VAR_HOST,
    ?VAR_PORT,
    ?VAR_CERT_CN_NAME,
    ?VAR_CERT_SUBJECT,
    ?VAR_ZONE,
    ?VAR_LISTENER,
    ?VAR_NS_CLIENT_ATTRS
]).

-doc """
Parse topic templates at config load.
Unknown placeholders are kept as literal topic text and reported in a warning log.
""".
-spec generate(list() | map()) -> list() | map().
generate(Topics) when is_list(Topics) ->
    [generate(Topic) || Topic <- Topics];
generate(T = #{topic := Topic}) ->
    T#{placeholder => parse(Topic)}.

-doc """
Render the parsed topic templates for one client connection.
A topic with an unresolved placeholder (undefined username, missing client
attribute) or with control characters in a rendered value is skipped with an
`auto_subscribe_ignored' warning.
""".
-spec to_topic_table(list(), map(), map()) -> list().
to_topic_table(PHs, ClientInfo, ConnInfo) ->
    Fold = fun(
        #{
            qos := Qos,
            rh := RH,
            rap := RAP,
            nl := NL,
            placeholder := Template,
            topic := RawTopic
        },
        Acc
    ) ->
        case to_topic(Template, ClientInfo, ConnInfo) of
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "auto_subscribe_ignored",
                    topic => RawTopic,
                    reason => Reason
                }),
                Acc;
            <<>> ->
                ?SLOG(warning, #{
                    msg => "auto_subscribe_ignored",
                    topic => RawTopic,
                    reason => empty_topic
                }),
                Acc;
            Topic0 ->
                {Topic, Opts} = emqx_topic:parse(Topic0),
                [{Topic, Opts#{qos => Qos, rh => RH, rap => RAP, nl => NL}} | Acc]
        end
    end,
    lists:foldl(Fold, [], PHs).

lookup([<<?VAR_CLIENTID>>], {#{clientid := ClientId}, _ConnInfo}) ->
    {ok, ClientId};
lookup([<<?VAR_USERNAME>>], {#{username := Username}, _ConnInfo}) when Username =/= undefined ->
    {ok, Username};
lookup([<<?VAR_HOST>>], {_ClientInfo, #{peername := {Host, _Port}}}) ->
    {ok, list_to_binary(inet:ntoa(Host))};
lookup([<<?VAR_PORT>>], {_ClientInfo, #{peername := {_Host, Port}}}) ->
    {ok, integer_to_binary(Port)};
lookup([<<?VAR_CERT_CN_NAME>>], {#{cn := CN}, _ConnInfo}) when is_binary(CN) ->
    {ok, CN};
lookup([<<?VAR_CERT_SUBJECT>>], {#{dn := DN}, _ConnInfo}) when is_binary(DN) ->
    {ok, DN};
lookup([<<?VAR_ZONE>>], {#{zone := Zone}, _ConnInfo}) when is_atom(Zone), Zone =/= undefined ->
    {ok, atom_to_binary(Zone)};
lookup([<<?VAR_LISTENER>>], {#{listener := Listener}, _ConnInfo}) when
    is_atom(Listener), Listener =/= undefined
->
    {ok, atom_to_binary(Listener)};
lookup([<<"client_attrs">>, Name], {ClientInfo, _ConnInfo}) ->
    case ClientInfo of
        #{client_attrs := #{Name := Value}} ->
            {ok, Value};
        _ ->
            {error, undefined}
    end;
lookup(_Accessor, _Bindings) ->
    {error, undefined}.

%%--------------------------------------------------------------------
%% internal

parse(Topic) ->
    Template = emqx_template:parse(Topic),
    case emqx_template:validate(?ALLOWED_VARS, Template) of
        ok ->
            Template;
        {error, Disallowed} ->
            ?SLOG(warning, #{
                msg => "auto_subscribe_unknown_placeholder",
                topic => Topic,
                unknown_placeholders => [list_to_binary(Var) || {Var, disallowed} <- Disallowed],
                hint => "unknown placeholders are kept as literal topic text"
            }),
            Escaped = emqx_template:escape_disallowed(Template, ?ALLOWED_VARS),
            emqx_template:parse(Escaped)
    end.

to_topic(Template, ClientInfo, ConnInfo) ->
    case emqx_template:render(Template, {?MODULE, {ClientInfo, ConnInfo}}) of
        {String, []} ->
            check_safe(unicode:characters_to_binary(String));
        {_String, Errors} ->
            {error, unresolved_reason(Errors)}
    end.

%% Rendered values can come from sources the MQTT frame layer does not
%% byte-check (client attributes extracted from user properties or passwords,
%% for example). Skip topics carrying control characters such as CRLF.
check_safe(Topic) when is_binary(Topic) ->
    case emqx_utils:is_mqtt_safe_utf8(Topic) of
        true ->
            Topic;
        false ->
            {error, unsafe_characters_in_topic}
    end;
check_safe(_NotUtf8) ->
    {error, unsafe_characters_in_topic}.

unresolved_reason(Errors) ->
    case lists:usort([Name || {Name, _Reason} <- Errors]) of
        [?VAR_USERNAME] ->
            username_undefined;
        Names ->
            #{unresolved_placeholders => [list_to_binary(Name) || Name <- Names]}
    end.
