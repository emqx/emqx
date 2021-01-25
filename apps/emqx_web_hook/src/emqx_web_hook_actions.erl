%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% Define the default actions.
-module(emqx_web_hook_actions).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_rule_engine/include/rule_actions.hrl").
-include("emqx_web_hook.hrl").

-define(RESOURCE_TYPE_WEBHOOK, 'web_hook').
-define(RESOURCE_CONFIG_SPEC, #{
            url => #{
                order => 1,
                type => string,
                format => url,
                required => true,
                title => #{en => <<"URL">>,
                           zh => <<"URL"/utf8>>},
                description => #{en => <<"The URL of the server that will receive the Webhook requests.">>,
                                zh => <<"用于接收 Webhook 请求的服务器的 URL。"/utf8>>}
            },
            connect_timeout => #{
                order => 2,
                type => number,
                default => 5,
                title => #{en => <<"Connect Timeout">>,
                           zh => <<"连接超时时间"/utf8>>},
                description => #{en => <<"Connect Timeout In Seconds">>,
                                 zh => <<"连接超时时间，单位秒"/utf8>>}},
            request_timeout => #{
                order => 3,
                type => number,
                default => 5,
                title => #{en => <<"Request Timeout">>,
                           zh => <<"请求超时时间时间"/utf8>>},
                description => #{en => <<"Request Timeout In Seconds">>,
                                 zh => <<"请求超时时间，单位秒"/utf8>>}},
            cacertfile => #{
                order => 4,
                type => file,
                default => <<>>,
                title => #{en => <<"CA Certificate File">>,
                           zh => <<"CA 证书文件"/utf8>>},
                description => #{en => <<"CA Certificate File.">>,
                                 zh => <<"CA 证书文件。"/utf8>>}
            },
            certfile => #{
                order => 5,
                type => file,
                default => <<>>,
                title => #{en => <<"Certificate File">>,
                           zh => <<"证书文件"/utf8>>},
                description => #{en => <<"Certificate File.">>,
                                 zh => <<"证书文件。"/utf8>>}
            },
            keyfile => #{
                order => 6,
                type => file,
                default => <<>>,
                title => #{en => <<"Private Key File">>,
                           zh => <<"私钥文件"/utf8>>},
                description => #{en => <<"Private key file.">>,
                                 zh => <<"私钥文件。"/utf8>>}
            },
            verify => #{
                order => 7,
                type => boolean,
                default => true,
                title => #{en => <<"Verify">>,
                           zh => <<"Verify"/utf8>>},
                description => #{en => <<"Turn on peer certificate verification.">>,
                                 zh => <<"是否开启对端证书验证。"/utf8>>}
            },
            pool_size => #{
                order => 8,
                type => number,
                default => 32,
                title => #{en => <<"Pool Size">>,
                           zh => <<"连接池大小"/utf8>>},
                description => #{en => <<"Pool Size for HTTP Server.">>,
                                 zh => <<"HTTP Server 连接池大小。"/utf8>>}
            }
        }).

-define(ACTION_PARAM_RESOURCE, #{
            order => 0,
            type => string,
            required => true,
            title => #{en => <<"Resource ID">>,
                       zh => <<"资源 ID"/utf8>>},
            description => #{en => <<"Bind a resource to this action.">>,
                             zh => <<"给动作绑定一个资源。"/utf8>>}
        }).

-define(ACTION_DATA_SPEC, #{
            '$resource' => ?ACTION_PARAM_RESOURCE,
            method => #{
                order => 1,
                type => string,
                enum => [<<"POST">>,<<"DELETE">>,<<"PUT">>,<<"GET">>],
                default => <<"POST">>,
                title => #{en => <<"Method">>,
                           zh => <<"Method"/utf8>>},
                description => #{en => <<"HTTP Method.\n"
                                         "Note that: the Body option in the Action will be discarded in case of GET or DELETE method.">>,
                                 zh => <<"HTTP Method。\n"
                                         "注意：当方法为 GET 或 DELETE 时，动作中的 Body 选项会被忽略。"/utf8>>}},
            path => #{
                order => 2,
                type => string,
                required => false,
                default => <<"">>,
                title => #{en => <<"Path">>,
                           zh => <<"Path"/utf8>>},
                description => #{en => <<"The path part of the URL, support using ${Var} to get the field value output by the rule.">>,
                                 zh => <<"URL 的路径部分，支持使用 ${Var} 获取规则输出的字段值。\n"/utf8>>}
            },
            headers => #{
                order => 3,
                type => object,
                schema => #{},
                default => #{<<"content-type">> => <<"application/json">>},
                title => #{en => <<"Headers">>,
                           zh => <<"Headers"/utf8>>},
                description => #{en => <<"HTTP headers.">>,
                                 zh => <<"HTTP headers。"/utf8>>}},
            body => #{
                order => 5,
                type => string,
                input => textarea,
                required => false,
                default => <<"">>,
                title => #{en => <<"Body">>,
                           zh => <<"Body"/utf8>>},
                description => #{en => <<"The HTTP body supports the use of ${Var} to obtain the field value output by the rule.\n"
                                         "The content of the default HTTP request body is a JSON string composed of the keys and values of all fields output by the rule.">>,
                                 zh => <<"HTTP 请求体，支持使用 ${Var} 获取规则输出的字段值\n"
                                         "默认 HTTP 请求体的内容为规则输出的所有字段的键和值构成的 JSON 字符串。"/utf8>>}}
            }).

-resource_type(#{name => ?RESOURCE_TYPE_WEBHOOK,
                 create => on_resource_create,
                 status => on_get_resource_status,
                 destroy => on_resource_destroy,
                 params => ?RESOURCE_CONFIG_SPEC,
                 title => #{en => <<"WebHook">>,
                            zh => <<"WebHook"/utf8>>},
                 description => #{en => <<"WebHook">>,
                                  zh => <<"WebHook"/utf8>>}
                }).

-rule_action(#{name => data_to_webserver,
               category => data_forward,
               for => '$any',
               create => on_action_create_data_to_webserver,
               params => ?ACTION_DATA_SPEC,
               types => [?RESOURCE_TYPE_WEBHOOK],
               title => #{en => <<"Data to Web Server">>,
                          zh => <<"发送数据到 Web 服务"/utf8>>},
               description => #{en => <<"Forward Messages to Web Server">>,
                                zh => <<"将数据转发给 Web 服务"/utf8>>}
              }).

-type(url() :: binary()).

-export([ on_resource_create/2
        , on_get_resource_status/2
        , on_resource_destroy/2
        ]).

-export([ on_action_create_data_to_webserver/2
        , on_action_data_to_webserver/2
        ]).

%%------------------------------------------------------------------------------
%% Actions for web hook
%%------------------------------------------------------------------------------

-spec(on_resource_create(binary(), map()) -> map()).
on_resource_create(ResId, Conf) ->
    {ok, _} = application:ensure_all_started(ehttpc),
    Options = pool_opts(Conf),
    PoolName = pool_name(ResId),
    start_resource(ResId, PoolName, Options),
    Conf#{<<"pool">> => PoolName, options => Options}.

start_resource(ResId, PoolName, Options) ->
    case ehttpc_pool:start_pool(PoolName, Options) of
        {ok, _} ->
            ?LOG(info, "Initiated Resource ~p Successfully, ResId: ~p",
                 [?RESOURCE_TYPE_WEBHOOK, ResId]);
        {error, {already_started, _Pid}} ->
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            start_resource(ResId, PoolName, Options);
        {error, Reason} ->
            ?LOG(error, "Initiate Resource ~p failed, ResId: ~p, ~0p",
                 [?RESOURCE_TYPE_WEBHOOK, ResId, Reason]),
            error({{?RESOURCE_TYPE_WEBHOOK, ResId}, create_failed})
    end.

-spec(on_get_resource_status(binary(), map()) -> map()).
on_get_resource_status(ResId, #{<<"url">> := Url}) ->
    #{is_alive =>
        case emqx_rule_utils:http_connectivity(Url) of
            ok -> true;
            {error, Reason} ->
                ?LOG(error, "Connectivity Check for ~p failed, ResId: ~p, ~0p",
                     [?RESOURCE_TYPE_WEBHOOK, ResId, Reason]),
                false
        end}.

-spec(on_resource_destroy(binary(), map()) -> ok | {error, Reason::term()}).
on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
    ?LOG(info, "Destroying Resource ~p, ResId: ~p", [?RESOURCE_TYPE_WEBHOOK, ResId]),
    case ehttpc_pool:stop_pool(PoolName) of
        ok ->
            ?LOG(info, "Destroyed Resource ~p Successfully, ResId: ~p", [?RESOURCE_TYPE_WEBHOOK, ResId]);
        {error, Reason} ->
            ?LOG(error, "Destroy Resource ~p failed, ResId: ~p, ~p", [?RESOURCE_TYPE_WEBHOOK, ResId, Reason]),
            error({{?RESOURCE_TYPE_WEBHOOK, ResId}, destroy_failed})
    end.

%% An action that forwards publish messages to a remote web server.
-spec(on_action_create_data_to_webserver(Id::binary(), #{url() := string()}) -> {bindings(), NewParams :: map()}).
on_action_create_data_to_webserver(Id, Params) ->
    #{method := Method,
      path := Path,
      headers := Headers,
      body := Body,
      pool := Pool,
      request_timeout := RequestTimeout} = parse_action_params(Params),
    BodyTokens = emqx_rule_utils:preproc_tmpl(Body),
    PathTokens = emqx_rule_utils:preproc_tmpl(Path),
    Params.

on_action_data_to_webserver(Selected, _Envs =
                            #{?BINDING_KEYS := #{
                                'Id' := Id,
                                'Method' := Method,
                                'Headers' := Headers,
                                'PathTokens' := PathTokens,
                                'BodyTokens' := BodyTokens,
                                'Pool' := Pool,
                                'RequestTimeout' := RequestTimeout},
                              clientid := ClientID}) ->
    NBody = format_msg(BodyTokens, Selected),
    NPath = emqx_rule_utils:proc_tmpl(PathTokens, Selected),
    Req = create_req(Method, NPath, Headers, NBody),
    case ehttpc:request(ehttpc_pool:pick_worker(Pool, ClientID), Method, Req, RequestTimeout) of
        {ok, StatusCode, _} when StatusCode >= 200 andalso StatusCode < 300 ->
            ok;
        {ok, StatusCode, _, _} when StatusCode >= 200 andalso StatusCode < 300 ->
            ok;
        {ok, StatusCode, _} ->
            ?LOG(warning, "[WebHook Action] HTTP request failed with status code: ~p", [StatusCode]),
            ok;
        {ok, StatusCode, _, _} ->
            ?LOG(warning, "[WebHook Action] HTTP request failed with status code: ~p", [StatusCode]),
            ok;
        {error, Reason} ->
            ?LOG(error, "[WebHook Action] HTTP request error: ~p", [Reason]),
            emqx_rule_metrics:inc_actions_error(Id)
    end.

format_msg([], Data) ->
    emqx_json:encode(Data);
format_msg(Tokens, Data) ->
     emqx_rule_utils:proc_tmpl(Tokens, Data).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

create_req(Method, Path, Headers, _Body)
  when Method =:= get orelse Method =:= delete ->
    {Path, Headers};
create_req(_, Path, Headers, Body) ->
  {Path, Headers, Body}.

parse_action_params(Params = #{<<"url">> := URL}) ->
    try
        #{path := CommonPath} = uri_string:parse(URL),
        Method = method(maps:get(<<"method">>, Params, <<"POST">>)),
        Headers = headers(maps:get(<<"headers">>, Params, undefined)),
        NHeaders = ensure_content_type_header(Headers, Method),
        #{method => Method,
          path => path(filename:join(CommonPath, maps:get(<<"path">>, Params, <<>>))),
          headers => NHeaders,
          body => maps:get(<<"body">>, Params, <<>>),
          request_timeout => timer:seconds(maps:get(<<"request_timeout">>, Params, 5)),
          pool => maps:get(<<"pool">>, Params)}
    catch _:_ ->
        throw({invalid_params, Params})
    end.

ensure_content_type_header(Headers, Method) when Method =:= post orelse Method =:= put ->
    Headers;
ensure_content_type_header(Headers, _Method) ->
    lists:keydelete("content-type", 1, Headers).

path(<<>>) -> <<"/">>;
path(Path) -> Path.

method(GET) when GET == <<"GET">>; GET == <<"get">> -> get;
method(POST) when POST == <<"POST">>; POST == <<"post">> -> post;
method(PUT) when PUT == <<"PUT">>; PUT == <<"put">> -> put;
method(DEL) when DEL == <<"DELETE">>; DEL == <<"delete">> -> delete.

headers(undefined) -> [];
headers(Headers) when is_map(Headers) ->
    headers(maps:to_list(Headers));
headers(Headers) when is_list(Headers) ->
    [{string:to_lower(str(K)), str(V)} || {K, V} <- Headers].

str(Str) when is_list(Str) -> Str;
str(Atom) when is_atom(Atom) -> atom_to_list(Atom);
str(Bin) when is_binary(Bin) -> binary_to_list(Bin).

pool_opts(Params = #{<<"url">> := URL}) ->
    #{host := Host0,
      scheme := Scheme} = URIMap = uri_string:parse(binary_to_list(URL)),
    Port = maps:get(port, URIMap, case Scheme of
                                      "https" -> 443;
                                      _ -> 80
                                  end),
    PoolSize = maps:get(<<"pool_size">>, Params, 32),
    ConnectTimeout = timer:seconds(maps:get(<<"connect_timeout">>, Params, 5)),
    Host = case inet:parse_address(Host0) of
                       {ok, {_,_,_,_} = Addr} -> Addr;
                       {ok, {_,_,_,_,_,_,_,_} = Addr} -> Addr;
                       {error, einval} -> Host0
                   end,
    Inet = case Host of
               {_,_,_,_} -> inet;
               {_,_,_,_,_,_,_,_} -> inet6;
               _ ->
                   case inet:getaddr(Host, inet6) of
                       {error, _} -> inet;
                       {ok, _} -> inet6
                   end
           end,
    MoreOpts = case Scheme of
                   "http" ->
                       [{transport_opts, [Inet]}];
                   "https" ->
                       KeyFile = maps:get(<<"keyfile">>, Params),
                       CertFile = maps:get(<<"certfile">>, Params),
                       CACertFile = maps:get(<<"cacertfile">>, Params),
                       VerifyType = case maps:get(<<"verify">>, Params) of
                                        true -> verify_peer;
                                        false -> verify_none
                                    end,
                       TLSOpts = lists:filter(fun({_K, V}) when V =:= <<>> ->
                                                  false;
                                                 (_) ->
                                                  true
                                              end, [{keyfile, KeyFile}, {certfile, CertFile}, {cacertfile, CACertFile}]),
                       TlsVers = ['tlsv1.2','tlsv1.1',tlsv1],
                       NTLSOpts = [{verify, VerifyType},
                                   {versions, TlsVers},
                                   {ciphers, lists:foldl(fun(TlsVer, Ciphers) ->
                                                             Ciphers ++ ssl:cipher_suites(all, TlsVer)
                                                         end, [], TlsVers)} | TLSOpts],
                       [{transport, ssl}, {transport_opts, [Inet | NTLSOpts]}]
              end,
    [{host, Host},
     {port, Port},
     {pool_size, PoolSize},
     {pool_type, hash},
     {connect_timeout, ConnectTimeout},
     {retry, 5},
     {retry_timeout, 1000}] ++ MoreOpts.

pool_name(ResId) ->
    list_to_atom("webhook:" ++ str(ResId)).
