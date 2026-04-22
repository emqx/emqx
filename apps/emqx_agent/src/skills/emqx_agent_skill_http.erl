%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% HTTP tool skill backed by hackney.
%%
%% Invoke topic:  cap/http/<id>/request
%% Reply  topic:  cap/http/<id>/response/<req_id>
%%
%% Context keys:
%%   skill_id      => binary()         — unique instance identifier
%%   desc          => binary()         — human-readable description
%%   method        => atom() | binary() — http method: get | post | put | patch | delete
%%   url           => binary()         — base URL (query string appended for GET)
%%   headers       => map() | [{binary(), binary()}]  — request headers
%%   input_schema  => map()            — full JSON Schema for request args
%%   output_schema => map()            — full JSON Schema for response
%%
%% For GET, input args are serialised as a URL query string.
%% For all other methods, input args are sent as a JSON body.
%%
%% Lifecycle:
%%   init()        — register the message.publish hook (once per node)
%%   create(Ctx)   — register a skill instance; skill_id taken from Ctx
%%   destroy(Id)   — unregister a skill instance from the registry
%%   deinit()      — remove the message.publish hook

-module(emqx_agent_skill_http).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(SKILL_TYPE, <<"http">>).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1]).
-export([on_message_publish/1]).

%% Exported for testing
-export([append_query/2]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST),
    ok.

-spec deinit() -> ok.
deinit() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    ok.

-spec create(Context :: map()) -> ok | {error, term()}.
create(
    #{
        skill_id := SkillId,
        desc := Desc,
        method := _Method,
        url := _Url,
        input_schema := InputSchema,
        output_schema := OutputSchema
    } = Context
) ->
    Skill = #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        display_name => <<"HTTP Tool">>,
        description => Desc,
        context => Context,
        input_schema => InputSchema,
        output_schema => OutputSchema
    },
    emqx_agent_skill_registry:register(Skill).

-spec destroy(emqx_agent_skill_registry:skill_id()) -> ok.
destroy(SkillId) ->
    emqx_agent_skill_registry:unregister(?SKILL_TYPE, SkillId).

-spec to_map(map()) -> map().
to_map(#{
    skill_id := Id,
    description := Desc,
    context := Ctx,
    input_schema := InSchema,
    output_schema := OutSchema
}) ->
    Method = maps:get(method, Ctx, post),
    MethodBin =
        if
            is_atom(Method) -> atom_to_binary(Method, utf8);
            true -> Method
        end,
    #{
        <<"skill_id">> => Id,
        <<"type">> => ?SKILL_TYPE,
        <<"description">> => Desc,
        <<"method">> => MethodBin,
        <<"url">> => maps:get(url, Ctx, <<>>),
        <<"input_schema">> => InSchema,
        <<"output_schema">> => OutSchema
    }.

%%--------------------------------------------------------------------
%% Hook callbacks
%%--------------------------------------------------------------------

on_message_publish(Msg) ->
    emqx_agent_skill_helpers:if_skill_request(
        ?SKILL_TYPE,
        fun(SkillId, #message{payload = Payload}) ->
            handle_invoke(SkillId, Payload)
        end,
        Msg
    ).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

handle_invoke(SkillId, Payload) ->
    case emqx_agent_skill_registry:lookup(?SKILL_TYPE, SkillId) of
        {error, not_found} ->
            ok;
        {ok, #{context := Context}} ->
            Request = emqx_utils_json:decode(Payload),
            do_reply(SkillId, Context, Request)
    end.

do_reply(SkillId, Context, Request) ->
    #{method := Method, url := BaseUrl} = Context,
    Headers = normalize_headers(maps:get(headers, Context, [])),
    Args = maps:get(<<"args">>, Request, #{}),

    {ok, RespBody} = call(normalize_method(Method), BaseUrl, Headers, Args),
    Data = decode_response(RespBody),

    emqx_agent_skill_helpers:publish_reply(?SKILL_TYPE, SkillId, Request, Data).

%% GET — append input args as query string; no body.
call(get, BaseUrl, Headers, Args) ->
    Url = append_query(BaseUrl, Args),
    {ok, _Status, _RespHeaders, Body} = hackney:request(get, Url, Headers, <<>>, [with_body]),
    {ok, Body};
%% All other methods — send args as JSON body.
call(Method, BaseUrl, Headers, Args) ->
    ReqBody = emqx_utils_json:encode(Args),
    AllHeaders = [{<<"content-type">>, <<"application/json">>} | Headers],
    {ok, _Status, _RespHeaders, Body} = hackney:request(
        Method, BaseUrl, AllHeaders, ReqBody, [with_body]
    ),
    {ok, Body}.

-spec append_query(binary(), map()) -> binary().
append_query(BaseUrl, Args) when map_size(Args) =:= 0 ->
    BaseUrl;
append_query(BaseUrl, Args) ->
    Pairs = [{to_str(K), to_str(V)} || {K, V} <- maps:to_list(Args)],
    QS = list_to_binary(uri_string:compose_query(Pairs)),
    <<BaseUrl/binary, "?", QS/binary>>.

normalize_method(get) -> get;
normalize_method(post) -> post;
normalize_method(put) -> put;
normalize_method(patch) -> patch;
normalize_method(delete) -> delete;
normalize_method(<<"get">>) -> get;
normalize_method(<<"post">>) -> post;
normalize_method(<<"put">>) -> put;
normalize_method(<<"patch">>) -> patch;
normalize_method(<<"delete">>) -> delete.

normalize_headers(Headers) when is_map(Headers) -> maps:to_list(Headers);
normalize_headers(Headers) when is_list(Headers) -> Headers.

decode_response(Body) ->
    try
        emqx_utils_json:decode(Body)
    catch
        _:_ -> #{<<"raw">> => Body}
    end.

to_str(V) when is_binary(V) -> binary_to_list(V);
to_str(V) when is_integer(V) -> integer_to_list(V);
to_str(V) when is_float(V) -> float_to_list(V, [{decimals, 10}, compact]);
to_str(V) when is_atom(V) -> atom_to_list(V).
