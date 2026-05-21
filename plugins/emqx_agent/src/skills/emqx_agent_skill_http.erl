%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% HTTP tool skill backed by hackney.
%%
%% Invoke topic:  cap/http/<id>/request/<req_id>
%% Reply  topic:  cap/http/<id>/response/<req_id>
%%
%% Context keys:
%%   skill_id      => binary()         — unique instance identifier
%%   desc          => binary()         — human-readable description
%%   method        => atom() | binary() — http method: get | post | put | patch | delete
%%   url           => binary()         — base URL (query string appended for GET)
%%   headers       => map() | [{binary(), binary()}]  — request headers
%%   input_schema  => map()            — full JSON Schema for request args
%%
%% For GET, input args are serialised as a URL query string.
%% For all other methods, input args are sent as a JSON body.
%%
%% Lifecycle:
%%   init()        — register the skill type
%%   create(Ctx)   — build a runtime skill instance; skill_id taken from Ctx
%%   destroy(Skill) — clean up runtime resources owned by the skill
%%   deinit()      — unregister the skill type

-module(emqx_agent_skill_http).

-behaviour(emqx_agent_skill).

-define(SKILL_TYPE, <<"http">>).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

%% Exported for testing
-export([append_query/2]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    emqx_agent_skill_registry:register_type(?SKILL_TYPE, ?MODULE).

-spec deinit() -> ok.
deinit() ->
    emqx_agent_skill_registry:unregister_type(?SKILL_TYPE).

-spec create(Context :: map()) -> {ok, map()} | {error, term()}.
create(
    #{
        skill_id := SkillId,
        desc := Desc,
        method := _Method,
        url := _Url,
        input_schema := InputSchema
    } = Context
) ->
    {ok, #{
        skill_id => SkillId,
        type => ?SKILL_TYPE,
        module => ?MODULE,
        display_name => <<"HTTP Tool">>,
        description => Desc,
        context => Context,
        input_schema => InputSchema
    }}.

-spec destroy(map()) -> ok.
destroy(_Skill) ->
    ok.

-spec to_map(map()) -> map().
to_map(#{
    skill_id := Id,
    description := Desc,
    context := Ctx,
    input_schema := InSchema
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
        <<"input_schema">> => InSchema
    }.

-spec handle_invoke(map(), map()) -> {ok, term()} | {error, term()}.
handle_invoke(Context, Request) ->
    do_reply(Context, Request).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_reply(Context, Request) ->
    #{method := Method, url := BaseUrl} = Context,
    Headers = normalize_headers(maps:get(headers, Context, [])),
    Args = maps:get(<<"args">>, Request, #{}),

    {ok, RespBody} = call(normalize_method(Method), BaseUrl, Headers, Args),
    Data = decode_response(RespBody),

    {ok, Data}.

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
normalize_headers(Headers) when is_list(Headers) ->
    [normalize_header(H) || H <- Headers].

normalize_header({Name, Value}) ->
    {Name, Value};
normalize_header(#{<<"name">> := Name, <<"value">> := Value}) ->
    {Name, Value};
normalize_header(#{name := Name, value := Value}) ->
    {Name, Value}.

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
