%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_api_spec_handler).

-moduledoc """
Cowboy REST handler that serves focused, AI-consumable API specs.

All endpoints require authentication (basic with API key/secret, bearer
JWT from `POST /api/v5/login`, or the dashboard's `emqx_auth` session
cookie). Unauthenticated requests get a 401 with a minimal but valid
OpenAPI document (or its markdown equivalent for `/api-spec.md`) that
points the caller at how to authenticate. The `WWW-Authenticate` header
lists the supported schemes per RFC 7235 §4.1.

Endpoints:

  - `GET /api-spec.md`
    Returns a Markdown overview for human readers.
  - `GET /api-spec.json`
    Returns a JSON index listing all OpenAPI tags, their spec URLs, and
    drill-down URL templates for the big-union categories.
  - `GET /api-spec/:tag`
    Returns a self-contained OpenAPI 3.0.0 document containing only the
    endpoints whose swagger tag matches `tag` case-insensitively.
    Unreferenced component schemas are removed.
  - `GET /api-spec/:tag/:name`
    Further narrows the tag-scoped spec to endpoints whose oneOf
    request or response schemas contain a member whose `$ref` matches
    `name` case-insensitively.
  - `GET /api-docs/swagger.json`
    Returns the full, unfiltered OpenAPI 3.0.0 document for the
    listener. Kept for backward compatibility with external Swagger UI
    deployments that load EMQX's spec by URL.
""".

-behaviour(cowboy_rest).

%% Same caveat as `emqx_dashboard:authorize/2`: `emqx_mgmt_auth:authorize/4`
%% and `emqx_dashboard_admin:verify_token/3` have success typings dialyzer
%% can't see through (the scope-check helper is marked `no_return`), so the
%% `{ok, _} -> ok` clauses here look unreachable and the bearer branch
%% looks like it never returns. Silence both warning classes on the auth
%% helpers and their caller.
-dialyzer({nowarn_function, [init/2, authenticate/1, verify_bearer/2]}).

-export([
    init/2,
    allowed_methods/2,
    content_types_provided/2,
    resource_exists/2,
    handle_get/2,
    handle_get_markdown/2,
    handle_get_json/2,
    handle_get_html/2
]).

-define(WWW_AUTHENTICATE_FULL,
    <<"Basic realm=\"emqx-api-spec\", Bearer realm=\"emqx-api-spec\"">>
).
%% Browsers pop up their native HTTP Basic dialog when they see a `Basic`
%% challenge on a 401 to a top-level navigation. For `/api-spec.html` we
%% want the in-page sign-in widget to handle the flow instead, so the HTML
%% 401 advertises only `Bearer` — RFC 7235 still requires the header to be
%% present, but browsers have no native UI for `Bearer` and skip the popup.
-define(WWW_AUTHENTICATE_BEARER, <<"Bearer realm=\"emqx-api-spec\"">>).

%% Unauthenticated responses must not leak the EMQX release version or
%% edition (e.g. "Enterprise" vs "Open Source") so security scanners and
%% logged-out users see a generic stub. The authenticated spec keeps the
%% real title and version.
-define(GENERIC_API_TITLE, <<"EMQX API">>).

%%--------------------------------------------------------------------
%% Cowboy REST callbacks
%%--------------------------------------------------------------------

-doc "Initialize the Cowboy REST handler state.".
init(Req, Opts) ->
    case authenticate(Req) of
        ok ->
            {cowboy_rest, Req, Opts};
        unauthorized ->
            Req1 = reply_unauthorized(Req),
            {ok, Req1, Opts}
    end.

-doc "Advertise GET as the only supported HTTP method.".
allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

content_types_provided(Req, State) ->
    case {cowboy_req:path(Req), cowboy_req:binding(tag, Req)} of
        {<<"/api-spec.html">>, undefined} ->
            {[{<<"text/html">>, handle_get_html}], Req, State};
        {<<"/api-spec.json">>, undefined} ->
            {[{{<<"application">>, <<"json">>, '*'}, handle_get_json}], Req, State};
        {<<"/api-docs/swagger.json">>, undefined} ->
            {[{{<<"application">>, <<"json">>, '*'}, handle_get_json}], Req, State};
        {<<"/api-spec.md">>, undefined} ->
            {[{<<"text/markdown">>, handle_get_markdown}], Req, State};
        {_, _} ->
            {[{{<<"application">>, <<"json">>, '*'}, handle_get_json}], Req, State}
    end.

-doc """
Check tag existence early so Cowboy can return a clean 404 for unknown tags.

Name existence is checked later in `handle_get/2` to avoid computing the
full filtered spec twice.
""".
resource_exists(Req, State) ->
    case cowboy_req:binding(tag, Req) of
        undefined ->
            %% Index always exists.
            {true, Req, State};
        Tag ->
            AllTags = collect_all_tags(),
            TagLower = url_tag_lower(Tag),
            case lists:any(fun(T) -> string:lowercase(T) =:= TagLower end, AllTags) of
                true -> {true, Req, State};
                false -> {false, Req, State}
            end
    end.

handle_get(Req, State) ->
    handle_get_json(Req, State).

handle_get_html(Req, State) ->
    HtmlPath = filename:join(code:priv_dir(emqx_dashboard), "api-spec.html"),
    case file:read_file(HtmlPath) of
        {ok, Body} ->
            {Body, Req, State};
        {error, _Reason} ->
            Req1 = cowboy_req:reply(
                500,
                #{<<"content-type">> => <<"text/plain">>},
                <<"api-spec.html unavailable">>,
                Req
            ),
            {stop, Req1, State}
    end.

handle_get_markdown(Req, State) ->
    case {cowboy_req:path(Req), cowboy_req:binding(tag, Req)} of
        {<<"/api-spec.md">>, undefined} ->
            {overview_markdown(), Req, State};
        _ ->
            handle_get_json(Req, State)
    end.

handle_get_json(Req, State) ->
    case cowboy_req:path(Req) of
        <<"/api-docs/swagger.json">> ->
            {full_swagger_json(), Req, State};
        _ ->
            Tag = cowboy_req:binding(tag, Req),
            Name = cowboy_req:binding(name, Req),
            case build_response(Tag, Name) of
                {ok, Body} ->
                    {Body, Req, State};
                {error, not_found} ->
                    Req1 = cowboy_req:reply(
                        404,
                        #{<<"content-type">> => <<"application/json">>},
                        encode_pretty(#{<<"error">> => <<"not_found">>}),
                        Req
                    ),
                    {stop, Req1, State}
            end
    end.

%% Full unfiltered OpenAPI spec, for `/api-docs/swagger.json`.
full_swagger_json() ->
    cowboy_swagger:to_json(get_trails()).

%%--------------------------------------------------------------------
%% Authentication gate
%%--------------------------------------------------------------------

%% Accept a request only if the caller presents a valid API key (Basic),
%% bearer token, or the dashboard's `emqx_auth` session cookie. Spec
%% browsing is a read-only operation and does not depend on the API
%% key's role / scope assignment, but we reuse the same primitives that
%% guard the rest of the dashboard so the credential checks stay in one
%% place.
authenticate(Req) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, Username, Password} ->
            case emqx_mgmt_auth:authorize(handler_info(Req), Req, Username, Password) of
                {ok, _ActorContext} -> ok;
                _ -> unauthorized
            end;
        {bearer, Token} ->
            verify_bearer(Req, Token);
        _ ->
            authenticate_via_cookie(Req)
    end.

authenticate_via_cookie(Req) ->
    case cowboy_req:match_cookies([{emqx_auth, [], undefined}], Req) of
        #{emqx_auth := Token} when is_binary(Token), Token =/= <<>> ->
            verify_bearer(Req, Token);
        _ ->
            unauthorized
    end.

verify_bearer(Req, Token) ->
    case emqx_dashboard_admin:verify_token(Req, handler_info(Req), Token) of
        {ok, _ActorContext} -> ok;
        _ -> unauthorized
    end.

%% Synthetic HandlerInfo for the auth primitives. The path is the route
%% pattern rather than the request URL so `emqx_mgmt_auth:check_scopes/2`
%% treats it as an unmapped path (allow). The module/function are real so
%% dialyzer can see them.
handler_info(Req) ->
    #{
        module => ?MODULE,
        function => handle_get,
        method => 'GET',
        path => binary_to_list(cowboy_req:path(Req))
    }.

reply_unauthorized(Req) ->
    Path = cowboy_req:path(Req),
    {ContentType, Body} = unauthorized_body(Path),
    Headers = #{
        <<"content-type">> => ContentType,
        <<"www-authenticate">> => www_authenticate(Path)
    },
    cowboy_req:reply(401, Headers, Body, Req).

www_authenticate(<<"/api-spec.html">>) -> ?WWW_AUTHENTICATE_BEARER;
www_authenticate(_) -> ?WWW_AUTHENTICATE_FULL.

unauthorized_body(<<"/api-spec.md">>) ->
    {<<"text/markdown; charset=utf-8">>, unauthorized_markdown()};
unauthorized_body(<<"/api-spec.html">>) ->
    {<<"text/html; charset=utf-8">>, unauthorized_html()};
unauthorized_body(_) ->
    {<<"application/json">>, encode_pretty(unauthorized_openapi_doc())}.

unauthorized_openapi_doc() ->
    %% The stub is served before authentication: it must not leak the EMQX
    %% release version or edition. `info.version` is mandatory per
    %% OpenAPI 3.0, so emit a placeholder.
    #{
        <<"openapi">> => <<"3.0.0">>,
        <<"info">> => #{
            <<"title">> => ?GENERIC_API_TITLE,
            <<"version">> => <<"unknown">>,
            <<"description">> =>
                <<
                    "Authenticate via one of the security schemes to retrieve the "
                    "full specification, or create an API key in the dashboard."
                >>
        },
        <<"servers">> => [#{<<"url">> => <<"/api/v5">>}],
        <<"components">> => #{
            <<"securitySchemes">> => #{
                <<"basicAuth">> => #{
                    <<"type">> => <<"http">>,
                    <<"scheme">> => <<"basic">>,
                    <<"description">> =>
                        <<
                            "API key as username and API secret as password. "
                            "Manage keys in the dashboard or via "
                            "'emqx ctl api_keys'."
                        >>
                },
                <<"bearerAuth">> => #{
                    <<"type">> => <<"http">>,
                    <<"scheme">> => <<"bearer">>,
                    <<"description">> =>
                        <<"JWT obtained from POST /api/v5/login.">>
                }
            }
        },
        <<"security">> => [
            #{<<"basicAuth">> => []},
            #{<<"bearerAuth">> => []}
        ],
        <<"paths">> => #{
            <<"/login">> => #{
                <<"post">> => #{
                    <<"summary">> => <<"Obtain a bearer token.">>,
                    <<"tags">> => [<<"Authentication">>],
                    <<"security">> => [],
                    <<"responses">> => #{
                        <<"200">> => #{
                            <<"description">> =>
                                <<"Bearer token returned in the response body.">>
                        }
                    }
                }
            },
            <<"/status">> => #{
                <<"get">> => #{
                    <<"summary">> => <<"Broker health check.">>,
                    <<"tags">> => [<<"Status">>],
                    <<"security">> => [],
                    <<"responses">> => #{
                        <<"200">> => #{
                            <<"description">> =>
                                <<"Plain-text 'emqx is running' when the broker is up.">>
                        }
                    }
                }
            }
        }
    }.

unauthorized_html() ->
    <<
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<title>",
        ?GENERIC_API_TITLE/binary,
        " - Sign in</title>"
        "<meta name=\"robots\" content=\"noindex,nofollow\">"
        "<style>"
        "body{font:14px system-ui,sans-serif;background:#0e1116;color:#e6e6e6;"
        "display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}"
        "form{background:#161b22;border:1px solid #2a313a;border-radius:8px;"
        "padding:24px;width:min(360px,calc(100vw - 32px))}"
        "h1{margin:0 0 6px;font-size:16px}"
        "p{margin:0 0 16px;color:#9aa4ae;font-size:12px}"
        "label{display:block;font-size:12px;color:#9aa4ae;margin-bottom:4px}"
        "input{width:100%;padding:8px 10px;margin-bottom:12px;border:1px solid #2a313a;"
        "border-radius:6px;background:#0e1116;color:#e6e6e6;font-size:13px;box-sizing:border-box}"
        "button{width:100%;padding:9px 12px;border:0;border-radius:6px;"
        "background:#3b82f6;color:#fff;font-size:13px;cursor:pointer}"
        "button:disabled{opacity:.6;cursor:not-allowed}"
        ".err{color:#ffb4b4;font-size:12px;margin-bottom:10px;min-height:14px}"
        "</style></head><body>"
        "<form id=\"f\" autocomplete=\"off\">"
        "<h1>Sign in to view the API specification</h1>"
        "<p>Authenticate to access the in-tree API spec explorer.</p>"
        "<div class=\"err\" id=\"e\"></div>"
        "<label for=\"u\">Username</label>"
        "<input id=\"u\" name=\"username\" autocomplete=\"username\" required>"
        "<label for=\"p\">Password</label>"
        "<input id=\"p\" name=\"password\" type=\"password\" autocomplete=\"current-password\" required>"
        "<button id=\"b\" type=\"submit\">Sign in</button>"
        "</form>"
        "<script>"
        "var f=document.getElementById('f'),e=document.getElementById('e'),b=document.getElementById('b');"
        "f.addEventListener('submit',async function(ev){ev.preventDefault();e.textContent='';b.disabled=true;"
        "try{var r=await fetch('/api/v5/login',{method:'POST',credentials:'same-origin',"
        "headers:{'Content-Type':'application/json',Accept:'application/json'},"
        "body:JSON.stringify({username:f.username.value.trim(),password:f.password.value})});"
        "var body=null;try{body=await r.json()}catch(_){body=null}"
        "if(!r.ok){e.textContent=(body&&body.message)||('Sign-in failed (HTTP '+r.status+').');return}"
        "if(!body||!body.token){e.textContent='Sign-in succeeded but no token was returned.';return}"
        "var sec=location.protocol==='https:'?'; Secure':'';"
        "document.cookie='emqx_auth='+encodeURIComponent(body.token)+'; Path=/; SameSite=Strict'+sec;"
        "location.reload()}catch(err){e.textContent=String(err.message||err)}finally{b.disabled=false}});"
        "</script>"
        "</body></html>"
    >>.

unauthorized_markdown() ->
    Lines = [
        <<"# ", ?GENERIC_API_TITLE/binary, " - Authentication Required\n\n">>,
        <<
            "Authenticate via one of the security schemes to retrieve the full "
            "specification, or create an API key in the dashboard.\n\n"
        >>,
        <<"## Authentication\n\n">>,
        <<"- API key: `Authorization: Basic base64(api_key:api_secret)`\n">>,
        <<"- Bearer token: `POST /api/v5/login`, then `Authorization: Bearer <token>`\n\n">>,
        <<"## Public endpoints\n\n">>,
        <<"- `POST /api/v5/login` - interactive login (returns a bearer token).\n">>,
        <<"- `GET  /api/v5/status` - broker health check.\n">>
    ],
    unicode:characters_to_binary(Lines).

%%--------------------------------------------------------------------
%% Response builders
%%--------------------------------------------------------------------

encode_pretty(Term) ->
    emqx_utils_json:encode(Term, [pretty, force_utf8]).

build_response(undefined, _Name) ->
    %% Index: list of tags and drill-down info.
    {ok, index_json()};
build_response(Tag, undefined) ->
    %% Tag-scoped spec.
    Trails = get_trails(),
    FilteredTrails = filter_trails_by_tag(Trails, Tag),
    Spec = decode_swagger(cowboy_swagger:to_json(FilteredTrails)),
    Trimmed = trim_schemas(Spec),
    Enriched = maybe_add_resource_info(Trimmed, Tag),
    {ok, encode_pretty(Enriched)};
build_response(Tag, Name) ->
    %% Drill-down spec: tag-filtered + name-filtered.
    NameLower = string:lowercase(Name),
    Trails = get_trails(),
    FilteredTrails = filter_trails_by_tag(Trails, Tag),
    Spec = decode_swagger(cowboy_swagger:to_json(FilteredTrails)),
    Spec2 = filter_by_name(Spec, NameLower),
    Spec3 = filter_examples_by_name(Spec2, NameLower),
    Trimmed = trim_schemas(Spec3),
    %% Return 404 if no schemas contain the name after trimming.
    case has_name_match(Trimmed, NameLower) of
        true ->
            Enriched = maybe_add_resource_info(Trimmed, Tag),
            {ok, encode_pretty(Enriched)};
        false ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% Index JSON
%%--------------------------------------------------------------------

index_json() ->
    AllTags = collect_all_tags(),
    Tags = maps:from_list([
        {Tag, tag_info(Tag)}
     || Tag <- AllTags
    ]),
    DrillDown = drill_down_info(),
    encode_pretty(#{
        <<"bootstrap_help">> => overview_json_sections(),
        <<"api_auth">> => api_auth_info(),
        <<"tags">> => Tags,
        <<"drill_down">> => DrillDown
    }).

overview_markdown() ->
    AllTags = collect_all_tags(),
    TagInfoMap = maps:from_list([{Tag, tag_info(Tag)} || Tag <- AllTags]),
    DrillDown = drill_down_info(),
    TagLines = [
        markdown_tag_line(Tag, maps:get(Tag, TagInfoMap), DrillDown)
     || Tag <- AllTags
    ],
    unicode:characters_to_binary([
        <<"# EMQX HTTP API\n\n">>,
        render_markdown_sections(overview_sections()),
        <<"## Tags\n\n">>,
        TagLines
    ]).

overview_sections() ->
    [
        {<<"Authentication And Bootstrap">>, auth_help_lines()},
        {<<"Common Terms">>, terms_help_lines()},
        {<<"Resource IDs And Drill-Downs">>, drilldown_help_lines()}
    ].

auth_help_lines() ->
    [
        <<"All `/api/v5/*` endpoints require authentication, even on localhost.">>,
        <<"- API key: `Authorization: Basic base64(api_key:api_secret)`.">>,
        <<"  * Command to add API keys: `emqx ctl api_keys ...`">>,
        <<
            "  * Or bootstrap API keys from file by setting, API_KEY__BOOTSTRAP_FILE=/path/to/my-api-keys, "
            "with file lines in '{api_key}:{api_secret}' format."
        >>,
        <<"- Bearer token: `POST /api/v5/login`, then `Authorization: Bearer <token>`.">>,
        <<"  * Request body: `{ \"username\": \"admin\", \"password\": \"...\" }`">>,
        <<"  * Command to add new user: `emqx ctl admins add <Username> <Password> <Description> <Role>`">>,
        <<
            "The `/api-spec.md`, `/api-spec.json`, `/api-spec/...` and `/api-docs/swagger.json` "
            "endpoints also require authentication; unauthenticated requests receive a minimal "
            "OpenAPI document describing how to authenticate."
        >>
    ].

drilldown_help_lines() ->
    [
        <<"Drill-down endpoints return Swagger 3.0 JSON specs.">>,
        <<"Use `/api-spec/<tag>` or `/api-spec/<tag>/<name>` for machine-readable focused specs.">>,
        <<"Actions, sources and connectors commonly use `<type>:<name>` as the URL path ID.">>,
        <<"For actions and sources, the request body `connector` field is the connector NAME, not the full ID.">>,
        <<"Authentication IDs are`<mechanism>:<backend>`, while JWT uses just `jwt`.">>,
        <<"Authorization source IDs use the source type name such as `built_in_database`, `http` or `mysql`.">>
    ].

terms_help_lines() ->
    [
        <<"Data integration egress means a rule plus an action plus a connector.">>,
        <<"Action and sink are often used interchangeably.">>,
        <<
            "Ingress data integration means a source plus a connector. "
            "Rules can then process or forward the produced messages."
        >>,
        <<"Connectors are reusable resources and can be shared by multiple actions or sources.">>,
        <<"Authorization and ACL are often used interchangeably in EMQX API descriptions and UI text.">>,
        <<
            "Rule engine resources are separate from actions and sources: "
            "a rule references actions, while sources ingest data into EMQX."
        >>
    ].

render_markdown_sections(Sections) ->
    lists:map(
        fun({Title, Lines}) ->
            [
                <<"## ">>,
                Title,
                <<"\n\n">>,
                render_markdown_lines(Lines),
                <<"\n">>
            ]
        end,
        Sections
    ).

render_markdown_lines(Lines) ->
    [[Line, <<"\n">>] || Line <- Lines].

markdown_tag_line(Tag, TagInfo, DrillDown) ->
    Url = maps:get(<<"url">>, TagInfo),
    Desc =
        case maps:get(<<"description">>, TagInfo, undefined) of
            undefined -> <<>>;
            D -> <<": ", D/binary>>
        end,
    Drill =
        case drill_down_hint(Tag, DrillDown) of
            undefined -> <<>>;
            Hint -> <<"\n  Drill-down: ", Hint/binary>>
        end,
    [<<"- [`">>, Tag, <<"`](">>, Url, <<")">>, Desc, Drill, <<"\n">>].

drill_down_hint(Tag, DrillDown) ->
    Key = string:lowercase(binary:replace(Tag, <<" ">>, <<"_">>, [global])),
    case maps:get(Key, DrillDown, undefined) of
        #{<<"url_template">> := Template} -> Template;
        undefined -> undefined
    end.

tag_info(Tag) ->
    Base = #{<<"url">> => tag_url(Tag)},
    case tag_description(Tag) of
        undefined -> Base;
        Desc -> Base#{<<"description">> => Desc}
    end.

tag_description(<<"Actions">>) ->
    <<"Data integration actions (sinks).">>;
tag_description(<<"Sources">>) ->
    <<"Data integration sources.">>;
tag_description(<<"Connectors">>) ->
    <<"Connector lifecycle management.">>;
tag_description(<<"Authentication">>) ->
    <<"Client authentication chain configuration (password, JWT, SCRAM, etc.).">>;
tag_description(<<"Authorization">>) ->
    <<"Client authorization (ACL) source configuration.">>;
tag_description(<<"Rules">>) ->
    <<"SQL-based rule engine for message routing and transformation.">>;
tag_description(<<"Clients">>) ->
    <<"Connected MQTT client queries, kick, and subscription management.">>;
tag_description(<<"Status">>) ->
    <<"Broker node status, health checks, and monitoring.">>;
tag_description(<<"Topics">>) ->
    <<"Topic listing and metrics.">>;
tag_description(<<"Publish">>) ->
    <<"Publish MQTT messages via HTTP API.">>;
tag_description(<<"Subscriptions">>) ->
    <<"Query current subscriptions across the cluster.">>;
tag_description(<<"Listeners">>) ->
    <<"MQTT listener configuration and management.">>;
tag_description(_) ->
    undefined.

api_auth_info() ->
    render_plain_text(auth_help_lines()).

overview_json_sections() ->
    [
        #{
            <<"title">> => Title,
            <<"text">> => render_plain_text(Lines)
        }
     || {Title, Lines} <- overview_sections()
    ].

render_plain_text(Lines) ->
    iolist_to_binary(
        lists:join(<<"\n">>, [strip_markdown_formatting(Line) || Line <- Lines])
    ).

strip_markdown_formatting(Line) ->
    Line1 = re:replace(Line, <<"\\[([^\\]]+)\\]\\([^\\)]+\\)">>, <<"\\1">>, [
        {return, binary}, global
    ]),
    binary:replace(Line1, <<"`">>, <<>>, [global]).

%%--------------------------------------------------------------------
%% Resource info enrichment
%%--------------------------------------------------------------------

%% Add x-resource-info to spec responses for tags that have resource IDs.
maybe_add_resource_info(Spec, Tag) ->
    TagLower = url_tag_lower(Tag),
    case resource_info(TagLower) of
        undefined -> Spec;
        Info -> Spec#{<<"x-resource-info">> => Info}
    end.

resource_info(<<"connectors">>) ->
    #{
        <<"id_format">> => <<"<type>:<name>">>,
        <<"id_examples">> => [
            <<"kafka_producer:my_kafka_connector">>,
            <<"http:my_http_connector">>,
            <<"dynamo:my_dynamo_connector">>
        ],
        <<"note">> => <<
            "The resource ID in URL paths is composed as "
            "'<type>:<name>' where 'type' and 'name' are from the "
            "create request body."
        >>
    };
resource_info(<<"actions">>) ->
    #{
        <<"id_format">> => <<"<type>:<name>">>,
        <<"id_examples">> => [
            <<"kafka_producer:my_kafka_action">>,
            <<"http:my_http_action">>
        ],
        <<"note">> => <<
            "The action ID in URL paths is '<type>:<name>'. "
            "IMPORTANT: The 'connector' field in the request body uses "
            "the connector NAME only (e.g. 'my_kafka_connector'), not "
            "the full connector ID (e.g. 'kafka_producer:my_kafka_connector')."
        >>
    };
resource_info(<<"sources">>) ->
    #{
        <<"id_format">> => <<"<type>:<name>">>,
        <<"id_examples">> => [
            <<"kafka_consumer:my_kafka_source">>,
            <<"mqtt:my_mqtt_source">>
        ],
        <<"note">> => <<
            "The source ID in URL paths is '<type>:<name>'. "
            "The 'connector' field in the request body uses the "
            "connector NAME only."
        >>
    };
resource_info(<<"authentication">>) ->
    #{
        <<"id_format">> => <<"<mechanism>:<backend> or <mechanism>">>,
        <<"id_examples">> => [
            <<"password_based:built_in_database">>,
            <<"password_based:mysql">>,
            <<"password_based:http">>,
            <<"jwt">>,
            <<"scram:built_in_database">>
        ],
        <<"note">> => <<
            "Authenticator IDs are '<mechanism>:<backend>'. "
            "For mechanisms without a backend (e.g. JWT), the ID is "
            "just the mechanism name. Sub-resource paths like "
            "/authentication/{id}/users use this ID."
        >>
    };
resource_info(<<"authorization">>) ->
    #{
        <<"id_format">> => <<"<type>">>,
        <<"id_examples">> => [
            <<"built_in_database">>,
            <<"http">>,
            <<"mysql">>,
            <<"file">>
        ],
        <<"note">> => <<
            "Authorization source ID is the source type name. "
            "Use as the {type} path parameter. Rules sub-resources "
            "are under /authorization/sources/built_in_database/rules/..."
        >>
    };
resource_info(<<"api keys">>) ->
    #{
        <<"id_format">> => <<"<name>">>,
        <<"id_examples">> => [<<"EMQX-API-KEY-1">>],
        <<"note">> => <<
            "API key ID in URL paths is the key name "
            "set during creation."
        >>
    };
resource_info(_) ->
    undefined.

%% Build the /api-spec/<tag> URL for the index.
%% Tag names may contain spaces (e.g. "AI Completion"); replace spaces with _.
tag_url(Tag) ->
    Lower = string:lowercase(binary_to_list(Tag)),
    Slug = [encode_path_char(C) || C <- Lower],
    list_to_binary("/api-spec/" ++ Slug).

encode_path_char($\s) -> $_;
encode_path_char(C) -> C.

%% Normalise a URL tag segment for case-insensitive comparison against stored
%% tags.  The URL uses _ as a word separator (e.g. "ai_completion"), while
%% stored tags use spaces (e.g. "AI Completion").
url_tag_lower(Tag) ->
    string:lowercase(binary:replace(Tag, <<"_">>, <<" ">>, [global])).

%% Returns metadata for drill-down categories.
drill_down_info() ->
    #{
        <<"actions">> => #{
            <<"url_template">> => <<"/api-spec/actions/{name}">>,
            <<"names">> => enrich_names(action_names())
        },
        <<"sources">> => #{
            <<"url_template">> => <<"/api-spec/sources/{name}">>,
            <<"names">> => enrich_names(source_names())
        },
        <<"connectors">> => #{
            <<"url_template">> => <<"/api-spec/connectors/{name}">>,
            <<"names">> => enrich_names(connector_names())
        },
        <<"authentication">> => #{
            <<"url_template">> => <<"/api-spec/authentication/{name}">>,
            <<"names">> => enrich_names(authn_names())
        },
        <<"authorization">> => #{
            <<"url_template">> => <<"/api-spec/authorization/{name}">>,
            <<"names">> => enrich_names(authz_names())
        },
        <<"listeners">> => #{
            <<"url_template">> => <<"/api-spec/listeners/{name}">>,
            <<"names">> => enrich_names(listener_names())
        }
    }.

enrich_names(Names) ->
    [#{<<"name">> => N, <<"display">> => display_name(N)} || N <- Names].

%% "kafka_producer" -> "Kafka Producer"
display_name(Name) when is_binary(Name) ->
    Words = binary:split(Name, <<"_">>, [global]),
    Titled = lists:join(<<" ">>, [titlecase_word(W) || W <- Words]),
    iolist_to_binary(Titled).

titlecase_word(<<>>) ->
    <<>>;
titlecase_word(<<C, Rest/binary>>) when C >= $a, C =< $z ->
    <<(C - 32), Rest/binary>>;
titlecase_word(Word) ->
    Word.

connector_names() ->
    safe_call(fun() ->
        Types = emqx_connector_info:connector_types(),
        lists:sort([atom_to_binary(T, utf8) || T <- Types])
    end).

action_names() ->
    safe_call(fun() ->
        Pairs = emqx_action_info:registered_schema_modules_actions(),
        lists:sort([atom_to_binary(T, utf8) || {T, _} <- Pairs])
    end).

source_names() ->
    safe_call(fun() ->
        Pairs = emqx_action_info:registered_schema_modules_sources(),
        lists:sort([atom_to_binary(T, utf8) || {T, _} <- Pairs])
    end).

authn_names() ->
    safe_call(fun() ->
        %% authenticator_type/0 is the exported public API that returns
        %% a hoconsc union of all registered authenticator schemas.
        %% union_members/1 calls the selector with `all_union_members'
        %% to get the list of refs without needing to call the private
        %% provider_schema_mods/0.
        UnionType = emqx_authn_schema:authenticator_type(),
        {union, Selector, _} = UnionType,
        Refs = hoconsc:union_members(Selector),
        Names = lists:usort([ref_schema_name(R) || R <- Refs, ref_schema_name(R) =/= undefined]),
        lists:sort(Names)
    end).

ref_schema_name({ref, _Mod, Name}) when is_atom(Name) ->
    atom_to_binary(Name, utf8);
ref_schema_name({ref, _Mod, Name}) when is_list(Name) ->
    list_to_binary(Name);
ref_schema_name({ref, _Mod, Name}) when is_binary(Name) ->
    Name;
ref_schema_name(_) ->
    undefined.

authz_names() ->
    safe_call(fun() ->
        Types = emqx_authz_schema:source_types(),
        lists:sort([atom_to_binary(T, utf8) || T <- Types])
    end).

listener_names() ->
    safe_call(fun() ->
        Types = emqx_schema:listeners(),
        lists:sort([list_to_binary(Type) || {Type, _} <- Types])
    end).

safe_call(Fun) ->
    try
        Fun()
    catch
        _:_ -> []
    end.

%%--------------------------------------------------------------------
%% Trail / spec utilities
%%--------------------------------------------------------------------

%% Returns all dashboard trails, removing the cowboy_swagger internal ones.
%% Dashboard listeners are named 'http:dashboard' and 'https:dashboard'.
%% We pick the first started one and fetch its trails directly, avoiding
%% the `multiple_servers` exception that `trails:all/0` throws when both
%% HTTP and HTTPS listeners are running.
get_trails() ->
    case find_dashboard_listener() of
        undefined ->
            [];
        Server ->
            AllTrails = trails:all(Server, '_'),
            cowboy_swagger:filter_cowboy_swagger_handler(AllTrails)
    end.

find_dashboard_listener() ->
    find_dashboard_listener(['http:dashboard', 'https:dashboard']).

find_dashboard_listener([]) ->
    undefined;
find_dashboard_listener([Name | Rest]) ->
    try
        _ = ranch_server:get_listener_sup(Name),
        Name
    catch
        error:badarg ->
            find_dashboard_listener(Rest)
    end.

%% Keep trails that have at least one HTTP method tagged with NormTag.
%% NormTag is the raw URL binding (underscores as word separators).
filter_trails_by_tag(Trails, NormTag) ->
    TagLower = url_tag_lower(NormTag),
    lists:filter(
        fun(Trail) ->
            MD = cowboy_swagger:normalize_json(trails:metadata(Trail)),
            maps:fold(
                fun(_Method, MethodMD, Acc) ->
                    Acc orelse has_matching_tag(MethodMD, TagLower)
                end,
                false,
                MD
            )
        end,
        Trails
    ).

has_matching_tag(#{<<"tags">> := Tags}, TagLower) when is_list(Tags) ->
    lists:any(fun(Tag) -> string:lowercase(Tag) =:= TagLower end, Tags);
has_matching_tag(_, _TagLower) ->
    false.

%% Collect all unique tags present in any stored trail.
collect_all_tags() ->
    Trails = get_trails(),
    TagSets = lists:flatmap(
        fun(Trail) ->
            MD = cowboy_swagger:normalize_json(trails:metadata(Trail)),
            maps:fold(
                fun(_Method, MethodMD, Acc) ->
                    case MethodMD of
                        #{<<"tags">> := Tags} when is_list(Tags) -> Tags ++ Acc;
                        _ -> Acc
                    end
                end,
                [],
                MD
            )
        end,
        Trails
    ),
    lists:usort(TagSets).

%% Decode the binary JSON produced by cowboy_swagger:to_json/1.
decode_swagger(JsonBin) ->
    emqx_utils_json:decode(JsonBin).

%%--------------------------------------------------------------------
%% Schema trimming
%%--------------------------------------------------------------------

%% Remove component schemas that are not reachable from the paths section.
trim_schemas(Spec) ->
    Paths = maps:get(<<"paths">>, Spec, #{}),
    Components = maps:get(<<"components">>, Spec, #{}),
    AllSchemas = maps:get(<<"schemas">>, Components, #{}),
    InitRefs = collect_refs(Paths, sets:new()),
    AllRefs = transitive_refs(sets:to_list(InitRefs), InitRefs, AllSchemas),
    FilteredSchemas = maps:with(sets:to_list(AllRefs), AllSchemas),
    Spec#{<<"components">> => Components#{<<"schemas">> => FilteredSchemas}}.

%% Recursively collect all "$ref" values from an arbitrary term.
%% Only extracts schema refs of the form "#/components/schemas/NAME".
collect_refs(Map, Acc) when is_map(Map) ->
    Acc1 =
        case maps:find(<<"$ref">>, Map) of
            {ok, Ref} ->
                case schema_name_from_ref(Ref) of
                    undefined -> Acc;
                    Name -> sets:add_element(Name, Acc)
                end;
            error ->
                Acc
        end,
    maps:fold(fun(_K, V, A) -> collect_refs(V, A) end, Acc1, Map);
collect_refs(List, Acc) when is_list(List) ->
    lists:foldl(fun collect_refs/2, Acc, List);
collect_refs(_Scalar, Acc) ->
    Acc.

schema_name_from_ref(<<"#/components/schemas/", Name/binary>>) -> Name;
schema_name_from_ref(_) -> undefined.

%% Transitively resolve schema references until the set stabilises.
transitive_refs([], Resolved, _AllSchemas) ->
    Resolved;
transitive_refs([Name | Rest], Resolved, AllSchemas) ->
    case maps:find(Name, AllSchemas) of
        {ok, SchemaDef} ->
            NewRefs = collect_refs(SchemaDef, sets:new()),
            Fresh = sets:subtract(NewRefs, Resolved),
            Resolved2 = sets:union(Resolved, Fresh),
            transitive_refs(sets:to_list(Fresh) ++ Rest, Resolved2, AllSchemas);
        error ->
            transitive_refs(Rest, Resolved, AllSchemas)
    end.

%%--------------------------------------------------------------------
%% Drill-down: filter oneOf arrays by name substring
%%--------------------------------------------------------------------

%% Walk the entire spec and, wherever a oneOf is found, keep only members
%% whose $ref contains NameLower as a case-insensitive substring.
filter_by_name(Spec, NameLower) ->
    Components = maps:get(<<"components">>, Spec, #{}),
    AllSchemas = maps:get(<<"schemas">>, Components, #{}),
    walk_filter(Spec, NameLower, AllSchemas).

walk_filter(Map, NameLower, AllSchemas) when is_map(Map) ->
    Map2 =
        case maps:find(<<"oneOf">>, Map) of
            {ok, Members} when is_list(Members) ->
                Matched = [M || M <- Members, oneof_member_matches_name(M, NameLower, AllSchemas)],
                Kept =
                    case Matched of
                        %% If nothing matches the name, leave the oneOf unchanged
                        %% rather than destroying the path spec entirely.
                        [] -> Members;
                        _ -> Matched
                    end,
                Map#{<<"oneOf">> => Kept};
            _ ->
                Map
        end,
    maps:map(fun(_K, V) -> walk_filter(V, NameLower, AllSchemas) end, Map2);
walk_filter(List, NameLower, AllSchemas) when is_list(List) ->
    [walk_filter(Item, NameLower, AllSchemas) || Item <- List];
walk_filter(Scalar, _NameLower, _AllSchemas) ->
    Scalar.

oneof_member_matches_name(#{<<"$ref">> := Ref} = Member, NameLower, AllSchemas) ->
    ref_matches_name(Member, NameLower) orelse
        case schema_name_from_ref(Ref) of
            undefined ->
                false;
            SchemaName ->
                case maps:find(SchemaName, AllSchemas) of
                    {ok, SchemaDef} -> schema_matches_name(SchemaDef, NameLower);
                    error -> false
                end
        end;
oneof_member_matches_name(#{<<"oneOf">> := Members}, NameLower, AllSchemas) when is_list(Members) ->
    lists:any(fun(M) -> oneof_member_matches_name(M, NameLower, AllSchemas) end, Members);
oneof_member_matches_name(#{<<"anyOf">> := Members}, NameLower, AllSchemas) when is_list(Members) ->
    lists:any(fun(M) -> oneof_member_matches_name(M, NameLower, AllSchemas) end, Members);
oneof_member_matches_name(#{<<"allOf">> := Members}, NameLower, AllSchemas) when is_list(Members) ->
    lists:any(fun(M) -> oneof_member_matches_name(M, NameLower, AllSchemas) end, Members);
oneof_member_matches_name(Member, NameLower, _AllSchemas) when is_map(Member) ->
    schema_matches_name(Member, NameLower);
oneof_member_matches_name(_Member, _NameLower, _AllSchemas) ->
    false.

ref_matches_name(#{<<"$ref">> := Ref}, NameLower) ->
    RefLower = to_lower(Ref),
    NameLower1 = to_lower(NameLower),
    case type_name_matches(RefLower, NameLower1) of
        true ->
            true;
        false ->
            lists:any(
                fun(Candidate) ->
                    string:find(RefLower, Candidate) =/= nomatch
                end,
                ref_alias_candidates(NameLower1)
            )
    end;
ref_matches_name(_, _) ->
    false.

ref_alias_candidates(NameLower) ->
    N0 = to_lower(NameLower),
    N1 = to_binary(N0),
    N2 = drop_suffix(N1, <<"_producer">>),
    N3 = drop_suffix(N1, <<"_consumer">>),
    N4 = binary:replace(N1, <<"cassandra">>, <<"cassa">>, [global]),
    N5 = binary:replace(N1, <<"confluent_producer">>, <<"confluent">>, [global]),
    N6 = binary:replace(N1, <<"kafka_producer">>, <<"bridge_kafka">>, [global]),
    N7 = binary:replace(N1, <<"azure_event_hub_producer">>, <<"azure_event_hub">>, [global]),
    N8 = binary:replace(N1, <<"built_in_database">>, <<"builtin_db">>, [global]),
    N9 = binary:replace(N1, <<"pgsql">>, <<"postgres">>, [global]),
    N10 = binary:replace(N1, <<"syskeeper_forwarder">>, <<"syskeeper">>, [global]),
    lists:usort([N1, N2, N3, N4, N5, N6, N7, N8, N9, N10]).

drop_suffix(Name, Suffix) ->
    Size = byte_size(Name),
    SSize = byte_size(Suffix),
    case Size >= SSize andalso binary:part(Name, Size - SSize, SSize) =:= Suffix of
        true -> binary:part(Name, 0, Size - SSize);
        false -> Name
    end.

schema_matches_name(Map, NameLower) when is_map(Map) ->
    EnumMatched =
        case find_map_key(Map, [<<"enum">>, enum]) of
            {ok, _EnumKey, EnumVals} when is_list(EnumVals) ->
                lists:any(fun(V) -> type_name_matches(V, NameLower) end, EnumVals);
            _ ->
                false
        end,
    TypeMatched =
        case find_map_key(Map, [<<"type">>, type]) of
            {ok, _TypeKey, TypeVal} when is_binary(TypeVal); is_list(TypeVal); is_atom(TypeVal) ->
                type_name_matches(TypeVal, NameLower);
            _ ->
                false
        end,
    EnumMatched orelse
        TypeMatched orelse
        maps:fold(fun(_K, V, Acc) -> Acc orelse schema_matches_name(V, NameLower) end, false, Map);
schema_matches_name(List, NameLower) when is_list(List) ->
    lists:any(fun(Item) -> schema_matches_name(Item, NameLower) end, List);
schema_matches_name(_Scalar, _NameLower) ->
    false.

%%--------------------------------------------------------------------
%% Drill-down: filter examples maps by name substring
%%--------------------------------------------------------------------

%% Walk the spec and filter example data to only include entries matching
%% NameLower.  Handles two OpenAPI patterns:
%%   - "examples" (plural): a map keyed by type name → filter keys
%%   - "example" (singular): when it's an array of typed objects → filter items
%% This complements filter_by_name/2 which filters oneOf schema arrays.
filter_examples_by_name(Spec, NameLower) ->
    Paths = maps:get(<<"paths">>, Spec, #{}),
    NewPaths = walk_filter_examples(Paths, NameLower),
    Spec#{<<"paths">> => NewPaths}.

walk_filter_examples(Map, NameLower) when is_map(Map) ->
    %% Filter plural "examples" map (keyed by type name).
    Map2 = filter_plural_examples(Map, NameLower),
    %% Filter singular "example" when it's an array of typed objects.
    Map3 = filter_singular_example(Map2, NameLower),
    maps:map(fun(_K, V) -> walk_filter_examples(V, NameLower) end, Map3);
walk_filter_examples(List, NameLower) when is_list(List) ->
    [walk_filter_examples(Item, NameLower) || Item <- List];
walk_filter_examples(Scalar, _NameLower) ->
    Scalar.

%% "examples": { "kafka_producer": {...}, "http": {...}, ... }
%% Keep only entries whose key matches NameLower.
filter_plural_examples(Map, NameLower) ->
    case find_map_key(Map, [<<"examples">>, examples]) of
        {ok, ExamplesKey, Examples} when is_map(Examples), map_size(Examples) > 1 ->
            Matched = maps:filter(
                fun(Key, _Val) ->
                    string:find(to_lower(Key), to_lower(NameLower)) =/= nomatch
                end,
                Examples
            ),
            case maps:size(Matched) of
                0 -> Map;
                _ -> Map#{ExamplesKey => Matched}
            end;
        _ ->
            Map
    end.

%% "example": [ {"type": "kafka_producer", ...}, {"type": "http", ...}, ... ]
%% Keep only array items whose "type" field matches NameLower.
filter_singular_example(Map, NameLower) ->
    case find_map_key(Map, [<<"example">>, example]) of
        {ok, ExampleKey, Example} when is_list(Example), length(Example) > 1 ->
            Matched = [
                Item
             || Item <- Example,
                is_map(Item),
                example_type_matches(Item, NameLower)
            ],
            case Matched of
                [] -> Map;
                _ -> Map#{ExampleKey => Matched}
            end;
        _ ->
            Map
    end.

example_type_matches(#{<<"type">> := Type}, NameLower) ->
    string:find(to_lower(Type), to_lower(NameLower)) =/= nomatch;
example_type_matches(#{type := Type}, NameLower) ->
    string:find(to_lower(Type), to_lower(NameLower)) =/= nomatch;
example_type_matches(_, _NameLower) ->
    true.

find_map_key(Map, [Key | Rest]) ->
    case maps:find(Key, Map) of
        {ok, Value} -> {ok, Key, Value};
        error -> find_map_key(Map, Rest)
    end;
find_map_key(_Map, []) ->
    error.

to_lower(V) when is_binary(V) ->
    string:lowercase(V);
to_lower(V) when is_list(V) ->
    string:lowercase(V);
to_lower(V) when is_atom(V) ->
    string:lowercase(atom_to_binary(V));
to_lower(V) ->
    string:lowercase(iolist_to_binary(io_lib:format("~p", [V]))).

to_binary(V) when is_binary(V) ->
    V;
to_binary(V) when is_list(V) ->
    unicode:characters_to_binary(V);
to_binary(V) ->
    iolist_to_binary(io_lib:format("~p", [V])).

type_name_matches(A, B) ->
    A1 = to_lower(A),
    B1 = to_lower(B),
    A2 = canonical_type_name(A1),
    B2 = canonical_type_name(B1),
    string:find(A1, B1) =/= nomatch orelse
        string:find(B1, A1) =/= nomatch orelse
        string:find(A2, B2) =/= nomatch orelse
        string:find(B2, A2) =/= nomatch orelse
        A2 =:= B2.

canonical_type_name(V) ->
    V1 = to_lower(V),
    V2 = binary:replace(V1, <<"database">>, <<"db">>, [global]),
    V3 = binary:replace(V2, <<"postgresql">>, <<"pgsql">>, [global]),
    V4 = binary:replace(V3, <<"mongodb">>, <<"mongo">>, [global]),
    iolist_to_binary([C || <<C>> <= V4, is_alnum(C)]).

is_alnum(C) when C >= $a, C =< $z -> true;
is_alnum(C) when C >= $0, C =< $9 -> true;
is_alnum(_) -> false.

%% Check whether the trimmed spec actually contains any schemas with the name.
%% Used to distinguish "no match" (404) from "matched but schemas empty" (unlikely).
has_name_match(Spec, NameLower) ->
    Components = maps:get(<<"components">>, Spec, #{}),
    Schemas = maps:get(<<"schemas">>, Components, #{}),
    Paths = maps:get(<<"paths">>, Spec, #{}),
    lists:any(
        fun(SName) ->
            string:find(to_lower(SName), to_lower(NameLower)) =/= nomatch
        end,
        maps:keys(Schemas)
    ) orelse has_name_match_in_term(Paths, NameLower).

has_name_match_in_term(Map, NameLower) when is_map(Map) ->
    RefMatched =
        case find_map_key(Map, [<<"$ref">>, '$ref']) of
            {ok, _RefKey, Ref} ->
                ref_matches_name(#{<<"$ref">> => Ref}, NameLower);
            error ->
                false
        end,
    ExamplesMatched =
        case find_map_key(Map, [<<"examples">>, examples]) of
            {ok, _ExamplesKey, Examples} when is_map(Examples) ->
                lists:any(
                    fun(Key) ->
                        string:find(to_lower(Key), to_lower(NameLower)) =/= nomatch
                    end,
                    maps:keys(Examples)
                );
            _ ->
                false
        end,
    ExampleMatched =
        case find_map_key(Map, [<<"example">>, example]) of
            {ok, _ExampleKey, Example} when is_list(Example) ->
                lists:any(fun(Item) -> example_type_matches(Item, NameLower) end, Example);
            _ ->
                false
        end,
    RefMatched orelse
        ExamplesMatched orelse
        ExampleMatched orelse
        maps:fold(
            fun(_K, V, Acc) -> Acc orelse has_name_match_in_term(V, NameLower) end, false, Map
        );
has_name_match_in_term(List, NameLower) when is_list(List) ->
    lists:any(fun(Item) -> has_name_match_in_term(Item, NameLower) end, List);
has_name_match_in_term(_Scalar, _NameLower) ->
    false.
