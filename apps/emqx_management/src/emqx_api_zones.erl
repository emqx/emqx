-module(emqx_api_zones).

-behaviour(cowboy_rest).
-behaviour(trails_handler).

-export([ init/2
        , rest_init/2
        , content_types_accepted/2
        , content_types_provided/2
        , forbidden/2
        , resource_exists/2
        , allowed_methods/2
        , handle_get/2
        , handle_post/2
        ]).

%trails
-export([trails/0]).

trails() ->
    SchemaName = <<"zone-create-post">>,
    Schema =
        #{<<"name">> =>
            #{ type => <<"string">>
             , description => <<"name of the zone">>
             },
          <<"description">> =>
            #{ type => <<"string">>
             , description => <<"zone description">>
             }
         },
    ok = cowboy_swagger:add_definition(SchemaName, Schema),
    RequestBody =
    #{ name => <<"request body">>
     , in => body
     , description => <<"request body (as json)">>
     , required => true
     , schema => cowboy_swagger:schema(SchemaName)
     },
    Metadata =
    #{post =>
       # { tags => ["zones"]
         , description => "Creates a new zone"
         , consumes => ["application/json"]
         , produces => ["text/plain"]
         , parameters => [RequestBody] % and then use that parameter here
         }
     },
  Path = "/zones",
  Options = #{path => Path},
  [trails:trail(Path, ?MODULE, Options, Metadata)].


init(Req, _Opts) ->
    {cowboy_rest, Req, #{}}.

rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

content_types_accepted(Req, State) ->
    {[{<<"application/json">>, handle_post}], Req, State}.

content_types_provided(Req, State) ->
    {[{<<"text/plain">>, handle_post}], Req, State}.

forbidden(Req, State) ->
    {false, Req, State}.

resource_exists(Req, State) ->
    {true, Req, State}.

%% cowboy
allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

%% internal
handle_get(_Req, _State) ->
    error(not_implemented).

handle_post(Req, State) ->
    Req1 = cowboy_req:set_resp_body("ok", Req),
    {true, Req1, State}.
