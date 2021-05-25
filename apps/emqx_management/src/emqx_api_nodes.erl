-module(emqx_api_nodes).

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
        ]).

%trails
-export([trails/0]).

trails() ->
  Metadata =
    #{get =>
      #{tags => ["system"],
        description => "Retrives EMQ X nodes in the cluster",
        responses => #{
          <<"200">> => #{
            description => <<"Retrives EMQ X Nodes in the cluster 200 OK">>,
            content => #{
              'text/plain' =>
                #{schema => #{
                    type => string
                  }
                }
            }
          }
        }
      }
    },
  [trails:trail("/nodes", ?MODULE, [], Metadata)].

init(Req, _Opts) ->
  {cowboy_rest, Req, #{}}.

rest_init(Req, _Opts) ->
  {ok, Req, #{}}.

content_types_accepted(Req, State) ->
  {[{'*', handle_put}], Req, State}.

content_types_provided(Req, State) ->
  {[{<<"text/plain">>, handle_get}], Req, State}.

forbidden(Req, State) ->
  {false, Req, State}.

resource_exists(Req, State) ->
  {true, Req, State}.
%% cowboy
allowed_methods(Req, State) ->
  {[<<"GET">>], Req, State}.

%% internal
handle_get(Req, State) ->
  {io_lib:format("~p~n", [emqx_mgmt:list_nodes()]), Req, State}.
