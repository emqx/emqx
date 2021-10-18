%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
-module(emqx_gateway_api_authn).

-behaviour(minirest_api).

-import(emqx_gateway_http,
        [ return_http_error/2
        , schema_bad_request/0
        , schema_not_found/0
        , schema_internal_error/0
        , schema_no_content/0
        , with_gateway/2
        , checks/2
        ]).

%% minirest behaviour callbacks
-export([api_spec/0]).

%% http handlers
-export([authn/2]).

%% internal export for emqx_gateway_api_listeners module
-export([schema_authn/0]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    {metadata(apis()), []}.

apis() ->
    [ {"/gateway/:name/authentication", authn}
    ].

%%--------------------------------------------------------------------
%% http handlers

authn(get, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        {200, emqx_gateway_http:authn(GwName)}
    end);

authn(put, #{bindings := #{name := Name0},
             body := Body}) ->
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:update_authn(GwName, Body),
        {204}
    end);

authn(post, #{bindings := #{name := Name0},
              body := Body}) ->
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:add_authn(GwName, Body),
        {204}
    end);

authn(delete, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:remove_authn(GwName),
        {204}
    end).

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

metadata(APIs) ->
    metadata(APIs, []).
metadata([], APIAcc) ->
    lists:reverse(APIAcc);
metadata([{Path, Fun}|More], APIAcc) ->
    Methods = [get, post, put, delete, patch],
    Mds = lists:foldl(fun(M, Acc) ->
              try
                  Acc#{M => swagger(Path, M)}
              catch
                  error : function_clause ->
                      Acc
              end
          end, #{}, Methods),
    metadata(More, [{Path, Mds, Fun} | APIAcc]).

swagger("/gateway/:name/authentication", get) ->
    #{ description => <<"Get the gateway authentication">>
     , parameters => params_gateway_name_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"200">> => schema_authn()
         }
     };
swagger("/gateway/:name/authentication", put) ->
    #{ description => <<"Update authentication for the gateway">>
     , parameters => params_gateway_name_in_path()
     , requestBody => schema_authn()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
         }
     };
swagger("/gateway/:name/authentication", post) ->
    #{ description => <<"Add authentication for the gateway">>
     , parameters => params_gateway_name_in_path()
     , requestBody => schema_authn()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
         }
     };
swagger("/gateway/:name/authentication", delete) ->
    #{ description => <<"Remove the gateway authentication">>
     , parameters => params_gateway_name_in_path()
     , responses =>
        #{ <<"400">> => schema_bad_request()
         , <<"404">> => schema_not_found()
         , <<"500">> => schema_internal_error()
         , <<"204">> => schema_no_content()
         }
     }.

%%--------------------------------------------------------------------
%% params defines

params_gateway_name_in_path() ->
    [#{ name => name
      , in => path
      , schema => #{type => string}
      , required => true
      }].

%%--------------------------------------------------------------------
%% schemas

schema_authn() ->
    #{ description => <<"OK">>
     , content => #{
        'application/json' => #{
            schema => minirest:ref(<<"AuthenticatorInstance">>)
       }}
     }.
