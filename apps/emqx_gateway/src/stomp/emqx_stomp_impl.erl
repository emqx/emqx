%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_stomp_impl).

-include_lib("emqx_gateway/include/emqx_gateway.hrl").

-behavior(emqx_gateway_impl).

%% APIs
-export([ load/0
        , unload/0
        ]).

-export([ init/1
        , on_insta_create/2
        , on_insta_update/4
        , on_insta_destroy/3
        ]).

%% APIs
load() ->
    %% FIXME: Is the following concept belong to gateway???
    %%       >> No
    %%
    %% emqx_stomp_schema module is
    %% the schema file from emqx_stomp/priv/schema/emqx_stomp_schema.erl
    %%
    %% It's aim is to parse configurations:
    %% ```
    %% stomp.$name {
    %%   max_frame_size: 1024
    %%   ...
    %%
    %%   authenticators: [...]
    %%
    %%   listeners: [...]
    %% }
    %% ```
    %%
    %%  Conf + Schema => RuntimeOptions
    %%  Conf + Schema => APIs's Descriptor
    %%  Schema        => APIs's Descriptor
    %%  Schema        => APIs's Validator
    %%
    RegistryOptions = [ {cbkmod, ?MODULE}
                      , {schema, emqx_stomp_schema}
                      ],

    YourOptions = [param1, param2],
    emqx_gateway_registry:load(stomp, RegistryOptions, YourOptions).

unload() ->
    emqx_gateway_registry:unload(stomp).

init([param1, param2]) ->
    GwState = #{},
    {ok, GwState}.

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_insta_create(Insta = #instance{rawconf = RawConf0}, GwState) ->
    %% Old
    %% #{allow_anonymous => true,
    %% default_user => [{login,"guest"},{passcode,"guest"}],
    %% frame =>
    %%     [{max_headers,10},
    %%      {max_header_length,1024},
    %%      {max_body_length,8192}],
    %% listener => {61613,[{acceptors,4},{max_connections,512}]}}

    %% #{
    %%  
    %%    frame: #{
    %%      max_headers => 10,
    %%      max_headers_length => 1024,
    %%      max_body_length => 8192
    %%    }
    %% 
    %%    # copy to ctx
    %%    #authenticator => allow_anonymouse
    %%    
    %%    # put into ctx
    %%    clientinfo_override => #{
    %%      username => <<"guest">> 
    %%      password => <<"guest">>
    %%    }
    %%
    %%    listener: [
    %%      #{type => tcp,
    %%        listen_on => 
    %%      }
    %%    ]
    %% }
    %%
    RawConf = #{}.

    GwInstPid = spawn(fun() -> timer:sleep(3000) end),
    {ok, GwInstPid, #{}}.

on_insta_update(NewInsta, OldInstace, GwInstaState, GwState) ->
    %% XXX:
    ok.

on_insta_destroy(Insta = #instance{id = Id}, GwInstaState, GwState) ->
    %% XXX:
    emqx_stomp_sup:delete(Id).
