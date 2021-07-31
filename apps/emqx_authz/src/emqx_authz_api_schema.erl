%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_api_schema).

-export([definitions/0]).

definitions() ->
    RetruenedRules = #{
        allOf => [ #{type => object,
                     properties => #{
                        annotations => #{
                            type => object,
                            required => [id],
                            properties => #{
                                id => #{
                                    type => string
                                },
                                principal => minirest:ref(<<"principal">>)
                            }
                            
                        }
                        
                     }
                   }
                 , minirest:ref(<<"rules">>)
                 ]
    },
    Rules = #{
        oneOf => [ minirest:ref(<<"simple_rule">>)
                 % , minirest:ref(<<"connector_redis">>)
                 ]
    },
    % ConnectorRedis = #{
    %     type => object,
    %     required => [principal, type, enable, config, cmd]
    %     properties => #{
    %         principal => minirest:ref(<<"principal">>),
    %         type => #{
    %             type => string,
    %             enum => [<<"redis">>],
    %             example => <<"redis">>
    %         },
    %         enable => #{
    %             type => boolean,
    %             example => true
    %         }
    %         config => #{
    %             type => 
    %         }
    %     }
    % }
    SimpleRule = #{
        type => object,
        required => [principal, permission, action, topics],
        properties => #{
            action => #{
                type => string,
                enum => [<<"publish">>, <<"subscribe">>, <<"all">>],
                example => <<"publish">>
            },
            permission => #{
                type => string,
                enum => [<<"allow">>, <<"deny">>],
                example => <<"allow">>
            },
            topics => #{
                type => array,
                items => #{
                    oneOf => [ #{type => string, example => <<"#">>}
                             , #{type => object,
                                 required => [eq],
                                 properties => #{
                                    eq => #{type => string}
                                 },
                                 example => #{eq => <<"#">>}
                                }
                             ]
                }
            },
            principal => minirest:ref(<<"principal">>)
        }
    },
    Principal = #{
      oneOf => [ minirest:ref(<<"principal_username">>)
               , minirest:ref(<<"principal_clientid">>)
               , minirest:ref(<<"principal_ipaddress">>)
               , #{type => string, enum=>[<<"all">>], example => <<"all">>}
               , #{type => object,
                   required => ['and'],
                   properties => #{'and' => #{type => array,
                                              items => #{oneOf => [ minirest:ref(<<"principal_username">>)
                                                                  , minirest:ref(<<"principal_clientid">>)
                                                                  , minirest:ref(<<"principal_ipaddress">>)
                                                                  ]}}},
                   example => #{'and' => [#{username => <<"emqx">>}, #{clientid => <<"emqx">>}]}
                  }
               , #{type => object,
                   required => ['or'],
                   properties => #{'and' => #{type => array,
                                              items => #{oneOf => [ minirest:ref(<<"principal_username">>)
                                                                  , minirest:ref(<<"principal_clientid">>)
                                                                  , minirest:ref(<<"principal_ipaddress">>)
                                                                  ]}}},
                   example => #{'or' => [#{username => <<"emqx">>}, #{clientid => <<"emqx">>}]}
                  }
               ]
    },
    PrincipalUsername = #{type => object,
                           required => [username],
                           properties => #{username => #{type => string}},
                           example => #{username => <<"emqx">>}
                          },
    PrincipalClientid = #{type => object,
                           required => [clientid],
                           properties => #{clientid => #{type => string}},
                           example => #{clientid => <<"emqx">>}
                          },
    PrincipalIpaddress = #{type => object,
                            required => [ipaddress],
                            properties => #{ipaddress => #{type => string}},
                            example => #{ipaddress => <<"127.0.0.1">>}
                           },
    [ #{<<"returned_rules">> => RetruenedRules}
    , #{<<"rules">> => Rules}
    , #{<<"simple_rule">> => SimpleRule}
    , #{<<"principal">> => Principal}
    , #{<<"principal_username">> => PrincipalUsername}
    , #{<<"principal_clientid">> => PrincipalClientid}
    , #{<<"principal_ipaddress">> => PrincipalIpaddress}
    ].
