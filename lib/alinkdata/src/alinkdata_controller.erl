-module(alinkdata_controller).
-include("alinkdata.hrl").
-behavior(ehttpd_rest).
-ehttpd_rest(default).
-export([swagger/1, handle/4]).
-export([response/3]).



swagger(default) ->
    Path = code:priv_dir(alinkdata),
    Global = filename:join([Path, "swagger/swagger_global.json"]),
    {ok, Data} = file:read_file(Global),
    Schema = jiffy:decode(Data, [return_maps]),
    [Schema | swagger()].


-spec handle(OperationID :: atom(), Args :: map(), Context :: map(), Req :: ehttpd_req:req()) ->
    {Status :: ehttpd_req:http_status(), Body :: map()} |
    {Status :: ehttpd_req:http_status(), Headers :: map(), Body :: map()} |
    {Status :: ehttpd_req:http_status(), Headers :: map(), Body :: map(), Req :: ehttpd_req:req()}.
handle(OperationID, Args, Context, Req) ->
    logger:debug("handle ~p ~p", [OperationID, maps:get(path, Req)]),
    case alinkdata_service:select_service(OperationID, Args, Context, Req) of
        no_service ->
            common_handle(OperationID, Args, Context, Req);
        {Mod, Fun} ->
            NArgs =
                maps:fold(fun(K, V, Acc) ->
                    case V =:= null of
                        true -> Acc;
                        _ -> Acc#{K => V}
                    end
                          end, #{}, Args),
            Mod:Fun(OperationID, NArgs, Context, Req)
    end.


common_handle(_OperationID, Args, Context, Req) ->
    Extend = maps:get(extend, Context, #{}),
    case Extend of
        #{<<"action">> := Act, <<"table">> := Table} when
            Act == <<"QUERY">>; Act == <<"GET">>; Act == <<"DELETE">> ->
            {Args1, Where} = alinkdata_formater:to_field(Table, Args),
            common_query(Context, Args1#{<<"where">> => Where}, Req);
        #{<<"action">> := Act} when Act == <<"ADD">>; Act == <<"PUT">> ->
            {{Y,M,D},{H,N,S}} = calendar:local_time(),
            Now = list_to_binary(lists:concat([Y, "-", M, "-", D, " ", H, ":", N, ":", S])),
            Args1 =
                case Act of
                    <<"ADD">> -> Args#{<<"createTime">> => Now};
                    <<"PUT">> -> Args#{<<"updateTime">> => Now}
                end,
            common_query(Context, Args1, Req);
        _ ->
            common_query(Context, Args, Req)
    end.



common_query(#{
    extend := #{
        <<"table">> := Table,
        <<"action">> := <<"QUERY">>
    }
} = _Context, Args, Req) ->
    Format =
        fun(Field, Value, Acc) ->
            alinkdata_formater:format_field(Table, Field, Value, Acc)
        end,
    Data = #{code => 200},
    Show = maps:get(<<"show">>, Args, undefined),
    case alinkdata_mysql:query(default, get_table(Table), Args, Format) of
        {ok, Rows} when Show == <<"treeselect">> ->
            Fun = fun(Row) -> alinkdata_formater:format_tree(Table, Row) end,
            Tree = alinkdata_utils:create_tree(Rows, Fun),
            response(200, Data#{data => Tree}, Req);
        {ok, Rows} ->
            response(200, Data#{data => Rows}, Req);
        {ok, Count, Rows} ->
            response(200, Data#{total => Count, rows => Rows}, Req);
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            response(500, #{code => 500, msg => Msg}, Req)
    end;

common_query(#{
    extend := #{
        <<"table">> := <<"user">> = Table,
        <<"action">> := <<"GET">>
    }
} = _Context, Args, Req) ->
    Format =
        fun(Field, Value, Acc) ->
            alinkdata_formater:format_field(Table, Field, Value, Acc)
        end,
    Args1 = Args#{
        <<"pageSize">> => 1
%%        <<"left">> =>
    },
    case alinkdata_mysql:query(default, get_table(Table), Args1, Format) of
        {ok, []} ->
            Response = #{code => 404},
            response(404, Response, Req);
        {ok, [Row]} ->
            Data = #{code => 200},
            Response = Data#{data => Row},
            response(200, Response, Req);
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            response(500, #{code => 500, msg => Msg}, Req)
    end;

common_query(#{
    extend := #{
        <<"table">> := <<"role">> = Table,
        <<"action">> := <<"GET">>
    }
} = _Context, Args, Req) ->
    Format =
        fun(Field, Value, Acc) ->
            alinkdata_formater:format_field(Table, Field, Value, Acc)
        end,
    Args1 = Args#{ <<"pageSize">> => 1 },
    case alinkdata_mysql:query(default, get_table(Table), Args1, Format) of
        {ok, []} ->
            Response = #{code => 404},
            response(404, Response, Req);
        {ok, [Row]} ->
            Data = #{code => 200},
            Response = Data#{data => Row},
            response(200, Response, Req);
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            response(500, #{code => 500, msg => Msg}, Req)
    end;


common_query(#{
    extend := #{
        <<"table">> := Table,
        <<"action">> := <<"GET">>
    }
} = _Context, Args, Req) ->
    Format =
        fun(Field, Value, Acc) ->
            alinkdata_formater:format_field(Table, Field, Value, Acc)
        end,
    case alinkdata_mysql:query(default, get_table(Table), Args#{ <<"pageSize">> => 1 }, Format) of
        {ok, []} ->
            Response = #{code => 404},
            response(404, Response, Req);
        {ok, [Row]} ->
            Data = #{code => 200},
            Response = Data#{data => Row},
            response(200, Response, Req);
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            response(500, #{code => 500, msg => Msg}, Req)
    end;

common_query(#{
    extend := #{
        <<"table">> := Table,
        <<"action">> := <<"ADD">>
    },
    user := User
} = _Context, Args, Req) ->
    #{ <<"user">> := #{ <<"username">> := UserName  } } = User,
    {_, Data} = alinkdata_formater:to_field(Table, Args#{
        <<"createBy">> => UserName
    }),
    case alinkdata_mysql:create(default, get_table(Table), Data) of
        ok ->
            response(200, #{}, Req);
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            response(500, #{code => 500, msg => Msg}, Req)
    end;

common_query(#{
    extend := #{
        <<"table">> := Table,
        <<"action">> := <<"PUT">>,
        <<"id">> := Key
    },
    user := User
} = _Context, Args, Req) ->
    case maps:get(Key, Args, <<>>) of
        <<>> ->
            response(400, #{}, Req);
        Id ->
            #{ <<"user">> := #{ <<"username">> := UserName  } } = User,
            Args1 = maps:without([Key], Args),
            {_, Data} = alinkdata_formater:to_field(Table, Args1#{ <<"updateBy">> => UserName }),
            {_, Where} = alinkdata_formater:to_field(Table, #{Key => Id}),
            case alinkdata_mysql:update(default, get_table(Table), Where, Data) of
                ok ->
                    response(200, #{}, Req);
                {error, Reason} ->
                    Msg = list_to_binary(io_lib:format("~p", [Reason])),
                    response(500, #{code => 500, msg => Msg}, Req)
            end
    end;

common_query(#{
    extend := #{
        <<"table">> := Table,
        <<"action">> := <<"DELETE">>
    }
} = _Context, Args, Req) ->
    case alinkdata_mysql:delete(default, get_table(Table), Args) of
        ok ->
            response(200, #{}, Req);
        {error, Reason} ->
            response(400, #{ msg => Reason }, Req)
    end;

common_query(#{
    extend := #{
        <<"table">> := _Table,
        <<"action">> := <<"IMPORT">>
    }
} = _Context, _Args, Req) ->
    response(500, #{}, Req);

common_query(#{
    extend := #{
        <<"table">> := _Table,
        <<"action">> := <<"EXPORT">>
    }
} = _Context, _Args, Req) ->
    response(500, #{}, Req).

response(Status, Data, Req) ->
    Header = #{},
    {Status, Header, Data, Req}.



get_table(<<"gen_table">>) -> <<"gen_table">>;
get_table(<<"gen_table_column">>) -> <<"gen_table_column">>;
get_table(Table) -> <<?PREFIX/binary, Table/binary>>.

swagger() ->
    {ok, Tables} = load_from_file(),
    OtherTables = alinkdata_generate:generate_swagger_table_infos(),
    lists:foldl(
        fun(Table, Acc) ->
            [create_swagger(Table) | Acc]
        end, [], Tables ++ OtherTables).

load_from_file() ->
    Path = code:priv_dir(alinkdata),
    {ok, Data} = file:read_file(filename:join([Path, "table/tables.json"])),
    Tables = jiffy:decode(Data, [return_maps]),
    {ok, Tables}.



create_swagger(#{
    <<"tableName">> := TableName0,
    <<"fields">> := Fields
} = Table) ->
    TableName = binary:replace(TableName0, ?PREFIX, <<>>),
    {Properties, Maps} = get_definitions(Fields),
    alinkdata_formater:save(TableName, Maps),
    create_swagger1(Table, TableName, Properties).



create_swagger1(#{<<"alinkdata_generated">> := true,
                  <<"like_fields">> := LikeFields} = Table, TableName, Properties) ->
    #{<<"parameters">> := QueryObjectParams} = query_object(TableName),
    LiekFieldsParams =
        lists:map(
            fun(#{<<"alias">> := F}) ->
                #{
                    <<"in">> => <<"query">>,
                    <<"name">> => F,
                    <<"description">> => F,
                    <<"required">> => false,
                    <<"type">> => <<"string">>
                }
        end, LikeFields),
    Paths0 = #{
        <<"/row/", TableName/binary>> => get_generated_row(Table, #{}),
        <<"/table/", TableName/binary, "/list">> => #{
            <<"get">> => (query_object(TableName))#{
                <<"parameters">> => LiekFieldsParams ++ QueryObjectParams
            }
        }
    },
    Paths1 = append_export_api(Paths0, TableName),

    Paths = maps:merge(Paths1, get_extra_generated_paths(TableName)),
    #{
        <<"tags">> => [#{
            <<"name">> => TableName,
            <<"description">> => <<>>
        }],
        <<"definitions">> => #{
            TableName => #{
                <<"type">> => <<"object">>,
                <<"properties">> => Properties
            }
        },
        <<"paths">> => Paths
    };
create_swagger1(Table, TableName, Properties) ->
    Paths0 = #{
        <<"/import/", TableName/binary>> => #{
            <<"get">> => import_table(TableName)
        },
        <<"/row/", TableName/binary>> => get_row(Table, #{
            <<"post">> => post_object(TableName)
        }),
        <<"/table/", TableName/binary, "/list">> => #{
            <<"get">> => query_object(TableName)
        }
    },
    Paths1 = append_export_api(Paths0, TableName),
    Paths = maps:merge(Paths1, get_extra_paths(TableName)),

    #{
        <<"tags">> => [#{
            <<"name">> => TableName,
            <<"description">> => <<>>
        }],
        <<"definitions">> => #{
            TableName => #{
                <<"type">> => <<"object">>,
                <<"properties">> => Properties
            }
        },
        <<"paths">> => Paths
    }.

get_row(#{
    <<"id">> := Id,
    <<"tableName">> := TableName0
}, Map) ->
    TableName = binary:replace(TableName0, ?PREFIX, <<>>),
    Map#{
        <<"get">> => get_object(TableName, Id),
        <<"delete">> => delete_object(TableName, Id),
        <<"put">> => update_object(TableName, Id)
    };
get_row(_, Map) -> Map.

get_generated_row(#{
    <<"id">> := Id,
    <<"tableName">> := TableName0
}, Map) ->
    TableName = binary:replace(TableName0, ?PREFIX, <<>>),
    Map#{
        <<"get">> => get_object2(TableName, Id),
        <<"delete">> => delete_object(TableName, Id),
        <<"put">> => update_object2(TableName, Id),
        <<"post">> => post_object2(TableName)
    };
get_generated_row(_, Map) -> Map.



get_extra_paths(<<"post">>) ->
    #{
        <<"/table/post/optionselect">> => #{
            <<"get">> => query_object(<<"post">>)
        }
    };
get_extra_paths(<<"dict_type">>) ->
    #{
        <<"/table/dict_type/optionselect">> => #{
            <<"get">> => query_object(<<"dict_type">>)
        }
    };
get_extra_paths(<<"dept">>) ->
    #{
        <<"/table/dept/roleDeptTreeselect">> => #{
            <<"get">> => get_object(<<"dept">>, <<"roleId">>)
        },
        <<"/table/dept/list/exclude/{deptId}">> => #{
            <<"get">> => get_object(<<"dept">>, <<"deptId">>)
        }
    };
get_extra_paths(<<"role">>) ->
    #{
        <<"/table/role/changeStatus">> => #{
            <<"put">> => update_object(<<"role">>, undefined)
        },
        <<"/table/role/dataScope">> => #{
            <<"put">> => update_object(<<"role">>, undefined)
        },
        <<"/table/role/optionselect">> => #{
            <<"get">> => query_object(<<"role">>)
        },
        <<"/table/role/authUser/allocatedList">> => #{
            <<"get">> => query_object(<<"role">>)
        },
        <<"/table/role/authUser/unallocatedList">> => #{
            <<"get">> => query_object(<<"role">>)
        },
        <<"/table/role/authUser/cancelAll">> => #{
            <<"put">> => update_object(<<"role">>, undefined)
        },
        <<"/table/role/authUser/cancel">> => #{
            <<"put">> => update_object(<<"role">>, undefined)
        },
        <<"/table/role/authUser/selectAll">> => #{
            <<"put">> => update_object(<<"role">>, undefined)
        }
    };
get_extra_paths(<<"user">>) ->
     #{
        <<"/table/user/changeStatus">> => #{
            <<"put">> => update_object(<<"user">>, undefined)
        },
         <<"/table/user/profile">> => #{
             <<"get">> => (query_object(<<"user">>))#{<<"permission">> => <<"system:profile:get">>},
             <<"put">> => (update_object(<<"user">>, undefined))#{<<"permission">> => <<"system:profile:update">>}
         },
         <<"/table/user/importTemplate">> => #{
             <<"post">> => export_table(<<"user">>)
         },
         <<"/table/user/importData">> => #{
             <<"post">> => import_table(<<"user">>)
         },

         <<"/table/user/resetPwd">> => #{
            <<"put">> => (update_object(<<"user">>, undefined))#{<<"permission">> => <<"system:user:resetPwd">>}
         },
         <<"/table/user/authRole">> => #{
             <<"get">> => (get_object(<<"user">>, <<"userId">>))#{<<"permission">> => <<"system:user:query">>},
             <<"put">> =>  (update_object(<<"user">>, <<"userId">>))#{<<"permission">> => <<"system:user:edit">>}
         },
         <<"/table/user/wechat/qr">> => #{
             <<"get">> =>  (maps:remove(<<"permission">>, query_object(<<"user">>)))#{
                 <<"security">> => [],
                 <<"parameters">> => []
             }
         },
         <<"/table/user/wechat/bind">> => #{
             <<"post">> =>  (post_object(<<"user">>))#{
                 <<"summary">> => <<"微信绑定"/utf8>>,
                 <<"description">> => <<"微信绑定"/utf8>>,
                 <<"permission">> => <<"system:profile:edit">>,
                 <<"parameters">> => [
                     #{
                         <<"in">> => <<"body">>,
                         <<"name">> => <<"wechatSession">>,
                         <<"description">> => <<"微信凭证"/utf8>>,
                         <<"required">> => true
                     }
                 ]
             }
         },
         <<"/table/user/wechat/verify">> => #{
             <<"post">> =>  (maps:remove(<<"permission">>, post_object(<<"user">>)))#{
                 <<"summary">> => <<"验证微信扫码"/utf8>>,
                 <<"description">> => <<"验证微信扫码"/utf8>>,
                 <<"security">> => [],
                 <<"parameters">> => [
                     #{
                         <<"in">> => <<"body">>,
                         <<"name">> => <<"sceneId">>,
                         <<"description">> => <<"微信场景id"/utf8>>,
                         <<"required">> => true
                     }
                 ]
             }
         },
         <<"/table/user/wechat/unbind">> => #{
             <<"post">> =>  (post_object(<<"user">>))#{
                 <<"summary">> => <<"微信解绑"/utf8>>,
                 <<"description">> => <<"微信解绑"/utf8>>,
                 <<"permission">> => <<"system:profile:edit">>,
                 <<"parameters">> => []
             }
         },
         <<"/table/user/mini/bind">> => #{
             <<"post">> =>  (post_object(<<"user">>))#{
                 <<"summary">> => <<"小程序绑定"/utf8>>,
                 <<"description">> => <<"小程序绑定"/utf8>>,
                 <<"permission">> => <<"system:profile:edit">>,
                 <<"parameters">> => [
                     #{
                         <<"in">> => <<"body">>,
                         <<"name">> => <<"sessionKey">>,
                         <<"description">> => <<"小程序凭证"/utf8>>,
                         <<"required">> => true
                     }
                 ]
             }
         }

%%         <<"/table/user/profile/updatePwd">> => #{
%%             <<"put">> => (update_object(<<"user">>, undefined))#{<<"permission">> => <<"system:profile:update">>}
%%         }
     };
get_extra_paths(_) -> #{}.


get_extra_generated_paths(<<"device">>) ->
    #{
        <<"/table/import/device">> => #{
            <<"post">> => import_table(<<"device">>)
        },
        <<"/table/device/online">> => #{
            <<"get">> => (query_object(<<"device">>))#{
                <<"parameters">> => [
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"addr">>,
                        <<"description">> => <<"addr">>,
                        <<"required">> => false,
                        <<"type">> => <<"string">>
                    }
                ]
            }
        },
        <<"/table/device/instruction">> => #{
            <<"post">> => (query_object(<<"device">>))#{
                <<"parameters">> => [
                    #{
                        <<"in">> => <<"body">>,
                        <<"name">> => <<"addr">>,
                        <<"description">> => <<"设备地址"/utf8>>,
                        <<"required">> => true,
                        <<"type">> => <<"string">>
                    },
                    #{
                        <<"in">> => <<"body">>,
                        <<"name">> => <<"name">>,
                        <<"description">> => <<"物模型定义的变量名"/utf8>>,
                        <<"required">> => true,
                        <<"type">> => <<"string">>
                    },
                    #{
                        <<"in">> => <<"body">>,
                        <<"name">> => <<"value">>,
                        <<"description">> => <<"下发的指令数值"/utf8>>,
                        <<"required">> => true,
                        <<"type">> => <<"int">>
                    }
                ]
            }
        },
        <<"/table/device/children">> => #{
            <<"get">> => (query_object(<<"device">>))#{
                <<"summary">> => <<"Query device children">>,
                <<"description">> => <<"Query device children">>,
                <<"parameters">> => [
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"orderBy[asc]">>,
                        <<"description">> => <<"order by asc">>,
                        <<"required">> => false,
                        <<"type">> => <<"string">>
                    },
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"orderBy[desc]">>,
                        <<"description">> => <<"order by desc">>,
                        <<"required">> => false,
                        <<"type">> => <<"string">>
                    },
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"pageSize">>,
                        <<"description">> => <<"page size">>,
                        <<"required">> => false,
                        <<"default">> => 10,
                        <<"type">> => <<"integer">>
                    },
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"pageNum">>,
                        <<"description">> => <<"page num">>,
                        <<"required">> => false,
                        <<"default">> => 1,
                        <<"type">> => <<"integer">>
                    },
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"gateway">>,
                        <<"description">> => <<"gateway">>,
                        <<"required">> => true,
                        <<"type">> => <<"string">>
                    }
                ]
            }
        },
        <<"/table/device/kick">> => #{
            <<"post">> => (post_object(<<"device">>))#{
                <<"permission">> => <<"system:device:kick">>,
                <<"summary">> => <<"kick device">>,
                <<"description">> => <<"kick device">>,
                <<"parameters">> => [
                    #{
                        <<"in">> => <<"body">>,
                        <<"name">> => <<"addr">>,
                        <<"description">> => <<"设备地址"/utf8>>,
                        <<"required">> => true,
                        <<"type">> => <<"string">>
                    }
                ]
            }
        },
        <<"/table/device/cehouAnalyze">> => #{
            <<"get">> => (query_object(<<"device">>))#{
                <<"parameters">> => [
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"addr">>,
                        <<"description">> => <<"addr">>,
                        <<"required">> => true,
                        <<"type">> => <<"string">>
                    },
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"createTime[begin]">>,
                        <<"description">> => <<"起始时间"/utf8>>,
                        <<"required">> => false,
                        <<"type">> => <<"string">>
                    },
                    #{
                        <<"in">> => <<"query">>,
                        <<"name">> => <<"createTime[end]">>,
                        <<"description">> => <<"结束时间"/utf8>>,
                        <<"required">> => false,
                        <<"type">> => <<"string">>
                    }
                ]
            }
        }
    };
get_extra_generated_paths(<<"app">>) ->
    #{
        <<"/table/app/sync">> => #{
            <<"post">> => (query_object(<<"app">>))#{
                <<"summary">> => <<"同步平台数据给app"/utf8>>,
                <<"description">> => <<"同步平台数据给app"/utf8>>,
                <<"parameters">> => [
                    #{
                        <<"in">> => <<"body">>,
                        <<"name">> => <<"id">>,
                        <<"description">> => <<"app的id"/utf8>>,
                        <<"required">> => true,
                        <<"type">> => <<"integer">>
                    },
                    #{
                        <<"in">> => <<"body">>,
                        <<"name">> => <<"event">>,
                        <<"description">> => <<"event type:syncDeviceStatus|syncBaseData"/utf8>>,
                        <<"required">> => true,
                        <<"type">> => <<"string">>
                    }
                ]
            }
        }
    };
get_extra_generated_paths(<<"project">>) ->
    #{
        <<"/table/project/noticeSync">> => #{
            <<"post">> => (query_object(<<"project">>))#{
                <<"summary">> => <<"同步平台数据给app"/utf8>>,
                <<"description">> => <<"同步平台数据给app"/utf8>>,
                <<"parameters">> => [
                    #{
                        <<"in">> => <<"body">>,
                        <<"name">> => <<"id">>,
                        <<"description">> => <<"projectid"/utf8>>,
                        <<"required">> => true,
                        <<"type">> => <<"integer">>
                    },
                    #{
                        <<"in">> => <<"body">>,
                        <<"name">> => <<"event">>,
                        <<"description">> => <<"event type:syncDeviceStatus|syncBaseData"/utf8>>,
                        <<"required">> => true,
                        <<"type">> => <<"string">>
                    }
                ]
            }
        }
    };
get_extra_generated_paths(_) -> #{}.

get_definitions(Fields) ->
    lists:foldl(
        fun(#{<<"alias">> := Alias, <<"name">> := Name, <<"type">> := Type}, {Acc, Map}) ->
            Acc1 = Acc#{
                Alias => #{
                    <<"type">> => alinkdata_formater:format_type(Type),
                    <<"description">> => Alias
                }
            },
            {Acc1, [{Name, Alias}|Map]}
        end, {#{}, []}, Fields).



export_table(TableName) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":export">>,
        <<"summary">> => <<"Export ", TableName/binary>>,
        <<"description">> => <<"Export ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"EXPORT">>
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
            #{
                <<"in">> => <<"query">>,
                <<"name">> => <<"where">>,
                <<"description">> => <<>>,
                <<"required">> => false,
                <<"type">> => <<"string">>
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"return ", TableName/binary>>
            }
        })
    }.

import_table(TableName) ->
    #{
        <<"permission">> => <<"system:",TableName/binary,":import">>,
        <<"summary">> => <<"Import ", TableName/binary>>,
        <<"description">> => <<"Import ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"IMPORT">>
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
            #{
                <<"in">> => <<"body">>,
                <<"name">> => <<"updateSupport">>,
                <<"description">> => <<>>,
                <<"required">> => false,
                <<"type">> => <<"string">>
            },
            #{
                <<"in">> => <<"body">>,
                <<"name">> => <<"path">>,
                <<"description">> => <<>>,
                <<"required">> => false,
                <<"type">> => <<"string">>
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"return ", TableName/binary>>
            }
        })
    }.

query_object(TableName) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":list">>,
        <<"summary">> => <<"Query ", TableName/binary>>,
        <<"description">> => <<"Query ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"QUERY">>
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
            #{
                <<"in">> => <<"query">>,
                <<"name">> => <<"order">>,
                <<"description">> => <<"order by eg: score,-name">>,
                <<"required">> => false,
                <<"type">> => <<"string">>
            },
            #{
                <<"in">> => <<"query">>,
                <<"name">> => <<"pageSize">>,
                <<"description">> => <<"page size">>,
                <<"required">> => false,
                <<"default">> => 10,
                <<"type">> => <<"integer">>
            },
            #{
                <<"in">> => <<"query">>,
                <<"name">> => <<"pageNum">>,
                <<"description">> => <<"page num">>,
                <<"required">> => false,
                <<"default">> => 1,
                <<"type">> => <<"integer">>
            },
            #{
                <<"in">> => <<"query">>,
                <<"name">> => <<"keys">>,
                <<"description">> => <<>>,
                <<"required">> => false,
                <<"type">> => <<"string">>
            },
            #{
                <<"in">> => <<"query">>,
                <<"name">> => <<"where">>,
                <<"description">> => <<>>,
                <<"required">> => false,
                <<"type">> => <<"string">>
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<>>,
                <<"schema">> => #{
                    <<"type">> => <<"object">>,
                    <<"properties">> => #{
                        <<"rows">> =>  #{
                            <<"type">> => <<"array">>,
                            <<"items">> => #{
                                <<"$ref">> => <<"#/definitions/", TableName/binary>>
                            }
                        }
                    }
                }
            }
        })
    }.


post_object(TableName) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":add">>,
        <<"summary">> => <<"Add ", TableName/binary>>,
        <<"description">> => <<"Add ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"ADD">>
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
            #{
                <<"in">> => <<"body">>,
                <<"name">> => <<"body">>,
                <<"description">> => <<"">>,
                <<"required">> => true,
                <<"schema">> => #{
                    <<"$ref">> => <<"#/definitions/", TableName/binary>>
                }
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"Returns a confirmation message">>
            }
        })
    }.


post_object2(TableName) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":add">>,
        <<"summary">> => <<"Add ", TableName/binary>>,
        <<"description">> => <<"Add ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"POST">>
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
            #{
                <<"in">> => <<"body">>,
                <<"name">> => <<"body">>,
                <<"description">> => <<"">>,
                <<"required">> => true,
                <<"schema">> => #{
                    <<"$ref">> => <<"#/definitions/", TableName/binary>>
                }
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"Returns a confirmation message">>
            }
        })
    }.

get_object(TableName, Id) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":query">>,
        <<"summary">> => <<"Get ", TableName/binary>>,
        <<"description">> => <<"Get ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"GET">>
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
            #{
                <<"in">> => <<"path">>,
                <<"name">> => Id,
                <<"type">> => <<"integer">>,
                <<"description">> => <<TableName/binary, "' ", Id/binary>>,
                <<"required">> => true
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"return ", TableName/binary>>,
                <<"schema">> => #{
                    <<"$ref">> => <<"#/definitions/", TableName/binary>>
                }
            }
        })
    }.


get_object2(TableName, Id) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":query">>,
        <<"summary">> => <<"Get ", TableName/binary>>,
        <<"description">> => <<"Get ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"GET">>,
            <<"id">> => Id
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
            #{
                <<"in">> => <<"path">>,
                <<"name">> => Id,
                <<"type">> => <<"string">>,
                <<"description">> => <<TableName/binary, "' ", Id/binary>>,
                <<"required">> => true
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"return ", TableName/binary>>,
                <<"schema">> => #{
                    <<"$ref">> => <<"#/definitions/", TableName/binary>>
                }
            }
        })
    }.

update_object(TableName, Id) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":edit">>,
        <<"summary">> => <<"Update ", TableName/binary>>,
        <<"description">> => <<"Update ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"PUT">>,
            <<"id">> => Id
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
%%            #{
%%                <<"in">> => <<"path">>,
%%                <<"name">> => Id,
%%                <<"type">> => <<"integer">>,
%%                <<"description">> => <<TableName/binary, "' ", Id/binary>>,
%%                <<"required">> => true
%%            },
            #{
                <<"in">> => <<"body">>,
                <<"name">> => <<"body">>,
                <<"description">> => <<"">>,
                <<"required">> => true,
                <<"schema">> => #{
                    <<"$ref">> => <<"#/definitions/", TableName/binary>>
                }
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"Returns a confirmation message">>
            }
        })
    }.


update_object2(TableName, Id) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":edit">>,
        <<"summary">> => <<"Update ", TableName/binary>>,
        <<"description">> => <<"Update ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"PUT">>,
            <<"id">> => Id
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
%%            #{
%%                <<"in">> => <<"path">>,
%%                <<"name">> => Id,
%%                <<"type">> => <<"integer">>,
%%                <<"description">> => <<TableName/binary, "' ", Id/binary>>,
%%                <<"required">> => true
%%            },
            #{
                <<"in">> => <<"body">>,
                <<"name">> => <<"body">>,
                <<"description">> => <<"">>,
                <<"required">> => true,
                <<"schema">> => #{
                    <<"$ref">> => <<"#/definitions/", TableName/binary>>
                }
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"Returns a confirmation message">>
            }
        })
    }.

delete_object(TableName, Id) ->
    PermissionTab = permission_table(TableName),
    #{
        <<"permission">> => <<"system:",PermissionTab/binary,":remove">>,
        <<"summary">> => <<"Delete ", TableName/binary>>,
        <<"description">> => <<"Delete ", TableName/binary>>,
        <<"extend">> => #{
            <<"table">> => TableName,
            <<"action">> => <<"DELETE">>,
            <<"id">> => Id
        },
        <<"tags">> => [TableName],
        <<"parameters">> => [
            #{
                <<"in">> => <<"path">>,
                <<"name">> => Id,
                <<"type">> => <<"string">>,
                <<"description">> => <<TableName/binary, "' ", Id/binary>>,
                <<"required">> => true
            }
        ],
        <<"responses">> => get_responses(#{
            <<"200">> => #{
                <<"description">> => <<"Returns a confirmation message">>
            }
        })
    }.



get_responses(Responses) ->
    Responses#{
        <<"404">> => #{
            <<"description">> => <<"object not found">>,
            <<"schema">> => #{
                <<"$ref">> => <<"#/definitions/Error">>
            }
        },
        <<"400">> => #{
            <<"description">> => <<"Bad Request">>
        },
        <<"401">> => #{
            <<"description">> => <<"Unauthorized">>,
            <<"schema">> => #{
                <<"$ref">> => <<"#/definitions/Error">>
            }
        },
        <<"403">> => #{
            <<"description">> => <<"Forbidden">>
        },
        <<"500">> => #{
            <<"description">> => <<"Server Internal error">>,
            <<"schema">> => #{
                <<"$ref">> => <<"#/definitions/Error">>
            }
        }
    }.




permission_table(<<"dict_", _/binary>>) -> <<"dict">>;
permission_table(Table) -> Table.


append_export_api(Path, TableName) ->
    case is_export_enable(TableName) of
        true ->
            Path#{
                <<"/export/", TableName/binary>> => #{
                    <<"post">> => export_table(TableName)
                }
            };
        _ ->
            Path
    end.


is_export_enable(<<"history">>) ->
    true;
is_export_enable(Table) ->
    FileName = code:priv_dir(alinkdata) ++ "/export_desc/" ++ alinkutil_type:to_list(Table) ++ ".json",
    filelib:is_file(FileName).