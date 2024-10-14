## alinkiot_tdengine

TDengine for erlang

## example
```erlang
%% 创建数据库
alinkiot_tdengine:create_database(<<"alinkiot">>, 3650).

%% 创建超级表
alinkiot_tdengine:create_schema(#{
    <<"stable">> => <<"stablename">>,
    <<"fields">> => [
        #{<<"name">> => <<"fieldname1">>, <<"type">> => <<"int">>},
        #{<<"name">> => <<"fieldname2">>, <<"type">> => <<"float">>},
        #{<<"name">> => <<"fieldname3">>, <<"type">> => <<"string">>},
        #{<<"name">> => <<"fieldname4">>, <<"type">> => <<"time">>}
    ],
    <<"tags">> => [
        #{<<"name">> => <<"tagname1">>, <<"type">> => <<"int">>},
        #{<<"name">> => <<"tagname2">>, <<"type">> => <<"float">>},
        #{<<"name">> => <<"tagname3">>, <<"type">> => <<"string">>},
        #{<<"name">> => <<"tagname4">>, <<"type">> => <<"time">>}
    ]
}).

%% 创建子表
alinkiot_tdengine:create_schema(#{
    <<"table">> => <<"tablename">>,
    <<"using">> => <<"stablename">>,
    <<"tags">> => [
        #{<<"name">> => <<"tagname1">>, <<"value">> => 12},
        #{<<"name">> => <<"tagname2">>, <<"value">> => 1.0},
        #{<<"name">> => <<"tagname3">>, <<"value">> => <<"3333">>},
        #{<<"name">> => <<"tagname4">>, <<"value">> => 1234567890}
    ]
}).
%% 创建普通表
alinkiot_tdengine:create_schema(#{
    <<"table">> => <<"tablename2">>,
    <<"fields">> => [
        #{<<"name">> => <<"fieldname1">>, <<"type">> => <<"int">>},
        #{<<"name">> => <<"fieldname2">>, <<"type">> => <<"float">>},
        #{<<"name">> => <<"fieldname3">>, <<"type">> => <<"string">>},
        #{<<"name">> => <<"fieldname4">>, <<"type">> => <<"time">>}
    ],
    <<"tags">> => [
        #{<<"name">> => <<"tagname1">>, <<"type">> => <<"int">>},
        #{<<"name">> => <<"tagname2">>, <<"type">> => <<"float">>},
        #{<<"name">> => <<"tagname3">>, <<"type">> => <<"string">>},
        #{<<"name">> => <<"tagname4">>, <<"type">> => <<"time">>}
    ]
}).
%%查询表
alinkiot_tdengine:query_alink(#{<<"addr">> => <<"000001">>, <<"where">> => #{<<"time[begin]">> => <<"2023-03-23 07:50:35">>}, <<"orderBy[asc]">> => <<"time">>, <<"fields">> => [#{<<"name">> => <<"temperature">>, <<"type">> => <<"integer">>}], <<"interval">> => <<"1s">>, <<"pageSize">> => 2, <<"pageNum">> => 2}).


%% 插入设备
alinkiot_tdengine:insert_object(<<"SW00000001">>, #{
    <<"using">> => <<"Product">>,
    <<"tags">> => [<<"p0001">>, <<"00000001">>],
    <<"values">> => [
        [now, 222, 2.1, <<"aaaaa">>, false],
        [now, 222, 2.2, <<"aaaaa">>, false],
        [now, 222, 2.3, <<"aaaaa">>, false]
    ]
}),
F =
    fun(I) ->
        Dev = list_to_binary(io_lib:format("~8.10.0B,", [I])),
        create_object(<<"SW", Dev/binary>>, #{
            <<"using">> => <<"Product">>,
            <<"tags">> => [<<"Product">>, Dev],
            <<"values">> => [
                [now, 222, 2.1, <<"aaaaa">>, false],
                [now, 222, 2.2, <<"aaaaa">>, false],
                [now, 222, 2.3, <<"aaaaa">>, false]
            ]
        })
    end,
[F(I) || I <- lists:seq(1, 10)],
alinkiot_tdengine:query_object(<<"Product">>, #{
    <<"keys">> => <<"config,enable">>,
    <<"limit">> => 2,
    <<"skip">> => 0,
    <<"order">> => <<"-createdAt">>,
    <<"where">> => #{
        <<"flag">> => false
    }
}).

%% 创建普通表
alinkiot_tdengine:create_schemas(#{
    <<"tableName">> => <<"Device">>,
    <<"fields">> => [
        {<<"devaddr">>, #{
            <<"type">> => <<"NCHAR(10)">>
        }},
        {<<"enable">>, #{
            <<"type">> => <<"BOOL">>
        }},
        {<<"description">>, #{
            <<"type">> => <<"NCHAR(10)">>
        }}
    ]
}).

alinkiot_tdengine:create_schemas(#{
    <<"tableName">> => <<"Device2">>,
    <<"fields">> => [
        {<<"devaddr">>, #{
            <<"type">> => <<"NCHAR(10)">>
        }},
        {<<"enable">>, #{
            <<"type">> => <<"BOOL">>
        }},
        {<<"description">>, #{
            <<"type">> => <<"NCHAR(10)">>
        }}
    ]
}).

%% 插入一条记录
alinkiot_tdengine:create_object(<<"Device">>, #{
    <<"values">> => [now, <<"00000001">>, true, <<>>]
}).

%% 插入一条记录，数据对应到指定的列
alinkiot_tdengine:create_object(<<"Device">>, #{
    <<"fields">> => [<<"createdAt">>, <<"devaddr">>, <<"enable">>],
    <<"values">> => [now, <<"00000002">>, true]
}).

%% 插入多条记录
alinkiot_tdengine:batch(#{
    <<"tableName">> => <<"Device">>,
    <<"values">> => [
        [now, <<"00000003">>, true, <<>>],
        [now, <<"00000004">>, true, <<>>],
        [now, <<"00000005">>, true, <<>>]
    ]
}).

%% 按指定的列插入多条记录
alinkiot_tdengine:batch(#{
    <<"tableName">> => <<"Device">>,
    <<"fields">> => [<<"createdAt">>, <<"devaddr">>, <<"enable">>],
    <<"values">> => [
        [now, <<"00000006">>, true],
        [now, <<"00000007">>, true],
        [now, <<"00000008">>, true]
    ]
}).

%% 向多个表插入多条记录
alinkiot_tdengine:batch([
    #{
        <<"tableName">> => <<"Device">>,
        <<"values">> => [
            [now, <<"00000009">>, true, <<>>],
            [now, <<"00000010">>, true, <<>>]
        ]},
    #{
        <<"tableName">> => <<"Device2">>,
        <<"values">> => [
            [now, <<"00000001">>, true, <<>>],
            [now, <<"00000002">>, true, <<>>]
        ]}
]).

%% 同时向多个表按列插入多条记录
alinkiot_tdengine:batch([
    #{
        <<"tableName">> => <<"Device">>,
        <<"fields">> => [<<"createdAt">>, <<"devaddr">>, <<"enable">>],
        <<"values">> => [
            [now, <<"00000011">>, true],
            [now, <<"00000012">>, true],
            [now, <<"00000013">>, true]
        ]},
    #{
        <<"tableName">> => <<"Device2">>,
        <<"fields">> => [<<"createdAt">>, <<"devaddr">>, <<"enable">>],
        <<"values">> => [
            [now, <<"00000003">>, true],
            [now, <<"00000004">>, true],
            [now, <<"00000005">>, true]
        ]}
]).
```