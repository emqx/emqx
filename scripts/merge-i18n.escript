#!/usr/bin/env escript

-mode(compile).

main(_) ->
    {ok, BaseConf} = file:read_file("apps/emqx_dashboard/etc/emqx_dashboard_i18n.conf"),

    Cfgs = get_all_cfgs("apps/"),
    Conf = [merge(BaseConf, Cfgs),
            io_lib:nl()
            ],
    ok = file:write_file("apps/emqx_dashboard/etc/i18n.conf.all", Conf).

merge(BaseConf, Cfgs) ->
    lists:foldl(
      fun(CfgFile, Acc) ->
              case filelib:is_regular(CfgFile) of
                  true ->
                      {ok, Bin1} = file:read_file(CfgFile),
                      [Acc, io_lib:nl(), Bin1];
                  false -> Acc
              end
      end, BaseConf, Cfgs).

get_all_cfgs(Root) ->
    Apps = filelib:wildcard("*", Root) -- ["emqx_machine", "emqx_dashboard"],
    Dirs = [filename:join([Root, App]) || App <- Apps],
    lists:foldl(fun get_cfgs/2, [], Dirs).

get_all_cfgs(Dir, Cfgs) ->
    Fun = fun(E, Acc) ->
                  Path = filename:join([Dir, E]),
                  get_cfgs(Path, Acc)
          end,
    lists:foldl(Fun, Cfgs, filelib:wildcard("*", Dir)).

get_cfgs(Dir, Cfgs) ->
    case filelib:is_dir(Dir) of
        false ->
            Cfgs;
        _ ->
            Files = filelib:wildcard("*", Dir),
            case lists:member("etc", Files) of
                false ->
                    try_enter_child(Dir, Files, Cfgs);
                true ->
                    EtcDir = filename:join([Dir, "etc"]),
                    %% the conf name must start with emqx
                    %% because there are some other conf, and these conf don't start with emqx
                    Confs = filelib:wildcard("emqx*_i18n.conf", EtcDir),
                    NewCfgs = [filename:join([EtcDir, Name]) || Name <- Confs],
                    try_enter_child(Dir, Files, NewCfgs ++ Cfgs)
            end
    end.

try_enter_child(Dir, Files, Cfgs) ->
    case lists:member("src", Files) of
        false ->
            Cfgs;
        true ->
            get_all_cfgs(filename:join([Dir, "src"]), Cfgs)
    end.
