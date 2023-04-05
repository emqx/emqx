#!/usr/bin/env escript

-mode(compile).

main(_) ->
    main_per_lang("en"),
    main_per_lang("zh").

main_per_lang(Lang) ->
    BaseConf = <<"">>,
    Cfgs0 = get_all_files(Lang),
    Conf = merge(BaseConf, Cfgs0),
    OutputFile = "apps/emqx_dashboard/priv/i18n." ++ Lang ++ ".conf",
    ok = filelib:ensure_dir(OutputFile),
    ok = file:write_file(OutputFile, Conf).

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

get_all_files(Lang) ->
    Dir =
        case Lang of
            "en" -> filename:join(["rel", "i18n"]);
            "zh" -> filename:join(["rel", "i18n", "zh"])
        end,
    Files = filelib:wildcard("*.hocon", Dir),
    lists:map(fun(Name) -> filename:join([Dir, Name]) end, Files).
