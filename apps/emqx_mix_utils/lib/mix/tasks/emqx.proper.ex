defmodule Mix.Tasks.Emqx.Proper do
  use Mix.Task

  alias Mix.Tasks.Emqx.Ct, as: ECt

  # todo: invoke the equivalent of `make merge-config` as a requirement...
  @requirements ["compile", "loadpaths"]

  @impl true
  def run(args) do
    input_opts = parse_args!(args)

    Enum.each([:common_test, :eunit, :mnesia], &ECt.add_to_path_and_cache/1)

    ECt.ensure_whole_emqx_project_is_loaded()
    ECt.unload_emqx_applications!()

    {_, 0} = System.cmd("epmd", ["-daemon"])
    node_name = :"test@127.0.0.1"
    :net_kernel.start([node_name, :longnames])

    # unmangle PROFILE env because some places (`:emqx_conf.resolve_schema_module`) expect
    # the version without the `-test` suffix.
    System.fetch_env!("PROFILE")
    |> String.replace_suffix("-test", "")
    |> then(&System.put_env("PROFILE", &1))

    ECt.maybe_start_cover()
    if ECt.cover_enabled?(), do: ECt.cover_compile_files()

    for {mod, fun} <- discover_props() do
      Mix.shell().info("testing #{mod}:#{fun}")
      opts = fetch_opts(mod, fun)
      :proper.quickcheck(apply(mod, fun, []), opts)
    end
    |> then(fn results ->
      if Enum.all?(results) do
        if ECt.cover_enabled?(), do: ECt.write_coverdata(input_opts)
        :ok
      else
        System.halt(1)
      end
    end)
  end

  defp add_to_path_and_cache(lib_name) do
    :code.lib_dir()
    |> Path.join("#{lib_name}-*")
    |> Path.wildcard()
    |> hd()
    |> Path.join("ebin")
    |> to_charlist()
    |> :code.add_path(:cache)
  end

  ## TODO: allow filtering modules and test names
  defp discover_props() do
    Mix.Dep.Umbrella.cached()
    |> Enum.map(fn dep ->
      dep.opts[:path]
      |> Path.join("test")
    end)
    |> Mix.Utils.extract_files("prop_*.erl")
    |> Enum.flat_map(fn suite_path ->
      suite_path
      |> Path.basename(".erl")
      |> String.to_atom()
      |> then(fn suite_mod ->
        suite_mod.module_info(:exports)
        |> Enum.filter(fn {name, _arity} -> to_string(name) =~ ~r/^prop_/ end)
        |> Enum.map(fn {name, _arity} -> {suite_mod, name} end)
      end)
    end)
  end

  defp fetch_opts(mod, fun) do
    try do
      mod.fun(:opts)
    rescue
      e in [FunctionClauseError, UndefinedFunctionError] -> []
    end
  end

  defp parse_args!(args) do
    {opts, _rest} =
      OptionParser.parse!(
        args,
        strict: [
          cover_export_name: :string
        ]
      )

    %{
      cover_export_name: Keyword.get(opts, :cover_export_name, "proper")
    }
  end
end
