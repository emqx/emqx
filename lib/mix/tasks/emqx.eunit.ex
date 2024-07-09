defmodule Mix.Tasks.Emqx.Eunit do
  use Mix.Task

  alias EMQXUmbrella.MixProject, as: UMP

  if UMP.new_mix_build?() do
    Code.require_file("emqx.ct.ex", __DIR__)
  end

  alias Mix.Tasks.Emqx.Ct, as: ECt

  # todo: invoke the equivalent of `make merge-config` as a requirement...
  @requirements ["compile", "loadpaths"]

  @impl true
  def run(args) do
    Mix.debug(true)
    IO.inspect(args)


    Enum.each([:common_test, :eunit, :mnesia], &ECt.add_to_path_and_cache/1)

    ECt.ensure_whole_emqx_project_is_loaded!()
    ECt.unload_emqx_applications!()

    {_, 0} = System.cmd("epmd", ["-daemon"])
    node_name = :"test@127.0.0.1"
    :net_kernel.start([node_name, :longnames])

    # unmangle PROFILE env because some places (`:emqx_conf.resolve_schema_module`) expect
    # the version without the `-test` suffix.
    System.fetch_env!("PROFILE")
    |> String.replace_suffix("-test", "")
    |> then(& System.put_env("PROFILE", &1))

    discover_tests()
    |> :eunit.test(
      verbose: true,
      print_depth: 100
    )
    |> case do
         :ok -> :ok
         :error -> Mix.raise("errors found in tests")
       end
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
  defp discover_tests() do
    Mix.Dep.Umbrella.cached()
    |> Enum.map(& {:application, &1.app})
  end
end
