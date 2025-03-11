defmodule Mix.Tasks.Emqx.Dialyzer do
  use Mix.Task

  alias Mix.Tasks.Emqx.Ct, as: ECt

  @requirements ["compile", "loadpaths"]

  @excluded_mods (
    [
      :emqx_exproto_v_1_connection_unary_handler_bhvr,
      :emqx_exproto_v_1_connection_handler_client,
      :emqx_exproto_v_1_connection_handler_bhvr,
      :emqx_exproto_v_1_connection_adapter_client,
      :emqx_exproto_v_1_connection_adapter_bhvr,
      :emqx_exproto_v_1_connection_unary_handler_client,
      :emqx_exhook_v_2_hook_provider_client,
      :emqx_exhook_v_2_hook_provider_bhvr,
      __MODULE__,
      Mix.Tasks.Compile.Asn1,
      Mix.Tasks.Emqx.Ct,
      Mix.Tasks.Emqx.Eunit,
      Mix.Tasks.Emqx.Proper,
      Mix.Tasks.Compile.Grpc,
      Mix.Tasks.Compile.CopySrcs,
    ]
    |> MapSet.new(&to_string/1)
  )
  ## Warnings such as "Expression produces a value of type bitstring(), but this value is
  ## unmatched" are not generated for these modules
  @excluded_mods_from_warnings (
    [
      :DurableMessage,
    ]
    |> MapSet.new(&to_string/1)
    |> MapSet.union(@excluded_mods)
  )

  @impl true
  def run(_args) do
    ECt.add_to_path_and_cache(:dialyzer)

    %{
      umbrella_apps: umbrella_apps,
      dep_apps: dep_apps
    } = resolve_apps()
    umbrella_files = Enum.flat_map(umbrella_apps, & resolve_files/1)
    dep_files = Enum.flat_map(dep_apps, & resolve_files/1)
    # Files to be considered; will be analyzed and their contracts taken into account.
    files =
      (umbrella_files ++ dep_files)
      |> Enum.reject(fn path ->
        name = Path.basename(path, ".beam")
        MapSet.member?(@excluded_mods, name)
      end)
      |> Enum.map(&to_charlist/1)
    # Files that might have warnings for them
    warning_files =
      umbrella_files
      |> Enum.reject(fn path ->
        name = Path.basename(path, ".beam")
        MapSet.member?(@excluded_mods_from_warnings, name)
      end)
      |> Enum.map(&to_charlist/1)
    warning_apps = Enum.sort(umbrella_apps)

    try do
      :dialyzer.run(
        analysis_type: :incremental,
        warnings: [
          :unmatched_returns,
          :error_handling
        ],
        # plt_location: ~c".",
        # plt_prefix: ~c"emqx_dialyzer",
        warning_files: warning_files,
        warning_files_rec: warning_files,
        # apps: umbrella_apps ++ dep_apps,
        # warning_apps: warning_apps,
        get_warnings: false,
        files: files,
        files_rec: files
      )
      |> Enum.map(& :dialyzer.format_warning(&1, filename_opt: :fullpath, indent_opt: false))
      |> tap(&IO.puts/1)
      |> case do
           [] ->
             Mix.shell().info("ok")

           [_ | _] ->
             Mix.raise("Errors found!")
         end
    catch
      {:dialyzer_error, msg} ->
        {:dialyzer_error, to_string(msg)}
      err ->
        {:throw, err}
    end
    |> IO.inspect(limit: :infinity)
  end

  defp resolve_apps() do
    base_apps = MapSet.new([:erts, :crypto])
    excluded_apps = MapSet.new([:emqx_mix_utils])
    acc = %{
      umbrella_apps: [],
      dep_apps: base_apps
    }

    Mix.Dep.Umbrella.loaded()
    |> Enum.reduce(acc, fn dep, acc ->
      # IO.inspect(dep)
      props = dep.opts[:app_properties]
      optional_apps = Keyword.get(props, :optional_applications, [])
      apps = Keyword.get(props, :applications, [])
      included_apps = Keyword.get(props, :included_applications, [])
      dep_apps = MapSet.new(optional_apps ++ apps ++ included_apps)
      acc
      |> Map.update!(:umbrella_apps, & [dep.app | &1])
      |> Map.update!(:dep_apps, & MapSet.union(&1, dep_apps))
    end)
    |> then(fn acc ->
      dep_apps =
        acc.dep_apps
        |> MapSet.difference(MapSet.new(acc.umbrella_apps))
        |> MapSet.difference(excluded_apps)
        |> Enum.reduce(MapSet.new(), &find_nested_apps/2)
        |> MapSet.difference(excluded_apps)
        |> Enum.filter(&app_present?/1)
      %{acc | dep_apps: dep_apps}
    end)
  end

  defp app_present?(app) do
    match?({:ok, _}, ebin_dir(app))
  end

  defp find_nested_apps(app, seen) do
    if MapSet.member?(seen, app) do
      seen
    else
      seen = MapSet.put(seen, app)
      apps = case :application.get_key(app, :applications) do
        {:ok, apps} -> apps
        :undefined -> []
      end
      included_apps = case :application.get_key(app, :included_applications) do
        {:ok, apps} -> apps
        :undefined -> []
      end
      optional_apps = case :application.get_key(app, :optional_applications) do
        {:ok, apps} -> apps
        :undefined -> []
      end
      Enum.reduce(apps ++ included_apps, seen, &find_nested_apps/2)
    end
  end

  defp resolve_files(app) do
    with {:ok, dir} <- ebin_dir(app) do
      Mix.Utils.extract_files([dir], [:beam])
    else
      _ -> []
    end
  end

  defp ebin_dir(app) do
    with dir when is_list(dir) <- :code.lib_dir(app, :ebin),
         dir = to_string(dir),
         true <- File.dir?(dir) || {:error, :not_a_dir} do
      {:ok, to_string(dir)}
    else
      error ->
        Mix.shell().info(IO.ANSI.format([
              [:yellow,
               "Unknown application: #{app}; error: #{inspect(error)}",
               "; if this is is an optional application, ignore."
              ],
            ]))
        :error
    end
  end
end
