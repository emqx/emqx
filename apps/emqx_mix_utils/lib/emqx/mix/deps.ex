defmodule EMQX.Mix.Deps do
  @moduledoc """
  Generate "used-by" dependency relationships for EMQX apps.

  This module analyzes BEAM files to determine which apps use (depend on) each app.
  The output is in "used-by" format (reverse dependency direction), meaning if app1 uses app2,
  the output will show "app2: app1" (app2 is used by app1).

  Analysis methods:
  1. Remote function calls: Uses xref to scan all BEAM files and find all remote calls (XC query).
     Maps caller and callee modules to their respective apps using module-to-app mapping from .app files.
  2. Include directives: Parses -include_lib() directives from BEAM files' abstract code to find
     header file dependencies between apps.

  Special handling:
  - emqx and emqx_conf are set to be used by all other apps (handled in gen_deps script)
  - Self-dependencies are excluded
  - Transitive closure is computed: if app1 uses app2, and app2 uses app3, then app1 transitively uses app3
  """

  @doc """
  Get "used-by" dependencies from remote function calls.

  ## Parameters
  - `rel_dir` - Path to the release lib directory (e.g., "_build/emqx-enterprise/lib")
  - `mod_to_app_map` - Map of Module => App atom

  ## Returns
  Map of App => Set of apps that call it (directly)
  """
  def get_call_dependents(rel_dir, mod_to_app_map) do
    all_remote_calls = get_all_remote_calls(rel_dir)
    emqx_apps_set = MapSet.new(Map.values(mod_to_app_map))
    emqx_apps = MapSet.to_list(emqx_apps_set)

    # Initialize with empty sets for all apps
    acc0 =
      Enum.reduce(emqx_apps, %{}, fn app, acc ->
        Map.put(acc, app, MapSet.new())
      end)

    collect_callee_to_caller_map(all_remote_calls, mod_to_app_map, emqx_apps_set, acc0)
  end

  @doc """
  Get "used-by" dependencies from include_lib directives.

  ## Parameters
  - `lib_dir` - Path to the release lib directory (e.g., "_build/emqx-enterprise/lib")
  - `app_names` - List of app atoms

  ## Returns
  Map of App => Set of apps that include headers from it (directly)
  """
  def get_include_dependents(lib_dir, app_names) do
    emqx_apps_set = MapSet.new(app_names)

    # Filter to only apps that have ebin directories with BEAM files
    apps_with_beams =
      Enum.filter(app_names, fn app ->
        app_name = Atom.to_string(app)
        ebin_path = Path.join([lib_dir, app_name, "ebin"])
        File.dir?(ebin_path)
      end)

    # Initialize with empty sets for all apps
    acc0 =
      Enum.reduce(app_names, %{}, fn app, acc ->
        Map.put(acc, app, MapSet.new())
      end)

    get_include_dependents_loop(apps_with_beams, lib_dir, emqx_apps_set, acc0)
  end

  @doc """
  Compute transitive closure of "used-by" relationships.

  ## Parameters
  - `used_by_maps` - List of maps, each map is App => Set of apps that directly use it
  - `all_apps` - List of all apps

  ## Returns
  Map of App => Set of apps that transitively use it
  """
  def compute_transitive_closure(used_by_maps, all_apps) when is_list(used_by_maps) do
    # Merge all maps first
    merged_map = merge_dependents(used_by_maps)

    # Initialize result map with all apps (even if they have no dependencies)
    # For each app, compute all apps that transitively use it
    Enum.reduce(all_apps, %{}, fn app, acc ->
      transitive_users = compute_transitive_users(app, merged_map, all_apps)
      Map.put(acc, app, transitive_users)
    end)
  end

  # Internal functions for remote calls

  defp get_all_remote_calls(lib_dir) do
    unless File.dir?(lib_dir) do
      raise "Directory not found: #{lib_dir}"
    end

    xref_server = :gen_deps_xref
    {:ok, _} = :xref.start(xref_server)

    try do
      :ok = :xref.set_default(xref_server, warnings: false)

      # Add the entire release (works correctly with Mix/Elixir)
      case :xref.add_release(xref_server, String.to_charlist(lib_dir)) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          raise "Failed to add release to xref: #{inspect(reason)}"
      end

      # Query for external calls (cross-module calls)
      case :xref.q(xref_server, "XC") do
        {:ok, calls} ->
          calls

        {:error, :xref_base, {:invalid_query, _}} ->
          # Try with charlist instead of string
          case :xref.q(xref_server, ~c"XC") do
            {:ok, calls} -> calls
            {:error, reason} -> raise "Xref query failed: #{inspect(reason)}"
          end

        {:error, reason} ->
          raise "Xref query failed: #{inspect(reason)}"
      end
    after
      :xref.stop(xref_server)
    end
  end

  defp collect_callee_to_caller_map(remote_calls, module_to_app_map, emqx_apps_set, acc0) do
    Enum.reduce(remote_calls, acc0, fn
      {{caller_module, _fun, _arity}, {callee_module, _fun2, _arity2}}, acc ->
        with {:ok, caller_app} <- get_emqx_app(caller_module, module_to_app_map, emqx_apps_set),
             {:ok, callee_app} <- get_emqx_app(callee_module, module_to_app_map, emqx_apps_set),
             false <- caller_app == callee_app do
          # Add CallerApp to CalleeApp's caller set
          add_dependent(acc, callee_app, caller_app)
        else
          _ -> acc
        end
    end)
  end

  defp get_emqx_app(module, module_to_app_map, emqx_apps_set) do
    case Map.get(module_to_app_map, module) do
      nil ->
        :not_emqx_app

      app ->
        if MapSet.member?(emqx_apps_set, app) do
          {:ok, app}
        else
          :not_emqx_app
        end
    end
  end

  # Internal functions for include_lib directives

  defp get_include_dependents_loop([], _lib_dir, _emqx_apps_set, acc), do: acc

  defp get_include_dependents_loop([app | rest], lib_dir, emqx_apps_set, acc) do
    app_name = Atom.to_string(app)
    ebin_path = Path.join([lib_dir, app_name, "ebin"])
    include_deps = get_include_lib_deps_from_beams(ebin_path, emqx_apps_set)

    # For each app that App includes headers from, add App to that app's including set
    new_acc =
      Enum.reduce(include_deps, acc, fn included_app, acc_map ->
        add_dependent(acc_map, included_app, app)
      end)

    get_include_dependents_loop(rest, lib_dir, emqx_apps_set, new_acc)
  end

  defp get_include_lib_deps_from_beams(ebin_path, emqx_apps_set) do
    # Find all BEAM files in ebin directory
    pattern = Path.join(ebin_path, "*.beam")
    beam_files = Path.wildcard(pattern)

    Enum.reduce(beam_files, MapSet.new(), fn beam_file, acc ->
      case get_include_lib_from_beam(beam_file, emqx_apps_set) do
        {:ok, deps} -> MapSet.union(acc, deps)
        :error -> acc
      end
    end)
  end

  defp get_include_lib_from_beam(beam_file, emqx_apps_set) do
    case :beam_lib.chunks(String.to_charlist(beam_file), [:abstract_code]) do
      {:ok, {_module, [{:abstract_code, {:raw_abstract_v1, forms}}]}} ->
        deps = extract_include_lib_from_forms(forms, emqx_apps_set)
        {:ok, deps}

      {:ok, {_module, [{:abstract_code, :no_abstract_code}]}} ->
        # BEAM file compiled without debug_info, skip
        :error

      {:error, :beam_lib, _} ->
        :error
    end
  end

  defp extract_include_lib_from_forms(forms, emqx_apps_set) do
    all_deps =
      Enum.reduce(forms, MapSet.new(), fn form, acc ->
        case form do
          {:attribute, _line, :include_lib, path} ->
            case extract_app_from_include_lib_path(path) do
              {:ok, app} -> MapSet.put(acc, app)
              :error -> acc
            end

          _ ->
            acc
        end
      end)

    MapSet.intersection(all_deps, emqx_apps_set)
  end

  defp extract_app_from_include_lib_path(path) do
    # Path in AST is a string (list of integers) like "emqx/include/file.hrl"
    cond do
      is_list(path) and length(path) > 0 and is_integer(hd(path)) ->
        # Extract app name from path (first component)
        [app_name | _] = String.split(List.to_string(path), "/", parts: 2)
        {:ok, String.to_atom(app_name)}

      true ->
        :error
    end
  end

  # Helper functions

  defp add_dependent(dependents, app, user_app) do
    Map.update(dependents, app, MapSet.new([user_app]), fn old_set ->
      MapSet.put(old_set, user_app)
    end)
  end

  defp merge_dependents(used_by_maps) do
    Enum.reduce(used_by_maps, %{}, fn map, acc ->
      Enum.reduce(map, acc, fn {app, caller_set}, acc_map ->
        Map.update(acc_map, app, caller_set, fn existing_set ->
          MapSet.union(existing_set, caller_set)
        end)
      end)
    end)
  end

  # BFS traversal: start with TargetApp, find all apps that use it (directly or transitively)
  # UsedByMap: App => Set of apps that directly use it
  # We want: all apps that transitively use TargetApp
  defp compute_transitive_users(target_app, used_by_map, _all_apps) do
    compute_transitive_users_bfs([target_app], used_by_map, MapSet.new(), MapSet.new())
  end

  defp compute_transitive_users_bfs([], _used_by_map, _visited, acc), do: acc

  defp compute_transitive_users_bfs([app | queue], used_by_map, visited, acc) do
    if MapSet.member?(visited, app) do
      # Already visited, skip
      compute_transitive_users_bfs(queue, used_by_map, visited, acc)
    else
      # Mark as visited
      new_visited = MapSet.put(visited, app)

      # Get direct users of this app
      direct_users = Map.get(used_by_map, app, MapSet.new())

      # Add direct users to accumulator
      new_acc = MapSet.union(acc, direct_users)

      # Add direct users to queue for further traversal (find their users)
      new_queue = queue ++ MapSet.to_list(direct_users)
      compute_transitive_users_bfs(new_queue, used_by_map, new_visited, new_acc)
    end
  end
end
