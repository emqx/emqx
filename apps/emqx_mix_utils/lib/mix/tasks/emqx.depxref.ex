defmodule Mix.Tasks.Emqx.Depxref do
  use Mix.Task

  @shortdoc "Run depxref analysis"
  @requirements ["compile", "loadpaths"]

  @xref :depxref
  @xref_ignore_apps [:emqx_mix_utils]
  @xref_common_libs [:erts, :stdlib, :kernel, :elixir]

  @impl true
  def run(_args) do
    :ok = ensure_xref()

    apps = Mix.Dep.Umbrella.cached()
    apps = apps |> Stream.reject(fn app -> app.app in @xref_ignore_apps end)
    deps = Mix.Dep.cached()
    deps = deps |> Stream.reject(fn dep -> dep.app in @xref_ignore_apps end)

    rel_otp = :code.lib_dir()
    app_names = apps |> Stream.map(fn app -> app.app end)
    dep_names = deps |> Stream.map(fn dep -> dep.app end)

    dir_apps =
      apps
      |> Stream.map(fn app -> to_charlist(app.opts[:build]) end)

    dir_deps =
      deps
      |> Stream.reject(fn app -> app.app in app_names end)
      |> Stream.map(fn dep -> to_charlist(dep.opts[:build]) end)

    # Add Erlang/OTP applications as release:
    {:ok, _} = :xref.add_release(@xref, rel_otp, name: :otp)

    # Add applications and dependencies:
    Enum.each(dir_deps, fn lib -> {:ok, _} = :xref.add_application(@xref, lib) end)
    Enum.each(dir_apps, fn lib -> {:ok, _} = :xref.add_application(@xref, lib) end)

    # Setup shortcut variables:
    # * `EMQX` is the applications in the umbrella project
    # * `DEPS` is the dependencies for those applications
    {:ok, _} = query("EMQX := (App) #{as_set(app_names)}")
    {:ok, _} = query("DEP := (App) #{as_set(dep_names)}")
  end

  def unneeded_dependencies(app) do
    with %Mix.Dep{opts: opts} <- find_app_info(app) do
      {:ok, dependencies} = query("range (strict AE | #{app})")
      declared = opts[:app_properties][:applications]
      unneeded = declared -- dependencies
      unneeded -- @xref_common_libs
    end
  end

  def undeclared_dependencies(app) do
    with %Mix.Dep{opts: opts} <- find_app_info(app) do
      {:ok, dependencies} = query("range (strict AE | #{app})")
      declared = opts[:app_properties][:applications]
      dependencies = dependencies -- @xref_common_libs
      undeclared = dependencies -- declared
      undeclared
    end
  end

  def circular_dependencies(app) do
    case query("range (strict AE | #{app} || EMQX)") do
      {:ok, dependencies = [_ | _]} ->
        {:ok, backdeps} = query("domain (strict AE | #{as_set(dependencies)} || #{app})")

        Enum.map(backdeps, fn dep ->
          {:ok, callsites} = query("ME * (App) [#{app} -> #{dep}]")
          {:ok, backsites} = query("ME * (App) [#{dep} -> #{app}]")
          {dep, Enum.map(callsites ++ backsites, &as_edge/1)}
        end)

      {:ok, []} ->
        []
    end
  end

  def query(query) do
    :xref.q(@xref, to_charlist(query))
  end

  def apps() do
    Mix.Dep.Umbrella.cached()
    |> Stream.reject(fn app -> app.app in @xref_ignore_apps end)
    |> Enum.map(fn app -> app.app end)
  end

  defp find_app_info(app) do
    apps = Mix.Dep.Umbrella.cached()
    Enum.find(apps, fn dep -> dep.app == app end)
  end

  defp as_set(atoms) do
    "[#{atoms |> Enum.map(&to_string/1) |> Enum.join(", ")}]"
  end

  defp as_edge({from, to}) do
    "#{as_vertex(from)} -> #{as_vertex(to)}"
  end

  defp as_vertex({mod, func, arity}) do
    "#{mod}:#{func}/#{arity}"
  end

  defp as_vertex(mod) do
    "#{mod}"
  end

  def ensure_xref() do
    case :xref.start(@xref) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        :xref.stop(@xref)
        ensure_xref()
    end
  end

  def stop_xref() do
    :xref.stop(@xref)
  end
end
