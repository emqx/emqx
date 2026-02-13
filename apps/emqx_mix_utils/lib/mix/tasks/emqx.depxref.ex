defmodule Mix.Tasks.Emqx.Depxref do
  use Mix.Task

  @shortdoc "Run depxref analysis"
  @requirements ["compile", "loadpaths"]

  @xref :depxref
  @xref_ignore_apps [:emqx_mix_utils]
  @xref_common_libs [:erts, :stdlib, :kernel, :elixir]
  @xref_ignore_missing [:snabbkaffe]
  @ignorelist_limit 20

  @impl true
  def run(args) do
    ignorelist = depxref_ignores!()
    {:ok, app_names} = initialize(ignorelist)

    {opts, rest} =
      OptionParser.parse!(
        args,
        strict: [
          report_md: :string,
          summarize: :boolean
        ]
      )

    {format, iodev} =
      case opts[:report_md] do
        nil ->
          {:text, :stdio}

        "-" ->
          {:text, :stdio}

        filename ->
          {:md, File.open!(filename, [:write])}
      end

    apps_report =
      case rest do
        [] ->
          app_names

        names ->
          apps_in = names |> Enum.map(&String.to_atom/1)
          apps_in = apps_in -- @xref_ignore_apps
          unknown = apps_in -- app_names

          if unknown != [] do
            Mix.raise("Unknown applications: #{inspect(unknown)}")
          end

          apps_in
      end

    apps_issues =
      Enum.flat_map(apps_report, fn app ->
        undeclared = undeclared_dependencies(app)
        unneeded = unneeded_dependencies(app)
        cyclic = cyclic_dependencies(app)

        case length(undeclared) + length(unneeded) + length(cyclic) do
          n when n > 0 ->
            [
              {app,
               %{
                 undeclared: undeclared,
                 unneeded: unneeded,
                 cyclic: cyclic,
                 total: n
               }}
            ]

          0 ->
            []
        end
      end)

    issues? = length(apps_issues) > 0

    Mix.Tasks.Emqx.Depxref.Report.produce(apps_issues, iodev, format)

    if opts[:summarize] do
      n_ignores = length(ignorelist)

      if n_ignores > 0 do
        IO.puts("[Depxref] Ignored #{n_ignores} module dependencies via mix.exs")
      end

      if issues? do
        n_apps = length(apps_issues)
        n_issues = Enum.sum_by(apps_issues, fn {_, issues} -> issues[:total] end)
        IO.puts("[Depxref] Found #{n_issues} issues across #{n_apps} applications")
      else
        IO.puts("[Depxref] No issues were found")
      end
    end

    if issues? do
      System.halt(1)
    else
      :ok
    end
  end

  def unneeded_dependencies(app) do
    with %Mix.Dep{opts: opts} <- find_app_info(app) do
      {:ok, dependencies} = query("range (strict AE | #{app})")
      declared = opts[:app_properties][:applications] || []
      unneeded = declared -- dependencies
      unneeded = unneeded -- @xref_common_libs
      unneeded -- @xref_ignore_missing
    end
  end

  def undeclared_dependencies(app) do
    with %Mix.Dep{opts: opts} <- find_app_info(app) do
      {:ok, dependencies} = query("range (strict AE | #{app})")
      declared = opts[:app_properties][:applications] || []
      dependencies = dependencies -- @xref_common_libs
      undeclared = dependencies -- declared

      Enum.map(undeclared, fn dep ->
        {:ok, callsites} = query("(ME - IgnoreS) * (App) [#{app} -> #{dep}]")
        if callsites != [], do: {dep, callsites}
      end)
      |> Enum.reject(&is_nil/1)
    end
  end

  def cyclic_dependencies(app) do
    case query("range (strict AE | #{app} || EMQX)") do
      {:ok, dependencies = [_ | _]} ->
        {:ok, backdeps} = query("domain (strict AE | #{as_set(dependencies)} || #{app})")

        Enum.map(backdeps, fn dep ->
          {:ok, callsites} = query("(ME - IgnoreS) * (App) [#{app} -> #{dep}]")
          {:ok, backsites} = query("(ME - IgnoreS) * (App) [#{dep} -> #{app}]")
          if callsites != [] or backsites != [], do: {dep, callsites, backsites}
        end)
        |> Enum.reject(&is_nil/1)

      {:ok, []} ->
        []
    end
  end

  def query(query) do
    :xref.q(@xref, to_charlist(query))
  end

  defp depxref_ignores!() do
    Mix.Dep.Umbrella.cached()
    |> Stream.reject(fn %Mix.Dep{app: app} -> app in @xref_ignore_apps end)
    |> Stream.flat_map(fn %Mix.Dep{app: app, opts: opts} ->
      Mix.Project.in_project(app, opts[:path], [], fn app_mix_mod ->
        ignorelist =
          if function_exported?(app_mix_mod, :depxref, 0) do
            app_mix_mod.depxref() |> Keyword.get(:ignore, [])
          else
            []
          end

        limit = @ignorelist_limit

        if length(ignorelist) > limit do
          Mix.raise("Too many ignore statements: #{app}: #{length(ignorelist)} / #{limit}")
        end

        Enum.map(ignorelist, fn ignore ->
          case ignore_to_modedge(ignore) do
            {:ok, modedge} ->
              modedge

            {:error, reason} ->
              Mix.raise("Invalid ignore statement (#{reason}): #{app}: #{inspect(ignore)}")
          end
        end)
      end)
    end)
    |> Enum.to_list()
  end

  defp ignore_to_modedge([app_mod, :->, dep_mod | props]) do
    reason = Keyword.get(props, :reason)

    unless is_binary(reason) and String.trim(reason) != "" do
      {:error, "missing reason"}
    else
      {:ok, {app_mod, :->, dep_mod}}
    end
  end

  defp ignore_to_modedge(_) do
    {:error, "bad format"}
  end

  def apps() do
    Mix.Dep.Umbrella.cached()
    |> Stream.reject(fn app -> app.app in @xref_ignore_apps end)
    |> Enum.map(fn app -> app.app end)
  end

  def initialize(ignorelist) do
    :ok = ensure_xref()

    case query("EMQX") do
      {:ok, [_ | _]} = ok ->
        ok

      _undefined ->
        ok = rebuild_xref()
        build_ignore_set(ignorelist)
        ok
    end
  end

  defp rebuild_xref() do
    apps = Mix.Dep.Umbrella.cached()
    apps = apps |> Stream.reject(fn app -> app.app in @xref_ignore_apps end)
    deps = Mix.Dep.cached()
    deps = deps |> Stream.reject(fn dep -> dep.app in @xref_ignore_apps end)

    app_names = apps |> Enum.map(fn app -> app.app end)
    dep_names = deps |> Enum.map(fn dep -> dep.app end)

    rel_otp = :code.lib_dir()

    dir_apps =
      apps
      |> Stream.map(fn app -> to_charlist(app.opts[:build]) end)

    dir_deps =
      deps
      |> Stream.reject(fn app -> app.app in app_names end)
      |> Stream.map(fn dep -> to_charlist(dep.opts[:build]) end)

    # Add Erlang/OTP applications as release:
    {:ok, _} = :xref.add_release(@xref, rel_otp, name: :otp, warnings: false)

    # Add applications and dependencies:
    Enum.each(dir_deps, fn lib ->
      {:ok, _} = :xref.add_application(@xref, lib, warnings: false)
    end)

    Enum.each(dir_apps, fn lib ->
      {:ok, _} = :xref.add_application(@xref, lib)
    end)

    # Setup shortcut variables:
    # * `EMQX` is the applications in the umbrella project
    # * `DEPS` is the dependencies for those applications
    {:ok, _} = query("EMQX := (App) #{as_set(app_names)}")
    {:ok, _} = query("DEP := (App) #{as_set(dep_names)}")
    {:ok, app_names}
  end

  defp build_ignore_set(edges) do
    {:ok, modules} = query("(Mod) A")
    edges = Enum.filter(edges, fn {m1, :->, m2} -> m1 in modules and m2 in modules end)

    if edges != [] do
      {:ok, _} = query("IgnoreS := #{as_set(edges)} : Mod")
    else
      {:ok, _} = query("IgnoreS := ME - ME")
    end
  end

  defp find_app_info(app) do
    apps = Mix.Dep.Umbrella.cached()
    Enum.find(apps, fn dep -> dep.app == app end)
  end

  defp as_set(xs) do
    "[#{xs |> Enum.map(&as_elem/1) |> Enum.join(", ")}]"
  end

  defp as_elem({a, :->, b}), do: "#{as_elem(a)} -> #{as_elem(b)}"
  defp as_elem(a) when is_atom(a), do: to_string(a)

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

  defmodule Report do
    def produce(apps_issues, iodev, format) when is_list(apps_issues) do
      Enum.each(apps_issues, fn app_issues = {app, _} ->
        format == :md && IO.puts(iodev, "<details><summary>#{app}</summary>")
        format == :text && IO.puts(iodev, String.duplicate("-----", 16))
        IO.puts(iodev, "")
        produce_app(app_issues, iodev, format)
        format == :md && IO.puts(iodev, "</details>")
        format == :md && IO.puts(iodev, "")
      end)

      format == :text && IO.puts(iodev, String.duplicate("-----", 16))
    end

    def produce_app({app, issues}, iodev, _format) do
      %{
        undeclared: undeclared,
        unneeded: unneeded,
        cyclic: cyclic
      } = issues

      if undeclared != [] do
        IO.puts(iodev, "### Application `#{app}` uses undeclared direct dependencies")

        Enum.each(undeclared, fn {dep, callsites} ->
          if List.keyfind(cyclic, dep, 0) != nil do
            IO.puts(iodev, " * `#{dep}` (cyclic)")
          else
            IO.puts(iodev, " * `#{dep}`")

            Enum.each(callsites, fn {from, to} ->
              IO.puts(
                iodev,
                "    - #{as_vtype(from)} `#{as_vertex(from)}` calls `#{as_vertex(to)}`"
              )
            end)
          end
        end)

        IO.puts(iodev, "")
      end

      if unneeded != [] do
        IO.puts(iodev, "### Application `#{app}` declares seemingly unneeded dependencies")
        Enum.each(unneeded, fn dep -> IO.puts(iodev, " * `#{dep}`") end)
        IO.puts(iodev, "")
      end

      if cyclic != [] do
        IO.puts(iodev, "### Application `#{app}` has cyclic dependencies")

        Enum.each(cyclic, fn {dep, callsites, backsites} ->
          IO.puts(iodev, " * `#{dep}`")

          if List.keyfind(undeclared, dep, 0) != nil do
            Enum.each(callsites, fn {from, to} ->
              IO.puts(
                iodev,
                "    - #{as_vtype(from)} `#{as_vertex(from)}` calls `#{as_vertex(to)}`"
              )
            end)
          else
            Enum.each(backsites, fn {from, to} ->
              IO.puts(
                iodev,
                "    - #{as_vtype(to)} `#{as_vertex(to)}` is called by `#{as_vertex(from)}`"
              )
            end)
          end
        end)

        IO.puts(iodev, "")
      end
    end

    # defp as_edge({from, to}) do
    #   "#{as_vertex(from)} -> #{as_vertex(to)}"
    # end

    defp as_vtype({_mod, _func, _arity}) do
      "function"
    end

    defp as_vtype(_mod) do
      "module"
    end

    defp as_vertex({mod, func, arity}) do
      "#{mod}:#{func}/#{arity}"
    end

    defp as_vertex(mod) do
      "#{mod}"
    end
  end
end
