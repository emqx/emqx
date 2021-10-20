defmodule EmqxReleaseHelper.DSL.Application do
  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
      import EmqxReleaseHelper.DSL.Overlay, only: [
        overlay: 1,
        overlay: 2,
        copy: 2,
        template: 2
      ]
      Module.register_attribute(__MODULE__, :applications, accumulate: true)
      @before_compile unquote(__MODULE__)
      @overlay_source_path :app_source_path
    end
  end

  defmacro application(app, condition, do: block) do
    func =
      Macro.escape(
        quote do
          fn config -> match?(unquote(condition), config) end
        end
      )

    quote do
      @current_application %{name: unquote(app), enable?: unquote(func)}
      @overlays []
      unquote(block)
      overlays = Enum.reverse(@overlays)
      @applications Map.put(@current_application, :overlays, overlays)
      @current_application nil
      @overlays []
    end
  end

  defmacro application(app, do: block) do
    quote do
      @current_application %{name: unquote(app)}
      @overlays []
      unquote(block)
      overlays = Enum.reverse(@overlays)
      @applications Map.put(@current_application, :overlays, overlays)
      @current_application nil
      @overlays []
    end
  end

  defmacro start_type(type) do
    quote do
      @current_application Map.put(@current_application, :start_type, unquote(type))
    end
  end

  defmacro __before_compile__(%Macro.Env{module: module}) do
    block =
      module
      |> Module.get_attribute(:applications)
      |> Enum.reverse()
      |> Enum.map(fn app -> {:%{}, [], Map.to_list(app)} end)

    quote do
      def __all__, do: unquote(block)

      def run(release, config) do
        %{project_path: project_path, apps_paths: apps_paths} = config

        __all__()
        |> Enum.filter(fn %{name: name} -> Map.has_key?(apps_paths, name) end)
        |> Enum.filter(fn
          %{enable?: fun} -> fun.(config)
          _ -> true
        end)
        |> Enum.each(fn %{name: name, overlays: overlays} ->
          app_path = Map.get(apps_paths, name)
          config = Map.put(config, :app_source_path, Path.join(project_path, app_path))
          Enum.each(overlays, fn overlay -> overlay.(config) end)
        end)

        release
      end
    end
  end
end
