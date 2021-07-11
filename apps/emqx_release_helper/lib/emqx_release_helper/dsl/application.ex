defmodule EmqxReleaseHelper.DSL.Application do
  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
      Module.register_attribute(__MODULE__, :applications, accumulate: true)
      @before_compile unquote(__MODULE__)
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

  defmacro overlay(:plugin) do
    block =
      Macro.escape(
        quote do
          &plugin_overlay/1
        end
      )

    quote do
      @overlays [unquote(block) | @overlays]
    end
  end

  defmacro overlay(do: block) do
    block =
      Macro.escape(
        quote do
          fn unquote(Macro.var(:config, nil)) ->
            unquote(block)
          end
        end
      )

    quote do
      @overlays [unquote(block) | @overlays]
    end
  end

  defmacro overlay(literal_config, do: block) do
    block =
      Macro.escape(
        quote do
          fn unquote(literal_config) = unquote(Macro.var(:config, nil)) ->
            unquote(block)
          end
        end
      )

    quote do
      @overlays [unquote(block) | @overlays]
    end
  end

  defmacro copy(from_path, to_path) do
    from_path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:app_source_path)
        |> Path.join(unquote(from_path))
      end

    to_path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:release_path)
        |> Path.join(unquote(to_path))
      end

    quote do
      EmqxReleaseHelper.DSL.Overlay.run_copy(unquote(from_path), unquote(to_path))
    end
  end

  defmacro template(from_path, to_path) do
    from_path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:app_source_path)
        |> Path.join(unquote(from_path))
      end

    to_path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:release_path)
        |> Path.join(unquote(to_path))
      end

    quote do
      EmqxReleaseHelper.DSL.Overlay.run_template(
        unquote(from_path),
        unquote(to_path),
        unquote(Macro.var(:config, nil))
      )
    end
  end

  def plugin_overlay(%{app_source_path: app_source_path, release_path: release_path} = config) do
    "#{app_source_path}/etc"
    |> File.ls()
    |> case do
      {:ok, files} -> files
      {:error, _} -> []
    end
    |> Enum.filter(fn file -> String.ends_with?(file, ".conf") end)
    |> Enum.each(fn file ->
      EmqxReleaseHelper.DSL.Overlay.run_template(
        "#{app_source_path}/etc/#{file}",
        "#{release_path}/etc/plugins/#{file}",
        config
      )
    end)
  end

  defmacro __before_compile__(%Macro.Env{module: module}) do
    block =
      module
      |> Module.get_attribute(:applications)
      |> Enum.reverse()
      |> Enum.map(fn app -> {:%{}, [], Map.to_list(app)} end)

    quote do
      def __all__, do: unquote(block)
    end
  end
end
