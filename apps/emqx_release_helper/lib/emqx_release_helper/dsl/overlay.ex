defmodule EmqxReleaseHelper.DSL.Overlay do
  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
      Module.register_attribute(__MODULE__, :overlays, accumulate: true)
      @before_compile unquote(__MODULE__)
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
      @overlays unquote(block)
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
      @overlays unquote(block)
    end
  end

  defmacro mkdir(path) do
    path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:release_path)
        |> Path.join(unquote(path))
      end

    quote do
      run_mkdir(unquote(path))
    end
  end

  defmacro copy(from_path, to_path) do
    from_path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:project_path)
        |> Path.join(unquote(from_path))
      end

    to_path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:release_path)
        |> Path.join(unquote(to_path))
      end

    quote do
      run_copy(unquote(from_path), unquote(to_path))
    end
  end

  defmacro template(from_path, to_path) do
    from_path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:project_path)
        |> Path.join(unquote(from_path))
      end

    to_path =
      quote do
        unquote(Macro.var(:config, nil))
        |> Map.get(:release_path)
        |> Path.join(unquote(to_path))
      end

    quote do
      run_template(
        unquote(from_path),
        unquote(to_path),
        unquote(Macro.var(:config, nil))
      )
    end
  end

  def run_mkdir(path) do
    File.mkdir_p!(path)
  end

  def run_copy(from_path, to_path) do
    to_path |> Path.dirname() |> File.mkdir_p!()
    File.cp_r!(from_path, to_path)
  end

  def run_template(from_path, to_path, config) do
    config = Enum.map(config, fn {key, value} -> {to_charlist(key), value} end)
    to_path |> Path.dirname() |> File.mkdir_p!()

    content =
      from_path
      |> File.read!()
      |> :bbmustache.render(config)

    File.write!(to_path, content)
  end

  defmacro __before_compile__(%Macro.Env{module: module}) do
    block =
      module
      |> Module.get_attribute(:overlays)
      |> Enum.reverse()

    quote do
      def __overlays__ do
        unquote(block)
      end
    end
  end
end
