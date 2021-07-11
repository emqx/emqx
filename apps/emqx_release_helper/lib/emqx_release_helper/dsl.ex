defmodule EmqxReleaseHelper.DSL do
  defmacro __using__(_) do
    quote do
      import unquote(__MODULE__)
      Module.register_attribute(__MODULE__, :applications, accumulate: true)
      import EmqxReleaseHelper.DSL.Application
      # @before_compile unquote(__MODULE__)
    end
  end

  defmacro overlay(_) do
  end

  defmacro overlay(_, _) do
  end

  # defmacro __before_compile__(%Macro.Env{}=env) do
  #   EmqxReleaseHelper.DSL.Application.__before_compile__(env)
  # end
end
