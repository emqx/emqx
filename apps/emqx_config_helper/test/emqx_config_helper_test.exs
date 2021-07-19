defmodule EmqxConfigHelperTest do
  use ExUnit.Case
  doctest EmqxConfigHelper

  test "greets the world" do
    assert EmqxConfigHelper.hello() == :world
  end
end
