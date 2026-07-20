defmodule W3Test do
  use ExUnit.Case
  doctest W3

  test "greets the world" do
    assert W3.hello() == :world
  end
end
