require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Pipeline::DepthFirstIterator do
  before :each do
    @pipeline = stub(:pipeline).as_null_object
    @a,@b,@c,@d,@e = 5.times.map { Chicago::ETL::Pipeline::Node.new(@pipeline) }
  end

  it "should iterate in the right order" do
    @a > @b > @c
    @a > @d > @e

    Set.new([[@a, @b, @c, @d, @e], [@a, @d, @e, @b, @c]]).
      should include(described_class.downstream(@a).to_a)
  end

  it "only yields each node once in the case of cycles" do
    @a > @b > @c
    @a > @d > @e
    @b > @a
    
    Set.new([[@a, @b, @c, @d, @e], [@a, @d, @e, @b, @c]]).
      should include(described_class.downstream(@a).to_a)
  end
end

describe Chicago::ETL::Pipeline::BreadthFirstIterator do
  before :each do
    @pipeline = stub(:pipeline).as_null_object
    @a,@b,@c,@d,@e = 5.times.map { Chicago::ETL::Pipeline::Node.new(@pipeline) }
  end

  it "should iterate in the right order" do
    @a > @b > @c
    @a > @d > @e

    Set.new([[@a, @b, @d, @c, @e], [@a, @d, @b, @e, @c]]).
      should include(described_class.downstream(@a).to_a)
  end

  it "only yields each node once in the case of cycles" do
    @a > @b > @c
    @a > @d > @e
    @b > @a
    
    Set.new([[@a, @b, @d, @c, @e], [@a, @d, @b, @e, @c]]).
      should include(described_class.downstream(@a).to_a)
  end
end
