require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Pipeline::Node do
  before :each do
    @pipeline = stub(:pipeline).as_null_object
    @a,@b,@c,@d = 4.times.map { described_class.new(@pipeline) }
  end
  
  it "should create a directed edge from node a to node b, using >" do
    @a > @b
    @a.downstream.should == Set.new([@b])
  end

  it "should create a back edge from node b to node a using >" do
    @a > @b
    @b.upstream.should == Set.new([@a])
  end

  it "should add the node to the pipeline" do
    @pipeline.should_receive(:add).with(kind_of(described_class))
    described_class.new(@pipeline)
  end

  it "should be a source if it has no inbound nodes" do
    @a > @b
    @a.should be_origin
    @b.should_not be_origin
  end

  it "should be a target if it has no outbound nodes" do
    @a > @b
    @a.should_not be_target
    @b.should be_target
  end

  it "should return itself as out and in" do
    @a.in.should == @a
    @a.out.should == @a
  end

  it "should return node b as the out from a connection" do
    (@a > @b).out.should == @b
  end

  it "should return node a as the in from a connection" do
    (@a > @b).in.should == @a
  end

  it "should link multiple nodes together" do
    @a > @b > @c
    @a.downstream.should == Set.new([@b])
    @b.downstream.should == Set.new([@c])
  end

  it "should be flowing to a target via out links" do
    @a > @b > @c
    @a.should be_flowing_to(@b)
    @a.should be_flowing_to(@c)
    @c.should_not be_flowing_to(@a)
    @a.should_not be_flowing_to(@d)
  end

  it "should be flowing from a origin via in links" do
    @a > @b > @c

    @b.should be_flowing_from(@a)
    @c.should be_flowing_from(@a)
    @a.should_not be_flowing_from(@c)
    @a.should_not be_flowing_from(@d)
  end

  it "should allow pipelines to be built up incrementally" do
    @a > @b > @d
    @b > @c
    
    @a.should be_flowing_to(@c)
    @a.should be_flowing_to(@d)
  end

  it "shouldn't be connected to itself" do
    @a.should_not be_flowing_to(@a)
    @a.should_not be_flowing_from(@a)
  end

  it "should have targets" do
    @a > @b > @d
    @b > @c

    @a.targets.should == Set.new([@d, @c])
    @d.targets.should == Set.new
  end

  it "should have origins" do
    @a > @b > @d
    @c > @d

    @d.origins.should == Set.new([@a, @c])
    @c.origins.should == Set.new
  end

  it "should be cyclic if it includes itself in targets" do
    @a > @b > @c
    @b > @d > @a
    @a.targets.should == Set.new([@c])
    @a.should be_in_cycle
    @c.should_not be_in_cycle
  end

  it "should be cyclic if it includes itself in origins" do
    @a > @b > @c
    @b > @d > @a

    @a.should be_in_cycle
  end

  it "should not be a table, by default" do
    @a.should_not be_table
  end

  it "can be assigned, and in and out wire up correctly" do
    x = @b > @c
    @a > x

    @a.downstream.should == Set.new([@b])
    @b.upstream.should == Set.new([@a])
  end

  it "can be assigned multiple times, and in and out wire up correctly" do
    x = @a > @b
    y = @c > @d
    x > y

    @b.downstream.should == Set.new([@c])
    @c.upstream.should == Set.new([@b])
  end

  it "has upstream and downstream sets, when assigned" do
    x = @b > @c
    @a > x
    x > @d

    x.upstream.should == Set.new([@a])
    x.downstream.should == Set.new([@d])
  end

  it "should be assignable multiple times" do
    x = @b > @c
    y = x > @d
    @a > y

    @a.downstream.should == Set.new([@b])
    @b.upstream.should == Set.new([@a])
  end

  it "should create a directed edge from node a to node b, using <" do
    @b < @a
    @a.downstream.should == Set.new([@b])
  end

  it "should have left to right precedence when using both < and >, check 1" do
    @b < @a > @c
    @a.downstream.should == Set.new([@b])
    @b.downstream.should == Set.new([@c])
    @c.downstream.should == Set.new()
  end

  it "should have left to right precedence when using both < and >, check 2" do
    @b > @c > @d < @a
    @a.downstream.should == Set.new([@b])
    @b.downstream.should == Set.new([@c])
    @c.downstream.should == Set.new([@d])
    @d.downstream.should == Set.new()
  end

  it "should have precedence overridden by brackets" do
    @a > @b > (@d < @c)
    @a.downstream.should == Set.new([@b])
    @b.downstream.should == Set.new([@c])
    @c.downstream.should == Set.new([@d])
    @d.downstream.should == Set.new()
  end
end
