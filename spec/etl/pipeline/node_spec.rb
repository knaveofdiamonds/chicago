require 'spec_helper'
require 'chicago/etl'

describe Chicago::ETL::Pipeline::Node do
  before :each do
    @pipeline = stub(:pipeline).as_null_object
    @a,@b,@c,@d = 4.times.map { described_class.new(@pipeline) }
  end
  
  it "should create a directed edge from node a to node b, using >" do
    @a > @b
    @a.out.should == Set.new([@b])
  end

  it "should create a back edge from node b to node a using >" do
    @a > @b
    @b.in.should == Set.new([@a])
  end

  it "should add the node to the pipeline" do
    @pipeline.should_receive(:add).with(kind_of(described_class))
    described_class.new(@pipeline)
  end

  it "should be a source if it has no inbound nodes" do
    @a > @b
    @a.should be_source
    @b.should_not be_source
  end

  it "should be a target if it has no outbound nodes" do
    @a > @b
    @a.should_not be_target
    @b.should be_target
  end

  it "should return node b from pipeline" do
    (@a > @b).should == @b
  end

  it "should link multiple nodes together" do
    (@a > @b > @c).should == @c
    @a.out.should == Set.new([@b])
    @b.out.should == Set.new([@c])
  end

  it "should be flowing to a target via out links" do
    @a > @b > @c
    @a.should be_flowing_to(@b)
    @a.should be_flowing_to(@c)
    @c.should_not be_flowing_to(@a)
    @a.should_not be_flowing_to(@d)
  end

  it "should be flowing from a source via in links" do
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

  it "should have sources" do
    @a > @b > @d
    @c > @d

    @d.sources.should == Set.new([@a, @c])
    @c.sources.should == Set.new
  end

  it "should be cyclic if it includes itself in targets" do
    @a > @b > @c
    @b > @d > @a
    @a.targets.should == Set.new([@a, @c])
    @a.should_not be_marked
    @b.should_not be_marked
    @c.should_not be_marked
    @a.should be_in_cycle
    @c.should_not be_in_cycle
  end

  it "should be cyclic if it includes itself in sources" do
    @a > @b > @c
    @b > @d > @a

    @a.sources.should include(@a)
  end
end
