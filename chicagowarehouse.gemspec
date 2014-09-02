# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-
# stub: chicagowarehouse 0.6.1 ruby lib

Gem::Specification.new do |s|
  s.name = "chicagowarehouse"
  s.version = "0.6.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ["lib"]
  s.authors = ["Roland Swingler"]
  s.date = "2014-09-02"
  s.description = "Simple Data Warehouse toolkit for ruby"
  s.email = "roland.swingler@gmail.com"
  s.extra_rdoc_files = [
    "LICENSE",
    "README"
  ]
  s.files = [
    ".document",
    ".rspec",
    "Gemfile",
    "LICENSE",
    "README",
    "Rakefile",
    "chicagowarehouse.gemspec",
    "lib/chicago.rb",
    "lib/chicago/core_ext/hash.rb",
    "lib/chicago/core_ext/sequel/dataset.rb",
    "lib/chicago/data/month.rb",
    "lib/chicago/database/concrete_schema_strategies.rb",
    "lib/chicago/database/constants.rb",
    "lib/chicago/database/dataset_builder.rb",
    "lib/chicago/database/filter.rb",
    "lib/chicago/database/index_generator.rb",
    "lib/chicago/database/migration_file_writer.rb",
    "lib/chicago/database/schema_generator.rb",
    "lib/chicago/database/value_parser.rb",
    "lib/chicago/errors.rb",
    "lib/chicago/query.rb",
    "lib/chicago/rake_tasks.rb",
    "lib/chicago/schema/builders/column_builder.rb",
    "lib/chicago/schema/builders/dimension_builder.rb",
    "lib/chicago/schema/builders/fact_builder.rb",
    "lib/chicago/schema/builders/shrunken_dimension_builder.rb",
    "lib/chicago/schema/builders/table_builder.rb",
    "lib/chicago/schema/column.rb",
    "lib/chicago/schema/column_parser.rb",
    "lib/chicago/schema/dimension.rb",
    "lib/chicago/schema/dimension_reference.rb",
    "lib/chicago/schema/fact.rb",
    "lib/chicago/schema/measure.rb",
    "lib/chicago/schema/named_element.rb",
    "lib/chicago/schema/named_element_collection.rb",
    "lib/chicago/schema/query_column.rb",
    "lib/chicago/schema/table.rb",
    "lib/chicago/star_schema.rb",
    "spec/data/month_spec.rb",
    "spec/database/concrete_schema_strategies.rb",
    "spec/database/migration_file_writer_spec.rb",
    "spec/database/schema_generator_spec.rb",
    "spec/db_connections.yml.dist",
    "spec/query_spec.rb",
    "spec/schema/column_spec.rb",
    "spec/schema/dimension_builder_spec.rb",
    "spec/schema/dimension_reference_spec.rb",
    "spec/schema/dimension_spec.rb",
    "spec/schema/fact_spec.rb",
    "spec/schema/measure_spec.rb",
    "spec/schema/named_element_collection_spec.rb",
    "spec/schema/pivoted_column_spec.rb",
    "spec/schema/query_column_spec.rb",
    "spec/spec_helper.rb",
    "spec/star_schema_spec.rb",
    "spec/support/matchers/be_one_of.rb",
    "spec/support/matchers/column_matchers.rb",
    "spec/support/shared_examples/column.rb",
    "spec/support/shared_examples/schema_table.rb",
    "spec/support/shared_examples/schema_visitor.rb",
    "tasks/stats.rake"
  ]
  s.homepage = "http://github.com/notonthehighstreet/chicago"
  s.licenses = ["MIT"]
  s.rubygems_version = "2.2.1"
  s.summary = "Ruby Data Warehousing"

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<sequel>, [">= 4.7"])
      s.add_runtime_dependency(%q<sequel_migration_builder>, [">= 0.4.1"])
      s.add_runtime_dependency(%q<chronic>, [">= 0"])
      s.add_development_dependency(%q<mysql2>, [">= 0"])
      s.add_development_dependency(%q<yard>, [">= 0"])
      s.add_development_dependency(%q<rspec>, ["~> 2.0"])
      s.add_development_dependency(%q<bundler>, [">= 0"])
      s.add_development_dependency(%q<jeweler>, [">= 0"])
      s.add_development_dependency(%q<simplecov>, [">= 0"])
      s.add_development_dependency(%q<flog>, [">= 0"])
      s.add_development_dependency(%q<timecop>, [">= 0"])
    else
      s.add_dependency(%q<sequel>, [">= 4.7"])
      s.add_dependency(%q<sequel_migration_builder>, [">= 0.4.1"])
      s.add_dependency(%q<chronic>, [">= 0"])
      s.add_dependency(%q<mysql2>, [">= 0"])
      s.add_dependency(%q<yard>, [">= 0"])
      s.add_dependency(%q<rspec>, ["~> 2.0"])
      s.add_dependency(%q<bundler>, [">= 0"])
      s.add_dependency(%q<jeweler>, [">= 0"])
      s.add_dependency(%q<simplecov>, [">= 0"])
      s.add_dependency(%q<flog>, [">= 0"])
      s.add_dependency(%q<timecop>, [">= 0"])
    end
  else
    s.add_dependency(%q<sequel>, [">= 4.7"])
    s.add_dependency(%q<sequel_migration_builder>, [">= 0.4.1"])
    s.add_dependency(%q<chronic>, [">= 0"])
    s.add_dependency(%q<mysql2>, [">= 0"])
    s.add_dependency(%q<yard>, [">= 0"])
    s.add_dependency(%q<rspec>, ["~> 2.0"])
    s.add_dependency(%q<bundler>, [">= 0"])
    s.add_dependency(%q<jeweler>, [">= 0"])
    s.add_dependency(%q<simplecov>, [">= 0"])
    s.add_dependency(%q<flog>, [">= 0"])
    s.add_dependency(%q<timecop>, [">= 0"])
  end
end

