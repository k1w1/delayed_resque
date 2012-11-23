require 'spec_helper'

describe DelayedResque do
  before do
    ResqueSpec.reset!
  end

  context "class methods can be delayed" do
    class DummyObject
      include DelayedResque::MessageSending
    
      def self.first_method(param)
      end
    end

    it "can delay method" do
      DummyObject.delay.first_method(123)
      DelayedResque::PerformableMethod.should have_queued("CLASS:DummyObject", :first_method, 123).in(:default)
    end
    
    it "delayed method is called" do
      DummyObject.stub(:second_method).with(123, 456)
      with_resque do
        DummyObject.delay.second_method(123, 456)
      end
    end
    
    it "can't delay missing method" do
      expect {
        DummyObject.delay.non_existent_method
      }.to raise_error(NoMethodError)
    end
    
  end
  
  context "active record methods can be delayed" do
  
    ActiveRecord::Base.connection.execute("DROP TABLE IF EXISTS 'dummy_models'")
    ActiveRecord::Base.connection.create_table(:dummy_models) do |t|
      t.integer :value
    end

    class DummyModel < ActiveRecord::Base
      def update_value(new_value1, new_value2)
        self.value = new_value1 + new_value2
        save!
      end
      
      def copy_value(record)
        self.value = record.value
        save!
      end
    end
    
    it "can delay method" do
      record = DummyModel.create(:value => 1)
      with_resque do
        record.delay.update_value(3, 7)
      end
      record.reload.value.should eq(10)
    end
    
    it "AR model can be parameter to delay" do
      record1 = DummyModel.create(:value => 1)
      record2 = DummyModel.create(:value => 3)
      with_resque do
        record1.delay.copy_value(record2)
      end
      record1.reload.value.should eq(3)
    end
    
  end
  
end
