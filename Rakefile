require 'rubygems'
require 'rake'

namespace :jar do
  task :build do
    `mkdir -p classes`
    `javac -d classes -cp vendors/jruby.jar:vendors/gson.jar:vendors/msgpack.jar:vendors/hadoop.jar:. lib/jmapreduce/JsonProperty.java lib/jmapreduce/ValuePacker.java`
    `javac -d classes -cp vendors/jruby.jar:vendors/gson.jar:vendors/msgpack.jar:vendors/hadoop.jar:classes/:. lib/jmapreduce/MapperWrapper.java lib/jmapreduce/ReducerWrapper.java`
    `jrubyc -t classes -c vendors/hadoop.jar:classes:. --javac lib/jmapreduce.rb `
    `jrubyc -t classes -c vendors/hadoop.jar:classes:. --javac lib/jmapreduce/jmapper.rb lib/jmapreduce/jreducer.rb lib/jmapreduce/job.rb`
    
    `rm classes/org/fingertap/jmapreduce/*.java`
    `mkdir -p release`
    `jar cvf release/jmapreduce.jar -C classes/ .`
  end
  
  task :clean do
    `rm -rf classes`
    `rm -rf release`
  end
end
