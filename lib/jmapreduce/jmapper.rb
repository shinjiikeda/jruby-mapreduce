require 'java'

java_package 'org.fingertap.jmapreduce'

java_import 'java.io.IOException'

java_import 'org.apache.hadoop.io.Text'
java_import 'org.apache.hadoop.mapreduce.Mapper'

java_import 'org.fingertap.jmapreduce.JMapReduce'


class JMapper < Mapper
  
  java_signature 'void setup(org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def setup(context)
    @jmapreduce_mapper_key = Text.new
    @jmapreduce_mapper_value = Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
    JMapReduce.set_properties(conf.get('jmapreduce.property'))
    
    require script
    @jmapreduce_mapper_job = JMapReduce.jobs[job_index]
    @jmapreduce_mapper_job.set_conf(conf)
    @jmapreduce_mapper_job.set_context(context)
    @jmapreduce_mapper_job.set_properties(conf.get('jmapreduce.property'))
    @jmapreduce_mapper_job.running_last_emit if conf.get('jmapreduce.last_job.mapper')
    
    @jmapreduce_mapper_job.get_setup.call if @jmapreduce_mapper_job.setup_exists
    
    @delimiter = conf.get('jmapreduce.delimiter', "\t")
  end
  
  java_signature 'void map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def map(key, value, context)
    value = value.to_s
    
    if value.include?(@delimiter)
      tokens = value.split(@delimiter, 2)
      key = tokens[0]
      value = tokens[1]
    else
      key = value
      value = nil
    end
    
    if @jmapreduce_mapper_job.mapper.nil?
      @jmapreduce_mapper_key.set(key)
      @jmapreduce_mapper_value.set(value)
      context.write(@jmapreduce_mapper_key, @jmapreduce_mapper_value)
      return
    end
    
    @jmapreduce_mapper_job.mapper.call(key, @jmapreduce_mapper_job.unpack(value))
  end
end
