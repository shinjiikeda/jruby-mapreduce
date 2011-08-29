require 'java'

import java.io.IOException

class JMapper < org.apache.hadoop.mapreduce.Mapper
  java_signature 'void setup(org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def setup(context)
    @key = org.apache.hadoop.io.Text.new
    @value = org.apache.hadoop.io.Text.new
    
    conf = context.getConfiguration
    script = conf.get('jmapreduce.script.name')
    job_index = conf.get('jmapreduce.job.index').to_i
    
    require script
    @mapper = JMapReduce.jobs[job_index].mapper
  end

  java_signature 'void map(java.lang.Object, org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context) throws IOException'
  def map(key, value, context)
    results = []
    value = value.to_s
    k,v = *value.split("\t")
    if v.nil?
      v = value
      k = key
    end
    @mapper.call(k, v, results)
    results.each do |tuple|
      tuple.each do |(k,v)|
        @key.set(k.to_s)
        @value.set(v.to_s)
        context.write(@key, @value)
      end
    end
  end
end