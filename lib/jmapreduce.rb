require 'java'

java_package 'org.fingertap.jmapreduce'

java_import 'org.fingertap.jmapreduce.JsonProperty'
java_import 'org.fingertap.jmapreduce.JMapReduceJob'
java_import 'org.fingertap.jmapreduce.MapperWrapper'
java_import 'org.fingertap.jmapreduce.ReducerWrapper'

java_import 'org.apache.hadoop.fs.Path'
java_import 'org.apache.hadoop.io.Text'
java_import 'org.apache.hadoop.io.IntWritable'

java_import 'org.apache.hadoop.mapreduce.Job'
java_import 'org.apache.hadoop.conf.Configuration'
java_import 'org.apache.hadoop.util.GenericOptionsParser'

java_import 'org.apache.hadoop.mapreduce.lib.input.TextInputFormat'
java_import 'org.apache.hadoop.mapreduce.lib.input.FileInputFormat'
java_import 'org.apache.hadoop.mapreduce.lib.output.FileOutputFormat'

class JMapReduce
  def self.jobs
    @@jobs
  end
  
  def self.job(name, blk)
    job = JMapReduceJob.new
    job.set_name(name)
    @@jobs ||= []
    @@jobs << job
    job.set_mapreduce(blk)
  end
  
  def self.set_properties(properties)
    return unless properties
    
    @@properties = {}
    props = properties.split(',')
    props.each do |property|
      key,value = *property.split('=')
      if key == 'json'
        JsonProperty.parse(value).each do |(k,v)|
          @@properties[k] = v
        end
      else
        @@properties[key] = value
      end
    end
  end
  
  def self.property(key)
    @@properties[key] if @@properties
  end
  
  java_signature 'void main(String[])'
  def self.main(args)
    conf = Configuration.new
    otherArgs = GenericOptionsParser.new(conf, args).getRemainingArgs.to_ary
    
    if (otherArgs.size < 3)
      java.lang.System.err.println("Usage: JMapReduce <script> <in> <out>")
      java.lang.System.exit(2)
    end
    
    conf.set("mapred.output.compress", "true");
    
    script = otherArgs.shift
    opts = otherArgs.pop
    script_output = otherArgs.pop
    script_inputs = otherArgs
    conf.set('jmapreduce.script.name', script)
    
    if opts.include?('=')
      conf.set('jmapreduce.property', opts)
      set_properties(opts)
    end
    
    #if otherArgs.size > 3
    #  (3..(otherArgs.size-1)).each do |index|
    #    if otherArgs[index].include?('=')
    #      conf.set('jmapreduce.property', otherArgs[index])
    #      set_properties(otherArgs[index])
    #    end
    #  end
    #end
    
    @@jobs ||= []
    require script
    inputs = script_inputs
    output = script_output
    
    set_last_job
    tmp_outputs = []
    
    @@jobs.each_with_index do |jmapreduce_job,index|
      jmapreduce_job.set_conf(conf)
      jmapreduce_job.set_properties(conf.get('jmapreduce.property'))
      
      conf.set('jmapreduce.job.index', index.to_s)
      conf.set('jmapreduce.last_job.reducer', "true") if jmapreduce_job.is_last_reducer
      conf.set('jmapreduce.last_job.mapper', "true") if jmapreduce_job.is_last_mapper

      if @@jobs.size > 1
        if index == @@jobs.size-1
          output = script_output
        else
          output = "#{script_output}-part-#{index}"
          tmp_outputs << output
        end
      end
      
      if jmapreduce_job.get_custom_job
        job = jmapreduce_job.get_custom_job.call(conf)
      else
        job = Job.new(conf, jmapreduce_job.name)
        job.setOutputKeyClass(Text.java_class)
        job.setOutputValueClass(Text.java_class)
      end
      
      job.setJarByClass(JMapReduce.java_class)
      job.setMapperClass(MapperWrapper.java_class)
      job.setReducerClass(ReducerWrapper.java_class)
      job.setNumReduceTasks(jmapreduce_job.num_of_reduce_tasks)
      
      inputs.each do | input |
         FileInputFormat.addInputPath(job, Path.new(input))
      end
      FileOutputFormat.setOutputPath(job, Path.new(output))
      
      jmapreduce_job.before_job_hook.call(job) if jmapreduce_job.before_job_hook
      job.waitForCompletion(true)
      inputs = [ output ]
    end
    
    # clean up
    tmp_outputs.each do |tmp_output|
      tmp_path = Path.new(tmp_output)
      hdfs = tmp_path.getFileSystem(conf)
      hdfs.delete(tmp_path, true) if hdfs.exists(tmp_path)
    end
  end
  
  def self.set_last_job
    return if @@jobs.empty?
      
    # find last job that has a mapper or reducer defined
    @@jobs.reverse.each do |job|
      if job.mapper || job.reducer
        job.reducer ? job.set_last_reducer : job.set_last_mapper
        break
      end
    end
  end
end
