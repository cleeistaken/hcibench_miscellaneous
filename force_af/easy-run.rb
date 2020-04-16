require 'yaml'
require 'fileutils'
require 'timeout'
require 'shellwords'
require_relative "rvc-util.rb"
require_relative "util.rb"

@log_file = "#{$log_path}/easy-run.log"
host_num = _get_hosts_list.count
vsan_datastore_name = ""
ftt = 1
policy_valid = false
policy_rule_map = {}
default_policy_rule_map = {}
vsan_default_policy = ""
default_policy_ftt = 1
policy_ftt = 1
ratio = 0.25
disk_init = "ZERO"
_test_time = 3600
_warmup_time = 1800

cmd_run= _get_vsandatastore_name_in_cluster
if cmd_run == ""
  puts "------------------------------------------------------------------------------",@log_file
  puts "VSAN Is Not Enabled in Cluster #{$cluster_name}!",@log_file
  puts "------------------------------------------------------------------------------",@log_file
  exit(255)
else
  cmd_run=cmd_run.chomp
  @vsan_datastore_name = `echo  \"#{cmd_run}\" | sed 's/.*VSAN Datastore Name: *\\(.*\\)/\\1/'`.encode('UTF-8', :invalid => :replace).chomp
  cmd_run = false
  @ds_path, @ds_path_escape = _get_ds_path_escape(@vsan_datastore_name)
  puts "vSAN Datastore Name: #{@vsan_datastore_name}", @log_file
  rules = `rvc #{$vc_rvc} --path #{@ds_path_escape} -c "vsantest.spbm_hcibench.get_vsandatastore_default_policy ."\
   -c 'exit' -q | grep -E "^Rule-Sets:" -A 100 | tee -a #{$log_file}`.split("\n")

  rules.each do |rule|
    rule = rule.delete(' ')
    if not rule.include? "Rule-Set"
      default_policy_rule_map[rule.split(":").first] = rule.split(":").last
    end
  end
  policy_pftt = default_policy_rule_map["VSAN.hostFailuresToTolerate"] || "1"
  policy_sftt = default_policy_rule_map["VSAN.subFailuresToTolerate"] || "0"
  policy_csc = default_policy_rule_map["VSAN.checksumDisabled"] || "false"

  default_policy_ftt = ( policy_pftt.to_i + 1 ) * ( policy_sftt.to_i + 1 )

  if $storage_policy and not $storage_policy.empty? and not $storage_policy.strip.empty?
    get_rules_escape = Shellwords.escape(%{vsantest.perf.get_policy_rules_by_name . "#{$storage_policy}"})
    rules = `rvc #{$vc_rvc} --path #{$dc_path_escape} -c #{get_rules_escape} -c 'exit' -q | grep -E "^Rule-Sets:" -A 100 | tee -a #{$log_file}`.encode('UTF-8', :invalid => :replace).split("\n")
    rules.each do |rule|
      rule = rule.delete(' ')
      if not rule.include? "Rule-Set"
        policy_rule_map[rule.split(":").first] = rule.split(":").last
      end
    end
    policy_pftt = policy_rule_map["VSAN.hostFailuresToTolerate"] || "1"
    policy_sftt = policy_rule_map["VSAN.subFailuresToTolerate"] || "0"
    policy_csc = policy_rule_map["VSAN.checksumDisabled"] || "false"

    policy_ftt = ( policy_pftt.to_i + 1 ) * ( policy_sftt.to_i + 1 )
    policy_valid = true  
  end

  if policy_valid 
    ftt = policy_ftt.to_i
  else
    ftt = default_policy_ftt.to_i
  end
end

total_cache_size = `rvc #{$vc_rvc} --path #{$cl_path_escape} -c 'vsantest.vsan_hcibench.disks_stats .' -c 'exit' -q | grep Total_Cache_Size | cut -d " " -f2`.to_i
num_of_dg = `rvc #{$vc_rvc} --path #{$cl_path_escape} -c 'vsantest.vsan_hcibench.disks_stats .' -c 'exit' -q | grep Total_DiskGroup_Number | cut -d " " -f2`.to_i
num_of_cap = `rvc #{$vc_rvc} --path #{$cl_path_escape} -c 'vsantest.vsan_hcibench.disks_stats .' -c 'exit' -q | grep Total_Capacity_Disk_Number | cut -d " " -f2`.to_i
vsan_type = `rvc #{$vc_rvc} --path #{$cl_path_escape} -c 'vsantest.vsan_hcibench.vsan_type .' -c 'exit' -q | grep VSAN_Type | cut -d " " -f2`.chomp
dedup = `rvc #{$vc_rvc} --path #{$cl_path_escape} -c 'vsantest.vsan_hcibench.vsan_type .' -c 'exit' -q | grep Dedup_Scope | cut -d " " -f2`.chomp

# charlesl+ force to all-flash for witness
vsan_type = "All-Flash"
# charles-

if vsan_type == "All-Flash"
  total_cache_size = [num_of_dg * 600,total_cache_size].min
  ratio = 0.75
end

vm_deployed_size = total_cache_size * ratio / ftt
@vm_num = num_of_dg * 2 * $total_datastore
@data_disk_num = 8 #num_of_cap * 2 / vm_num

if @vm_num % host_num != 0
  @vm_num += (host_num - @vm_num % host_num)
end

thread_num = 32 / @data_disk_num
@disk_size = [(vm_deployed_size / (@vm_num / $total_datastore * @data_disk_num)).floor,1].max
time = Time.now.to_i

if dedup and dedup == "2"
  disk_init = "RANDOM"
end

pref = "hci-vdb"
if $tool == "fio"
  pref = "hci-fio"
end
  
`sed -i "s/^vm_prefix.*/vm_prefix: '#{pref}'/g" /opt/automation/conf/perf-conf.yaml`
`sed -i "s/^number_vm.*/number_vm: #{@vm_num}/g" /opt/automation/conf/perf-conf.yaml`
`sed -i "s/^number_data.*/number_data_disk: #{@data_disk_num}/g" /opt/automation/conf/perf-conf.yaml`
`sed -i "s/^size_data.*/size_data_disk: #{@disk_size}/g" /opt/automation/conf/perf-conf.yaml`
`sed -i "s/^warm_up_disk_before_.*/warm_up_disk_before_testing: '#{disk_init}'/g" /opt/automation/conf/perf-conf.yaml`
`rm -rf /opt/tmp/tmp* ; mkdir -m 755 -p /opt/tmp/tmp#{time}` 

devider = 4
if policy_csc == "true"
  devider = 1
end

gotodir = "cd /opt/automation/#{$tool}-param-files;"
executable = "fioconfig create"

if $tool == "vdbench"
  executable = "sh /opt/automation/generate-vdb-param-file.sh"
end

workloadParam = ""
for workload in $workloads
  case workload
  when "4k70r"
    workloadParam = " -n #{@data_disk_num} -w 100 -t #{thread_num} -b 4k -r 70 -s 100 -e #{_test_time} -m #{_warmup_time}"
  when "4k100r"
    workloadParam = " -n #{@data_disk_num} -w 100 -t #{thread_num} -b 4k -r 100 -s 100 -e #{_test_time} -m #{_warmup_time}"
  when "8k50r"
    workloadParam = " -n #{@data_disk_num} -w 100 -t #{thread_num} -b 8k -r 50 -s 100 -e #{_test_time} -m #{_warmup_time}"
  when "256k0r"
    workloadParam = " -n #{@data_disk_num} -w 100 -t #{thread_num/devider} -b 256k -r 0 -s 0 -e #{_test_time} -m #{_warmup_time}"
  end
  puts `#{gotodir + executable + workloadParam}`,@log_file
  `FILE=$(ls /opt/automation/#{$tool}-param-files/ -tr | grep -v / |tail -1); cp /opt/automation/#{$tool}-param-files/${FILE} /opt/tmp/tmp#{time}`
end

`sed -i "s/^self_defined_param.*/self_defined_param_file_path: '\\/opt\\/tmp\\/tmp#{time}' /g" /opt/automation/conf/perf-conf.yaml`
`sed -i "s/^output_path.*/output_path: 'easy-run-#{time}'/g" /opt/automation/conf/perf-conf.yaml`
`sed -i "s/^testing_duration.*/testing_duration:/g" /opt/automation/conf/perf-conf.yaml`
`sed -i "s/^cleanup_vm.*/cleanup_vm: false/g" /opt/automation/conf/perf-conf.yaml`
`sync; sleep 1`

`ruby #{$allinonetestingfile}`
`rm -rf /opt/tmp/tmp#{time}`
