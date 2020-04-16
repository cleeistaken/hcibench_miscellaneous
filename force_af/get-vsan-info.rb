require 'shellwords'
require_relative "rvc-util.rb"
require_relative "util.rb"

host_num = _get_hosts_list.count
vsan_datastore_name = ""
ftt = 1
policy_valid = false
policy_rule_map = {}
default_policy_rule_map = {}
vsan_default_policy = ""
default_policy_ftt = 1
policy_ftt = 1

cmd_run= _get_vsandatastore_name_in_cluster

if cmd_run == ""
  print "vSAN is not enabled!\n"
  exit(255)
else
  cmd_run=cmd_run.chomp
  @vsan_datastore_name = `echo  \"#{cmd_run}\" | sed 's/.*VSAN Datastore Name: *\\(.*\\)/\\1/'`.encode('UTF-8', :invalid => :replace).chomp

  is_test_on_vsan = false
  $datastore_names.each do |datastore_name|
    if datastore_name == @vsan_datastore_name
      is_test_on_vsan = true
    end
  end
  if !is_test_on_vsan
    exit
  end

  cmd_run = false
  @ds_path, @ds_path_escape = _get_ds_path_escape(@vsan_datastore_name)

  rules = `rvc #{$vc_rvc} --path #{@ds_path_escape} -c "vsantest.spbm_hcibench.get_vsandatastore_default_policy ."\
   -c 'exit' -q | grep -E "^Rule-Sets:" -A 100`.split("\n")

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
    rules = `rvc #{$vc_rvc} --path #{$dc_path_escape} -c #{get_rules_escape} -c 'exit' -q | grep -E "^Rule-Sets:" -A 100`.encode('UTF-8', :invalid => :replace).split("\n")
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
end

num_dg_p_host = num_of_dg/host_num
cap_model = num_of_cap/host_num/num_dg_p_host

file = File.open("#{ARGV[0]}/vsan.cfg", 'w')
file.puts "vSAN Datastore Name: #{@vsan_datastore_name}\n"
file.puts "vSAN Type: #{vsan_type}\n"
file.puts "Number of Hosts: #{host_num}\n"
file.puts "Disk Groups per Host: #{num_dg_p_host}\n"
#file.puts "Cache model: 1 \n"
#file.puts "Total Cache Disk Size:#{total_cache_size} GB"
file.puts "Capacity Disk per Disk Group: #{cap_model}\n"
file.puts "Deduplication/Compression Enabled: #{dedup}\n"
file.puts "Host Primary Fault Tolerance: #{policy_pftt}\n"
file.puts "Host Secondary Fault Tolerance: #{policy_sftt}\n"
file.puts "Checksum Disabled: #{policy_csc}\n"
file.close()
