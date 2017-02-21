macro(apply_patch)
	execute_process(COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/${ARGV0}.patch
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/${ARGV1}/)
endmacro(apply_patch)

apply_patch(cluster_run ns_server)
apply_patch(cluster_connect ns_server)
apply_patch(bucket_info_cache ns_server)
apply_patch(ns_config_default ns_server)
apply_patch(ns_ports_setup ns_server)
apply_patch(menelaus_pluggable_ui ns_server)
apply_patch(mn_services ns_server)
apply_patch(ns_cluster_membership ns_server)
apply_patch(classic_ui ns_server)


