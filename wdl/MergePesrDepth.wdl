version 1.0

import "ShardedCluster.wdl" as ShardedCluster
import "Structs.wdl"

workflow MergePesrDepth {
    input {
        File vcf
        File vcf_index
        String prefix
        String contig
        String sv_base_mini_docker
        String sv_pipeline_docker
        RuntimeAttr? runtime_attr_override
    }

    call GetSitesOnlyVcf {
        input:
            vcf=vcf,
            prefix="~{prefix}.sites_only",
            sv_base_mini_docker=sv_base_mini_docker,
            runtime_attr_override=runtime_attr_override
    }

    call ShardedCluster.ShardClusters {
        input:
            vcf=vcf,
            prefix="~{prefix}.shard_clusters",
            dist=1000000000,
            frac=0.5,
            svsize=50,
            sv_types=["DEL","DUP","INV","BND","INS"],
            sv_pipeline_docker=sv_pipeline_docker,
            runtime_attr_override=runtime_override_shard_clusters
    }

    call MiniTasks.ShardVids {
        input:
            clustered_vcf=ShardClusters.out,
            prefix=prefix,
            records_per_shard=merge_shard_size,
            sv_pipeline_docker=sv_pipeline_docker,
            runtime_attr_override=runtime_override_shard_vids
    }

    call MergePesrDepth {
        input:
            vcf=MergePesrDepth.out,
            vcf_index=MergePesrDepth.out_index,
            contig=contig,
            prefix="~{prefix}.merge_pesr_depth",
            sv_pipeline_docker=sv_pipeline_docker,
            runtime_attr_override=runtime_attr_override
    }

    output {
        File merged_vcf = MergePesrDepth.merged_vcf
    }
}

task GetSitesOnlyVcf {
    input {
        File vcf
        String prefix
        String sv_base_mini_docker
        RuntimeAttr? runtime_attr_override
    }

    Float input_size = size(vcf, "GiB")
    RuntimeAttr runtime_default = object {
                                      mem_gb: 2.0,
                                      disk_gb: ceil(10.0 + input_size * 1.2),
                                      cpu_cores: 1,
                                      preemptible_tries: 3,
                                      max_retries: 1,
                                      boot_disk_gb: 10
                                  }
    RuntimeAttr runtime_override = select_first([runtime_attr_override, runtime_default])
    runtime {
        memory: "~{select_first([runtime_override.mem_gb, runtime_default.mem_gb])} GiB"
        disks: "local-disk ~{select_first([runtime_override.disk_gb, runtime_default.disk_gb])} HDD"
        cpu: select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
        preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
        maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
        docker: sv_base_mini_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command <<<
        set -euxo pipefail
        bcftools view -G ~{vcf} -Oz -o ~{prefix}.vcf.gz
        tabix ~{prefix}.vcf.gz
    >>>

    output {
        File out = "~{prefix}.vcf.gz"
        File out_index = "~{prefix}.vcf.gz.tbi"
    }
}

task MergePesrDepth {
    input {
        File vcf
        File vcf_index
        String prefix
        String contig
        String sv_pipeline_docker
        RuntimeAttr? runtime_attr_override
    }

    String output_file = prefix + ".vcf.gz"

    # when filtering/sorting/etc, memory usage will likely go up (much of the data will have to
    # be held in memory or disk while working, potentially in a form that takes up more space)
    Float input_size = size(vcf, "GiB")
    RuntimeAttr runtime_default = object {
                                      mem_gb: 2.0 + 0.6 * input_size,
                                      disk_gb: ceil(10.0 + 4 * input_size),
                                      cpu_cores: 1,
                                      preemptible_tries: 3,
                                      max_retries: 1,
                                      boot_disk_gb: 10
                                  }
    RuntimeAttr runtime_override = select_first([runtime_attr_override, runtime_default])
    runtime {
        memory: "~{select_first([runtime_override.mem_gb, runtime_default.mem_gb])} GiB"
        disks: "local-disk ~{select_first([runtime_override.disk_gb, runtime_default.disk_gb])} HDD"
        cpu: select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
        preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
        maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
        docker: sv_pipeline_docker
        bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
    }

    command <<<
        set -euo pipefail
        /opt/sv-pipeline/04_variant_resolution/scripts/merge_pesr_depth.py \
        --prefix pesr_depth_merged_~{contig} \
        ~{vcf} \
        ~{output_file}
    >>>

    output {
        File merged_vcf = output_file
    }
}