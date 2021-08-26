version 1.0

import "Structs.wdl"

workflow HailMerge {
  input {
    Array[File] vcfs
    File hail_script
    String prefix
    String project
    String sv_base_mini_docker
  }

  call HailMerge {
    input:
      vcfs = vcfs,
      hail_script = hail_script,
      prefix = prefix,
      project = project
  }

  call FixHeader {
    input:
      merged_vcf = HailMerge.merged_vcf,
      example_vcf = vcfs[0],
      prefix = prefix + ".reheadered",
      sv_base_mini_docker = sv_base_mini_docker
  }

  output {
    File merged_vcf = FixHeader.out
    File merged_vcf_index = FixHeader.out_index
  }
}

task HailMerge {
  input {
    Array[File] vcfs
    String prefix
    String project
    File hail_script
    String region = "us-central1"
  }

  parameter_meta {
    vcfs: {
      localization_optional: true
    }
  }

  String cluster_name_prefix="gatk-sv-cluster-"

  command <<<
    set -euxo pipefail

    cp ~{write_lines(vcfs)} "files.list"

    python <<CODE
import hail as hl
import os
import uuid
from google.cloud import dataproc_v1 as dataproc

cluster_name = "gatk-sv-hail-{}".format(uuid.uuid4())

try:
  print(os.popen("hailctl dataproc start --region {} --project {} --num-master-local-ssds 1 --num-worker-local-ssds 1 --max-idle=60m --max-age=120m {}".format("~{region}", "~{project}", cluster_name)).read())

  cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"~{region}-dataproc.googleapis.com:443"}
  )

  for cluster in cluster_client.list_clusters(request={"project_id": "~{project}", "region": "~{region}"}):
    if cluster.cluster_name == cluster_name:
      cluster_staging_bucket = cluster.config.temp_bucket
      os.popen("gcloud dataproc jobs submit pyspark {} --cluster={} --project {} --files=files.list --region={} --driver-log-levels root=WARN -- {} {}".format("~{hail_script}", cluster_name, "~{project}", "~{region}", cluster_staging_bucket, cluster_name)).read()
      os.popen("gsutil cp -r gs://{}/{}/merged.vcf.bgz .".format(cluster_staging_bucket, cluster_name)).read()
      break

except Exception as e:
  print(e)
  raise
finally:
  os.popen("gcloud dataproc clusters delete --project {} --region {} {}".format("~{project}", "~{region}", cluster_name)).read()
CODE

  mv merged.vcf.bgz ~{prefix}.vcf.gz
  tabix ~{prefix}.vcf.gz
  >>>

  runtime {
    memory: "6.5 GB"
    disks: "local-disk 100 HDD"
    docker: "us.gcr.io/broad-dsde-methods/cwhelan/sv-pipeline-hail:0.2.71"
    bootDiskSizeGb: 10
  }

  output {
    File merged_vcf = "~{prefix}.vcf.gz"
    File merged_vcf_index = "~{prefix}.vcf.gz.tbi"
  }
}

task FixHeader {
  input {
    File merged_vcf
    File example_vcf
    String prefix
    String sv_base_mini_docker
    RuntimeAttr? runtime_attr_override
  }

  RuntimeAttr runtime_default = object {
                                  mem_gb: 3.75,
                                  disk_gb: ceil(10 + size(merged_vcf, "GB") * 2 + size(example_vcf, "GB")),
                                  cpu_cores: 1,
                                  preemptible_tries: 3,
                                  max_retries: 1,
                                  boot_disk_gb: 10
                                }
  RuntimeAttr runtime_override = select_first([runtime_attr_override, runtime_default])
  runtime {
    memory: select_first([runtime_override.mem_gb, runtime_default.mem_gb]) + " GB"
    disks: "local-disk " + select_first([runtime_override.disk_gb, runtime_default.disk_gb]) + " SSD"
    cpu: select_first([runtime_override.cpu_cores, runtime_default.cpu_cores])
    preemptible: select_first([runtime_override.preemptible_tries, runtime_default.preemptible_tries])
    maxRetries: select_first([runtime_override.max_retries, runtime_default.max_retries])
    docker: sv_base_mini_docker
    bootDiskSizeGb: select_first([runtime_override.boot_disk_gb, runtime_default.boot_disk_gb])
  }

  command <<<
    set -euxo pipefail

    # Insert source line
    bcftools view --no-version -h ~{merged_vcf} | grep -v ^#CHROM | grep -v ^##source > header
    bcftools view -h ~{example_vcf} | { grep ^##source || true; } >> header
    bcftools view -h ~{merged_vcf} | grep ^#CHROM >> header
    bcftools reheader -h header ~{merged_vcf} > ~{prefix}.vcf.gz
    tabix ~{prefix}.vcf.gz
  >>>

  output {
    File out = "~{prefix}.vcf.gz"
    File out_index = "~{prefix}.vcf.gz.tbi"
  }
}
