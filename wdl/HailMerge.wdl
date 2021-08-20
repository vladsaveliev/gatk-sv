version 1.0

workflow HailMerge {
  input {
    Array[File] vcfs
    File hail_script
    String prefix
    String project
  }

  call HailMerge {
    input:
      vcfs = vcfs,
      hail_script = hail_script,
      prefix = prefix,
      project = project
  }

  output {
    File merged_vcf = HailMerge.merged_vcf
    File merged_vcf_index = HailMerge.merged_vcf_index
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
    set -eu    

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
  tabix -p vcf ~{prefix}.vcf.gz
  >>>

  runtime {
    memory: "6.5 GB"
    disks: "local-disk 50 HDD"
    docker: "us.gcr.io/broad-dsde-methods/cwhelan/sv-pipeline-hail:0.2.71"    
    bootDiskSizeGb: 10
  }

  output {
    File merged_vcf = "~{prefix}.vcf.gz"
    File merged_vcf_index = "~{prefix}.vcf.gz.tbi"
  }
}
