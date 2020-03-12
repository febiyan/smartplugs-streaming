gcloud services enable dataproc
gsutil mb gs://smartplugs-streaming
bq mk --dataset smartplugs-streaming:smartplugs