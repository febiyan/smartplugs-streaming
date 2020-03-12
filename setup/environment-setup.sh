# The Jupyter notebooks run in  Google Cloud Dataproc -- a cloud Hadoop deployment. I wanted to use only a cluster,


# In Dataproc, however, the first submitted streaming jobs from Jupyter Notebooks always end up eating all the available memory. 
# Maybe that has something to do with Dataproc's recommendation to have a separate cluster for each jobs.
# However, when deploying in production not using notebooks, we can definitely control the resource usage if we
# use `spark-submit`. With that, we can group the related resources in the same Dataproc cluster, if necessary.

# Kafka cluster
gcloud beta dataproc clusters create kafka \
--enable-component-gateway \
--region europe-west1 \
--subnet default \
--zone europe-west1-b \
--single-node \
--master-machine-type n1-standard-2 \
--master-boot-disk-type pd-ssd \
--master-boot-disk-size 50 \
--optional-components ZOOKEEPER,ANACONDA,JUPYTER \
--tags dataproc \
--project smartplugs-streaming \
--image-version=1.4 \
--metadata "run-on-master=true" \
--initialization-actions 'gs://dataproc-initialization-actions/kafka/kafka.sh' \
--bucket smartplugs-streaming \
--async

# Data preparation
gcloud beta dataproc clusters create spark-dataprep \
--enable-component-gateway \
--region europe-west1 \
--subnet default \
--zone europe-west1-b \
--single-node \
--master-machine-type n1-standard-2 \
--master-boot-disk-type pd-ssd \
--master-boot-disk-size 50 \
--optional-components ANACONDA,JUPYTER \
--tags dataproc \
--project smartplugs-streaming \
--image-version=1.4 \
--properties ^*^spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta \
--bucket smartplugs-streaming \
--async

# Stats calculation of Alert 1 and the alerting itself
gcloud beta dataproc clusters create spark-alert-1-stats \
--enable-component-gateway \
--region europe-west1 \
--subnet default \
--zone europe-west1-b \
--single-node \
--master-machine-type n1-standard-2 \
--master-boot-disk-type pd-ssd \
--master-boot-disk-size 50 \
--optional-components ANACONDA,JUPYTER \
--tags dataproc \
--project smartplugs-streaming \
--image-version=1.4 \
--properties ^*^spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 \
--bucket smartplugs-streaming \
--async

gcloud beta dataproc clusters create spark-alert-1-detect \
--enable-component-gateway \
--region europe-west1 \
--subnet default \
--zone europe-west1-b \
--single-node \
--master-machine-type n1-standard-2 \
--master-boot-disk-type pd-ssd \
--master-boot-disk-size 50 \
--optional-components ANACONDA,JUPYTER \
--tags dataproc \
--project smartplugs-streaming \
--image-version=1.4 \
--properties ^*^spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta \
--bucket smartplugs-streaming \
--async

# Stats calculation of Alert 2 and the alerting itself
gcloud beta dataproc clusters create spark-alert-2-stats \
--enable-component-gateway \
--region europe-west1 \
--subnet default \
--zone europe-west1-b \
--single-node \
--master-machine-type n1-standard-2 \
--master-boot-disk-type pd-ssd \
--master-boot-disk-size 50 \
--optional-components ANACONDA,JUPYTER \
--tags dataproc \
--project smartplugs-streaming \
--image-version=1.4 \
--properties ^*^spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 \
--bucket smartplugs-streaming \
--async

gcloud beta dataproc clusters create spark-alert-2-detect \
--enable-component-gateway \
--region europe-west1 \
--subnet default \
--zone europe-west1-b \
--single-node \
--master-machine-type n1-standard-2 \
--master-boot-disk-type pd-ssd \
--master-boot-disk-size 50 \
--optional-components ANACONDA,JUPYTER \
--tags dataproc \
--project smartplugs-streaming \
--image-version=1.4 \
--properties ^*^spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta \
--bucket smartplugs-streaming \
--async

# Ingestion to BigQuery
gcloud beta dataproc clusters create spark-ingest \
--enable-component-gateway \
--region europe-west1 \
--subnet default \
--zone europe-west1-b \
--single-node \
--master-machine-type n1-standard-2 \
--master-boot-disk-type pd-ssd \
--master-boot-disk-size 50 \
--optional-components ANACONDA,JUPYTER \
--tags dataproc \
--project smartplugs-streaming \
--image-version=1.4 \
--properties ^*^spark:spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,com.google.cloud.spark:spark-bigquery-with-dependencies_2.11:0.13.1-beta \
--bucket smartplugs-streaming \
--async
