output "cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.ml_cluster.name
}

output "cluster_endpoint" {
  description = "GKE cluster endpoint"
  value       = google_container_cluster.ml_cluster.endpoint
  sensitive   = true
}

output "cluster_ca_certificate" {
  description = "GKE cluster CA certificate"
  value       = google_container_cluster.ml_cluster.master_auth[0].cluster_ca_certificate
  sensitive   = true
}

output "mlflow_bucket" {
  description = "MLflow artifacts bucket name"
  value       = google_storage_bucket.mlflow_artifacts.name
}

output "mlflow_db_connection" {
  description = "MLflow database connection string"
  value       = "postgresql://mlflow:${var.mlflow_db_password}@${google_sql_database_instance.mlflow_db.connection_name}/mlflow"
  sensitive   = true
}

output "static_ip" {
  description = "Static IP for ML application"
  value       = google_compute_address.ml_app_ip.address
}

output "vpc_name" {
  description = "VPC network name"
  value       = google_compute_network.vpc.name
}

output "kubectl_config" {
  description = "kubectl configuration command"
  value       = "gcloud container clusters get-credentials ${google_container_cluster.ml_cluster.name} --region ${var.region} --project ${var.project_id}"
}

# AIDE 2 outputs
output "lakehouse_bucket" {
  description = "Delta Lake / Lakehouse storage bucket"
  value       = google_storage_bucket.lakehouse.name
}

output "raw_data_bucket" {
  description = "Raw data ingestion bucket"
  value       = google_storage_bucket.raw_data.name
}

output "feast_registry_bucket" {
  description = "Feast feature store registry bucket"
  value       = google_storage_bucket.feast_registry.name
}

output "minio_instance_ip" {
  description = "MinIO GCE instance external IP"
  value       = google_compute_instance.minio.network_interface[0].access_config[0].nat_ip
}

output "composer_airflow_uri" {
  description = "Cloud Composer Airflow web UI URL"
  value       = google_composer_environment.airflow.config[0].airflow_uri
}

# AIDE 3 outputs
output "aide3_model_registry_bucket" {
  description = "AIDE 3 model registry bucket for Kubeflow/MLflow artifacts"
  value       = google_storage_bucket.aide3_model_registry.name
}

output "aide3_node_pool_name" {
  description = "AIDE 3 dedicated GKE node pool name"
  value       = google_container_node_pool.aide3_nodes.name
}
