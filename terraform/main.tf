terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
  
  backend "gcs" {
    bucket = "ml-devops-terraform-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GKE Cluster
resource "google_container_cluster" "ml_cluster" {
  name     = "${var.project_name}-gke-cluster"
  location = var.region
  
  # We can't create a cluster with no node pool defined, but we want to only use
  # separately managed node pools. So we create the smallest possible default
  # node pool and immediately delete it.
  remove_default_node_pool = true
  initial_node_count       = 1
  
  network    = google_compute_network.vpc.name
  subnetwork = google_compute_subnetwork.subnet.name
  
  # Workload Identity
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }
  
  # Enable Autopilot (optional, for fully managed cluster)
  # enable_autopilot = true
  
  # Monitoring and logging
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
    
    managed_prometheus {
      enabled = true
    }
  }
  
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS", "WORKLOADS"]
  }
  
  addons_config {
    http_load_balancing {
      disabled = false
    }
    horizontal_pod_autoscaling {
      disabled = false
    }
  }
}

# Separately Managed Node Pool
resource "google_container_node_pool" "ml_nodes" {
  name       = "${var.project_name}-node-pool"
  location   = var.region
  cluster    = google_container_cluster.ml_cluster.name
  node_count = var.node_count
  
  autoscaling {
    min_node_count = var.min_node_count
    max_node_count = var.max_node_count
  }
  
  node_config {
    preemptible  = var.use_preemptible_nodes
    machine_type = var.machine_type
    
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    service_account = google_service_account.gke_sa.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    
    labels = {
      environment = var.environment
      managed-by  = "terraform"
    }
    
    tags = ["gke-node", "${var.project_name}-gke"]
    
    disk_size_gb = 50
    disk_type    = "pd-standard"
    
    metadata = {
      disable-legacy-endpoints = "true"
    }
  }
  
  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# VPC Network
resource "google_compute_network" "vpc" {
  name                    = "${var.project_name}-vpc"
  auto_create_subnetworks = false
}

# Subnet
resource "google_compute_subnetwork" "subnet" {
  name          = "${var.project_name}-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.name
  
  secondary_ip_range {
    range_name    = "services-range"
    ip_cidr_range = "10.1.0.0/16"
  }
  
  secondary_ip_range {
    range_name    = "pod-ranges"
    ip_cidr_range = "10.2.0.0/16"
  }
}

# Service Account for GKE nodes
resource "google_service_account" "gke_sa" {
  account_id   = "${var.project_name}-gke-sa"
  display_name = "Service Account for GKE nodes"
}

# IAM bindings for the service account
resource "google_project_iam_member" "gke_sa_roles" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/storage.objectViewer"
  ])
  
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gke_sa.email}"
}

# Cloud Storage bucket for MLflow artifacts
resource "google_storage_bucket" "mlflow_artifacts" {
  name          = "${var.project_name}-mlflow-artifacts"
  location      = var.region
  force_destroy = false
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# Cloud SQL for MLflow backend store
resource "google_sql_database_instance" "mlflow_db" {
  name             = "${var.project_name}-mlflow-db"
  database_version = "POSTGRES_15"
  region           = var.region
  
  settings {
    tier = "db-f1-micro"
    
    backup_configuration {
      enabled = true
      start_time = "03:00"
    }
    
    ip_configuration {
      ipv4_enabled = true
      authorized_networks {
        name  = "gke-cluster"
        value = google_container_cluster.ml_cluster.endpoint
      }
    }
  }
  
  deletion_protection = true
}

resource "google_sql_database" "mlflow" {
  name     = "mlflow"
  instance = google_sql_database_instance.mlflow_db.name
}

resource "google_sql_user" "mlflow" {
  name     = "mlflow"
  instance = google_sql_database_instance.mlflow_db.name
  password = var.mlflow_db_password
}

# Static IP for Load Balancer
resource "google_compute_address" "ml_app_ip" {
  name   = "${var.project_name}-ml-app-ip"
  region = var.region
}

# Firewall rules
resource "google_compute_firewall" "allow_internal" {
  name    = "${var.project_name}-allow-internal"
  network = google_compute_network.vpc.name
  
  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }
  
  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }
  
  allow {
    protocol = "icmp"
  }
  
  source_ranges = ["10.0.0.0/8"]
}

# ============================================================================
# AIDE 2: Data Engineering Resources
# ============================================================================

# GCS bucket for Delta Lake / Lakehouse storage
resource "google_storage_bucket" "lakehouse" {
  name          = "${var.project_name}-lakehouse"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }
}

# GCS bucket for raw data ingestion
resource "google_storage_bucket" "raw_data" {
  name          = "${var.project_name}-raw-data"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true
}

# GCS bucket for Feast feature registry
resource "google_storage_bucket" "feast_registry" {
  name          = "${var.project_name}-feast-registry"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true
}

# GCS bucket for Flink checkpoints
resource "google_storage_bucket" "flink_checkpoints" {
  name          = "${var.project_name}-flink-checkpoints"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

# Data engineering node pool (larger machines for Spark/Flink)
resource "google_container_node_pool" "data_nodes" {
  name       = "${var.project_name}-data-pool"
  location   = var.region
  cluster    = google_container_cluster.ml_cluster.name
  node_count = 2

  autoscaling {
    min_node_count = 1
    max_node_count = 6
  }

  node_config {
    preemptible  = var.use_preemptible_nodes
    machine_type = "e2-standard-4"

    service_account = google_service_account.gke_sa.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      environment = var.environment
      workload    = "data-engineering"
      managed-by  = "terraform"
    }

    tags = ["gke-node", "${var.project_name}-data"]

    disk_size_gb = 100
    disk_type    = "pd-ssd"

    taint {
      key    = "workload"
      value  = "data-engineering"
      effect = "PREFER_NO_SCHEDULE"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# Cloud Composer for managed Airflow
resource "google_composer_environment" "airflow" {
  name   = "${var.project_name}-composer"
  region = var.region

  config {
    software_config {
      image_version = "composer-2.5.0-airflow-2.6.3"

      pypi_packages = {
        "feast"        = ">=0.34.0"
        "delta-spark"  = ">=3.0.0"
        "boto3"        = ">=1.34.0"
      }
    }

    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 2
        memory_gb  = 4
        storage_gb = 10
        min_count  = 1
        max_count  = 3
      }
    }

    node_config {
      network    = google_compute_network.vpc.id
      subnetwork = google_compute_subnetwork.subnet.id
      service_account = google_service_account.gke_sa.email
    }
  }
}

# GCE instance for MinIO (persistent object storage)
resource "google_compute_instance" "minio" {
  name         = "${var.project_name}-minio"
  machine_type = "e2-standard-2"
  zone         = "${var.region}-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 200
      type  = "pd-ssd"
    }
  }

  network_interface {
    network    = google_compute_network.vpc.name
    subnetwork = google_compute_subnetwork.subnet.name
    access_config {}
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    apt-get update && apt-get install -y docker.io
    systemctl enable docker && systemctl start docker
    docker run -d --name minio \
      -p 9000:9000 -p 9001:9001 \
      -v /data:/data \
      -e MINIO_ROOT_USER=minioadmin \
      -e MINIO_ROOT_PASSWORD=minioadmin \
      minio/minio server /data --console-address ":9001"
  EOF

  tags = ["minio", "${var.project_name}-data"]

  service_account {
    email  = google_service_account.gke_sa.email
    scopes = ["cloud-platform"]
  }
}

# Firewall for MinIO
resource "google_compute_firewall" "allow_minio" {
  name    = "${var.project_name}-allow-minio"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["9000", "9001"]
  }

  source_ranges = ["10.0.0.0/8"]
  target_tags   = ["minio"]
}

# ============================================================================
# AIDE 3: Kubeflow + Knative Resources
# ============================================================================

# GCS bucket for AIDE 3 model registry/artifacts (Kubeflow + MLflow integration)
resource "google_storage_bucket" "aide3_model_registry" {
  name          = "${var.project_name}-aide3-model-registry"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type = "Delete"
    }
  }
}

# Dedicated node pool for real-time/event workloads used by AIDE 3.
resource "google_container_node_pool" "aide3_nodes" {
  name       = "${var.project_name}-aide3-pool"
  location   = var.region
  cluster    = google_container_cluster.ml_cluster.name
  node_count = 1

  autoscaling {
    min_node_count = 1
    max_node_count = 4
  }

  node_config {
    preemptible  = var.use_preemptible_nodes
    machine_type = var.aide3_machine_type

    service_account = google_service_account.gke_sa.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    labels = {
      environment = var.environment
      workload    = "aide3-realtime"
      managed-by  = "terraform"
    }

    tags = ["gke-node", "${var.project_name}-aide3"]
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

resource "google_compute_firewall" "allow_external" {
  name    = "${var.project_name}-allow-external"
  network = google_compute_network.vpc.name
  
  allow {
    protocol = "tcp"
    ports    = ["80", "443", "22"]
  }
  
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["gke-node"]
}
