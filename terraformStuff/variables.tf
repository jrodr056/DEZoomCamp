locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
    description = "Your GCP Project ID"
}

variable "region" {
    description = "Region for GCP resources. Choose as per your location"
    default = "us-west2"
}

variable "bucket_name" {
  default = ""
}

variable "storage_class" {
  default = "STANDARD"
}

variable "BQ_DATASET" {
  type = string
  default = "trips_data_all"
}

variable "TABLE_NAME" {
  type = string
  default = "ny_trips"
}

variable "credentials" {
  default = "D:/CODING/packages/jsonKey/focus-vim-397017-32c591f45127.json"
}