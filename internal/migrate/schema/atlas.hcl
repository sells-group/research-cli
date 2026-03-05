variable "url" {
  type    = string
  default = getenv("RESEARCH_DATABASE_URL")
}

variable "dev_url" {
  type    = string
  default = getenv("RESEARCH_ATLAS_DEV_URL")
}

env "prod" {
  url     = var.url
  dev     = var.dev_url
  schemas = ["public", "fed_data"]
  src = [
    "file://extensions.sql",
    "file://public.sql",
    "file://fed_data.sql",
  ]
  diff {
    skip {
      drop_schema = true
      drop_table  = true
      drop_column = true
    }
    concurrent_index {
      add = true
    }
  }
}

env "local" {
  url     = var.url
  dev     = "docker://postgis/postgis/17-3.5"
  schemas = ["public", "fed_data"]
  src = [
    "file://extensions.sql",
    "file://public.sql",
    "file://fed_data.sql",
  ]
}
