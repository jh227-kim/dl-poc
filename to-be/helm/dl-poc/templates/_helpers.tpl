{{/*
Expand the name of the chart.
*/}}
{{- define "dl-poc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dl-poc.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dl-poc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dl-poc.labels" -}}
helm.sh/chart: {{ include "dl-poc.chart" . }}
{{ include "dl-poc.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "dl-poc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dl-poc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Postgres Namespace Resolver
*/}}
{{- define "dl-poc.postgresNamespace" -}}
{{- .Values.postgres.namespace | default "ns-dl-infra" -}}
{{- end -}}

{{/*
MinIO Namespace Resolver
*/}}
{{- define "dl-poc.minioNamespace" -}}
{{- .Values.minio.namespace | default "ns-dl-infra" -}}
{{- end -}}

{{/*
Kafka Namespace Resolver
*/}}
{{- define "dl-poc.kafkaNamespace" -}}
{{- .Values.kafka.namespace | default "ns-dl-infra" -}}
{{- end -}}

{{/*
Kafka-UI Namespace Resolver
*/}}
{{- define "dl-poc.kafkaUiNamespace" -}}
{{- .Values.kafkaUi.namespace | default "ns-dl-infra" -}}
{{- end -}}

{{/*
Spark Ingest Namespace Resolver
*/}}
{{- define "dl-poc.sparkMailIngestNamespace" -}}
{{- .Values.sparkMailIngest.namespace | default "ns-dl-pipeline" -}}
{{- end -}}

{{/*
Spark DLQ Processor Namespace Resolver
*/}}
{{- define "dl-poc.sparkDlqProcessorNamespace" -}}
{{- .Values.sparkDlqProcessor.namespace | default "ns-dl-pipeline" -}}
{{- end -}}

{{/*
API Gateway Namespace Resolver
*/}}
{{- define "dl-poc.apiGatewayNamespace" -}}
{{- .Values.apiGateway.namespace | default "ns-dl-serving" -}}
{{- end -}}

{{/*
Serving Service Namespace Resolver
*/}}
{{- define "dl-poc.appsNamespace" -}}
{{- .Values.servingService.namespace | default "ns-dl-serving" -}}
{{- end -}}

{{/*
Mocks Namespace Resolver
*/}}
{{- define "dl-poc.mocksNamespace" -}}
{{- .Values.mocks.namespace | default "ns-dl-mocks" -}}
{{- end -}}

{{/*
Dashboard Namespace Resolver
*/}}
{{- define "dl-poc.dashboardNamespace" -}}
{{- .Values.dashboard.namespace | default "ns-dl-mocks" -}}
{{- end -}}

{{/*
Postgres Host FQDN Helper
*/}}
{{- define "dl-poc.postgresHost" -}}
{{- if .Values.postgres.enabled -}}
{{- printf "%s-postgres.%s.svc.cluster.local" (include "dl-poc.fullname" .) (include "dl-poc.postgresNamespace" .) -}}
{{- else -}}
{{- .Values.externalPostgres.host -}}
{{- end -}}
{{- end -}}

{{/*
Postgres URI FQDN Helper for Python services
*/}}
{{- define "dl-poc.postgresUri" -}}
{{- if .Values.postgres.enabled -}}
{{- printf "postgresql://admin:password@%s-postgres.%s.svc.cluster.local:5432/iceberg_catalog" (include "dl-poc.fullname" .) (include "dl-poc.postgresNamespace" .) -}}
{{- else -}}
{{- printf "postgresql://%s:%s@%s:%d/%s" .Values.externalPostgres.user .Values.externalPostgres.password .Values.externalPostgres.host (int .Values.externalPostgres.port) .Values.externalPostgres.database -}}
{{- end -}}
{{- end -}}

{{/*
Postgres JDBC URI FQDN Helper for Spark Ingest
*/}}
{{- define "dl-poc.postgresJdbcUri" -}}
{{- if .Values.postgres.enabled -}}
{{- printf "jdbc:postgresql://%s-postgres.%s.svc.cluster.local:5432/iceberg_catalog" (include "dl-poc.fullname" .) (include "dl-poc.postgresNamespace" .) -}}
{{- else -}}
{{- printf "jdbc:postgresql://%s:%d/%s" .Values.externalPostgres.host (int .Values.externalPostgres.port) .Values.externalPostgres.database -}}
{{- end -}}
{{- end -}}

{{/*
MinIO Endpoint FQDN Helper
*/}}
{{- define "dl-poc.minioEndpoint" -}}
{{- if .Values.minio.enabled -}}
{{- printf "http://%s-minio.%s.svc.cluster.local:9000" (include "dl-poc.fullname" .) (include "dl-poc.minioNamespace" .) -}}
{{- else -}}
{{- .Values.externalMinio.endpoint -}}
{{- end -}}
{{- end -}}

{{/*
MinIO Access Key helper
*/}}
{{- define "dl-poc.minioAccessKey" -}}
{{- if .Values.minio.enabled -}}
{{- .Values.minio.rootUser -}}
{{- else -}}
{{- .Values.externalMinio.accessKey -}}
{{- end -}}
{{- end -}}

{{/*
MinIO Secret Key helper
*/}}
{{- define "dl-poc.minioSecretKey" -}}
{{- if .Values.minio.enabled -}}
{{- .Values.minio.rootPassword -}}
{{- else -}}
{{- .Values.externalMinio.secretKey -}}
{{- end -}}
{{- end -}}

{{/*
Kafka Bootstrap Servers FQDN Helper
*/}}
{{- define "dl-poc.kafkaBootstrap" -}}
{{- if .Values.kafka.enabled -}}
{{- printf "%s-kafka.%s.svc.cluster.local:29092" (include "dl-poc.fullname" .) (include "dl-poc.kafkaNamespace" .) -}}
{{- else -}}
{{- .Values.externalKafka.bootstrapServers -}}
{{- end -}}
{{- end -}}
