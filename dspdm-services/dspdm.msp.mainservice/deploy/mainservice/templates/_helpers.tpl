{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "phoenixmsp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "phoenixmsp.fullname" -}}
{{- if (eq .Values.global.env.production true) -}}
{{- if  .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "phoenixmsp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Deriving FileStorageName
*/}}
{{- define "phoenixmsp.filestoragename" -}}
{{- if (.Values.fileStorage.name) -}}
{{- .Values.fileStorage.name -}}
{{- else -}}
{{ include "phoenixmsp.fullname" . }}-filestorage
{{- end -}}
{{- end -}}

{{/*
Deriving BlockStorageName
*/}}
{{- define "phoenixmsp.blockstoragename" -}}
{{ include "phoenixmsp.fullname" . }}-blockstorage
{{- end -}}

{{/*
Deriving Configmapfrom literal name
*/}}
{{- define "phoenixmsp.configmap.env" -}}
{{- if (.Values.configmap.env.name) -}}
{{- .Values.configmap.env.name -}}
{{- else -}}
{{ include "phoenixmsp.fullname" . }}-envvar
{{- end -}}
{{- end -}}

{{/*
Deriving Configmapfrom file name
*/}}
{{- define "phoenixmsp.configmap.file" -}}
{{- if (.Values.configmap.file.name) -}}
{{- .Values.configmap.file.name -}}
{{- else -}}
{{ include "phoenixmsp.fullname" . }}
{{- end -}}
{{- end -}}

{{/*
Deriving Secretfrom literal name
*/}}
{{- define "phoenixmsp.secret.env" -}}
{{- if (.Values.secret.env.name) -}}
{{- .Values.secret.env.name -}}
{{- else -}}
{{ include "phoenixmsp.fullname" . }}-envvar
{{- end -}}
{{- end -}}

{{/*
Deriving Secretfrom file name
*/}}
{{- define "phoenixmsp.secret.file" -}}
{{- if (.Values.secret.file.name) -}}
{{- .Values.secret.file.name -}}
{{- else -}}
{{ include "phoenixmsp.fullname" . }}
{{- end -}}
{{- end -}}

{{/*
Newrelic environments settings 
*/}}
{{- define "phoenixmsp.newrelicenvs" -}}
  # NewRelic license key for the specific account defined using a secret
  - name: NEW_RELIC_LICENSE_KEY
    valueFrom:
      secretKeyRef:
        name: newrelic-agent
        key: licensekey
  - name: NEW_RELIC_CHART_NAME
    value: {{ .Chart.Name }}
  - name: NEW_RELIC_PRODUCT_NAME
    value: {{ .Values.productName}}
  - name: NEW_RELIC_ENV_NAME
    valueFrom:
      configMapKeyRef:
        name: plat-config
        key: DPLAT_ENV_NAME
  # Following labels defined separated by semi colon to filter apps on NewRelic APM application page
  - name: NEW_RELIC_LABELS
    value: SERVICE_DEPLOYMENT_NAME:{{ include "phoenixmsp.fullname" . }};CLUSTER:{{ .Values.global.cluster.hosts}};
  - name: NEW_RELIC_METADATA_KUBERNETES_CLUSTER_NAME
    value: {{ .Values.global.cluster.hosts}}
  - name: NEW_RELIC_METADATA_KUBERNETES_DEPLOYMENT_NAME
    value: {{ include "phoenixmsp.fullname" . }}
  - name: NEW_RELIC_DISTRIBUTED_TRACING_ENABLED
    value: "true"
{{- end -}}

