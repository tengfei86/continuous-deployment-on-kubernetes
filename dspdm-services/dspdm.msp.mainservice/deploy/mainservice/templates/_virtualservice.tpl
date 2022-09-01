{{- define "phoenix.virtualservice" }}
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
  labels:
    app: {{ include "phoenixmsp.fullname" . }}
    app.kubernetes.io/name: {{ include "phoenixmsp.name" . }}
    helm.sh/chart: {{ include "phoenixmsp.chart" . }}
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/managed-by: {{ .Release.Service }}
spec:
  hosts:
  - {{ include "phoenixmsp.virtualservice.hosts" . }}
  gateways:
  - {{ include "phoenixmsp.virtualservice.gateways" . }}
  http:
{{ include "phoenixmsp.virtualservice.http.routes" . | indent 2 }}
  - match:
    - uri:
        prefix: {{ include "phoenixmsp.virtualservice.prefix" . }}
    rewrite:
        uri: /
    route:
    - destination:
        port:
          number: 80
        host: {{ include "phoenixmsp.fullname" . }} 
    corsPolicy:
      allowOrigin:
       ['*']
      allowMethods:
      - POST
      - GET
      - OPTIONS
      - PUT
      - DELETE
      allowHeaders:
      - "*"
      allowCredentials: false     
    {{- if eq .Values.websocketUpgrade true }}
    websocketUpgrade: true
    {{- end }}
    {{- if .Values.extraHttpRules }}
{{ toYaml .Values.extraHttpRules | indent 4 }}
    {{- end }} 
{{- end }}

{{/* Evaluate hosts */}}
{{- define "phoenixmsp.virtualservice.hosts" -}}
{{- if contains "web-app" .Values.app_type -}}
{{- include "phoenixmsp.fullname" .}}.{{ .Values.global.cluster.hosts }}
{{- else }}
{{- .Values.global.cluster.hosts }}
{{- end }}
{{- end }}

{{/* Evaluate gateways value */}}
{{- define "phoenixmsp.virtualservice.gateways" -}}
{{- if eq .Values.global.cloud.vendor "azure" -}}
{{- printf "svc-gateway.plat-system.svc.cluster.local" -}}
{{- else -}} 
{{- if contains "web-app" .Values.app_type -}}
{{- printf "app-gateway.plat-system.svc.cluster.local" -}}
{{- else }}
{{- printf "svc-gateway.plat-system.svc.cluster.local" -}}
{{- end }}
{{- end }}
{{- end }}

{{/* Evaluate prefix value */}}
{{- define "phoenixmsp.virtualservice.prefix" }}
{{- if contains "web-app" .Values.app_type -}}
{{- printf "/" -}}
{{- else }}
{{- printf "/services/" -}}{{ include "phoenixmsp.fullname" . }}{{- printf "/" -}}
{{- end }}
{{- end }}


{{/*Adding more routes*/}}
{{- define "phoenixmsp.virtualservice.http.routes" }}
{{- range $key,$value := .Values.routes }}
- match:
  - uri:
  {{- if $value.prefix }}
      prefix: {{ $value.prefix }}
  {{- else }}
      prefix: {{ $value.uri }}
  {{- end }}
{{- if $value.morematchrules }}
{{ toYaml $value.morematchrules | indent 4 }}
{{- end }}
  rewrite:
    uri: {{ $value.uri }}       
  route:
  - destination:
      port:
        number: 80 
      host: {{ $value.host }}
{{- end }}
{{- end }}
