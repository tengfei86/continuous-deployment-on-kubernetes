{{- define "phoenix.configmapfromfile" }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
data:
  {{- if .Values.configmap.file.isauth -}}
  {{- if eq .Values.configmap.file.isauth true }}
  keycloak.json: |
    {
      "realm": "{{ .Values.keycloak.realm }}",
      "auth-server-url": "https://{{ .Values.global.cluster.hosts }}/auth",
      "ssl-required": "none",
      "resource": "{{ .Values.keycloak.resource }}",
      "public-client": true,
      "confidential-port": 0
    }
  {{- end -}}
  {{- end -}}
  {{- if .Values.configmap.file.path }}
  {{- (.Files.Glob .Values.configmap.file.path).AsConfig | nindent 2 }}
  {{- end -}}
{{- end -}}