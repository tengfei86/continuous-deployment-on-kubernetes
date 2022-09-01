{{- define "phoenix.servicemonitor" }}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
  labels:
    app: {{ include "phoenixmsp.fullname" . }}
    app.kubernetes.io/instance: {{ .Release.Name }}
    release: prom
spec:
  {{- if .Values.servicemonitor.endpoints }}
  endpoints:
{{ toYaml .Values.servicemonitor.endpoints | indent 5 }}
  {{- end }}
  namespaceSelector:
    matchNames:
    - {{ .Release.Namespace }}
  selector:
    matchLabels:
      app: {{ include "phoenixmsp.fullname" . }}
      app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

