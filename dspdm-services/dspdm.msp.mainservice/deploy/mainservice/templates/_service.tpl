{{- define "phoenix.service" }}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
  annotations: {
  {{- range $key,$value := .Values.service.annotations }}
     {{ $key }}: {{ $value | quote }},
  {{- end }}
     app_type: "{{ .Values.app_type }}"
  }
  labels:
    app: {{ include "phoenixmsp.fullname" . }} 
    app.kubernetes.io/name: {{ include "phoenixmsp.name" . }}
    helm.sh/chart: {{ include "phoenixmsp.chart" . }}
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/managed-by: {{ .Release.Service }}
    {{- if .Values.service.labels }} 
{{ toYaml .Values.service.labels | indent 4 }}   
    {{- end }}
spec:
  type: {{ .Values.service.type }}
  ports:
    {{- range .Values.service.ports }}
    - name: {{ .name }}
      port: {{ .port }}
      targetPort: {{ .containerPort }}
      protocol: {{ .protocol }}
    {{- end }}
  selector:
    app: {{ include "phoenixmsp.fullname" . }}
    app.kubernetes.io/name: {{ include "phoenixmsp.name" . }}
    app.kubernetes.io/instance: {{ .Release.Name }} 
{{- end }}
