{{- define "phoenix.configmapfromliteral" }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "phoenixmsp.configmap.env" . }}
data:
{{ toYaml .Values.configmap.env.data | indent 2 }}
{{- end }}