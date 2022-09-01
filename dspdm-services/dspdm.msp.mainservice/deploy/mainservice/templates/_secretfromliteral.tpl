{{- define "phoenix.secretfromliteral" }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "phoenixmsp.fullname" . }}-envvar
data:
{{ toYaml .Values.secret.env | indent 2 }}
{{- end }}