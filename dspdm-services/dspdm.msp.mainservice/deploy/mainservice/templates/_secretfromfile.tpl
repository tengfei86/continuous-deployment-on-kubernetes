{{- define "phoenix.secretfromfile" }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
data:
  {{- ( .Files.Glob .Values.secret.file.path ).AsConfig | nindent 2 }}
{{- end }}