{{- define "phoenix.filestorageclaim" }}
{{- if (.Values.fileStorage) }}
{{- if and (.Values.fileStorage.type) (eq .Values.fileStorage.type "dynamic") }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ include "phoenixmsp.filestoragename" . }}
spec:
  accessModes:
    - ReadWriteMany
  {{- if (eq .Values.global.cloud.vendor "aws") }}
  storageClassName: aws-efs
  {{- else }}
  storageClassName: azurefile
  {{- end }}
  resources:
    requests:
      storage: {{ .Values.fileStorage.storage }}
{{- end }}
{{- end }}
{{- end }}