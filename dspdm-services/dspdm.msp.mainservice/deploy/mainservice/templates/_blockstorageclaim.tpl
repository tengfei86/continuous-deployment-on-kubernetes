{{- define "phoenix.blockstorageclaim" }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ include "phoenixmsp.blockstoragename" . }}
spec:
  accessModes:
    - ReadWriteOnce
{{- if (eq .Values.global.cloud.vendor "azure") }}
  storageClassName: default
{{- end }}
  resources:
    requests:
      storage: {{.Values.blockStorage.storage }}
{{- end }}