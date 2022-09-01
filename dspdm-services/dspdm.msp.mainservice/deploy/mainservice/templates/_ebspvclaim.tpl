{{- define "phoenix.ebspvclaim" }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ include "phoenixmsp.fullname" . }}-ebs
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: {{.Values.ebs.storage }}
{{- end }}