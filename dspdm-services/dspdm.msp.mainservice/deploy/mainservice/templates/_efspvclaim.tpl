{{- define "phoenix.efspvclaim" }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ include "phoenixmsp.fullname" . }}-efs
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: aws-efs
  resources:
    requests:
      storage: {{ .Values.efs.storage }}
{{- end }}