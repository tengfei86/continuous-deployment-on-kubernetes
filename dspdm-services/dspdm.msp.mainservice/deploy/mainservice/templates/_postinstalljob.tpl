{{- define "phoenix.post-install-job" }}
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "phoenixmsp.fullname" . }}-post-install-job
  labels:
    app: {{ include "phoenixmsp.fullname" . }}-post-install-job
  annotations:
    # This is what defines this resource as a hook. Without this line, the
    # job is considered part of the release.
    "helm.sh/hook": post-install
    "helm.sh/hook-delete-policy": hook-succeeded
spec:
  activeDeadlineSeconds: 60
  template:
    metadata:
      name: {{ include "phoenixmsp.fullname" . }}-post-install-job
      labels:
        app: {{ include "phoenixmsp.fullname" . }}-post-install-job
      annotations:
        sidecar.istio.io/inject: "false"
    spec:
      serviceAccountName: {{ .Release.Namespace }}
      restartPolicy: Never
      containers:
      - name: {{ include "phoenixmsp.fullname" . }}-post-install-job
        image: "{{ .Values.schema_image.repository }}:{{ .Values.schema_image.tag }}"
        args: ["post-install"]
        imagePullPolicy: {{ .Values.schema_image.pullPolicy }}
{{- end }}