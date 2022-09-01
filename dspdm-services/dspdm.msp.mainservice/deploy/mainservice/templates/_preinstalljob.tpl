{{- define "phoenix.pre-install-job" }}
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "phoenixmsp.fullname" . }}-pre-install-job
  labels:
    app: {{ include "phoenixmsp.fullname" . }}-pre-install-job
  annotations:
    # This is what defines this resource as a hook. Without this line, the
    # job is considered part of the release.
    "helm.sh/hook": pre-install
    "helm.sh/hook-delete-policy": hook-succeeded
spec:
  activeDeadlineSeconds: 60
  template:
    metadata:
      name: {{ include "phoenixmsp.fullname" . }}-pre-install-job
      labels:
        app: {{ include "phoenixmsp.fullname" . }}-pre-install-job
      annotations:
        sidecar.istio.io/inject: "false"
    spec:
      serviceAccountName: {{ .Release.Namespace }}
      restartPolicy: Never
      containers:
      - name: {{ include "phoenixmsp.fullname" . }}-pre-install-job
        image: "{{ .Values.schema_image.repository }}:{{ .Values.schema_image.tag }}"
        env:
          {{- if .Values.env}}
{{ toYaml .Values.env | indent 10}}
          {{- end }}
          {{- range $key,$value := .Values.global.env }}
          - name: {{ $key }}
            value: {{ $value | quote }}
          {{- end }}
        args: [ "pre-install",{{ .Values.global.cluster.hosts }},{{ .Values.keycloak.resource }},{{  include "phoenixmsp.fullname" . }},{{ .Values.keycloak.realm }}]
        imagePullPolicy: {{ .Values.schema_image.pullPolicy }}
{{- end }}