{{- define "phoenix.disableauthentication" }}

apiVersion: "authentication.istio.io/v1alpha1"
kind: "Policy"
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
spec:
  targets:
  - name: {{ include "phoenixmsp.fullname" . }}
  origins:
  - jwt:
      issuer: "https://{{ .Values.global.cluster.hosts }}/auth/realms/{{ .Values.configmap.file.realm }}"
      jwksUri: "https://{{ .Values.global.cluster.hosts }}/auth/realms/{{ .Values.configmap.file.realm }}/protocol/openid-connect/certs"
      trigger_rules:
      - excluded_paths:
        - prefix: /
  principalBinding: USE_ORIGIN

{{- end }}    
