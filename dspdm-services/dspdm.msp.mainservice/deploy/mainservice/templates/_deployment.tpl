{{- define "phoenix.deployment"  }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
  labels:
    app: {{ include "phoenixmsp.fullname" . }}
    app.kubernetes.io/name: {{ include "phoenixmsp.name" . }}
    helm.sh/chart: {{ include "phoenixmsp.chart" . }}
    app.kubernetes.io/instance: {{ .Release.Name }}
    app.kubernetes.io/managed-by: {{ .Release.Service }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      app: {{ include "phoenixmsp.fullname" . }}
      app.kubernetes.io/name: {{ include "phoenixmsp.name" . }}
      app.kubernetes.io/instance: {{ .Release.Name }}
  template:
    metadata:
      labels:
        app: {{ include "phoenixmsp.fullname" . }}
        app.kubernetes.io/name: {{ include "phoenixmsp.name" . }}
        app.kubernetes.io/instance: {{ .Release.Name }}
    spec:
      serviceAccountName: {{ .Release.Namespace }}
      containers:
        - name: {{ .Chart.Name }}
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          ports:
            {{- range .Values.service.ports }}
            - name: {{ .name }}
              containerPort: {{ .containerPort }}
              protocol: {{ .protocol }}
            {{- end }}
          env:
            - name: MSP_SERVICE_URI
              value: /services/{{ include "phoenixmsp.fullname" . }}
           {{- range $key,$value := .Values.extraEnv }}
            - name: {{ $key }}
              value: {{ $value | quote }}
           {{- end }}   
          {{- if .Values.env}}
{{ toYaml .Values.env | indent 12}}
          {{- end }}
{{ include "phoenixmsp.newrelicenvs" . | indent 10 }}
          envFrom:
          - configMapRef:
              name: plat-config
          {{- if .Values.configmap.env }}
          - configMapRef:
              name: {{ include "phoenixmsp.configmap.env" . }}
          {{- end }} 
          {{- if .Values.secret.env }}
          - secretRef:
              name: {{ include "phoenixmsp.secret.env" . }}
          {{- end }}
          {{- if .Values.livenessProbe }}
          livenessProbe:
{{ toYaml .Values.livenessProbe | indent 12 }}
          {{- end }}
          {{- if .Values.readinessProbe }}
          readinessProbe:
{{ toYaml .Values.readinessProbe | indent 12 }}
          {{- end }}
          command: {{ .Values.command }}
          args: {{ .Values.args }}
          resources:
{{ toYaml .Values.resources | indent 12 }}
        {{- if ( or ( or .Values.configmap.file .Values.blockStorage) ( or .Values.fileStorage .Values.secret.file)) }} 
          volumeMounts:
          {{- if .Values.fileStorage }}
            - name: filestorage-volume
              mountPath: {{ .Values.fileStorage.mountpath }}
          {{- end }}
          {{- if .Values.blockStorage }}
            - name: blockstorage-volume
              mountPath: {{ .Values.blockStorage.mountpath }}
          {{- end }}
          {{- if .Values.configmap.file }}
            - name: new-config-volume
              mountPath: {{ .Values.configmap.file.mountpath }}
              {{- if .Values.configmap.file.subpath }}
              subPath: {{ .Values.configmap.file.subpath }}
              {{- end -}}
          {{- end }}
          {{- if .Values.secret.file }}
            - name: new-secret-volume
              mountPath: {{ .Values.secret.file.mountpath  }}
          {{- end }}
      volumes:
      {{- if .Values.fileStorage }}
      # to configure PVC, Refer phoenix developer guide - section 4
        - name: filestorage-volume
      {{- if (eq .Values.fileStorage.type "static") }}
        {{- if (eq .Values.global.cloud.vendor "azure") }}
          azureFile:
            secretName: azurefile-sa-secret
            shareName: azurefile-shared
            readOnly: false
        {{- else if (eq .Values.global.cloud.vendor "aws") }}
          nfs:
          {{- if (.Values.filesStorage.nfspoint) }}
            path: {{ .Values.fileStorage.nfspoint }}
          {{- else }}
            path: /
          {{- end }}
          {{- if (.Values.fileStorage.nfsserver) }}
            server: {{ .Values.fileStorage.nfsserver }}
          {{- else }}
            server: efs-server.plat-system.svc.cluster.local
          {{- end }}
        {{- end }}
      {{- else }}
          persistentVolumeClaim:
            claimName: {{ include "phoenixmsp.filestoragename" . }}
      {{- end }}
    {{- end }}
    {{- if .Values.blockStorage }}
      # to configure PVC, Refer phoenix developer guide - section 4
      # To refer the available Persistet volume claim name
        - name: blockstorage-volume
          persistentVolumeClaim:
            claimName: {{ include "phoenixmsp.blockstoragename" . }}
    {{- end }}
    {{- if .Values.configmap.file }}
      # To refer the config map name to bind its content to volume
        - name: new-config-volume
          configMap:
            name: {{ include "phoenixmsp.configmap.file" . }}
      {{- end}}
      {{- if .Values.secret.file }}
      # To refer the config map name to bind its content to volume
        - name: new-secret-volume
          secret:
            secretName: {{ include "phoenixmsp.secret.file" . }}
    {{- end }} 
    {{- end }}
{{- end}}
