pipeline {

  environment {
    PROJECT = "osdu-upstream"
    APP_NAME = "dspdm"
    FE_SVC_NAME = "${APP_NAME}-frontend"
    CLUSTER = "cluster-2"
    CLUSTER_ZONE = "us-central1-c"
    IMAGE_TAG = "gcr.io/${PROJECT}/${APP_NAME}:${env.BUILD_NUMBER}"
    //IMAGE_TAG = "gcr.io/${PROJECT}/${APP_NAME}:48"
    JENKINS_CRED = "${PROJECT}"
    BRANCH_NAME = "production"
  }

  agent {
    kubernetes {
      label 'dspdm-services'
      defaultContainer 'jnlp'
      yaml """
apiVersion: v1
kind: Pod
metadata:
labels:
  component: ci
spec:
  # Use service account that can deploy to all namespaces
  serviceAccountName: cd-jenkins
  containers:
  - name: maven
    image: maven:3.8.6-jdk-11
    command:
    - cat
    tty: true
  - name: gcloud
    image: gcr.io/cloud-builders/gcloud
    command:
    - cat
    tty: true
  - name: kubectl
    image: gcr.io/cloud-builders/kubectl
    command:
    - cat
    tty: true
"""
}
  }
  stages {
    stage("check out dspdm opengroup  project code") {
           steps {
             dir("dspdm-services") {
               git branch: 'main', credentialsId: 'tong_opengroup', url: 'https://community.opengroup.org/osdu/platform/domain-data-mgmt-services/production/core/dspdm-services.git'
             }
           }
       }

    stage('Build') {
      steps {
        container('maven') {
          sh """
            mount
            mkdir -p /java/src
            ls . -l
            ln -s `pwd`/dspdm-services /java/src/dspdm-services
            cd /java/src/dspdm-services
            sed -i "s~iIx8d3B6tjz3MUTHxsmB6w==~pmjdECSFG1cEJaPr7I2rew==~g;s~localhost~postgres-devops.plat-system~g" ./src/main/resources/tiger4/localhost/connection.properties
            mvn clean package -Dmaven.test.skip=true 
            ls . -l
          """
        }
      }
    }
    stage('Build and push image with Container Builder') {
	    steps {
		    container('gcloud') {
			    withCredentials([file(credentialsId: 'jenkins-sa', variable: 'GC_KEY')]) {
				    sh("gcloud auth activate-service-account --key-file=${GC_KEY}")
		                    sh("gcloud container clusters get-credentials cluster-2 --zone us-central1-c --project ${PROJECT}")
			            sh """
				       ls . -l
                                       rm -rf .gitignore
				       PYTHONUNBUFFERED=1 gcloud builds submit -t ${IMAGE_TAG} . 
			            """

			    }
		    }
	    }
    }
    stage('Deploy Production') {
      steps{
        container('kubectl') {
          sh("sed -i 's#{IMAGE_TAG}#${IMAGE_TAG}#' deploy/*.yaml")
          sh("cat deploy/*.yaml")
          sh("kubectl delete -f ./deploy/backend-production.yaml -n plat-system")
          sh("kubectl apply -f deploy -n plat-system")
        }
      }
    }
    
  }
}
