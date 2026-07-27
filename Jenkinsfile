pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    stages {
        stage('Build') {
            steps {
                sh './mvnw -B -DskipTests compile'
                dir('tf-console') {
                    sh 'npm ci'
                    sh 'npm run build'
                }
            }
        }
        stage('Test') {
            steps {
                sh "./mvnw -B -Dtest='!*IntegrationTest' test"
            }
        }
        stage('Coverage Gate') {
            steps {
                sh './mvnw -B -pl tf-common,tf-fusion -am verify'
            }
        }
        stage('Integration') {
            steps {
                sh "./mvnw -B -pl tf-service -am -Dtest='*IntegrationTest' -Dsurefire.failIfNoSpecifiedTests=false test"
            }
        }
        stage('Package') {
            steps {
                sh 'docker compose build'
            }
        }
    }

    post {
        always {
            junit allowEmptyResults: true, testResults: 'tf-*/target/surefire-reports/TEST-*.xml'
            archiveArtifacts allowEmptyArchive: true, artifacts: 'tf-*/target/site/jacoco/**'
        }
    }
}
