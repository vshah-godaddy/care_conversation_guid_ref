
pipeline {
    agent any

    options {
        buildDiscarder(
            logRotator(
                numToKeepStr: "30"
            )
        )
    }

    environment {
        GIT_ORG_NAME        = 'GDLakeDataProcessors'
        GIT_REPO_NAME       = 'care_conversation_guid_ref'
        GODADDY_ORG         = 'godaddy.okta.com'
        RUN_DATE            = sh(returnStdout: true, script: 'TZ=UTC date +%Y-%m-%dT%H:%M:%S').trim()
        BACKUP_TAG          = "build_date=${env.RUN_DATE}/build_num=${env.BUILD_NUMBER}"
    }

    stages {

        stage('Prepare Workspace') {
            steps {
                sh returnStdout: true, script: """
                   set -eo pipefail;
                   bash ./setup.sh;
                   """
            }
        }

        stage('Validate Creds') {
            steps {
                withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: get_credentials_id_for_env('dev-private')]]) {
                    sh """
                    set -eo pipefail;
                    source .venv/bin/activate;
                    aws sts get-caller-identity
                    deactivate
                    """
                }
            }
        }

        stage('Deploy dev-private') {
            when {
                beforeInput true
                expression { return env.GIT_BRANCH == 'origin/development' || env.GIT_BRANCH == 'remote/origin/development' }
            }
            steps {
                deploy_code('dev-private')
                script {env.DEV_DEPLOYED = true}
            }
        }

        stage('Validate dev-private') {
            when { expression { env.DEV_DEPLOYED } }
            steps {
                validate_deployment('dev-private')
            }
        }

        stage('Deploy stage') {
            steps {
                deploy_code('stage')
            }
        }

        stage('Validate stage') {
            steps {
                validate_deployment('stage')
            }
        }

        // Prod deployment is done for master branch only
        stage('Deploy prod') {
            when {
                beforeInput true
                expression { return env.GIT_BRANCH == 'origin/master' || env.GIT_BRANCH == 'remote/origin/master' }
            }
            input {
                message "Verify stage and confirm prod deployment by approving this."
                ok "Approve"
            }
            steps {
                deploy_code('prod')
                script {env.PROD_DEPLOYED = true}
            }
        }

        stage('Validate prod') {
            when { expression { env.PROD_DEPLOYED } }
            steps {
                validate_deployment('prod')
            }
        }
    }
}

def abort_pipeline(String error_message) {
    currentBuild.result = 'ABORTED'
    error(error_message)
}


String get_credentials_id_for_env(String aws_env) {
    String credentials_id = ''
    switch(aws_env) {
        case 'dev-private':
            credentials_id = 'ckp-etl-deploy-user-dev-private-creds'
            break
        case 'stage':
            credentials_id = 'ckp-etl-deploy-user-stage-creds'
            break
        case 'prod':
            credentials_id = 'ckp-etl-deploy-user-prod-creds'
            break
        default:
            abort_pipeline("Invalid AWS env: ${aws_env}. Valid values are dev-private, stage and prod.")
    }
    return credentials_id
}


String get_code_loc_for_env(String aws_env) {
    return "s3://gd-ckpetlbatch-${aws_env}-code/${env.GIT_ORG_NAME}/${env.GIT_REPO_NAME}"
}


String get_backup_code_loc_for_env(String aws_env) {
    return "s3://gd-ckpetlbatch-${aws_env}-code/${env.GIT_ORG_NAME}History/${env.GIT_REPO_NAME}/${env.BACKUP_TAG}"
}


def deploy_code(String aws_env) {
    String code_loc = get_code_loc_for_env(aws_env)
    String backup_code_loc = get_backup_code_loc_for_env(aws_env)
    withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: get_credentials_id_for_env(aws_env)]]) {
        sh """
        set -eo pipefail;
        source .venv/bin/activate;
        # backup existing code
        aws s3 sync ${code_loc} ${backup_code_loc} --delete
        # deploy new code
        sh -x ./makefile.sh
        aws s3 sync . ${code_loc} --delete --exclude ".venv/*" --exclude ".git/*"
        deactivate
        """
    }
}


def validate_deployment(String aws_env) {
    String code_loc = get_code_loc_for_env(aws_env)
    withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: get_credentials_id_for_env(aws_env)]]) {
        sh """
        set -eo pipefail;
        source .venv/bin/activate;
        # sync should return no data
        fc=`aws s3 sync . ${code_loc} --delete --exclude ".venv/*" --exclude ".git/*" --dryrun | wc -l`
        deactivate
        if [ \$fc -ne 0 ]; then
           echo "Deployed files don't match git!! Failing build."
           exit 1
        fi;
        """
    }
}