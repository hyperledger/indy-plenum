#!groovy​

stage('Test') {
    parallel 'ubuntu-test':{
        node('ubuntu') {
            stage('Ubuntu Test') {
                try {
                    echo 'Ubuntu Test: Checkout csm'
                    checkout scm

                    echo 'Ubuntu Test: Build docker image'
                    sh 'ln -sf ci/plenum-ubuntu.dockerfile Dockerfile'
                    def dockerContainers = sh(returnStdout: true, script: 'docker ps -a').trim()
                    echo "Existing docker containers: ${dockerContainers}"
                    if (dockerContainers.toLowerCase().contains('orientdb')) {
                        sh('docker start orientdb')
                    } else {
                        sh("docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=password -e ORIENTDB_OPTS_MEMORY=\"${env.ORIENTDB_OPTS_MEMORY}\" orientdb")
                    }

                    def testEnv = docker.build 'plenum-test'
                    
                    testEnv.inside('--network host') {
                        echo 'Ubuntu Test: Install dependencies'
                        sh 'cd /home/sovrin && virtualenv -p python3.5 test'
                        sh '/home/sovrin/test/bin/python setup.py install'
                        sh '/home/sovrin/test/bin/pip install pytest'

                        echo 'Ubuntu Test: Test'
                        sh '/home/sovrin/test/bin/python runner.py --pytest "/home/sovrin/test/bin/python -m pytest" --output "/home/sovrin/test-result.txt"'
                    }
                }
                finally {
                    echo 'Ubuntu Test: Cleanup'
                    sh "docker stop orientdb"
                    step([$class: 'WsCleanup'])
                }
            }
        }   
    }, 
    'windows-test':{
        echo 'TODO: Implement me'
    }
}

if (env.BRANCH_NAME != 'master' && env.BRANCH_NAME != 'stable') {
    echo "Plenum ${env.BRANCH_NAME}: skip publishing"
    return
}

stage('Publish to pypi') {
    node('ubuntu') {
        try {
            echo 'Publish to pypi: Checkout csm'
            checkout scm

            echo 'Publish to pypi: Prepare package'
            sh 'chmod -R 777 ci'
            sh 'ci/prepare-package.sh . $BUILD_NUMBER'

            echo 'Publish to pypi: Publish'
            withCredentials([file(credentialsId: 'pypi_credentials', variable: 'FILE')]) {
                sh 'ln -sf $FILE $HOME/.pypirc'
                sh 'ci/upload-pypi-package.sh .'
                sh 'rm -f $HOME/.pypirc'
            }
        }
        finally {
            echo 'Publish to pypi: Cleanup'
            step([$class: 'WsCleanup'])
        }
    }
}

stage('Build packages') {
    parallel 'ubuntu-build':{
        node('ubuntu') {
            stage('Build deb packages') {
                try {
                    echo 'Build deb packages: Checkout csm'
                    checkout scm

                    echo 'Build deb packages: Prepare package'
                    sh 'chmod -R 777 ci'
                    sh 'ci/prepare-package.sh . $BUILD_NUMBER'

                    echo 'Build deb packages: Build debs'
                    withCredentials([usernameColonPassword(credentialsId: 'evernym-githib-user', variable: 'USERPASS')]) {
                        sh 'git clone https://$USERPASS@github.com/evernym/sovrin-packaging.git'
                    }
                    echo 'TODO: Implement me'
                    // sh ./sovrin-packaging/pack-ledger.sh $BUILD_NUMBER


                    echo 'Build deb packages: Publish debs'
                    echo 'TODO: Implement me'
                    // sh ./sovrin-packaging/upload-build.sh $BUILD_NUMBER
                }
                finally {
                    echo 'Build deb packages: Cleanup'
                        step([$class: 'WsCleanup'])
                    }
                }
        }
    },
    'windows-build':{
        stage('Build msi packages') {
            echo 'TODO: Implement me'
        }
    }
}

stage('System tests') {
    echo 'TODO: Implement me'
}

if (env.BRANCH_NAME != 'stable') {
    return
}

stage('QA notification') {
    emailext (
        subject: "New release candidate '${JOB_NAME}' (${BUILD_NUMBER}) is waiting for input",
        body: "Please go to ${BUILD_URL} and verify the build",
        to: 'alexander.sherbakov@dsr-company.com'
    )
}

def qaApproval
stage('QA approval') {
    try {
        input(message: 'Do you want to publish this package?')
        qaApproval = true
        echo 'QA approval granted'
    }
    catch (Exception err) {
        qaApproval = false
        echo 'QA approval denied'
    }
}
if (!qaApproval) {
    return
}

stage('Release packages') {
    echo 'TODO: Implement me'
}

stage('System tests') {
    echo 'TODO: Implement me'
}
