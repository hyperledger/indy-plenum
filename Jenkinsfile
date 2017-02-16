echo 'Plenum build...'

stage('Ubuntu testing') {
    node {
        stage('Checkout csm') {
            echo 'Checkout csm...'
            checkout scm
            echo 'Checkout csm: done'
        }

        docker.image('python:3.5.3').inside {
            stage('Install deps') {
                echo 'Install deps...'
                //sh 'python setup.py install' 
                echo 'Install deps: done'
            }
            
            stage('Test') {
                echo 'Testing...'
                //sh 'python setup.py pytest' 
                echo 'Testesting: done'
            }
        }

        stage('Cleanup') {
            echo 'Cleanup workspace...'
            step([$class: 'WsCleanup'])
            echo 'Cleanup workspace: done'
        }
    }
}

stage('Publish artifacts') {
    node {
        stage('Checkout csm') {
            echo 'Checkout csm...'
            checkout scm
            echo 'Checkout csm: done'
        }
        
        stage('Publish pipy') {
            echo 'Publish to pipy...'
            //sh './publish_pipy.sh' 
            echo 'Publish pipy: done'
        }

        stage('Publish debs') {
            echo 'Publish to pipy...'
            //sh './publish_debs.sh' 
            echo 'Publish to pipy: done'
        }

        stage('Cleanup') {
            echo 'Cleanup workspace...'
            step([$class: 'WsCleanup'])
            echo 'Cleanup workspace: done'
        }
    }
}

echo 'Plenum build: done'
