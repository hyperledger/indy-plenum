#!groovy​

@Library('SovrinHelpers') _

def name = 'plenum'

def testUbuntu = {
    try {
        echo 'Ubuntu Test: Checkout csm'
        checkout scm

        echo 'Ubuntu Test: Build docker image'
        orientdb.start()

        def testEnv = dockerHelpers.build(name)

        testEnv.inside('--network host') {
            echo 'Ubuntu Test: Install dependencies'
            testHelpers.installDeps()

            echo 'Ubuntu Test: Test'
            /* try {
                sh 'python runner.py --output "/home/sovrin/test-result.txt"'
            }
            finally {
                archiveArtifacts artifacts: '/home/sovrin/test-result.txt'
            }*/
            // Run only orientdb test for POC purposes
            try {
                sh 'python -m pytest -k orientdb --junitxml=test-result.xml'
            }
            finally {
                junit 'test-result.xml'
            }
        }
    }
    finally {
        echo 'Ubuntu Test: Cleanup'
        orientdb.stop()
        step([$class: 'WsCleanup'])
    }
}

def testWindows = {
    echo 'TODO: Implement me'
}

def testWindowsNoDocker = {
    echo 'TODO: Implement me'
}



testAndPublish(name, [ubuntu: testUbuntu, windows: testWindows, windowsNoDocker: testWindowsNoDocker])