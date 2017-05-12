#!groovy

@Library('SovrinHelpers') _

def name = 'plenum'

def testUbuntu = {
    try {
        echo 'Ubuntu Test: Checkout csm'
        checkout scm

        echo 'Ubuntu Test: Build docker image'
        def testEnv = dockerHelpers.build(name)

        testEnv.inside('--network host') {
            echo 'Ubuntu Test: Install dependencies'
            testHelpers.install()

            echo 'Ubuntu Test: Test'
            testHelpers.testRunner(resFile: "test-result.${NODE_NAME}.txt")
        }
    }
    finally {
        echo 'Ubuntu Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def testWindows = {
    echo 'TODO: Implement me'

    /* win2016 for now (03-23-2017) is not supported by Docker for Windows
     * (Hyper-V version), so we can't use linux containers
     * https://github.com/docker/for-win/issues/448#issuecomment-276328342
     *
     * possible solutions:
     *  - use host-installed OrientDB (trying this one)
     *  - wait until Docker support will be provided for win2016
     */

    //try {
    //    echo 'Windows Test: Checkout csm'
    //    checkout scm

    //    echo 'Windows Test: Build docker image'
    //    dockerHelpers.buildAndRunWindows(name, testHelpers.installDepsWindowsCommands() + ["cd C:\\test && python -m pytest -k orientdb --junit-xml=C:\\testOrig\\$testFile"] /*testHelpers.testJunitWindowsCommands()*/)
    //    junit 'test-result.xml'
    //}
    //finally {
    //    echo 'Windows Test: Cleanup'
    //    step([$class: 'WsCleanup'])
    //}
}

def testWindowsNoDocker = {
    try {
        echo 'Windows No Docker Test: Checkout csm'
        checkout scm

        testHelpers.createVirtualEnvAndExecute({ python, pip ->
            echo 'Windows No Docker Test: Install dependencies'
            testHelpers.install(python: python, pip: pip, isVEnv: true)
            
            echo 'Windows No Docker Test: Test'
            testHelpers.testRunner(resFile: "test-result.${NODE_NAME}.txt", python: python)
        })
    }
    finally {
        echo 'Windows No Docker Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

testAndPublish(name, [ubuntu: testUbuntu])

