#!groovy

@Library('SovrinHelpers') _

def name = 'indy-plenum'

def plenumTestUbuntu = {
    try {
        echo 'Ubuntu Test: Checkout csm'
        checkout scm

        echo 'Ubuntu Test: Build docker image'
        def testEnv = dockerHelpers.build(name)

        testEnv.inside('--network host') {
            echo 'Ubuntu Test: Install dependencies'
            testHelpers.install()

            echo 'Ubuntu Test: Test'
            testHelpers.testRunner([resFile: "test-result-plenum.${NODE_NAME}.txt", testDir: 'plenum'])
        }
    }
    finally {
        echo 'Ubuntu Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def ledgerTestUbuntu = {
    try {
        echo 'Ubuntu Test: Checkout csm'
        checkout scm

        echo 'Ubuntu Test: Build docker image'
        def testEnv = dockerHelpers.build(name)

        testEnv.inside {
            echo 'Ubuntu Test: Install dependencies'
            testHelpers.install()

            echo 'Ubuntu Test: Test'
            testHelpers.testJUnit([testDir: 'ledger', resFile: "test-result-legder.${NODE_NAME}.xml"])
        }
    }
    finally {
        echo 'Ubuntu Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def stateTestUbuntu = {
    try {
        echo 'Ubuntu Test: Checkout csm'
        checkout scm

        echo 'Ubuntu Test: Build docker image'
        def testEnv = dockerHelpers.build(name)

        testEnv.inside {
            echo 'Ubuntu Test: Install dependencies'
            testHelpers.install()

            echo 'Ubuntu Test: Test'
            testHelpers.testJUnit([testDir: 'state', resFile: "test-result-state.${NODE_NAME}.xml"])
        }
    }
    finally {
        echo 'Ubuntu Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def stpTestUbuntu = {
    try {
        echo 'Ubuntu Test: Checkout csm'
        checkout scm

        echo 'Ubuntu Test: Build docker image'
        def testEnv = dockerHelpers.build(name)

        testEnv.inside {
            echo 'Ubuntu Test: Install dependencies'
            testHelpers.install()

            echo 'Ubuntu Test: Test'
            testHelpers.testJUnit([testDir: 'stp_raet', resFile: "test-result-stp-raet.${NODE_NAME}.xml"])
            testHelpers.testJUnit([testDir: 'stp_zmq', resFile: "test-result-stp-zmq.${NODE_NAME}.xml"])
        }
    }
    finally {
        echo 'Ubuntu Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def plenumTestWindows = {
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

def ledgerTestWindows = {
    try {
        echo 'Windows Test: Checkout csm'
        checkout scm

        echo 'Windows Test: Build docker image'
        dockerHelpers.buildAndRunWindows(name, testHelpers.installDepsWindowsCommands() + testHelpers.testJunitWindowsCommands())
        junit 'test-result.xml'
    }
    finally {
        echo 'Windows Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def stateTestWindows = {
    try {
        echo 'Windows Test: Checkout csm'
        checkout scm

        echo 'Windows Test: Build docker image'
        dockerHelpers.buildAndRunWindows(name, testHelpers.installDepsWindowsCommands() + testHelpers.testJunitWindowsCommands())
        junit 'test-result.xml'
    }
    finally {
        echo 'Windows Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def plenumTestWindowsNoDocker = {
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

def ledgerTestWindowsNoDocker = {
    try {
        echo 'Windows No Docker Test: Checkout csm'
        checkout scm   

        testHelpers.createVirtualEnvAndExecute({ python, pip ->
            echo 'Windows No Docker Test: Install dependencies'
            testHelpers.installDepsBat(python, pip)
            
            echo 'Windows No Docker Test: Test'
            testHelpers.testJunitBat(python, pip)
        })
    }
    finally {
        echo 'Windows No Docker Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def stateTestWindowsNoDocker = {
    try {
        echo 'Windows No Docker Test: Checkout csm'
        checkout scm   

        testHelpers.createVirtualEnvAndExecute({ python, pip ->
            echo 'Windows No Docker Test: Install dependencies'
            testHelpers.installDepsBat(python, pip)
            
            echo 'Windows No Docker Test: Test'
            testHelpers.testJunitBat(python, pip)
        })
    }
    finally {
        echo 'Windows No Docker Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}

def buildDebUbuntu = { repoName, releaseVersion, sourcePath ->
    def volumeName = "$name-deb-u1604"
    sh "docker volume rm -f $volumeName"
    dir('build-scripts/ubuntu-1604') {
        sh "./build-$name-docker.sh $sourcePath $releaseVersion"
        sh "./build-3rd-parties-docker.sh"
    }
    return "$volumeName"
}

def options = new TestAndPublishOptions()
testAndPublish(name, [ubuntu: [plenum: plenumTestUbuntu, ledger: ledgerTestUbuntu, state: stateTestUbuntu, stp: stpTestUbuntu]], true, options, [ubuntu: buildDebUbuntu])
