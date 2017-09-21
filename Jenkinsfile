#!/usr/bin/env groovy

/*
 * This Jenkinsfile is intended to run on https://ci.evernym.com and may fail anywhere else.
 *
 * Environment requirements:
 *  - environment variable:
 *      - INDY_LINUX_AGENT_LANBEL: label for linux agent
 *  - agents:
 *      - linux: docker
 *      - windows: cygwin
 */

name = 'indy-plenum'


def buildDocker(imageName, dockerfile) {
    def uid = sh(returnStdout: true, script: 'id -u').trim()
    return docker.build("$imageName", "--build-arg uid=$uid -f $dockerfile")
}


def install(options=[pip: 'pip', isVEnv: false]) {
    sh "$options.pip install " + (options.isVEnv ? "--ignore-installed" : "") + " pytest"
    sh "$options.pip install ."
}


def withTestEnv(body) {
    echo 'Test: Checkout csm'
    checkout scm

    if (isUnix()) {
        echo 'Test: Build docker image'
        buildDocker("$name-test", "ci/ubuntu.dockerfile ci").inside('--network host') {
            echo 'Test: Install dependencies'
            install(pip: 'pip')
            body.call('python')
        }
    } else { // windows expected
        echo 'Test: Build virtualenv'
        def virtualEnvDir = ".venv"
        sh "virtualenv --system-site-packages $virtualEnvDir"

        echo 'Test: Install dependencies'
        install(pip: "$virtualEnvDir/Scripts/pip", isVenv: true)
        body.call("$virtualEnvDir/Scripts/python")
    }
}


def test(options=[
        resFile: 'test-result.txt',
        testDir: '.',
        python: 'python',
        useRunner: false,
        testOnlySlice: '1/1']) {
    try {
        if (options.useRunner) {
            sh "PYTHONASYNCIODEBUG='0' $options.python runner.py --pytest \"$options.python -m pytest\" --dir $options.testDir --output \"$options.resFile\" --test-only-slice \"$options.testOnlySlice\""
        } else {
            sh "$options.python -m pytest --junitxml=$options.resFile $options.testDir"
        }
    }
    finally {
        try {
            sh "ls -la $options.resFile"
        } catch (Exception ex) {
            // pass
        }

        if (options.useRunner) {
            archiveArtifacts allowEmptyArchive: true, artifacts: "$options.resFile"
        } else {
            junit "$options.resFile"
        }
    }
}


def staticCodeValidation() {
    try {
        echo 'Static code validation'
        checkout scm

        buildDocker('code-validation', 'ci/code-validation.dockerfile ci').inside {
            sh "python3 -m flake8"
        }
    }
    finally {
        echo 'Static code validation: Cleanup'
        step([$class: 'WsCleanup'])
    }
}


// PIPELINE

// 1. STATIC CODE VALIDATION
stage('Static code validation') {
    node('ubuntu') {
        staticCodeValidation()
    }
}

// 2. TESTING
def failFast = false
def labels = [env.INDY_LINUX_AGENT_LANBEL] // TODO enable windows

def tests = [
    stp: { python ->
        test([testDir: 'stp_raet', resFile: "test-result-stp-raet.${NODE_NAME}.xml", python: python])
        test([testDir: 'stp_zmq', resFile: "test-result-stp-zmq.${NODE_NAME}.xml", python: python])
    },
    ledger: { python ->
        test([testDir: 'common', resFile: "test-result-common.${NODE_NAME}.xml", python: python])
        test([testDir: 'ledger', resFile: "test-result-ledger.${NODE_NAME}.xml", python: python])
        test([testDir: 'state', resFile: "test-result-state.${NODE_NAME}.xml", python: python])
        test([testDir: 'storage', resFile: "test-result-storage.${NODE_NAME}.xml", python: python])
    },
    plenum1: { python ->
        test([
            resFile: "test-result-plenum-1.${NODE_NAME}.txt",
            testDir: 'plenum',
            python: python,
            useRunner: true,
            testOnlySlice: "1/3"]
        )
    },
    plenum2: { python ->
        test([
            resFile: "test-result-plenum-2.${NODE_NAME}.txt",
            testDir: 'plenum',
            python: python,
            useRunner: true,
            testOnlySlice: "2/3"]
        )
    },
    plenum3: { python ->
        test([
            resFile: "test-result-plenum-3.${NODE_NAME}.txt",
            testDir: 'plenum',
            python: python,
            useRunner: true,
            testOnlySlice: "3/3"]
        )
    }
].collect {k, v -> [k, v]}


def builds = [:]
for (i = 0; i < labels.size(); i++) {
    def label = labels[i]
    def descr = "${label}Test"

    for(j = 0; j < tests.size(); j++) {
        def part = tests[j][0]
        def testFn = tests[j][1]
        def currDescr = "${descr}-${part}"
        builds[(currDescr)] = {
            node(label) {
                stage(currDescr) {
                    try {
                        withTestEnv() { python ->
                            echo 'Test'
                            testFn(python)
                        }
                    }
                    finally {
                        echo 'Cleanup'
                        step([$class: 'WsCleanup'])
                    }
                }
            }
        }
    }
}

builds.failFast = failFast
parallel builds
