#!groovy

name = 'indy-plenum'
file='ci/ubuntu.dockerfile ci'


def withEnv(body) {
    echo "$name"
    if (isUnix()) {
        echo 'Ubuntu Test: Build docker image'
        def uid = sh(returnStdout: true, script: 'id -u').trim()
        docker.build("$name-test", "--build-arg uid=$uid -f $file").inside('--network host') {
            body.call('python', 'pip')
        }
    } else { // windows expected
        echo 'Ubuntu Test: Build virtualenv'
        def virtualEnvDir = "venv"
        sh "virtualenv --system-site-packages $virtualEnvDir"
        body.call("$virtualEnvDir/Scripts/python", "$virtualEnvDir/Scripts/pip")
    }
}


def install(options=[deps: [], pip: 'pip', isVEnv: false]) {
    for (def dep : options.deps) {
        sh "$options.pip install " + (options.isVEnv ? "-U" : "") + " $dep"
    }
    sh "$options.pip install " + (options.isVEnv ? "--ignore-installed" : "") + " pytest"
    sh "$options.pip install ."
}


def test(options=[resFile: 'test-result.txt', testDir: '.', python: 'python', useRunner: false, testOnlySlice: '1/1']) {
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


def plenumTestUbuntu = { offset, increment ->
    try {
        echo 'Ubuntu Test: Checkout csm'
        checkout scm

        withEnv { python, pip, isVenv ->
            echo 'Ubuntu Test: Install dependencies'
            install(pip: pip, isVenv: isVenv)

            echo 'Ubuntu Test: Test'
            test([
                resFile: "test-result-plenum-$offset.${NODE_NAME}.txt",
                testDir: 'plenum',
                python: python,
                useRunner: true,
                testOnlySlice: "$offset/$increment"]
            )
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

        withEnv() { python, pip, isVenv ->
            echo 'Ubuntu Test: Install dependencies'
            install(pip: pip, isVenv: isVenv)

            echo 'Ubuntu Test: Test'
            test([testDir: 'common', resFile: "test-result-common.${NODE_NAME}.xml", python: python])
            test([testDir: 'ledger', resFile: "test-result-ledger.${NODE_NAME}.xml", python: python])
            test([testDir: 'state', resFile: "test-result-state.${NODE_NAME}.xml", python: python])
            test([testDir: 'storage', resFile: "test-result-storage.${NODE_NAME}.xml", python: python])
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

        withEnv() { python, pip, isVenv ->
            echo 'Ubuntu Test: Install dependencies'
            install(pip: pip, isVenv: isVenv)

            echo 'Ubuntu Test: Test'
            test([testDir: 'stp_raet', resFile: "test-result-stp-raet.${NODE_NAME}.xml", python: python])
            test([testDir: 'stp_zmq', resFile: "test-result-stp-zmq.${NODE_NAME}.xml", python: python])
        }
    }
    finally {
        echo 'Ubuntu Test: Cleanup'
        step([$class: 'WsCleanup'])
    }
}


def failFast = false
def labels = ['ubuntu']
def tests = [
    stp: stpTestUbuntu,
    ledger: ledgerTestUbuntu,
    plenum1: {
        plenumTestUbuntu(1, 3)
    },
    plenum2: {
        plenumTestUbuntu(1, 3)
    },
    plenum3: {
        plenumTestUbuntu(1, 3)
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
                    testFn()
                }
            }
        }
    }
}

builds.failFast = failFast
parallel builds
