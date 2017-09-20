#!groovy

def name = 'indy-plenum'


def withEnv(body) {
    if (isUnix()) {
        echo 'Ubuntu Test: Build docker image'
        def uid = sh(returnStdout: true, script: 'id -u').trim()
        docker.build("$name-test", "--build-arg uid=$uid -f $file").inside('--network host') {
            body.call(python: 'python', pip: 'pip')
        }
    } else { // windows expected
        echo 'Ubuntu Test: Build virtualenv'
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



def run(options=[resFile: 'test-result.txt', testDir: '.', python: 'python', useRunner: false, testOnlySlice: '1/1']) {
    try {
        if (options.useRunner) {
            sh "PYTHONASYNCIODEBUG='0' $options.python ci/runner.py --pytest \"$options.python -m pytest\" --dir $options.testDir --output \"$options.resFile\" --test-only-slice \"$options.testOnlySlice\""
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

        withEnv() { python, pip, isVenv ->
            echo 'Ubuntu Test: Install dependencies'
            install(pip: pip, isVenv: isVenv)

            echo 'Ubuntu Test: Test'
            run([
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

def plenumTestUbuntuPart1 = {
    plenumTestUbuntu(1, 3)
}

def plenumTestUbuntuPart2 = {
    plenumTestUbuntu(2, 3)
}

def plenumTestUbuntuPart3 = {
    plenumTestUbuntu(3, 3)
}

def ledgerTestUbuntu = {
    try {
        echo 'Ubuntu Test: Checkout csm'
        checkout scm

        withEnv() { python, pip, isVenv ->
            echo 'Ubuntu Test: Install dependencies'
            install(pip: pip, isVenv: isVenv)

            echo 'Ubuntu Test: Test'
            run([testDir: 'common', resFile: "test-result-common.${NODE_NAME}.xml", python: python])
            run([testDir: 'ledger', resFile: "test-result-ledger.${NODE_NAME}.xml", python: python])
            run([testDir: 'state', resFile: "test-result-state.${NODE_NAME}.xml", python: python])
            run([testDir: 'storage', resFile: "test-result-storage.${NODE_NAME}.xml", python: python])
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
            run([testDir: 'stp_raet', resFile: "test-result-stp-raet.${NODE_NAME}.xml", python: python])
            run([testDir: 'stp_zmq', resFile: "test-result-stp-zmq.${NODE_NAME}.xml", python: python])
        }
    }
    finally {
        echo 'Ubuntu Test: Cleanup'
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

//def options = new TestAndPublishOptions()
//testAndPublish(name, [ubuntu: [plenum1: plenumTestUbuntuPart1, plenum2: plenumTestUbuntuPart2, plenum3: plenumTestUbuntuPart3,
//ledger: ledgerTestUbuntu,
//stp: stpTestUbuntu]], true, options, [ubuntu: buildDebUbuntu])
