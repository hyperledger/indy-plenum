#!/usr/bin/env groovy

/*
 * This Jenkinsfile is intended to run on https://ci.evernym.com and may fail anywhere else.
 *
 * Environment requirements:
 *  - TODO ... (linux, windows with cygwin)
 */


name = 'indy-plenum'


def getRepoDetails(githubUrl) {
    def pattern = /github.com\/([^\/]+)\/([^\/]+)(\/|\.git)/
    def matcher = (githubUrl =~ pattern)
    try {
        return [owner: matcher[0][1], repo: matcher[0][2]]
    } catch (Exception ex) {
        return  null
    }
}


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


def isTested(branch="$BRANCH_NAME",
         contexts=["continuous-integration/jenkins/pr-merge"],
         updated=7) {

    def found = false

    try {
        echo 'Is Tested: Checkout csm'
        checkout scm

        echo 'Is Tested: prepare tools and env'

        def sha = sh(returnStdout: true, script: "git rev-parse HEAD^{commit}").trim()
        def gitOriginUrl = sh(returnStdout: true, script: "git config --get remote.origin.url").trim()
        def repoDetails = getRepoDetails(gitOriginUrl)

        if (repoDetails == null) {
            return false
        }

        def _branch = branch ? "--branch $branch" : ""

        def _contexts = ""
        for (int i = 0; i < contexts.size(); i++) {
            _contexts += " --context " + contexts[i]
        }

        echo 'Is Tested: matching'
        withCredentials([string(credentialsId: 'evernym-github-machine-user-token', variable: 'token')]) {
            buildDocker('match-passed-pr', 'ci/pr.dockerfile ci').inside {
                def prUrl = sh(returnStdout: true, script: """
                    python3 ci/match-passed-PR.py $repoDetails.owner $repoDetails.repo $sha $token \
                        $_branch $_contexts --updated $updated --verbose
                """).trim()

                if (prUrl) {
                    echo "Is Tested: found matched PR: $prUrl"
                } else {
                    echo "Is Tested: no matched PR found"
                }

                found = !!prUrl
            }
        }
    }
    finally {
        echo 'Is Tested: Cleanup'
        step([$class: 'WsCleanup'])
    }

    return found
}


def tryAutomergePR(owner, reponame, pr_number,
        approveMessage="Approved",
        status_state="success",
        status_url="$RUN_DISPLAY_URL",
        status_descr="Success: This commit looks good for auto merge",
        status_context="continuous-integration/jenkins/pr-merge") {

    try {
        echo 'Automerge PR: Checkout csm'
        checkout scm

        echo 'Automerge PR: approving'
        withCredentials([string(credentialsId: 'evernym-github-machine-user-token', variable: 'token')]) {
            buildDocker('pr-automerge', 'ci/pr.dockerfile ci').inside {
                sh """
                    python3 ci/automerge.py $owner $reponame $pr_number $token --body \"$approveMessage\" \
                        --status-update \
                        --status-state \"$status_state\" \
                        --status-url \"$status_url\" \
                        --status-descr \"$status_descr\" \
                        --status-context \"$status_context\" \
                        --verbose
                """
            }
        }
    }
    finally {
        echo 'Automerge PR: Cleanup'
        step([$class: 'WsCleanup'])
    }
}



// PIPELINE

// 1. CHECK IF NOT PR AND ALREADY TESTED AS PR
def istested = false
if (env.CHANGE_ID == null) {
    try {
        stage("Is Tested") {
            node('ubuntu') {
                istested = isTested()
            }
        }
    } catch (Exception ex) {
        echo "$ex (isTested scope, ignored)"
    }
}


if (istested) {
    echo "${env.BRANCH_NAME}: skip code validation and testing as we are on previously tested merge commit"
} else {
    // 2. STATIC CODE VALIDATION
    stage('Static code validation') {
        node('ubuntu') {
            staticCodeValidation()
        }
    }

    // 3. TESTING
    def labels = ['ubuntu']
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

    def failFast = false
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
}

// 4. TRY TO AUTOMERGE (PRs only)
if (env.CHANGE_ID != null) {
    def repoDetails = getRepoDetails(env.CHANGE_URL)
    if (repoDetails != null) {
        try {
            stage("Automerge PR") {
                node('ubuntu') {
                    tryAutomergePR(repoDetails["owner"], repoDetails["repo"], env.CHANGE_ID)
                }
            }
        } catch (Exception ex) {
            echo "$ex (automerge scope, ignored)"
        }

    }
}
