import os
import re
import sys
import argparse

import time


def run(pytest, output_file, repeatUntilFailure):
    if repeatUntilFailure:
        log("'repeatUntilFailure' is set")
        log("Is going to repeat the test suite until failure")
    log("Preparing test suite with {}".format(pytest))
    testListFile = "test_list.txt"
    os.system('{} --collect-only > {}'.format(pytest, testListFile))
    log("Reading collected modules file")
    collectedData = open(testListFile).read()
    os.remove(testListFile)
    log("Collecting modules")
    testList = re.findall("<Module '(.+)'>", collectedData)
    log("Found {} test modules".format(len(testList)))
    if not testList:
        m = re.search("errors during collection", collectedData)
        if m:
            log(collectedData)
            return -1
    retVal = 0
    totalPassed = 0
    totalFailed = 0
    totalSkipped = 0
    totalErros = 0
    runsCount = 0
    allFailedTests = []
    allErrorTests = []
    failureData = []
    testRep = 'currentTestReport.txt'
    passPat = re.compile(".* ([0-9]+) passed.*$")
    skipPat = re.compile(".* ([0-9]+) skipped.*$")
    failPat = re.compile(".* ([0-9]+) failed.*$")
    errPat = re.compile(".* ([0-9]+) error.*$")
    failedTestPat = re.compile('____ (test.+) ____')
    errorTestPat = re.compile('____ (ERROR.+) ____')
    while True:
        for i, test in enumerate(testList):
            # testRep = '{}.rep'.format(test.split("/")[-1])
            log("Going to run {}".format(test))
            testStartTime = time.time()
            r = os.system('{} -k "{}" > {}'.format(pytest, test, testRep))
            testExecutionTime = time.time() - testStartTime
            reportLines = open(testRep).readlines()
            output = ''.join(reportLines)
            pas = passPat.search(output)
            passed = int(pas.groups()[0]) if pas else 0
            skp = skipPat.search(output)
            skipped = int(skp.groups()[0]) if skp else 0
            if r:
                fai = failPat.search(output)
                err = errPat.search(output)
                if not (fai or err):
                    log("Non zero return value from {} run but no failures "
                        "or errors reported".format(test))
                    log(output)
                    return -1
                failed = int(fai.groups()[0]) if fai else 0
                errors = int(err.groups()[0]) if err else 0
                failedNames = []
                errorNames = []
                startedF = None
                startedE = None
                for line in reportLines:
                    if '= FAILURES =' in line:
                        startedF = True
                        startedE = None
                        continue
                    if '= ERRORS =' in line:
                        startedF = None
                        startedE = True
                        continue
                    if startedF:
                        failureData.append(line)
                        m = failedTestPat.search(line)
                        if m:
                            failedNames.append(m.groups()[0])
                    if startedE:
                        failureData.append(line)
                        m = errorTestPat.search(line)
                        if m:
                            errorNames.append(m.groups()[0])
            else:
                failed = 0
                errors = 0
            log('In {}, {} passed, {} failed, {} errors, {} skipped, {:.1f}s time '
                '({}/{} progress)'.
                format(test, passed, errors, failed, skipped,
                       testExecutionTime, i+1, len(testList)))
            if failed:
                logError("Failed tests: {}".format(', '.join(failedNames)))
                for nm in failedNames:
                    allFailedTests.append((test, nm))
            if errors:
                logError("Error in tests: {}".format(', '.join(errorNames)))
                for nm in errorNames:
                    allErrorTests.append((test, nm))
            retVal += r
            totalPassed += passed
            totalFailed += failed
            totalErros += errors
            totalSkipped += skipped
        runsCount += 1

        if repeatUntilFailure:
            if totalFailed or totalErros:
                break  # repeatUntilFailure set and failures happened
            else:
                logSuccess('Run #{} was successful'.format(runsCount))
                log('\n\n')

        else:
            break  # just one run

    summaryMsg = 'Total {} runs {} passed, {} failed, {} errors, {} skipped'.\
        format(runsCount, totalPassed, totalFailed, totalErros, totalSkipped)
    log(summaryMsg)

    if totalFailed:
        log("Failed tests:")
        for fm, fn in allFailedTests:
            log('{}:{}'.format(fm, fn))

    if totalErros:
        log("Error in tests:")
        for fm, fn in allErrorTests:
            log('{}:{}'.format(fm, fn))

    if failureData and output_file:
        log("Writing failure data in Test-Report.txt")
        with open(output_file, 'w') as f:
            f.write(summaryMsg)
            f.write(''.join(failureData))

    if os.path.exists(testRep):
        os.remove(testRep)

    log("Tests run. Returning {}".format(retVal))
    return retVal


def log(msg):
    return print(msg, flush=True)


def logError(msg):
    return print('\x1b[0;30;41m' + msg + '\x1b[0m', flush=True)


def logSuccess(msg):
    return print('\x1b[6;30;42m' + msg + '\x1b[0m')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pytest', type=str,
                        help='pytest instance', default='python -m pytest')
    parser.add_argument('--output', type=str,
                        help='result file', default='../Test-Report.txt')
    parser.add_argument('--nooutput',
                        help='no result file', action="store_true")
    parser.add_argument('--repeat', dest='repeatUntilFailure',
                        action="store_true",
                        help='repeat the test suite until failure')
    args = parser.parse_args()
    r = run(
        pytest=args.pytest,
        output_file=args.output if not args.nooutput else None,
        repeatUntilFailure=args.repeatUntilFailure,
    )
    sys.exit(0 if r == 0 else 1)
