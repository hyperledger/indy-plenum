import os
import re
import logging
import pytest


def run():
	  print("Preparing test suite")
    testListFile = "test_list.txt"
    os.system('pytest --collect-only > {}'.format(testListFile))
    print("Reading collected modules file")
    collectedData = open(testListFile).read()
    os.remove(testListFile)
    print("Collecting modules")
    testList = re.findall("<Module '(.+)'>", collectedData)
    print("Found {} test modules".format(len(testList)))
    if not testList:
        m = re.search("errors during collection", collectedData)
        if m:
            print(collectedData)
            exit()
    retVal = 0
    totalPassed = 0
    totalFailed = 0
    totalSkipped = 0
    totalErros = 0
    allFailedTests = []
    allErrorTests = []
    failureData = []
    testRep = 'currentTestReport.txt'
    passPat = re.compile("==.+ ([0-9]+) passed,?.+===\n")
    skipPat = re.compile("==.+ ([0-9]+) skipped,?.+===\n")
    failPat = re.compile("==.+ ([0-9]+) failed,?.+===\n")
    errPat = re.compile("==.+ ([0-9]+) error,?.+===\n")
    failedTestPat = re.compile('____ (test.+) ____')
    errorTestPat = re.compile('____ (ERROR.+) ____')

    for test in testList:
        # testRep = '{}.rep'.format(test.split("/")[-1])
        print("Going to run {}".format(test))
        r = os.system('pytest -k {} > {}'.format(test, testRep))
        reportLines = open(testRep).readlines()
        output = ''.join(reportLines)
        pas = passPat.search(output)
        passed = int(pas.groups()[0]) if pas else 0
        skp = skipPat.search(output)
        skipped = int(skp.groups()[0]) if skp else 0
        if r:
            fai = failPat.search(output)
            err = errPat.search(output)
            assert fai or err, "Non zero return value from test run but no failures or errors reported"
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
        print('In {}, {} passed, {} failed, {} errors, {} skipped'.
              format(test, passed, errors, failed, skipped))
        if failed:
            print("Failed tests: {}".format(', '.join(failedNames)))
            for nm in failedNames:
                allFailedTests.append((test, nm))
        if errors:
            print("Error in tests: {}".format(', '.join(errorNames)))
            for nm in errorNames:
                allErrorTests.append((test, nm))
        retVal += r
        totalPassed += passed
        totalFailed += failed
        totalErros += errors
        totalSkipped += skipped

    print('Total {} passed, {} failed, {} errors, {} skipped'.
          format(totalPassed, totalFailed, totalErros, totalSkipped))

    if totalFailed:
        print("Failed tests:")
        for fm, fn in allFailedTests:
            print('{}:{}'.format(fm, fn))

    if totalErros:
        print("Error in tests:")
        for fm, fn in allErrorTests:
            print('{}:{}'.format(fm, fn))

    if failureData:
        print("Writing failure data in Test-Report.txt")
        with open('Test-Report.txt', 'w') as f:
            sep = os.linesep
            f.write(sep.join(failureData))

    if os._exists(testRep):
        os.remove(testRep)

    return retVal


if __name__ == "__main__":
    run()
