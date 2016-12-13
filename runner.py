import os
import re
import logging
import pytest


def run():
    testListFile = "test_list.txt"
    os.system('pytest --collect-only > {}'.format(testListFile))
    testList = re.findall("<Module '(.+)'>", open(testListFile).read())
    print("Found {} test modules".format(len(testList)))
    os.remove(testListFile)
    retVal = 0
    totalPassed = 0
    totalFailed = 0
    totalSkipped = 0
    allFailedTests = []
    failureData = []
    testRep = 'currentTestReport.txt'
    for test in testList:
        # testRep = '{}.rep'.format(test.split("/")[-1])
        print("Going to run {}".format(test))
        r = os.system('pytest -k {} > {}'.format(test, testRep))
        reportLines = open(testRep).readlines()
        output = ''.join(reportLines)
        pas = re.search("==.+ ([0-9]+) passed,?.+===\n", output)
        passed = int(pas.groups()[0]) if pas else 0
        skp = re.search("==.+ ([0-9]+) skipped,?.+===\n", output)
        skipped = int(skp.groups()[0]) if skp else 0
        if r:
            fai = re.search("==.+ ([0-9]+) failed,?.+===\n", output)
            assert fai, "Non zero return value from test run but no failures reported"
            failed = int(fai.groups()[0])
            failedNames = []
            started = None
            for line in reportLines:
                if '= FAILURES =' in line:
                    started = True
                    continue
                if started:
                    failureData.append(line)
                    m = re.search('____ (test.+) ____', line)
                    if m:
                        failedNames.append(m.groups()[0])
        else:
            failed = 0
        print('In {}, {} passed, {} failed, {} skipped'.format(test, passed,
                                                               failed, skipped))
        if failed:
            print("Failed tests: {}".format(', '.join(failedNames)))
            for nm in failedNames:
                allFailedTests.append((test, nm))
        retVal += r
        totalPassed += passed
        totalFailed += failed
        totalSkipped += skipped

    print('Total {} passed, {} failed, {} skipped'.format(totalPassed,
                                                          totalFailed,
                                                          totalSkipped))
    print("Failed tests:")
    for fm, fn in allFailedTests:
        print('{}:{}'.format(fm, fn))
    print("Writing failure data in Test-Report.txt")
    with open('Test-Report.txt', 'w') as f:
        sep = os.linesep
        f.write(sep.join(failureData))
    os.remove(testRep)
    return retVal


if __name__ == "__main__":
    run()
