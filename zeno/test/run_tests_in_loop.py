import time

from id_test.test_rest_server import runAll

if __name__ == '__main__':
    for i in range(15):
        print("running %s time" % str(i+1))
        runAll()
        time.sleep(1)
