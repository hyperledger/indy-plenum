from plenum.common.util import randomString


# Checks if the function randomString() is returning correct
# length random string for various lengths
def test_random_string1():
    assert (len(randomString(3)) == 3), \
        "Function randomString(3) did not return string of len 3 characters"
    assert (len(randomString(20)) == 20), \
        "Function randomString() did not return string of default len 20 characters"
    assert (len(randomString(32)) == 32), \
        "Function call randomString(32) did not return string of len 32 characters"
    assert (len(randomString(128)) == 128), \
        "Function randomString(128) did not return string of len 128 characters"
    assert (len(randomString(12800)) == 12800), \
        "Function randomString(12800) did not return string of len 12800 characters"


# Checks if there is a collision of the returned random strings
# If we generate a random string with fewer number of characters collision will happen sooner
# Testing several times has shown numbers less than 5 will cause collision 100%
# times if tested for about 1000 iterations
def test_random_string2():
    test_iterations = 1000
    rss = []
    for i in range(test_iterations):
        rs = randomString(20)
        assert rs not in rss, "random string # %d exists in list, we have a collision" % i
        rss.append(rs)

# Test to check the performance of randomString() function
# def test_random_string3():
#     t0 = time.time()  # start time
#     test_iterations = 1000000
#     for i in range(test_iterations):
#         rs = randomString(5)
#     # the code to time goes here
#     t1 = time.time()  # end time
#     print("The time take is: %f" % (t1 - t0))
