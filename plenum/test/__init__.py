# -*- coding: utf-8 -*-

# Setting this to true will not apply spies on any method. This will lead to
# failure of lot of tests. Set this to True only for benchmarking
NO_SPIES = True


def run():
    import pytest
    pytest.main()


if __name__ == "__main__":
    run()
