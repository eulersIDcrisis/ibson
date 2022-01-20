# MIT License
#
# Copyright (c) 2022 Aaron Gibson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
"""main.py.

Main entrypoint for running these BSON format tests.
"""
import unittest
import argparse
# Import the tests from the various modules here.
from ibsontests.modules.primary_tests import BSONEncoderTests, BSONDecoderTests
from ibsontests.modules.stress_tests import DeeplyNestedDocumentTests


def run():
    """Main test runner."""
    parser = argparse.ArgumentParser(description="Run the BSON unittests.")
    parser.add_argument('--run-all', action='store_true', help=(
        "Include the tests that and take more resources and time to run."))
    parser.add_argument('-v', '--verbose', action='count', default=1,
                        help="Increase output verbosity.")
    args = parser.parse_args()

    test_cases = [
        BSONEncoderTests,
        BSONDecoderTests
    ]

    if args.run_all:
        # Add the 'long-running' tests here.
        test_cases.append(DeeplyNestedDocumentTests)

    loader = unittest.defaultTestLoader
    test_suite = unittest.TestSuite()
    for tc in test_cases:
        tests = loader.loadTestsFromTestCase(tc)
        test_suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=args.verbose)
    runner.run(test_suite)


if __name__ == '__main__':
    run()
