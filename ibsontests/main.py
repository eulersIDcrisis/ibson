# Copyright (C) 2022 Aaron Gibson (eulersidcrisis@yahoo.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
