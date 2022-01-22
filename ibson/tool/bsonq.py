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
"""bsonq.py.

CLI BSON Query Tool.

This tool is designed to emulate 'jq' but for BSON documents instead. This also
supports validating BSON documents to see if they are compliant.
"""
import argparse


def run():
    parser = argparse.ArgumentParser(
        description="Tool to print and query BSON documents.")

    parser.parse_args()


if __name__ == '__main__':
    run()
