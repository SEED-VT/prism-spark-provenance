# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""BigBench sentiment scoring written as a UDF that Prism can trace.

Each token carries a lexicon score, and a review's sentiment is the sum of the tokens
that carry sentiment. Only a few tokens of a review matter, so the data lineage of its
sentiment is exactly the sentiment-bearing words and not the filler.
"""

# A small signed lexicon with varied magnitudes so influence can rank tokens.
LEXICON = {
    "good": 1.0, "great": 1.5, "love": 2.0, "excellent": 2.0, "perfect": 2.5,
    "fast": 1.0, "recommend": 1.5,
    "bad": -1.0, "broken": -2.0, "terrible": -1.5, "hate": -2.5, "awful": -2.0,
    "slow": -1.0, "disappointed": -1.5,
}
FILLER = ["the", "a", "this", "product", "item", "and", "is", "was", "it",
          "arrived", "very", "really", "quite", "overall", "build", "quality",
          "price", "seems", "for", "with"]
SENT_WORDS = list(LEXICON)


def sentiment(row):
    # Sum the tokens that carry sentiment. Filler tokens with score 0 do not flow into
    # the result, but every token is a control dependency through the v != 0 test.
    s = 0.0
    for v in row["scores"]:
        s = s + v if v != 0.0 else s
    return s
