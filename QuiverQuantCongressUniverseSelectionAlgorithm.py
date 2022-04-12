# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from AlgorithmImports import *

class QuiverQuantCongresssUniverseAlgorithm(QCAlgorithm):
    def Initialize(self):
        # Data ADDED via universe selection is added with Daily resolution.
        self.UniverseSettings.Resolution = Resolution.Daily

        self.SetStartDate(2022, 2, 14)
        self.SetEndDate(2022, 2, 18)
        self.SetCash(100000)

        # add a custom universe data source (defaults to usa-equity)
        self.AddUniverse(QuiverQuantCongressUniverse, "QuiverQuantCongresssUniverse", Resolution.Daily, self.UniverseSelection)

    def UniverseSelection(self, data):
        for datum in data:
            self.Log(f"{datum.Symbol},{datum.Representative},{datum.Amount},{datum.Transaction}")

        # define our selection criteria
        return [d.Symbol for d in data \
            if d.Amount > 200000 and d.Transaction == OrderDirection.Buy]

    def OnSecuritiesChanged(self, changes):
        self.Log(changes.ToString())