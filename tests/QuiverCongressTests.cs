/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.Linq;
using Newtonsoft.Json;
using NodaTime;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;
using QuantConnect.Orders;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class QuiverCongressTests
    {
        private readonly Symbol _symbol = new(SecurityIdentifier.Parse("AAPL R735QTJ8XC9X"), "AAPL");

        private readonly JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };
        
        [Test]
        public void DeserializeRawQuiverCongressJson()
        {
            // This information is not factual and is only used for testing purposes
            var content = "{ \"Ticker\": \"CVS\", \"TickerType\": \"Stock\",\"Company\": \"CVS Corp\",\"Traded\": \"2023-08-22\",\"Transaction\": \"Sale (Full)\",\"Trade_Size_USD\": \"$1,001 - $15,000\",\"Status\": \"New\",\"Subholding\": null,\"Description\": null,\"Name\": \"Gardner, Cory\",\"Filed\": \"2023-09-18\",\"Party\": \"R\",\"District\": null,\"Chamber\": \"Senate\",\"Comments\": null,\"excess_return\": \"5.894026562437\",\"State\": \"Alaska\"}";
            var data = JsonConvert.DeserializeObject<QuiverCongressDataPoint>(content, _jsonSerializerSettings);

            AssertData(data);
        }

        [Test]
        public void ReaderTest()
        {
            // This information is not factual and is only used for testing purposes
            var content = "20230918,20230918,20230918,20230822,Gardner; Cory,Sell,15001,50000,Senate,Republican,,Alaska";
            var instance = CreateNewInstance();
            var config = new SubscriptionDataConfig(typeof(QuiverCongressDataPoint), _symbol, Resolution.Daily,
                DateTimeZone.Utc, DateTimeZone.Utc, false, false, false);
            var data = instance.Reader(config, content, DateTime.UtcNow, false);
            AssertData(data as QuiverCongressDataPoint);
            
        }

        [Test]
        public void UniverseReaderTest()
        {
            // This information is not factual and is only used for testing purposes
            var date = new DateTime(2023, 9, 18);
            var content = $"AAPL R735QTJ8XC9X,AAPL,{date:yyyyMMdd},20230918,20230822,Gardner; Cory,Sell,15001,50000,Senate,Republican,,Alaska";
            var instance = new QuiverQuantCongressUniverse();
            var config = new SubscriptionDataConfig(typeof(QuiverQuantCongressUniverse), Symbol.None, Resolution.Daily,
                DateTimeZone.Utc, DateTimeZone.Utc, false, false, false);
            var data = instance.Reader(config, content, date, false) as QuiverCongressDataPoint;

            Assert.AreEqual(date, data.Time);
            Assert.AreEqual(_symbol, data.Symbol);
            Assert.AreEqual(new DateTime(2023, 9, 18), data.ReportDate);
            Assert.AreEqual(new DateTime(2023, 8, 22), data.TransactionDate);
            Assert.AreEqual("Gardner, Cory", data.Representative);
            Assert.AreEqual(OrderDirection.Sell, data.Transaction);
            Assert.AreEqual(Congress.Senate, data.House);
            Assert.AreEqual(Party.Republican, data.Party);
            Assert.AreEqual("Alaska", data.State);
            Assert.IsTrue(string.IsNullOrWhiteSpace(data.District));
        }

        [Test]
        public void JsonRoundTrip()
        {
            var expected = CreateNewInstance();
            var type = expected.GetType();
            var serialized = JsonConvert.SerializeObject(expected);
            var result = JsonConvert.DeserializeObject(serialized, type);

            AssertAreEqual(expected, result);
        }


        [Test]
        public void Clone()
        {
            var expected = CreateNewInstance();
            var result = expected.Clone();

            AssertAreEqual(expected, result);
        }

        [Test]
        public void CloneCollection()
        {
            var expected = CreateNewCollectionInstance();
            var result = expected.Clone();

            AssertAreEqual(expected, result);
        }

        private void AssertAreEqual(object expected, object result, bool filterByCustomAttributes = false)
        {
            foreach (var propertyInfo in expected.GetType().GetProperties())
            {
                // we skip Symbol which isn't protobuffed
                if (filterByCustomAttributes && propertyInfo.CustomAttributes.Count() != 0)
                {
                    Assert.AreEqual(propertyInfo.GetValue(expected), propertyInfo.GetValue(result));
                }
            }
            foreach (var fieldInfo in expected.GetType().GetFields())
            {
                Assert.AreEqual(fieldInfo.GetValue(expected), fieldInfo.GetValue(result));
            }
        }

        private BaseData CreateNewInstance()
        {
            return new QuiverCongressDataPoint
            {
                Symbol = Symbol.Empty,
                Time = DateTime.Today,
                DataType = MarketDataType.Base,
                
                ReportDate = DateTime.Today,
                TransactionDate = DateTime.Today.AddDays(-60),
                Representative = "Ronald Lee Wyden",
                Transaction = OrderDirection.Buy,
                
                Amount = 15001m,
                House = Congress.Senate,
            };
        }

        private BaseDataCollection CreateNewCollectionInstance()
        {
            return new QuiverCongress
            {
                new QuiverCongressDataPoint
                {
                    Symbol = Symbol.Empty,
                    Time = DateTime.Today,
                    DataType = MarketDataType.Base,
                    
                    ReportDate = DateTime.Today,
                    TransactionDate = DateTime.Today.AddDays(-60),
                    Representative = "Ronald Lee Wyden",
                    Transaction = OrderDirection.Buy,
                    
                    Amount = 15001m,
                    House = Congress.Senate,
                },
                new QuiverCongressDataPoint
                {
                    Symbol = Symbol.Empty,
                    Time = DateTime.Today,
                    DataType = MarketDataType.Base,
                    
                    ReportDate = DateTime.Today,
                    TransactionDate = DateTime.Today.AddDays(-60),
                    Representative = "Ronald Lee Wyden",
                    Transaction = OrderDirection.Buy,
                    
                    Amount = 15001m,
                    House = Congress.Senate,
                }
            };
        }

        private static void AssertData(QuiverCongressDataPoint data)
        {
            Assert.AreEqual(new DateTime(2023, 9, 18), data.ReportDate);
            Assert.AreEqual(new DateTime(2023, 8, 22), data.TransactionDate);
            Assert.AreEqual("Gardner, Cory", data.Representative);
            Assert.AreEqual(OrderDirection.Sell, data.Transaction);
            Assert.AreEqual(Congress.Senate, data.House);
            Assert.AreEqual(Party.Republican, data.Party);
            Assert.AreEqual("Alaska", data.State);
            Assert.IsTrue(string.IsNullOrWhiteSpace(data.District));
        }
    }
}
