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
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Orders;
using QuantConnect.Util;
using static QuantConnect.StringExtensions;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Single data point for QuiverCongress data
    /// </summary>
    public class QuiverCongressDataPoint : BaseData
    {
        private static readonly TimeSpan _period = TimeSpan.FromDays(1);

        /// <summary>
        /// Data source ID
        /// </summary>
        public static int DataSourceId { get; } = 2000;

        /// <summary>
        /// The date the transaction was recorded by QuiverQuant. Value will always exist.
        /// </summary>
        public DateTime RecordDate { get; set; }

        /// <summary>
        /// The date the recorded transaction was updated by QuiverQuant. Alias for EndTime.
        /// </summary>
        public DateTime UpdatedAt => EndTime;

        /// <summary>
        /// The date the transaction was reported. Value will always exist.
        /// </summary>
        [JsonProperty(PropertyName = "Filed")]
        [JsonConverter(typeof(DateTimeJsonConverter), "yyyy-MM-dd")]
        public DateTime? ReportDate { get; set; }

        /// <summary>
        /// The date the transaction took place
        /// </summary>
        [JsonProperty(PropertyName = "Traded")]
        [JsonConverter(typeof(DateTimeJsonConverter), "yyyy-MM-dd")]
        public DateTime TransactionDate { get; set; }

        /// <summary>
        /// The Representative making the transaction
        /// </summary>
        [JsonProperty(PropertyName = "Name")]
        public string Representative { get; set; }

        /// <summary>
        /// The type of transaction
        /// </summary>
        [JsonProperty(PropertyName = "Transaction")]
        [JsonConverter(typeof(TransactionDirectionJsonConverter))]
        public OrderDirection Transaction { get; set; }

        /// <summary>
        /// The amount of the transaction (in USD). The Representative can report a range (see <see cref="MaximumAmount"/>).
        /// </summary>
        public decimal? Amount { get; set; }

        /// <summary>
        /// The maximum amount of the transaction (in USD). The Representative can report a range (see <see cref="Amount"/>).
        /// </summary>
        public decimal? MaximumAmount { get; set; }

        /// <summary>
        /// The Chamber of Congress that the trader belongs to
        /// </summary>
        [JsonProperty(PropertyName = "Chamber")]
        [JsonConverter(typeof(StringEnumConverter))]
        public Congress House { get; set; }

        /// <summary>
        /// The political party that the trader belongs to
        /// </summary>
        [JsonProperty(PropertyName = "Party")]
        [JsonConverter(typeof(StringEnumConverter))]
        public Party Party { get; set; }

        /// <summary>
        /// The district that the trader belongs to (null or empty for Senators)
        /// </summary>
        [JsonProperty(PropertyName = "District")]
        public string District { get; set; }

        /// <summary>
        /// The state that the trader belongs to
        /// </summary>
        [JsonProperty(PropertyName = "State")]
        public string State { get; set; }

        /// <summary>
        /// The time the data point ends at and becomes available to the algorithm
        /// </summary>
        public override DateTime EndTime => Time + _period;

        /// <summary>
        /// Creates a new instance of QuiverCongressDataPoint
        /// </summary>
        public QuiverCongressDataPoint()
        {
        }

        /// <summary>
        /// Creates a new instance of QuiverCongressDataPoint from a CSV line
        /// </summary>
        /// <param name="csvLine">CSV line</param>
        public QuiverCongressDataPoint(string csvLine)
        {
            // Time[0], RecordDate[1], ReportDate[2], TransactionDate[3], Representative[4], Transaction[5],Amount[6],MaximumAmount[7],House[8],Party[9],District[10],State[11]
            var csv = csvLine.Split(',');
            Time = Parse.DateTimeExact(csv[0], "yyyyMMdd");
            RecordDate = Parse.DateTimeExact(csv[1], "yyyyMMdd");
            ReportDate = Parse.DateTimeExact(csv[2], "yyyyMMdd");
            TransactionDate = Parse.DateTimeExact(csv[3], "yyyyMMdd");
            Representative = csv[4].Replace(";",",");
            Transaction = (OrderDirection)Enum.Parse(typeof(OrderDirection), csv[5], true);
            Amount = csv[6].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture));
            MaximumAmount = csv[7].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture));
            House = (Congress)Enum.Parse(typeof(Congress), csv[8], true);
            Party = (Party)Enum.Parse(typeof(Party), csv[9], true);
            District = csv[10];
            State = csv[11];
        }

        /// <summary>
        /// Reader converts each line of the data source into BaseData objects.
        /// </summary>
        /// <param name="config">Subscription data config setup object</param>
        /// <param name="line">Content of the source document</param>
        /// <param name="date">Date of the requested data</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>
        /// Quiver Congress object
        /// </returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            return new QuiverCongressDataPoint(line)
            {
                Symbol = config.Symbol
            };
        }

        /// <summary>
        /// Clones the data
        /// </summary>
        /// <returns>A clone of the object</returns>
        public override BaseData Clone()
        {
            return new QuiverCongressDataPoint
            {
                Symbol = Symbol,
                Time = Time,
                RecordDate = RecordDate,
                ReportDate = ReportDate,
                TransactionDate = TransactionDate,
                Representative = Representative,
                Transaction = Transaction,
                Amount = Amount,
                MaximumAmount = MaximumAmount,
                House = House,
                Party = Party,
                District = District,
                State = State,
            };
        }

        /// <summary>
        /// Formats a string with the Quiver Congress information.
        /// </summary>
        public override string ToString()
        {
            return Invariant($"{Symbol}({ReportDate}) :: ") +
                   Invariant($"Transaction Date: {TransactionDate} ") +
                   Invariant($"Representative: {Representative} ") +
                   Invariant($"House: {House} ") +
                   Invariant($"Transaction: {Transaction} ") +
                   Invariant($"Amount: {Amount}");
        }

        /// <summary>
        /// Indicates if there is support for mapping
        /// </summary>
        /// <returns>True indicates mapping should be used</returns>
        public override bool RequiresMapping()
        {
            return true;
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Specifies the data time zone for this data type. This is useful for custom data types
        /// </summary>
        /// <returns>The <see cref="DateTimeZone"/> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return TimeZones.Utc;
        }
    }
}
