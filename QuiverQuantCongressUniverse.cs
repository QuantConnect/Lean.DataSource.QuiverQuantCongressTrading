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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Orders;
using static QuantConnect.StringExtensions;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Universe Selection helper class for QuiverQuant Congress dataset
    /// </summary>
    public class QuiverQuantCongressUniverse : BaseData
    {
        /// <summary>
        /// ReportDate
        /// </summary>
        public DateTime ReportDate => Time;

        /// <summary>
        /// TransactionDate
        /// </summary>
        public DateTime TransactionDate { get; set; }

        /// <summary>
        /// Representative
        /// </summary>
        public string Representative { get; set; }

        /// <summary>
        /// Month-over-month change in company's follower count
        /// </summary>
        public OrderDirection Transaction { get; set; }

        /// <summary>
        /// The amount of the transaction (in USD)
        /// </summary>
        public decimal? Amount { get; set; }

        /// <summary>
        /// The House of Congress that the trader belongs to
        /// </summary>
        public Congress House { get; set; }

        /// <summary>
        /// Time passed between the date of the data and the time the data became available to us
        /// </summary>
        public TimeSpan Period { get; set; } = TimeSpan.FromDays(1);

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + Period;

        /// <summary>
        /// Return the URL string source of the file. This will be converted to a stream
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "quiver",
                    "congress",
                    "universe",
                    $"{date.ToStringInvariant(DateFormat.EightCharacter)}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses the data from the line provided and loads it into LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>New instance</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');
            var amount = csv[5].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture));

            return new QuiverQuantCongressUniverse
            {
                TransactionDate = Parse.DateTimeExact(csv[2], "yyyyMMdd"),
                Representative = csv[3],
                Transaction = (OrderDirection)Enum.Parse(typeof(OrderDirection), csv[4], true),
                Amount = amount,
                House = (Congress)Enum.Parse(typeof(Congress), csv[6], true),

                Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]),
                Time = date - Period,
                Value = amount ?? 0
            };
        }

        /// <summary>
        /// Converts the instance to string
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
        /// <returns>The <see cref="T:NodaTime.DateTimeZone" /> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return TimeZones.Chicago;
        }
    }
}