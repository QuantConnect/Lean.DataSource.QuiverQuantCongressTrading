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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Data.Auxiliary;
using QuantConnect.DataSource;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Util;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// QuiverCongressDataDownloader implementation. https://www.quiverquant.com/
    /// </summary>
    public class QuiverCongressDataDownloader : IDisposable
    {
        public const string VendorName = "quiver";
        public const string VendorDataName = "congresstrading";
        private const int MaxRetries = 5;

        private readonly string _destinationFolder;
        private readonly string _universeFolder;
        private readonly string _dataFolder = Globals.DataFolder;
        private readonly string _clientKey;
        private readonly bool _canCreateUniverseFiles;

        private readonly JsonSerializerSettings _jsonSerializerSettings = new()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };

        /// <summary>
        /// Control the rate of download per unit of time.
        /// </summary>
        private readonly RateGate _indexGate;

        /// <summary>
        /// Creates a new instance of <see cref="QuiverCongress"/>
        /// </summary>
        /// <param name="destinationFolder">The folder where the data will be saved</param>
        /// <param name="apiKey">The QuiverQuant API key</param>
        public QuiverCongressDataDownloader(string destinationFolder, string apiKey = null)
        {
            _destinationFolder = Directory.CreateDirectory(Path.Combine(destinationFolder, VendorDataName)).FullName;
            _universeFolder = Directory.CreateDirectory(Path.Combine(_destinationFolder, "universe")).FullName;

            _clientKey = apiKey ?? Config.Get("quiver-auth-token");
            _canCreateUniverseFiles = Directory.Exists(Path.Combine(_dataFolder, "equity", "usa", "map_files"));

            // Represents rate limits of 100 requests per 60 second
            _indexGate = new RateGate(100, TimeSpan.FromSeconds(60));
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            var coverage = 0;
            var startDate = DateTime.MinValue;
            var today = DateTime.UtcNow.Date;
            var stopwatch = Stopwatch.StartNew();
            Log.Trace($"QuiverCongressDataDownloader.Run(): Start downloading/processing QuiverQuant Congress data");

            try
            {
                var rawCongressData = HttpRequester($"bulk/congresstrading?version=v2").SynchronouslyAwaitTaskResult();
                if (string.IsNullOrWhiteSpace(rawCongressData))
                {
                    Log.Trace($@"QuiverCongressDataDownloader.Run(): Received no data - {rawCongressData}");
                    return false;
                }
                Log.Trace($@"QuiverCongressDataDownloader.Run(): Received data");

                var congressTradesByDate = JsonConvert
                    .DeserializeObject<List<RawQuiverCongressDataPoint>>(rawCongressData, _jsonSerializerSettings)!
                    .Where(x =>
                    {
                        // We don't have enough information to disambiguate whether this transaction,
                        // known as an "Exchange", is the acquisition or dumping of an asset.
                        // Also, ReportDate might be null, but we use it for setting the EndTime
                        // of the QuiverCongress type. So if it doesn't exist, we don't know
                        // when the data was made available to us.
                        var isStock = !string.IsNullOrWhiteSpace(x.TickerType) || x.TickerType != "Stock" || x.TickerType != "ST";
                        if (x.Transaction == OrderDirection.Hold || x.ReportDate == null || x.ReportDate > today || !isStock)
                        {
                            return false;
                        }
                        return true;
                    })
                    .OrderBy(x => x.ReportDate.Value)
                    .GroupBy(x => x.ReportDate.Value)
                    .ToDictionary(kvp => kvp.Key, kvp =>
                    {
                        // A Congressperson can make the same trade more than once per day
                        // To avoid confusion, we will group the same trades and sum their amount
                        var values = kvp.ToList();
                        var duplicates = kvp.GroupBy(x => $"{x.Ticker},{x}").Where(x => x.Count() > 1);
                        foreach (var duplicate in duplicates)
                        {
                            var trade = duplicate.FirstOrDefault();
                            values.RemoveAll(x => duplicate.Key == $"{x.Ticker},{x}");
                            values.Add(trade);
                        }
                        return values;
                    });
                
                var mapFileProvider = new LocalZipMapFileProvider();
                mapFileProvider.Initialize(new DefaultDataProvider());

                var invalidTickers = new HashSet<string>();
                var congressTradesByTicker = new Dictionary<string, List<string>>();
                
                foreach (var kvp in congressTradesByDate)
                {
                    var processDate = kvp.Key;
                    var universeCsvContents = new List<string>();

                    foreach (var congressTrade in kvp.Value)
                    {
                        var ticker = congressTrade.Ticker.Trim().ToUpperInvariant()
                                .Replace("- DEFUNCT", string.Empty)
                                .Replace("-DEFUNCT", string.Empty)
                                .Replace(" ", string.Empty)
                                .Replace("|", string.Empty)
                                .Replace("-", ".");
                        
                        if (!congressTradesByTicker.TryGetValue(ticker, out var _))
                        {
                            congressTradesByTicker.Add(ticker, new List<string>());
                        }

                        var curRow = congressTrade.ToString();

                        congressTradesByTicker[ticker].Add($"{processDate:yyyyMMdd},{curRow}");

                        if (!_canCreateUniverseFiles) continue;

                        var sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, true, mapFileProvider, processDate);
                        if (sid.Date.Year < 1998)
                        {
                            invalidTickers.Add(ticker);
                            continue;
                        }  
                        universeCsvContents.Add($"{sid},{ticker},{curRow}");
                    }

                    if (universeCsvContents.Any())
                    {
                        SaveContentToFile(_universeFolder, $"{processDate:yyyyMMdd}", universeCsvContents);
                    }
                }

                if (invalidTickers.Any())
                {
                    foreach (var ticker in invalidTickers)
                    {
                        congressTradesByTicker.Remove(ticker);
                    }
                    Log.Trace($"QuiverCongressDataDownloader.Run(): Invalid Tickers: {Environment.NewLine}{string.Join(", ", invalidTickers.OrderBy(x => x))}");
                }

                startDate = congressTradesByDate.FirstOrDefault().Key;
                coverage = congressTradesByTicker.Count;
                congressTradesByTicker.DoForEach(kvp => SaveContentToFile(_destinationFolder, kvp.Key, kvp.Value));
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"QuiverCongressDataDownloader.Run(): Coverage: {coverage} securities. Start Date: {startDate:yyyyMMdd}");
            Log.Trace($"QuiverCongressDataDownloader.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }

        /// <summary>
        /// Sends a GET request for the provided URL
        /// </summary>
        /// <param name="url">URL to send GET request for</param>
        /// <returns>Content as string</returns>
        /// <exception cref="Exception">Failed to get data after exceeding retries</exception>
        private async Task<string> HttpRequester(string url)
        {
            var baseUrl = "https://api.quiverquant.com/beta/";

            for (var retries = 1; retries <= MaxRetries; retries++)
            {
                try
                {
                    using var client = new HttpClient();
                    client.BaseAddress = new Uri(baseUrl);
                    client.DefaultRequestHeaders.Clear();

                    // You must supply your API key in the HTTP header,
                    // otherwise you will receive a 403 Forbidden response
                    client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", _clientKey);

                    // Responses are in JSON: you need to specify the HTTP header Accept: application/json
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    // Makes sure we don't overrun Quiver rate limits accidentally
                    _indexGate.WaitToProceed();

                    var response = await client.GetAsync(url);
                    if (response.StatusCode == HttpStatusCode.NotFound)
                    {
                        Log.Error($"QuiverCongressDataDownloader.HttpRequester(): Files not found at url: {url}");
                        response.DisposeSafely();
                        return string.Empty;
                    }

                    if (response.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        var finalRequestUri = response.RequestMessage.RequestUri; // contains the final location after following the redirect.
                        response = client.GetAsync(finalRequestUri).Result; // Reissue the request. The DefaultRequestHeaders configured on the client will be used, so we don't have to set them again.
                    }

                    response.EnsureSuccessStatusCode();

                    var result = await response.Content.ReadAsStringAsync();
                    response.DisposeSafely();

                    return result;
                }
                catch (Exception e)
                {
                    Log.Error(e, $"QuiverCongressDataDownloader.HttpRequester(): Error at HttpRequester. (retry {retries}/{MaxRetries})");
                    Thread.Sleep(1000);
                }
            }

            throw new Exception($"Request for {baseUrl}{url} failed with no more retries remaining (retry {MaxRetries}/{MaxRetries})");
        }
        
        /// <summary>
        /// Saves contents to disk, deleting existing zip files
        /// </summary>
        /// <param name="destinationFolder">Final destination of the data</param>
        /// <param name="name">File name</param>
        /// <param name="contents">Contents to write</param>
        private static void SaveContentToFile(string destinationFolder, string name, IEnumerable<string> contents)
        {
            static DateTime CustomParseExact(string line) => DateTime.ParseExact(line.Split(',').First(),
                "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);

            var lines = new HashSet<string>(contents);

            var finalLines = (destinationFolder.Contains("universe") ?
                lines.OrderBy(x => x.Split(',').First()) :
                lines.OrderBy(CustomParseExact)).ToList();

            var finalPath = Path.Combine(destinationFolder, $"{name.ToLowerInvariant()}.csv");
            File.WriteAllLines(finalPath, finalLines);
        }

        private class RawQuiverCongressDataPoint : QuiverCongressDataPoint
        {
            /// <summary>
            /// The ticker/symbol for the company
            /// </summary>
            [JsonProperty(PropertyName = "Ticker")]
            public string Ticker { get; set; }

            /// <summary>
            /// The security type
            /// </summary>
            [JsonProperty(PropertyName = "TickerType")]
            public string TickerType { get; set; }

            /// <summary>
            /// The trade size range
            /// </summary>
            [JsonProperty(PropertyName = "Trade_Size_USD")]
            public string TradeSizeRange { get; set; }

            /// <summary>
            /// Formats a string with the Raw Quiver Congress information.
            /// This information does not include the amount, since we will use this
            /// representation to group the trades of the same day, person and direction 
            /// </summary>
            public override string ToString()
            {
                var isSingle = true;
                var tradeSizeRange = new StringBuilder();
                foreach (var c in TradeSizeRange.AsSpan())
                {
                    if (char.IsDigit(c))
                    {
                        tradeSizeRange.Append(c);
                    }
                    else if (c == '-')
                    {
                        isSingle = false;
                        tradeSizeRange.Append(',');
                    }
                }

                if (isSingle)
                {
                    tradeSizeRange.Append(',');
                }

                return string.Join(",",
                        $"{TransactionDate.ToStringInvariant("yyyyMMdd")}",
                        $"{Representative.Trim().Replace(",", ";")}",
                        $"{Transaction}",
                        $"{tradeSizeRange}",
                        $"{House}",
                        $"{Party}",
                        $"{District?.Trim()}",
                        $"{State.Trim()}"
                    );
            }
        }

        /// <summary>
        /// Disposes of unmanaged resources
        /// </summary>
        public void Dispose() => _indexGate?.Dispose();
    }
}