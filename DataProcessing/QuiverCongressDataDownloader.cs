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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
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
        private readonly string _clientKey;
        private readonly bool _canCreateUniverseFiles;
        private static readonly List<char> _defunctDelimiters = new() { '-', '_' };
        
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
            _destinationFolder = Path.Combine(destinationFolder, VendorDataName);
            _universeFolder = Path.Combine(_destinationFolder, "universe");
            _clientKey = apiKey ?? Config.Get("quiver-auth-token");
            _canCreateUniverseFiles = Directory.Exists(Path.Combine(Globals.DataFolder, "equity", "usa", "map_files"));

            // Represents rate limits of 10 requests per 1.1 second
            _indexGate = new RateGate(10, TimeSpan.FromSeconds(1.1));

            Directory.CreateDirectory(_destinationFolder);
            Directory.CreateDirectory(_universeFolder);
        }

        /// <summary>
        /// Runs the instance of the object.
        /// </summary>
        /// <returns>True if process all downloads successfully</returns>
        public bool Run()
        {
            var stopwatch = Stopwatch.StartNew();
            var today = DateTime.UtcNow.Date;

            var mapFileProvider = new LocalZipMapFileProvider();
            mapFileProvider.Initialize(new DefaultDataProvider());

            try
            {
                var companies = GetCompanies().Result.DistinctBy(x => x.Ticker).ToList();
                var count = companies.Count;
                var companiesCompleted = 0;

                Log.Trace($"QuiverCongressDataDownloader.Run(): Start processing {count.ToStringInvariant()} companies");

                var tasks = new List<Task>();
                
                var tempData = new ConcurrentDictionary<string, ConcurrentQueue<string>>();

                foreach (var company in companies)
                {
                    // Include tickers that are "defunct".
                    // Remove the tag because it cannot be part of the API endpoint.
                    // This is separate from the NormalizeTicker(...) method since
                    // we don't convert tickers with `-`s into the format we can successfully
                    // index mapfiles with.
                    var quiverTicker = company.Ticker;

                    if (!TryNormalizeDefunctTicker(quiverTicker, out var ticker))
                    {
                        Log.Error($"QuiverCongressDataDownloader(): Defunct ticker {quiverTicker} is unable to be parsed. Continuing...");
                        continue;
                    }

                    // Begin processing ticker with a normalized value
                    Log.Trace($"QuiverCongressDataDownloader.Run(): Processing {ticker}");

                    tasks.Add(
                        HttpRequester($"historical/congresstrading/{ticker}")
                            .ContinueWith(
                                y =>
                                {
                                    if (y.IsFaulted)
                                    {
                                        Log.Error($"QuiverCongressDataDownloader.Run(): Failed to get data for {company}");
                                        return;
                                    }

                                    var result = y.Result;
                                    if (string.IsNullOrEmpty(result))
                                    {
                                        // We've already logged inside HttpRequester
                                        return;
                                    }

                                    var congressTrades =
                                        JsonConvert.DeserializeObject<List<QuiverCongressDataPoint>>(result,
                                            _jsonSerializerSettings);
                                    var csvContents = new List<string>();

                                    foreach (var congressTrade in congressTrades)
                                    {
                                        // We don't have enough information to disambiguate whether this transaction,
                                        // known as an "Exchange", is the acquisition or dumping of an asset.
                                        // Also, ReportDate might be null, but we use it for setting the EndTime
                                        // of the QuiverCongress type. So if it doesn't exist, we don't know
                                        // when the data was made available to us.
                                        if (congressTrade.Transaction == OrderDirection.Hold || congressTrade.ReportDate == null)
                                        {
                                            continue;
                                        }

                                        if (congressTrade.ReportDate.Value.Date == today)
                                        {
                                            Log.Trace($"Encountered data from today for {ticker}: {today:yyyy-MM-dd} - Skipping");
                                            continue;
                                        }

                                        var date = congressTrade.ReportDate.Value.ToStringInvariant("yyyyMMdd");

                                        var curRow = string.Join(",",
                                            $"{congressTrade.TransactionDate.ToStringInvariant("yyyyMMdd")}",
                                            $"{congressTrade.Representative.Trim()}",
                                            $"{congressTrade.Transaction}",
                                            $"{congressTrade.Amount.ToStringInvariant()}",
                                            $"{congressTrade.House}");

                                        csvContents.Add($"{date},{curRow}");

                                        if (!_canCreateUniverseFiles)
                                            continue;

                                        var sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, true, mapFileProvider,
                                            congressTrade.ReportDate.Value);

                                        var queue = tempData.GetOrAdd(date, new ConcurrentQueue<string>());
                                        queue.Enqueue($"{sid},{ticker},{curRow}");
                                    }

                                    if (csvContents.Count != 0)
                                    {
                                        SaveContentToFile(_destinationFolder, ticker, csvContents);
                                    }

                                    var newCompaniesCompleted = Interlocked.Increment(ref companiesCompleted);
                                    if (newCompaniesCompleted % 100 == 0)
                                    {
                                        Log.Trace($"QuiverQuantTwitterFollowersDataDownloader.Run(): {newCompaniesCompleted}/{count} complete");
                                    }
                                }
                            )
                    );

                    if (tasks.Count != 10) continue;

                    Task.WaitAll(tasks.ToArray());

                    foreach (var kvp in tempData)
                    {
                        SaveContentToFile(_universeFolder, kvp.Key, kvp.Value);
                    }

                    tempData.Clear();
                    tasks.Clear();
                }
            }
            catch (Exception e)
            {
                Log.Error(e);
                return false;
            }

            Log.Trace($"QuiverCongressDataDownloader.Run(): Finished in {stopwatch.Elapsed.ToStringInvariant(null)}");
            return true;
        }

        /// <summary>
        /// Gets the list of companies
        /// </summary>
        /// <returns>List of companies</returns>
        /// <exception cref="Exception"></exception>
        private async Task<List<Company>> GetCompanies()
        {
            try
            {
                var content = await HttpRequester("companies");
                return JsonConvert.DeserializeObject<List<Company>>(content);
            }
            catch (Exception e)
            {
                throw new Exception("QuiverDownloader.GetSymbols(): Error parsing companies list", e);
            }
        }

        /// <summary>
        /// Sends a GET request for the provided URL
        /// </summary>
        /// <param name="url">URL to send GET request for</param>
        /// <returns>Content as string</returns>
        /// <exception cref="Exception">Failed to get data after exceeding retries</exception>
        private async Task<string> HttpRequester(string url)
        {
            for (var retries = 1; retries <= MaxRetries; retries++)
            {
                try
                {
                    using (var client = new HttpClient())
                    {
                        client.BaseAddress = new Uri("https://api.quiverquant.com/beta/");
                        client.DefaultRequestHeaders.Clear();

                        // You must supply your API key in the HTTP header,
                        // otherwise you will receive a 403 Forbidden response
                        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Token", _clientKey);

                        // Responses are in JSON: you need to specify the HTTP header Accept: application/json
                        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                        // Makes sure we don't overrun Quiver rate limits accidentally
                        _indexGate.WaitToProceed();

                        var response = await client.GetAsync(Uri.EscapeUriString(url));
                        if (response.StatusCode == HttpStatusCode.NotFound)
                        {
                            Log.Error($"QuiverCongressDataDownloader.HttpRequester(): Files not found at url: {Uri.EscapeUriString(url)}");
                            response.DisposeSafely();
                            return string.Empty;
                        }

                        if (response.StatusCode == HttpStatusCode.Unauthorized)
                        {
                            var finalRequestUri = response.RequestMessage.RequestUri; // contains the final location after following the redirect.
                            response = client.GetAsync(finalRequestUri).Result; // Reissue the request. The DefaultRequestHeaders configured on the client will be used, so we don't have to set them again.

                        }

                        response.EnsureSuccessStatusCode();

                        var result =  await response.Content.ReadAsStringAsync();
                        response.DisposeSafely();

                        return result;
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e, $"QuiverCongressDataDownloader.HttpRequester(): Error at HttpRequester. (retry {retries}/{MaxRetries})");
                    Thread.Sleep(1000);
                }
            }

            throw new Exception($"Request failed with no more retries remaining (retry {MaxRetries}/{MaxRetries})");
        }

        /// <summary>
        /// Saves contents to disk, deleting existing zip files
        /// </summary>
        /// <param name="destinationFolder">Final destination of the data</param>
        /// <param name="name">File name</param>
        /// <param name="contents">Contents to write</param>
        private void SaveContentToFile(string destinationFolder, string name, IEnumerable<string> contents)
        {
            var finalPath = Path.Combine(destinationFolder, $"{name.ToLowerInvariant()}.csv");

            var lines = new HashSet<string>(contents);
            if (File.Exists(finalPath))
            {
                foreach (var line in File.ReadAllLines(finalPath))
                {
                    lines.Add(line);
                }
            }

            var finalLines = destinationFolder.Contains("universe")
                ? lines.OrderBy(x => x)
                : lines.OrderBy(x => DateTime.ParseExact(x.Split(',').First(), "yyyyMMdd",
                    CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal));

            var tempPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.tmp");
            File.WriteAllLines(tempPath, finalLines);
            var tempFilePath = new FileInfo(tempPath);
            tempFilePath.MoveTo(finalPath, true);
        }

        /// <summary>
        /// Tries to normalize a potentially defunct ticker into a normal ticker.
        /// </summary>
        /// <param name="ticker">Ticker as received from Estimize</param>
        /// <param name="nonDefunctTicker">Set as the non-defunct ticker</param>
        /// <returns>true for success, false for failure</returns>
        private static bool TryNormalizeDefunctTicker(string ticker, out string nonDefunctTicker)
        {
            // The "defunct" indicator can be in any capitalization/case
            if (ticker.IndexOf("defunct", StringComparison.OrdinalIgnoreCase) > 0)
            {
                foreach (var delimChar in _defunctDelimiters)
                {
                    var length = ticker.IndexOf(delimChar);

                    // Continue until we exhaust all delimiters
                    if (length == -1)
                    {
                        continue;
                    }

                    nonDefunctTicker = ticker.Substring(0, length).Trim();
                    return true;
                }

                nonDefunctTicker = string.Empty;
                return false;
            }

            nonDefunctTicker = ticker;
            return true;
        }


        /// <summary>
        /// Normalizes Estimize tickers to a format usable by the <see cref="Data.Auxiliary.MapFileResolver"/>
        /// </summary>
        /// <param name="ticker">Ticker to normalize</param>
        /// <returns>Normalized ticker</returns>
        private static string NormalizeTicker(string ticker)
        {
            return ticker.ToLowerInvariant()
                .Replace("- defunct", string.Empty)
                .Replace("-defunct", string.Empty)
                .Replace(" ", string.Empty)
                .Replace("|", string.Empty)
                .Replace("-", ".");
        }

        private class Company
        {
            /// <summary>
            /// The name of the company
            /// </summary>
            [JsonProperty(PropertyName = "Name")]
            public string Name { get; set; }

            /// <summary>
            /// The ticker/symbol for the company
            /// </summary>
            [JsonProperty(PropertyName = "Ticker")]
            public string Ticker { get; set; }
        }

        /// <summary>
        /// Disposes of unmanaged resources
        /// </summary>
        public void Dispose()
        {
            _indexGate?.Dispose();
        }
    }
}