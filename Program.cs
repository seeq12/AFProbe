using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using NDesk.Options;
using OSIsoft.AF;
using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using OSIsoft.AF.PI;
using OSIsoft.AF.Time;

namespace AFProbe {

    internal class Program {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly string MAX_COLLECT_EXCEPTION_STATUS_CODE = $"[{-11091}]";
        private static readonly ArcMaxCollectHelper arcMaxCollectHelper = new ArcMaxCollectHelper();
        private static readonly AFTimeSpan advance = AFTimeSpan.Parse("2ms");

        private static async Task Main(string[] args) {
            log4net.Config.XmlConfigurator.Configure();

            String AFServerID = null;
            String Username = null;
            String Password = null;
            String requestStart = "2019-01-02T00:00:00.0Z";
            String requestEnd = "2023-11-02T00:00:00.0Z";
            String file = null;
            int maxDegreeOfParallelism = 2;
            bool showHelp = args.Length == 0;
            int maxNumberOfSamplesPerChunk = 5000000;

            var options = new OptionSet {
                { "a|af_server_id=", "AF Server ID.", id => AFServerID = id },
                { "u|username=", "Optional username.", u => Username = u },
                { "p|password=", "Optional password.", p => Password = p },
                { "s|start=", $"Request start (ISO time. It defaults to {requestStart}).", x => requestStart = x },
                { "e|end=", $"Request end (ISO time). It defaults to {requestEnd}", x => requestEnd = x },
                { "c|samples_per_chunk=", $"Maximum number of samples to get in a chunk. It default to {maxNumberOfSamplesPerChunk}", (int x) => maxNumberOfSamplesPerChunk = x },
                { "t|threads=", "Max degree of parallelism.", (int x) => maxDegreeOfParallelism = x },
                { "f|file=", $"File from where to read the data IDs (one per line).", x => file = x }
            };

            options.Parse(args);

            log.Debug($"AFProbe called with\n" +
                $"  af_server_id={AFServerID}\n" +
                $"  username={Username}\n" +
                $"  start={requestStart}\n" +
                $"  end={requestEnd}\n" +
                $"  samples_per_chunk={maxNumberOfSamplesPerChunk}\n" +
                $"  threads={maxDegreeOfParallelism}\n" +
                $"  file={file}");

            if (showHelp) {
                ShowUsage(options);
                WaitForKey();
                return;
            }

            if (AFServerID == null) {
                Console.WriteLine("af_server_id parameter is mandatory");

                WaitForKey();
                return;
            }

            if (file == null) {
                Console.WriteLine("file parameter is mandatory");

                WaitForKey();
                return;
            }

            PISystem server = GetServer(AFServerID);

            try {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                log.Debug("Reading the file");
                List<string> dataIds = File.ReadAllLines(file).ToList();

                Connect(Username, Password, server);

                log.Debug($"Server Information - " +
                    $"Name: {server.Name}  " +
                    $"Id: {server.UniqueID}  " +
                    $"Version:{server.ServerVersion}  " +
                    $"Time:{server.ServerTime.UtcTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")}");

                SemaphoreSlim semaphore = new SemaphoreSlim(maxDegreeOfParallelism);

                log.Debug($"Creating {maxDegreeOfParallelism} threads");
                List<Task> tasks = new List<Task>();
                foreach (var dataId in dataIds) {
                    tasks.Add(Task.Run(async () => {
                        await semaphore.WaitAsync();
                        try {
                            GetSamples(server, dataId, requestStart, requestEnd, arcMaxCollectHelper, maxNumberOfSamplesPerChunk);
                        } catch (Exception e) {
                            log.Error($"Error getting samples for {dataId}", e);
                        } finally {
                            semaphore.Release();
                        }
                    }));
                }

                // Wait for all tasks to complete
                await Task.WhenAll(tasks);

                log.Debug($"GetSamples finished for all requests in {stopwatch.Elapsed.TotalSeconds} seconds");
            } catch (Exception e) {
                log.Error("Error", e);
            } finally {
                server.Disconnect();
                WaitForKey();
            }
        }

        private static void WaitForKey() {
            Console.WriteLine("Press any key to finish the application.");
            Console.ReadKey();
        }

        private static void ShowUsage(OptionSet options) {
            Console.WriteLine("Usage:");
            options.WriteOptionDescriptions(Console.Out);
        }

        private static void Connect(string Username, string Password, PISystem server) {
            NetworkCredential afSystemNetCredential = null;
            if (Username != null && Password != null)
                afSystemNetCredential = new NetworkCredential(Username, Password);

            if (afSystemNetCredential != null) {
                log.Debug("Connecting to AF Server with provided credentials");
                server.Connect(afSystemNetCredential);
            } else {
                log.Debug("Connecting to AF Server with Windows security");
                server.Connect();
            }

            log.Debug("Connected!");
        }

        private static PISystem GetServer(string AFServerID) {
            var piSystems = new OSIsoft.AF.PISystems().ToList();
            foreach (var piSystem in piSystems) {
                piSystem.ConnectionInfo.Preference = OSIsoft.AF.AFConnectionPreference.Any;
            }

            var server = piSystems.Find(x => x.ID.ToString() == AFServerID || x.UniqueID == AFServerID);
            return server;
        }

        private static void GetSamples(PISystem server, string DataId, string requestStart, string requestEnd, ArcMaxCollectHelper arcMaxCollectHelper, int maxNumberOfSamplesPerChunk) {
            AFElement afElement;
            AFAttribute afAttribute;
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            GetElementAndAttributeFromDataID(server, DataId, out afElement, out afAttribute);
            log.DebugFormat("GetElementAndAttributeFromDataID for {0} took {1} milliseconds", DataId, stopwatch.Elapsed.TotalMilliseconds);
            stopwatch.Restart();

            GetLastCertainSignalKey(server, afAttribute, DataId);
            log.DebugFormat("GetLastCertainSignalKey for {0} took {1} milliseconds", DataId, stopwatch.Elapsed.TotalMilliseconds);
            stopwatch.Restart();

            var count = HandleSeriesRequest(afAttribute, requestStart, requestEnd, DataId, arcMaxCollectHelper, maxNumberOfSamplesPerChunk);
            stopwatch.Stop();
            log.DebugFormat("HandleSeriesRequest for {0} took {1} milliseconds and returned {2} samples", DataId, stopwatch.Elapsed.TotalMilliseconds, count);
        }

        private delegate OSIsoft.AF.Asset.AFValues ValuesFunction(AFTimeRange timeRange, AFBoundaryType boundaryType, string filterExpression, bool includeFilteredValues, int maxCount);

        private static int HandleSeriesRequest(AFAttribute afAttribute, string requestStartStr, string requestEndStr, string dataId, ArcMaxCollectHelper arcMaxCollectHelper, int maxNumberOfSamplesPerChunk) {
            bool queryPiPoint = afAttribute.DataReference?.Name == PI_POINT;
            bool useRecordedValues = supportsRecordedValues(afAttribute);
            ValuesFunction valuesFn;

            if (useRecordedValues) {
                valuesFn = (AFTimeRange timeRange, AFBoundaryType boundaryType, string filterExpression, bool includeFilteredValues, int maxCount) => {
                    return afAttribute.Data.RecordedValues(timeRange, boundaryType, null, filterExpression, includeFilteredValues, maxCount);
                };
            } else {
                valuesFn = (AFTimeRange timeRange, AFBoundaryType boundaryType, string filterExpression, bool includeFilteredValues, int maxCount) => {
                    return afAttribute.GetValues(timeRange, maxCount, null);
                };
            }

            AFTime requestStart = new AFTime(requestStartStr);
            AFTime requestEnd = new AFTime(requestEndStr);

            int sampleLimit = 5000000;

            AFTime lastTimestamp = DateTime.MinValue;

            int afValuesCount = 0;

            bool hasMoreSamples = true;
            while (hasMoreSamples) {
                AFTimeRange interval = new AFTimeRange(requestStart, requestEnd);

                int currentSampleLimit;
                AFValues afValues = new AFValues();

                while (true) {
                    currentSampleLimit = Math.Min(maxNumberOfSamplesPerChunk, arcMaxCollectHelper.CalculateSampleLimit(sampleLimit));

                    // RequestCancellation.Check()
                    Thread.Sleep(0);

                    try {
                        log.DebugFormat($"Calling valueFn({dataId}, {interval}, limit={currentSampleLimit})");
                        if (queryPiPoint || useRecordedValues) {
                            afValues = valuesFn(interval, AFBoundaryType.Outside, "", true, currentSampleLimit);
                        } else {
                            afValues = valuesFn(interval, AFBoundaryType.Outside, "", true, 0);
                        }
                    } catch (PIException piException) {
                        if (piException.Message.Contains(MAX_COLLECT_EXCEPTION_STATUS_CODE)) {
                            // Lower the sample limit until it's low enough
                            arcMaxCollectHelper.ThisSampleLimitIsTooLarge(currentSampleLimit);
                            continue;
                        } else {
                            throw;
                        }
                    }
                    if (requestWasTooLarge(afValues)) {
                        // Lower the sample limit until it's low enough
                        arcMaxCollectHelper.ThisSampleLimitIsTooLarge(currentSampleLimit);
                        continue;
                    }

                    throwCertainLoneAFValueExceptions(afValues);

                    hasMoreSamples = afValues.Count >= currentSampleLimit;

                    arcMaxCollectHelper.ThisSampleCountWorked(afValues.Count);

                    break;
                }

                for (int i = 0; i < afValues.Count; i++) {
                    OSIsoft.AF.Asset.AFValue afValue = afValues.ElementAt(i);
                    if (i < afValues.Count - 1) {
                        OSIsoft.AF.Asset.AFValue nextValue = afValues.ElementAt(i + 1);
                        // In the case of multiple samples at the same value, we want to take the last one.
                        if (nextValue.Timestamp == afValue.Timestamp) {
                            continue;
                        }
                    } else if (hasMoreSamples) {
                        // If the number of samples at that timestamp fills the entire return collection, take the last one.
                        // Otherwise, we will keep requesting at the same timestamp and get into an infinite loop.
                        if (afValues.ElementAt(0).Timestamp == afValues.ElementAt(afValues.Count - 1).Timestamp) {
                            log.WarnFormat("Too many samples at key {0}, returning the last one received from PI with a sample limit of {1}",
                                afValue.Timestamp, currentSampleLimit);
                            // We need to set requestStart after the current timestamp, or else we will get the same set of results again.
                            requestStart = afValue.Timestamp + advance;
                        } else {
                            // If we think there may be more samples at the current key within the currentSampleLimit,
                            // we want to make a new request starting at that key.
                            requestStart = afValue.Timestamp;
                            continue;
                        }
                    }

                    if (afValue.Timestamp <= lastTimestamp) {
                        continue;
                    }

                    // lastTimestamp is used to filter out boundary values when multiple queries to PI are
                    // necessary to fulfill the request.
                    lastTimestamp = afValue.Timestamp;

                    // If hasMoreSamples is true, we will issue another request where the interval starts at
                    // the time of the last timestamp.
                    // If requestStart has already been incremented, we skip setting it here.
                    if (afValue.Timestamp > requestStart) {
                        requestStart = afValue.Timestamp;
                    }

                    afValuesCount++;
                }
            }

            return afValuesCount;
        }

        private static void throwCertainLoneAFValueExceptions(OSIsoft.AF.Asset.AFValues afValues) {
            if (afValues != null && afValues.Count == 1 && afValues.First().Value is Exception) {
                Exception loneAFValueException = (Exception)afValues.First().Value;
                List<string> ignorableExceptionMessages = new List<string> { "Data was not available for Attribute" };
                bool ignorable = ignorableExceptionMessages.Contains(loneAFValueException.Message);
                if (!ignorable) {
                    throw loneAFValueException;
                };
            }
        }

        private static bool requestWasTooLarge(OSIsoft.AF.Asset.AFValues afValues) {
            return afValues != null
                && afValues.Any()
                && afValues.First().Value is Exception
                && ((Exception)afValues.First().Value).Message.Contains(MAX_COLLECT_EXCEPTION_STATUS_CODE);
        }

        private static bool supportsRecordedValues(AFAttribute afAttribute) {
            return (afAttribute.SupportedDataMethods & AFDataMethods.RecordedValues) != AFDataMethods.None;
        }

        private static void GetLastCertainSignalKey(PISystem server, AFAttribute afAttribute, string dataId) {
            OSIsoft.AF.Asset.AFValue value;
            DateTime? valueDateTime;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            bool supportsRecorededValues = supportsRecordedValues(afAttribute);
            log.Debug($"supportsRecorededValues for {dataId} took {stopwatch.Elapsed.TotalMilliseconds} milliseconds");

            if (!supportsRecorededValues) {
                valueDateTime = null;
            } else {
                stopwatch.Restart();
                value = afAttribute.Data.RecordedValue(afOrPiServerTime(afAttribute), OSIsoft.AF.Data.AFRetrievalMode.AtOrBefore, null);
                log.Debug($" afAttribute.Data.RecordedValue for {dataId} took {stopwatch.Elapsed.TotalMilliseconds} milliseconds");

                valueDateTime = value?.Timestamp.UtcTime;
            }

            // production code would return the time. Not needed here.
        }

        private static readonly string PI_POINT = "PI Point";

        private static DateTime afOrPiServerTime(IAFAttribute afAttribute) {
            return afAttribute.DataReference?.Name == PI_POINT ?
                afAttribute.DataReference.PIPoint.Server.ServerTime.UtcTime :
                afAttribute.PISystem.ServerTime.UtcTime;
        }

        private static void GetElementAndAttributeFromDataID(PISystem server, string dataID, out AFElement afElement, out AFAttribute afAttribute) {
            Guid afElementID;
            Guid afAttributeID;
            DecodeElementAndAttributeIDsFromDataID(dataID, out afElementID, out afAttributeID);
            afElement = AFElement.FindElement(server, afElementID);
            if (afElement == null) {
                throw new Exception(string.Format("AFElement for DataId '{0}' not found on AF Server", dataID));
            }

            if (afAttributeID != Guid.Empty) {
                afAttribute = AFAttribute.FindAttribute(afElement, afAttributeID);
                if (afAttribute == null) {
                    throw new Exception(string.Format("AFAttribute for DataId '{0}' not found on AF Server", dataID));
                }
            } else {
                afAttribute = null;
            }
        }

        private static readonly string ScalarDataIdSuffix = "-Scalar";

        private static void DecodeElementAndAttributeIDsFromDataID(string dataID, out Guid afContainingElementID, out Guid afAttributeID) {
            if (dataID.EndsWith(ScalarDataIdSuffix)) {
                // We can safely strip the scalar part because it's only for our use, and is never used for a live request.
                dataID = dataID.Remove(dataID.Length - ScalarDataIdSuffix.Length, ScalarDataIdSuffix.Length);
            }
            if (IsElementReferenceDataId(dataID)) {
                dataID = GetOriginalDataIdFromPrefixed(dataID);
            }

            string[] ids = dataID.Split('/');
            afContainingElementID = Guid.Parse(ids[0]);
            afAttributeID = ids.Length > 1 ? Guid.Parse(ids[1]) : Guid.Empty;
        }

        public static string ElementReferencePrefix = "REF:";
        public static char ElementReferenceSeparator = '_';

        private static bool IsElementReferenceDataId(string dataId) {
            Regex guidRegex = new Regex(@"(\w{8}-(\w{4}-){3}\w{12})");
            return dataId.Contains(ElementReferencePrefix)
                && guidRegex.Matches(dataId).Count > 1;
        }

        public static string GetOriginalDataIdFromPrefixed(string dataId) {
            return dataId.Substring(dataId.LastIndexOf(ElementReferenceSeparator) + 1);
        }
    }

    public class Stopwatch {
        private readonly System.Diagnostics.Stopwatch real;

        public Stopwatch() {
            this.real = new System.Diagnostics.Stopwatch();
        }

        public virtual void Start() {
            this.real.Start();
        }

        public virtual void Stop() {
            this.real.Stop();
        }

        public virtual void Reset() {
            this.real.Reset();
        }

        public virtual void Restart() {
            this.real.Reset();
            this.real.Start();
        }

        public virtual TimeSpan Elapsed => this.real.Elapsed;

        public virtual bool IsRunning => this.real.IsRunning;
    }

    public class ArcMaxCollectHelper {
        private readonly object sync = new object();
        private int? tooLarge = null;
        private int okay = 1;

        /// <summary>
        /// Call this function if you execute a query and the EventCollectionExceededMaximumAllowed [-11091]
        /// exception is thrown.
        /// </summary>
        /// <param name="sampleLimitThatIsTooLarge">The sample limit (maxCount) that was attempted</param>
        public void ThisSampleLimitIsTooLarge(int sampleLimitThatIsTooLarge) {
            ensureNonNegativeLimit(sampleLimitThatIsTooLarge);

            lock (this.sync) {
                if (this.tooLarge.HasValue) {
                    this.tooLarge = Math.Min(this.tooLarge.Value, sampleLimitThatIsTooLarge);
                } else {
                    this.tooLarge = sampleLimitThatIsTooLarge;
                }

                if (this.tooLarge <= this.okay) {
                    // Suddenly a sample limit that worked before doesn't work anymore!
                    // ArcMaxCollect have changed on the PI server. Reset the binary search.
                    this.okay = 1;
                }
            }
        }

        /// <summary>
        /// Call this function if you execute a query and it was successful.
        /// </summary>
        /// <param name="sampleCountThatWorked">The number of samples returned by your query. Note that this may be different from the limit that you passed in.</param>
        public void ThisSampleCountWorked(int sampleCountThatWorked) {
            ensureNonNegativeLimit(sampleCountThatWorked);

            lock (this.sync) {
                this.okay = Math.Max(this.okay, sampleCountThatWorked);
            }
        }

        /// <summary>
        /// Call this function to determine the next sample limit to attempt, based on the binary search.
        /// </summary>
        /// <param name="seeqImposedSampleLimit">The sample limit that is dictated by Seeq AppServer, not to be exceeded</param>
        /// <returns></returns>
        public int CalculateSampleLimit(int seeqImposedSampleLimit) {
            lock (this.sync) {
                if (this.tooLarge.HasValue == false) {
                    return seeqImposedSampleLimit;
                }

                if (Math.Abs(this.tooLarge.Value - this.okay) < 10) {
                    return Math.Min(this.okay, seeqImposedSampleLimit);
                }

                int sampleLimit = (this.tooLarge.Value + this.okay) / 2;

                sampleLimit = Math.Min(sampleLimit, seeqImposedSampleLimit);

                return sampleLimit;
            }
        }

        private static void ensureNonNegativeLimit(int sampleLimit) {
            if (sampleLimit < 0) {
                throw new ArgumentException("Sample limit must be non-negative, but was " + sampleLimit);
            }
        }
    }
}