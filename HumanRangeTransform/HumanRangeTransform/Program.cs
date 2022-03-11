using CsvHelper;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace HumanRangeTransform
{
    class Program
    {
        static void Main(string[] args)
        {
            ConcurrentBag<CtHumanRange> humanRanges = new();
            var start = DateTime.Now;
            try
            {
                /*
                 * BCP Command
                 * ============
                 * bcp "select 'CustomerId','Country','City','Number' union all select * from [usr].[SyncableCTNumbersToRibbon]" queryout C:\temp\numbers.txt -T -S jk-dev-13 -d "Master Data Service" -t "," -w
                 */
                using (var reader = new StreamReader(@"C:\temp\numbers.txt"))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    var raw = csv.GetRecords<CtNumber>().OrderBy(m => m.CustomerId).ThenBy(m => m.Number).ToList();
                    var rawGrouped = raw.GroupBy(e => e.CustomerId);
                    var records2 = rawGrouped.Select(m => Tuple.Create<string, List<long?>>(m.Key, m.Select(o => o.Number).ToList()));

                    var startLong = DateTime.Now;
                    CheckUsingLong(humanRanges, records2);
                    var elaspedLong = DateTime.Now.Subtract(startLong).TotalSeconds;

                    Console.WriteLine($"Transforming to Human Range took: {elaspedLong} seconds");

                    var check = humanRanges.Sum(h => h.DdiCount) == raw.Count;
                    if (!check)
                    {
                        throw new Exception($"not match: {humanRanges.Sum(h => h.DdiCount)} {raw.Count}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            var elapsed = DateTime.Now.Subtract(start).TotalSeconds;
            Console.WriteLine($"done in {elapsed} seconds");
        }

        private static void CheckUsingLong(ConcurrentBag<CtHumanRange> humanRanges, IEnumerable<Tuple<string, List<long?>>> records2)
        {
            Parallel.ForEach(records2, r =>
            {
                Console.WriteLine("calculation account " + r.Item1);
                var r2 = r.Item2.ToArray();

                for (int i = 0; i < r2.Length; i++)
                {
                    int groupIndex = 0;
                    var j = i;
                    do
                    {
                        groupIndex++;
                        j++;
                    } while (j < r2.Length && (r2[i + (groupIndex - 1)] - r2[j]) == -1);

                    var group = new ArraySegment<long?>(r2, i, groupIndex);

                    if (group.Count == 0)
                    {

                        humanRanges.Add(new CtHumanRange()
                        {
                            CustomerId = r.Item1,
                            RangeStart = r.Item2[i],
                            RangeEnd = r.Item2[i]
                        });
                    }
                    else
                    {
                        i = i + group.Count - 1;
                        humanRanges.Add(new CtHumanRange()
                        {
                            CustomerId = r.Item1,
                            RangeStart = group[0],
                            RangeEnd = group[^1]
                        });
                    }
                }
            });
        }
    }

    class CtNumber
    {
        public string CustomerId { get; set; }

        public long? Number { get; set; }
    }

    class CtHumanRange
    {
        public string CustomerId { get; set; }

        public long? RangeStart { get; set; }

        public long? RangeEnd { get; set; }

        public int DdiCount => (int)(RangeEnd - RangeStart) + 1;
    }
}
