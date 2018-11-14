using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Async;
using System.Diagnostics;

namespace CartoTest
{
    class Program
    {
        //    //https://gist.github.com/jorgesancha/2a8027e5a89a2ea1693d63a45afdd8b6
        //    //Download this ~2GB file: https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv
        //    //Count the lines in the file
        //    //Calculate the average value of the tip_amount field.

        static long blockSize = 1024 * 512;
        static long maxLinSize = 1024;
        static int totalLength = 1708674492;

        static void Main(string[] args)
        {
            
            var watch = new Stopwatch();
            watch.Start();

            System.Collections.Concurrent.ConcurrentBag<Tuple<long, decimal>> lista = new System.Collections.Concurrent.ConcurrentBag<Tuple<long, decimal>>();
            ServicePointManager.DefaultConnectionLimit = 10000;

            //Mejora: Parametros parametrizables en variables.
            Enumerable.Range(0, totalLength / (int)blockSize).ParallelForEachAsync(
                async (x) =>
                {
                    var start = x * blockSize;

                    lista.Add(await Hilo(start, x));
                }, 16).Wait();


            watch.Stop();

            var total1 = lista.Sum(x => x.Item1);
            var total2 = lista.Sum(x => x.Item2);

            Console.WriteLine($"Total lineas: {total1}");
            Console.WriteLine($"Media: {total2 / total1}");       
            Console.WriteLine($"Han pasado {watch.Elapsed.Minutes}:{watch.Elapsed.Seconds} segundos");

            Console.ReadKey();
        }

        private static async Task<Tuple<long, decimal>> Hilo(long start, int bloque)
        {

            long lines = 0;
            var client = new HttpClient();
            client.DefaultRequestHeaders.Range = new System.Net.Http.Headers.RangeHeaderValue { };
            client.DefaultRequestHeaders.Range.Ranges.Add(new System.Net.Http.Headers.RangeItemHeaderValue(start, start + blockSize + maxLinSize));

            using (var st = new StreamReader(await client.GetStreamAsync(new Uri("https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv"))))
            {
                var headersPos = 15;
                decimal tipamountacum = 0m;
                long position = st.ReadLine().Length + 2; // la primera linea descartada. 2 es el número de intros

                while (!st.EndOfStream)
                {
                    var l = st.ReadLine();

                    if (decimal.TryParse(l.Split(',')[headersPos], out Decimal tipamountlocal))
                    {
                        tipamountacum += tipamountlocal;
                    }

                    var s = l.Split(',').Length;
                    if (s != 19) //19 es la posición del valor a calcular dentro del csv. Obtener dinamicamente.
                    {
                        throw new Exception("");
                    }

                    lines++;

                    position += l.Length + 2;
                    if (position > blockSize + 1)
                    {
                        break;
                    }
                }

                Console.WriteLine($"Hilo: {bloque} - Lines: {lines} Amount: {tipamountacum}");
                return new Tuple<long, decimal>(lines, tipamountacum);
            }

        }

    }
}

