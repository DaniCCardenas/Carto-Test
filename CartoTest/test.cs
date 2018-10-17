using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace CartoTest
{
    public class Test
    {

        public void Read()
        {
            Stream stream = null;
            int bytesToRead = 10000;
            // Buffer to read bytes in chunk size specified above
            byte[] buffer = new Byte[bytesToRead];

            try
            {
                HttpWebRequest fileReq = (HttpWebRequest) WebRequest.Create(@"https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv");
                HttpWebResponse fileResp = (HttpWebResponse) fileReq.GetResponse();

                if (fileReq.ContentLength > 0)
                {
                    fileResp.ContentLength = fileReq.ContentLength;
                }

                stream = fileResp.GetResponseStream();

                int length;
                do
                {
                    // Verify that the client is connected.
                    //if (!HttpContext.RequestAborted.IsCancellationRequested)
                    //{
                        // Read data into the buffer.
                        length = stream.Read(buffer, 0, bytesToRead);

                    // and write it out to the response's output stream
                    //resp.Body.Write(buffer, 0, length);
                    var str = Encoding.Default.GetString(buffer);

                    int? newlinePos = GetIndex(str, "\r") ?? GetIndex(str, "\n");
                    var cabecera = str.Substring(0, newlinePos.Value);

                    Console.WriteLine(str);

                    //Clear the buffer
                    buffer = new Byte[bytesToRead];
                   
                } while (length > 0);
            }
            finally
            {
                stream?.Close();
            }

        }

        // Not really necessary -- just a convenience method.
        static int? GetIndex(string text, string substr)
        {
            int index = text.IndexOf(substr, StringComparison.Ordinal);
            return index >= 0 ? (int?)index : null;
        }
    }
}
