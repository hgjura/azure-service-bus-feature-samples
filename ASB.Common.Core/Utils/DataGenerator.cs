using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ASB.Common.Core
{
    public static class DataGenerator
    {
        static Random rnd = new Random();

        static Func<Random, int> direction = (x) => rnd.Next(-1, 2);
        static Func<Random, double, double> volatility = (x, y) => (rnd.Next(2, 11) * y * 0.005);
        static Func<double, double> round = (x) => (Math.Round(x, 2));

        public static void GenerateStockQuoteWithCount(int counter, string quote, double imp_volatility, double initial_price, Func<Stock, Task<Stock>> process = null)
        {
            List<Task<Stock>> taskList = new List<Task<Stock>>();

            for (int i = 1; i <= counter; i++)
            {
                int d = direction(rnd);
                double v = volatility(rnd, imp_volatility);

                taskList.Add(process?.Invoke(new Stock { Name = quote, Price = initial_price += round(initial_price * d * (v)), Count = i }));
            }

            Task.WhenAll(taskList);
        }

        public static void GenerateStockQuoteWithCountAndPutItInList(int counter, string quote, double imp_volatility, double initial_price, List<Stock> list = null)
        {
            for (int i = 1; i <= counter; i++)
            {
                int d = direction(rnd);
                double v = volatility(rnd, imp_volatility);

                if (null != list)
                {
                    if (quote == "MSFT")
                    {
                        list.Add(new Stock { Name = quote, Price = initial_price += round(initial_price * d * (v)), Count = i, Exchange = "NASDAQ" });
                    }
                    else
                    {
                        list.Add(new Stock { Name = quote, Price = initial_price += round(initial_price * d * (v)), Count = i, Exchange = quote.Contains(".TO") ? "TSX" : "NYSE" });
                    }
                }
            }

        }


    }
}
