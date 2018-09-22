using System;
using System.Collections.Generic;
using System.Drawing;
using System.Text;
using System.Threading.Tasks;
using Alba.CsConsoleFormat;
using Newtonsoft.Json;

namespace ASB.Common.Core
{
    public static class Utils
    {
        static int[] cColors = {
                        0x000080, //DarkBlue = 1
                        0x008000, //DarkGreen = 2
                        0x008080, //DarkCyan = 3
                        0x800000, //DarkRed = 4
                        0x800080, //DarkMagenta = 5
                        0x808000, //DarkYellow = 6

                        0x808080, //DarkGray = 8
                        0x0000FF, //Blue = 9
                        0x00FF00, //Green = 10
                        0x00FFFF, //Cyan = 11
                        0xFF0000, //Red = 12
                        0xFF00FF, //Magenta = 13
                        0xFFFF00, //Yellow = 14
                    };
        public static Color RandomColor()
        {
            return Color.FromArgb(cColors[new Random().Next(0, cColors.Length-1)]);
        }
        public static Color GetColor(int X)
        {
            return X >= cColors.Length ? Color.FromArgb(cColors[cColors.Length % X]) : Color.FromArgb(cColors[X]);
        }
        public static void ConsoleWriteHeader(string Header, ConsoleColor Color)
        {
            var b = new Border
            {
                Stroke = LineThickness.Single,
                Align = Align.Left
            };
            b.Children.Add(new Span($" {Header} ") { Color = Color });
            ConsoleRenderer.RenderDocument(new Document(b));
        }

        public static void ConsoleWrite(string Text, ConsoleColor BackgroundColor = ConsoleColor.Black, ConsoleColor ForegroundColor = ConsoleColor.White)
        {
            ConsoleRenderer.RenderDocument(new Document(new Span($"{Text}") { Color = ForegroundColor, Background = BackgroundColor }));
        }

        public static byte[] Serialize<T>(T Item)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(Item));
        }

        public static T Deserialize<T>(byte[] Item)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(Item));
        }
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
}
