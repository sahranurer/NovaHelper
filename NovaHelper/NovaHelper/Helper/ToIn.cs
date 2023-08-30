using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RetSharp.Helper
{
    public static class RetSharpIntExtensions
    {
        public static string ToInSting<T>(this List<T> ls)
        {
            string result = string.Empty;
            string inList =  string.Join(", ", ls.Select(t => "'" + t+"'"));
            return inList;
        }
    }
}
