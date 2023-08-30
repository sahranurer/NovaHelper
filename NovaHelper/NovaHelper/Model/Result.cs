using System;
using System.Collections.Generic;

namespace RetSharp.Model
{
    public class Result<T> : IResult<T>
    {
        public Result(bool success)
        {
            Data = (T)Activator.CreateInstance(typeof(T));
            Success = success;
            Messages = new List<string>();
        }
        public bool Success { get; set; }
        public string Message { get; set; }
        public List<string> Messages { get; set; }
        public int TotalCount { get; set; }
        public int PageIndex { get; set; }
        public int PageSize { get; set; }

        private int _totalPages;
        public int TotalPages
        {
            get { return _totalPages; }
            set
            {
                _totalPages = value;
                HasPreviousPage = value != 0 && PageIndex != 0;
                HasNextPage = value != 0 && PageIndex < TotalPages;
            }
        }
        public bool HasPreviousPage { get; set; }
        public bool HasNextPage { get; set; }
        public T Data { get; set; }

    }

    public class ResultIntegrator<T>
    {
        public ResultIntegrator(bool successx)
        {
            Data = (T)Activator.CreateInstance(typeof(T));
            success = successx;
            message = new List<string>();
        }
        public bool success { get; set; }
        public List<string> message { get; set; }
        public T Data { get; set; }

    }
}
