using System.Collections.Generic;

namespace RetSharp.Model
{
    public interface IResult<T>
    {
        public bool Success { get; }
        public string Message { get; }
        public int PageIndex { get; set; }
        public int PageSize { get; set; }
        public int TotalCount { get; set; }
        public int TotalPages { get; set; }
        public bool HasPreviousPage { get; set; }
        public bool HasNextPage { get; set; }
        public List<string> Messages { get; set; }
    }
}
