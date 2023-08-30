using System;
using System.Collections.Generic;
using System.Linq;

namespace RetSharp.Model
{
    public class PagedList<T> : List<T>
    {
        public PagedList()
        {

        }
        public int CurrentPage { get;  set; }
        public int TotalPages { get;  set; }
        public int PageSize { get;  set; }
        public int TotalCount { get; set; }
        public bool HasPrevious => CurrentPage > 1;
        public bool HasNext => CurrentPage < TotalPages;
        public List<T> Items { get; set; }

        public PagedList(List<T> items, int pageNumber, int pageSize,int totalCount)
        {
            TotalCount = totalCount;
            PageSize = pageSize;
            CurrentPage = pageNumber;
            TotalPages = (int)Math.Ceiling(TotalCount / (double)pageSize);
            Items = new List<T>();
            Items = items;
            AddRange(items);
        }

       
        public static PagedList<T> ToPagedList(IEnumerable<T> source, int pageNumber, int pageSize, int totalCount)
        {
            var items = source.ToList();
            return new PagedList<T>(items, pageNumber, pageSize, totalCount);
        }
    }
}
