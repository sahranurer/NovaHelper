using RetSharp.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace RetSharp
{
    public interface IRetDbConnector
    {
        Result<DataTable> TableToList(string sql);
        IEnumerable<T> Query<T>(string sql) where T : class, new();
        PagedList<T> QueryPagedList<T>(string sql, int pagesize, int pagenumber) where T : class, new();
        bool UpdateQuery<T>(string sql) where T : class, new();
        bool CreateDatabaseQuery(string sql);
        IEnumerable<T> Where<T>(Expression<Func<T, bool>> predicate, int? limit = null, int? pagesize = null, int? pagenumber = null) where T : class, new();
        PagedList<T> WherePagedList<T>(Expression<Func<T, bool>> predicate, int pagesize, int pagenumber, int? limit = null) where T : class, new();
        Task<IEnumerable<T>> WhereAsync<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new();
        bool Delete<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new();
        bool Any<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new();
        int Count<T>(Expression<Func<T, bool>> predicate, string keyName, int? limit = null) where T : class, new();
        // IEnumerable<T> Table<T>(int? limit = null) where T : class, new();
        T Insert<T>(T model) where T : class, new();
        T InsertTable<T>(string databaseName, T model) where T : class, new();
        T InsertTableTest<T>(string databaseName, T model) where T : class, new();
        Result<bool> CreateSql(string databaseName, string sql);
        List<T> InsertRange<T>(List<T> model) where T : class, new();
        T Update<T>(T model) where T : class, new();
        List<T> UpdateRange<T>(List<T> model) where T : class, new();
        void Commit();
        void Rollback();
        Expression<Func<T, bool>> SearchPredicate<T>(IEnumerable<T> properties, string searchText);
        public List<T> DataTableToList<T>() where T : class, new();
    }
}
