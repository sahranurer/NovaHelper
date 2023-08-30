using RetSharp.ConnectionsModel;
using RetSharp.Handler;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using RetSharp.Model;

namespace RetSharp
{
    public class RetDbConnector : IRetDbConnector
    {
        public readonly string _connectionString;
        private readonly MysqlHandler _mysqlHandler;
        private readonly MsSqlHandler _mssqlHandler;
        private readonly ConnectionType _type;
        public RetDbConnector(string connectionString, ConnectionType type)
        {
            _type = type;
            _connectionString = connectionString;
            if (type == ConnectionType.MySql)
                _mysqlHandler = new MysqlHandler(_connectionString);
            else if (type == ConnectionType.MsSQL)
                _mssqlHandler = new MsSqlHandler(_connectionString);

        }
        public RetDbConnector(string connectionString, ConnectionType type, bool useTransaction)
        {
            _type = type;
            _connectionString = connectionString;
            if (useTransaction)
            {
                if (type == ConnectionType.MySql)
                    _mysqlHandler = new MysqlHandler(_connectionString, useTransaction);
                else if (type == ConnectionType.MsSQL)
                    _mssqlHandler = new MsSqlHandler(_connectionString, useTransaction);
            }
            else
            {
                if (type == ConnectionType.MySql)
                    _mysqlHandler = new MysqlHandler(_connectionString);
                else if (type == ConnectionType.MsSQL)
                    _mssqlHandler = new MsSqlHandler(_connectionString);
            }
        }

        #region Query
        public IEnumerable<T> Query<T>(string sql) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.Query<T>(sql);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.Query<T>(sql);
            }
            else
            {
                return null;
            }
        }
        public PagedList<T> QueryPagedList<T>(string sql, int pagesize, int pagenumber) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.QueryPagedList<T>(sql, pagesize, pagenumber);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.QueryPagedList<T>(sql, pagesize, pagenumber);
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Where
        public IEnumerable<T> Where<T>(Expression<Func<T, bool>> predicate, int? limit = null, int? pagesize = null, int? pagenumber = null) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.Where(predicate, limit, pagesize, pagenumber);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.Where(predicate);
            }
            else
            {
                return null;
            }         
        }
        public PagedList<T> WherePagedList<T>(Expression<Func<T, bool>> predicate, int pagesize, int pagenumber, int? limit = null) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.WherePagedList(predicate, pagesize, pagenumber, limit);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.WherePagedList(predicate, pagesize, pagenumber, limit);
            }
            else
            {
                return null;
            }
        }
        public async Task<IEnumerable<T>> WhereAsync<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return await _mysqlHandler.WhereAsync(predicate, limit);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.Where(predicate);
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Insert
        public T Insert<T>(T model) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.Insert<T>(model, _type);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return MsSqlHandler.Insert(model, _type);
            }
            else
            {
                return null;
            }

        }
        #endregion

        #region Update
        public T Update<T>(T model) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.Update<T>(model, _type);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return MsSqlHandler.Update(model, _type);
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region InsertRange
        public List<T> InsertRange<T>(List<T> model) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.InsertRange<T>(model, _type);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return MsSqlHandler.Insert(model, _type);
            }
            else
            {
                return null;
            }

        }
        #endregion

        #region UpdateRange
        public List<T> UpdateRange<T>(List<T> model) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.UpdateRange<T>(model, _type);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return MsSqlHandler.Update(model, _type);
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Commit
        public void Commit()
        {
            if (_type == ConnectionType.MySql)
                _mysqlHandler.Commit();
            else if (_type == ConnectionType.MsSQL)
                _mssqlHandler.Commit();
        }
        #endregion

        #region Any
        public bool Any<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new()
        {
            bool result = false;
            if (_type == ConnectionType.MySql)
            {
                result = _mysqlHandler.Any(predicate, limit);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                //return _mssqlHandler.Where(predicate);
            }
            else
            {
                return false;
            }
            return result;
        }

        #endregion

        #region SearchPredicate

        private static readonly MethodInfo Contains = typeof(string)
    .GetMethod(nameof(string.Contains), new Type[] { typeof(string) });
        public Expression<Func<T, bool>> SearchPredicate<T>(IEnumerable<T> properties, string searchText)
        {
            var param = Expression.Parameter(typeof(T));
            var search = Expression.Constant(searchText);
            var components = properties
                .Select(propName => Expression.Call(Expression.Property(param, propName.ToString()), Contains, search))
                .Cast<Expression>()
                .ToList();
            // This is the part that you were missing
            var body = components
                .Skip(1)
                .Aggregate(components[0], Expression.OrElse);
            return Expression.Lambda<Func<T, bool>>(body, param);
        }
        #endregion

        #region DataTableToList
        public List<T> DataTableToList<T>() where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.DataTableToList<T>();
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.DataTableToList<T>();
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region CreateDatabaseQuery
        public bool CreateDatabaseQuery(string sql)
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.CreateDatabaseQuery(sql);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.CreateDatabaseQuery(sql);
            }
            else
            {
                return false;
            }
        }
        #endregion

        #region InsertTable
        public T InsertTable<T>(string databaseName, T model) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.InsertTable<T>(databaseName, model, _type);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mysqlHandler.InsertTable<T>(databaseName, model, _type);
            }
            else
            {
                return null;
            }
        }

        public T InsertTableTest<T>(string databaseName, T model) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.InsertTableTest<T>(databaseName, model, _type);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mysqlHandler.InsertTable<T>(databaseName, model, _type);
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region CreateView
        public Result<bool> CreateSql(string databaseName, string sql)
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.CreateSql(databaseName, sql);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return null;
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Count
        public int Count<T>(Expression<Func<T, bool>> predicate, string keyName, int? limit = null) where T : class, new()
        {
            int result = 0;
            if (_type == ConnectionType.MySql)
            {
                result = _mysqlHandler.Count(predicate, keyName, limit);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                //return _mssqlHandler.Where(predicate);
            }
            else
            {
                return result;
            }
            return result;
        }
        #endregion

        #region Delete
        public bool Delete<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.Delete(predicate, limit);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.Delete(predicate);
            }
            else
            {
                return false;
            }
        }


        #endregion

        public Result<DataTable> TableToList(string sql)
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.TableToList(sql);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return null;
            }
            else
            {
                return null;
            }
        }

        public bool UpdateQuery<T>(string sql) where T : class, new()
        {
            if (_type == ConnectionType.MySql)
            {
                return _mysqlHandler.UpdateQuery<T>(sql);
            }
            else if (_type == ConnectionType.MsSQL)
            {
                return _mssqlHandler.UpdateQuery<T>(sql);
            }
            else
            {
                return false;
            }
        }

        public void Rollback()
        {
            if (_type == ConnectionType.MySql)
                _mysqlHandler.Rollback();
            else if (_type == ConnectionType.MsSQL)
                _mssqlHandler.Rollback();
        }
    }
}
