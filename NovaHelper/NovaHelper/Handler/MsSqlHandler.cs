using RetSharp.ConnectionsModel;
using RetSharp.Helper;
using RetSharp.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace RetSharp.Handler
{
	public class MsSqlHandler
	{
		private static string _connectionString;
		private readonly SqlConnection _connection;
		private readonly bool _useTransaction;
		private readonly SqlTransaction _transaction;
		public MsSqlHandler(string connectionString)
		{
			_connectionString = connectionString;
		}
		public MsSqlHandler(string connectionString, bool useTransaction)
		{
			_connectionString = connectionString;
			_useTransaction = useTransaction;
			_connection = new SqlConnection(_connectionString);
			if (_connection.State == ConnectionState.Closed)
				_connection.Open();
			_transaction = _connection.BeginTransaction();
		}
		#region Where
		public IEnumerable<T> Where<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new()
		{
			T obj = new();
			var data = new List<T>();
			string sql = string.Empty;
			string select = string.Format(@"Select * from  {0} ", obj.GetType().Name);
			string where = SQLHelper.CreateWhereClause<T>(predicate);
			if (!string.IsNullOrEmpty(where))
				sql = string.Format("{0} where {1}", select, where);
			else
				sql = select;
			if (limit.HasValue)
				sql = string.Format("{0} Limit {1}", sql, limit.Value);

			using (SqlConnection conn = new(_connectionString))
			{
				conn.Open();
				SqlCommand cmd = new(sql, conn);
				SqlDataReader dataReader = cmd.ExecuteReader();
				DataSet ds = new DataSet();
				DataTable dataTable = new DataTable();
				ds.Tables.Add(dataTable);
				ds.EnforceConstraints = false;
				dataTable.Load(dataReader);
				dataReader.Close();
				conn.Close();
				data = SQLHelper.DataTableToList<T>(dataTable);
			}
			foreach (var item in data)
			{
				foreach (var prop in item.GetType().GetProperties())
				{
					if (prop.PropertyType.FullName.StartsWith("System.Collections"))
					{
						var get = prop.GetGetMethod();
						var fullName = get.GetBaseDefinition().ReturnType.GenericTypeArguments[0].AssemblyQualifiedName;
						var myObj = Activator.CreateInstance(Type.GetType(fullName));
						var listType = typeof(List<>);
						var constructedListType = listType.MakeGenericType(myObj.GetType());
						var ls = Activator.CreateInstance(constructedListType);
						Type t = Type.GetType(fullName);
						foreach (var propertyInfo in myObj.GetType().GetProperties())
						{
							if (Attribute.GetCustomAttribute(propertyInfo, typeof(ForeignKeyAttribute)) is ForeignKeyAttribute fk)
							{
								// PropertyInfo propx = myObj.GetType().GetProperty(propertyInfo.Name);
							}
							else
							{
							}
						}
					}
					else if (!prop.PropertyType.FullName.StartsWith("System."))
					{
					}
				}
			}
			return data;
		}
		public PagedList<T> WherePagedList<T>(Expression<Func<T, bool>> predicate, int pagesize, int pagenumber, int? limit) where T : class, new()
		{
			T obj = new();
			var data = new List<T>();
			var countsql = string.Empty;
			string sql = string.Empty;
			string select = string.Format(@"Select * from  {0} ", obj.GetType().Name);
			string where = SQLHelper.CreateWhereClause<T>(predicate);
			if (!string.IsNullOrEmpty(where))
				sql = string.Format("{0} where {1}", select, where);
			else
				sql = select;
			if (limit.HasValue)
				sql = string.Format("{0} Limit {1}", sql, limit.Value);

			using (SqlConnection conn = new(_connectionString))
			{
				conn.Open();
				SqlCommand cmd = new(sql, conn);
				SqlDataReader dataReader = cmd.ExecuteReader();
				DataSet ds = new DataSet();
				DataTable dataTable = new DataTable();
				ds.Tables.Add(dataTable);
				ds.EnforceConstraints = false;
				dataTable.Load(dataReader);
				dataReader.Close();
				conn.Close();
				data = SQLHelper.DataTableToList<T>(dataTable);
			}
			foreach (var item in data)
			{
				foreach (var prop in item.GetType().GetProperties())
				{
					if (prop.PropertyType.FullName.StartsWith("System.Collections"))
					{
						var get = prop.GetGetMethod();
						var fullName = get.GetBaseDefinition().ReturnType.GenericTypeArguments[0].AssemblyQualifiedName;
						var myObj = Activator.CreateInstance(Type.GetType(fullName));
						var listType = typeof(List<>);
						var constructedListType = listType.MakeGenericType(myObj.GetType());
						var ls = Activator.CreateInstance(constructedListType);
						Type t = Type.GetType(fullName);
						foreach (var propertyInfo in myObj.GetType().GetProperties())
						{
							if (Attribute.GetCustomAttribute(propertyInfo, typeof(ForeignKeyAttribute)) is ForeignKeyAttribute fk)
							{
								// PropertyInfo propx = myObj.GetType().GetProperty(propertyInfo.Name);
							}
							else
							{
							}
						}
					}
					else if (!prop.PropertyType.FullName.StartsWith("System."))
					{
					}
				}
			}
			int totalcount = 0;
			if (data.Any())
			{
				using (SqlConnection conn = new(_connectionString))
				{
					conn.Open();
					SqlCommand cmd = new SqlCommand(countsql, conn);
					cmd.CommandTimeout = int.MaxValue;
					totalcount = Convert.ToInt32(cmd.ExecuteScalar());
					conn.Close();
				}
			}
			else
			{
				totalcount = 0;
			}
			var pageddata = PagedList<T>.ToPagedList(data, pagenumber, pagesize, totalcount);
			return pageddata;
		}
		#endregion

		#region Query
		public IEnumerable<T> Query<T>(string sql) where T : class, new()
		{
			using SqlDataAdapter da = new(sql, _connectionString);
			da.SelectCommand.CommandTimeout = 180;
			DataSet dataSet = new();
			da.Fill(dataSet);
			return SQLHelper.DataTableToList<T>(dataSet.Tables[0]);
		}
		public PagedList<T> QueryPagedList<T>(string sql, int pagesize, int pagenumber) where T : class, new()
		{
			string countsql = sql;
			if (pagesize != 0)
			{
				sql = string.Format(" {0} OFFSET {1} ROWS FETCH NEXT  {2} ROWS ONLY", sql, pagesize * pagenumber, pagesize);
			}
			using SqlDataAdapter da = new(sql, _connectionString);
			da.SelectCommand.CommandTimeout = 180;
			DataSet dataSet = new();
			da.Fill(dataSet);
			var data = SQLHelper.DataTableToList<T>(dataSet.Tables[0]);
			int totalcount = 0;
			if (data.Any())
			{
				string toReplace = Regex.Match(countsql, @"from([^\+]+)", RegexOptions.IgnoreCase).Groups[1].Value;
				//countsql = countsql.Replace(toReplace, " count(*) ");
				countsql = string.Format("Select count(*) from {0} ", toReplace);
				string orderbyregdecs = Regex.Match(countsql, "order by([^\\+]+) desc", RegexOptions.IgnoreCase).Groups[1].Value;
				string ascremove = "order by" + orderbyregdecs + " desc";
				countsql = countsql.Replace(ascremove, "", comparisonType: StringComparison.OrdinalIgnoreCase);
				string orderbyregasc = Regex.Match(countsql, "order by([^\\+]+) asc", RegexOptions.IgnoreCase).Groups[1].Value;
				string descremove = "order by" + orderbyregdecs + " asc";
				countsql = countsql.Replace(descremove, "", comparisonType: StringComparison.OrdinalIgnoreCase);
				using (SqlConnection conn = new(_connectionString))
				{
					conn.Open();
					using (SqlCommand cmd = conn.CreateCommand())
					{
						try
						{
							cmd.CommandText = countsql;
							totalcount = Convert.ToInt32(cmd.ExecuteScalar());
						}
						catch { }

					}
				}
			}
			else
			{
				totalcount = 0;
			}
			var pageddata = PagedList<T>.ToPagedList(data, pagenumber, pagesize, totalcount);
			return pageddata;
		}
		#endregion

		public bool UpdateQuery<T>(string sql) where T : class, new()
		{
			using SqlDataAdapter da = new(sql, _connectionString);
			da.SelectCommand.CommandTimeout = 180;
			DataSet dataSet = new();
			da.Fill(dataSet);
			return true;
		}


		#region Insert
		public static T Insert<T>(T model, ConnectionType _type) where T : class, new()
		{
			var insertSql = SQLHelper.GetInsertSql<T>(model, _type);
			using (SqlConnection conn = new(_connectionString))
			{
				conn.Open();
				using (SqlTransaction transaction = conn.BeginTransaction())
				using (SqlCommand cmd = conn.CreateCommand())
				{
					try
					{
						cmd.CommandText = insertSql.SqlCommand;
						foreach (var item in insertSql.SqlParameters)
						{
							SqlParameter p = new SqlParameter();
							p.ParameterName = item.ParameterName;
							p.Value = item.Value;
							cmd.Parameters.Add(p);
						}

						insertSql.PrimaryId = cmd.ExecuteScalar();
						if (insertSql.PrimaryId != null && insertSql.SubSqls.Any())
						{
							var primaryName = string.Format("{0}Id", model.GetType().Name);
							foreach (var isub in insertSql.SubSqls)
							{
								isub.PrimaryName = primaryName;
								InsertSub(isub, cmd, insertSql.PrimaryId);
							}
						}
						transaction.Commit();
						conn.Close();
					}
					catch (Exception e)
					{
						transaction.Rollback();
					}
				}
			}
			return model;
		}

		internal DbTransaction GetTransaction()
		{
			using (SqlConnection conn = new(_connectionString))
			{
				conn.Open();
				return conn.BeginTransaction();
			}
		}
		#endregion

		#region InserSub
		private static InsertSql InsertSub(InsertSql insertSql, SqlCommand cmd, object primaryId)
		{
			try
			{
				cmd.CommandText = insertSql.SqlCommand;
				cmd.Parameters.Clear();
				foreach (var item in insertSql.SqlParameters)
				{
					SqlParameter p = new SqlParameter();
					p.ParameterName = item.ParameterName;
					if (p.ParameterName == insertSql.PrimaryName)
					{
						p.Value = primaryId;
					}
					else
						p.Value = item.Value;
					cmd.Parameters.Add(p);
				}
				insertSql.PrimaryId = cmd.ExecuteScalar();
				if (insertSql.SubSqls.Any() && insertSql.SubSqls.Count != 0)
				{
					foreach (var parameter in insertSql.SubSqls.Where(x => x.SqlParameters != null).ToList())
					{
						cmd.CommandText = parameter.SqlCommand;
						cmd.Parameters.Clear();
						foreach (var item1 in parameter.SqlParameters)
						{
							SqlParameter p = new SqlParameter();
							p.ParameterName = item1.ParameterName;
							if (insertSql.PrimarySubSqls == p.ParameterName)
							{
								p.Value = insertSql.PrimaryId;
							}
							else
								p.Value = item1.Value;

							cmd.Parameters.Add(p);
						}

						insertSql.PrimaryId = cmd.ExecuteScalar();

					}

				}
				return insertSql;
			}
			catch (Exception ex)
			{
				throw new InvalidOperationException(ex.Message);
			}
		}
		#endregion

		#region Update
		public static T Update<T>(T model, ConnectionType _type) where T : class, new()
		{
			var updateSql = SQLHelper.UpdateSql<T>(model, _type);
			using (SqlConnection conn = new SqlConnection(_connectionString))
			{
				conn.Open();
				using (SqlTransaction transaction = conn.BeginTransaction())
				using (SqlCommand cmd = conn.CreateCommand())
				{
					try
					{
						cmd.CommandText = updateSql.SqlCommand;
						foreach (var item in updateSql.SqlParameters)
						{
							SqlParameter p = new SqlParameter();
							p.ParameterName = item.ParameterName;
							p.Value = item.Value;
							cmd.Parameters.Add(p);
						}
						updateSql.PrimaryId = cmd.ExecuteScalar();
						if (updateSql.PrimaryId != null || updateSql.SubSqls.Any())
						{
							var primaryName = string.Format("@{0}Id", model.GetType().Name);
							foreach (var isub in updateSql.SubSqls)
							{
								isub.PrimaryName = primaryName;
								UpdateSub(isub, cmd, updateSql.PrimaryId);
							}
						}
						transaction.Commit();
						conn.Close();

					}
					catch (Exception e)
					{
						transaction.Rollback();
					}
				}
			}
			return model;
		}
		#endregion

		#region UpdateSub
		private static UpdateSql UpdateSub(UpdateSql updateSql, SqlCommand cmd, object primaryId)
		{
			try
			{
				cmd.CommandText = updateSql.SqlCommand;
				cmd.Parameters.Clear();
				foreach (var item in updateSql.SqlParameters)
				{
					SqlParameter p = new SqlParameter();
					p.ParameterName = item.ParameterName;
					if (p.ParameterName == updateSql.PrimaryName)
					{
						p.Value = item.Value;
					}
					else
						p.Value = item.Value;
					cmd.Parameters.Add(p);
				}
				updateSql.PrimaryId = cmd.ExecuteScalar();
				if (updateSql.SubSqls.Any())
				{
					foreach (var item in updateSql.SubSqls)
					{
						UpdateSub(item, cmd, updateSql.PrimaryId);
					}
				}
			}
			catch (Exception ex)
			{
				throw;
			}
			return updateSql;
		}

		#endregion

		#region InsertRange
		public List<T> InsertRange<T>(List<T> model, ConnectionType _type) where T : class, new()
		{

			var insertSqls = SQLHelper.GetInsertSqlList<T>(model, _type);

			foreach (var item in insertSqls)
			{
				using (SqlConnection conn = new SqlConnection(_connectionString))
				{
					conn.Open();
					using SqlTransaction transaction = conn.BeginTransaction();

					try
					{
						using SqlCommand cmd = conn.CreateCommand();
						cmd.CommandText = item.SqlCommand;
						foreach (var item2 in item.SqlParameters)
						{
							SqlParameter p = new SqlParameter();
							p.ParameterName = item2.ParameterName;
							p.Value = item2.Value;
							cmd.Parameters.Add(p);
						}
						item.PrimaryId = cmd.ExecuteNonQuery();
						if (item.PrimaryId != null && item.SubSqls.Any())
						{
							var primaryName = string.Format("{0}Id", model.GetType().Name);
							foreach (var isub in item.SubSqls)
							{
								isub.PrimaryName = primaryName;
								InsertSub(isub, cmd, item.PrimaryId);
							}
						}
						transaction.Commit();
						conn.Close();
					}
					catch (Exception ex)
					{
						transaction.Rollback();
						throw new InvalidOperationException(ex.Message);
					}
				}
			}
			return model;
		}
		#endregion

		#region UpdateRange
		public List<T> UpdateRange<T>(List<T> model, ConnectionType _type) where T : class, new()
		{
			var updateSqls = SQLHelper.UpdateSqlList<T>(model, _type);

			foreach (var item in updateSqls)
			{
				using (SqlConnection conn = new SqlConnection(_connectionString))
				{
					conn.Open();
					using SqlTransaction transaction = conn.BeginTransaction();

					try
					{
						using SqlCommand cmd = conn.CreateCommand();
						cmd.CommandText = item.SqlCommand;
						foreach (var item2 in item.SqlParameters)
						{
							SqlParameter p = new SqlParameter();
							p.ParameterName = item2.ParameterName;
							p.Value = item2.Value;
							cmd.Parameters.Add(p);
						}
						item.PrimaryId = cmd.ExecuteNonQuery();
						if (item.PrimaryId != null || item.SubSqls.Any())
						{
							var primaryName = string.Format("{0}Id", model.GetType().Name);
							foreach (var isub in item.SubSqls)
							{
								isub.PrimaryName = primaryName;
								UpdateSub(isub, cmd, item.PrimaryId);
							}
						}
						transaction.Commit();
						conn.Close();
					}
					catch (Exception ex)
					{
						transaction.Rollback();
						throw new InvalidOperationException(ex.Message);
					}
				}
			}
			return model;
		}
		#endregion

		/// <summary>
		/// Kaydedilen verinin Id sini döner
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		#region SaveExecuteScalar
		private int SaveExecuteScalar(InsertSql model)
		{

			model.SqlCommand += ";SELECT SCOPE_IDENTITY()";
			SqlCommand cmd = new(model.SqlCommand, new SqlConnection(_connectionString));
			if (model.SqlParameters != null)
				foreach (var p in model.SqlParameters)
					cmd.Parameters.Add(p);


			cmd.Connection.Open();
			var i = cmd.ExecuteScalar();
			cmd.Connection.Close();

			return Convert.ToInt32(i);
		}
		#endregion

		/// <summary>
		/// update delete insert için kullanılır
		/// </summary>
		/// <param name="sql"></param>
		/// <param name="parameters"></param>
		/// <returns></returns>
		#region ExecuteNonQuery
		private int ExecuteNonQuery(string sql, List<SqlParameter> parameters)
		{

			SqlCommand cmd = new(sql, new SqlConnection(_connectionString));
			if (parameters != null)
				foreach (var p in parameters)
					cmd.Parameters.Add(p);

			cmd.Connection.Open();
			int i = cmd.ExecuteNonQuery();
			cmd.Connection.Close();
			return i;
		}
		#endregion

		#region Commit
		public void Commit()
		{
			if (_useTransaction && _connection != null && _transaction != null)
			{
				_transaction.Commit();
				if (_connection.State == ConnectionState.Open)
					_connection.Close();
			}
		}
		#endregion

		#region Rollback
		public void Rollback()
		{
			if (_useTransaction && _connection != null && _transaction != null)
			{
				_transaction.Rollback();
				if (_connection.State == ConnectionState.Open)
					_connection.Close();
			}
		}
		#endregion

		#region DataTableToList
		public List<T> DataTableToList<T>() where T : class, new()
		{
			try
			{
				List<T> list = new List<T>();
				//foreach (var row in table.AsEnumerable())
				//{
				//    T obj = new T();
				//    foreach (var prop in obj.GetType().GetProperties())
				//    {
				//        try
				//        {
				//            if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
				//            {
				//                Console.WriteLine("Not Mapped");
				//            }
				//            else
				//            {

				//                PropertyInfo propertyInfo = obj.GetType().GetProperty(prop.Name);
				//                propertyInfo.SetValue(obj, Convert.ChangeType(row[prop.Name], propertyInfo.PropertyType), null);
				//            }
				//        }
				//        catch (Exception ex)
				//        {
				//            continue;
				//        }
				//    }
				//    list.Add(obj);
				//}
				return list;
			}
			catch
			{
				return null;
			}
		}
		#endregion

		#region CreateDatabaseQuery
		public bool CreateDatabaseQuery(string sql)
		{
			if (_useTransaction)
			{
				if (_connection.State == ConnectionState.Closed)
					_connection.Open();
				SqlCommand cmd = new SqlCommand(sql, _connection);
				SqlDataReader dataReader = cmd.ExecuteReader();
				DataSet ds = new DataSet();
				DataTable dataTable = new DataTable();
				ds.Tables.Add(dataTable);
				ds.EnforceConstraints = false;
				dataTable.Load(dataReader);
				return true;
			}
			else
			{
				using (SqlConnection conn = new SqlConnection(_connectionString))
				{
					conn.Open();
					SqlCommand cmd = new SqlCommand(sql, conn);
					SqlDataReader dataReader = cmd.ExecuteReader();
					DataSet ds = new DataSet();
					DataTable dataTable = new DataTable();
					ds.Tables.Add(dataTable);
					ds.EnforceConstraints = false;
					dataTable.Load(dataReader);
					dataReader.Close();
					conn.Close();
					return true;
				}

			}
		}
		#endregion

		#region Delete

		public bool Delete<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new()
		{
			bool result = false;
			T obj = new();
			string select;

			select = string.Format(@"delete from  `{0}` ", obj.GetType().Name);

			string where = SQLHelper.CreateWhereClause<T>(predicate);
			string sql;
			if (!string.IsNullOrEmpty(where))
				sql = string.Format("{0} where {1}", select, where);
			else
				sql = select;
			if (limit.HasValue)
				sql = string.Format("{0} Limit {1}", sql, limit.Value);
			if (_useTransaction)
			{
				if (_connection.State == ConnectionState.Closed)
					_connection.Open();
				SqlCommand sqlCommand = new(sql, _connection);
				SqlCommand cmd = sqlCommand;
				cmd.CommandTimeout = int.MaxValue;
				cmd.CommandText = sql;
				sql = cmd.ExecuteScalar().ToString();
				if (!string.IsNullOrEmpty(sql))
				{
					result = true;
				}
				else
				{
					result = false;
				}
			}
			else
			{
				using SqlConnection conn = new(_connectionString);
				conn.Open();
				SqlCommand cmd = new(sql, conn)
				{
					CommandTimeout = int.MaxValue,
					CommandText = sql
				};
				sql = cmd.ExecuteScalar().ToString();
				if (!string.IsNullOrEmpty(sql))
				{
					result = true;
				}
				else
				{
					result = false;
				}
			}
			return result;
		}




		#endregion

	}
}
