using MySql.Data.MySqlClient;
using RetSharp.ConnectionsModel;
using RetSharp.Helper;
using RetSharp.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace RetSharp.Handler
{
	public class MysqlHandler
	{
		private readonly string _connectionString;
		private readonly MySqlConnection _connection;
		private readonly bool _useTransaction;
		private readonly MySqlTransaction _transaction;
		public MysqlHandler(string connectionString)
		{
			_connectionString = connectionString;
			_useTransaction = false;
		}
		public MysqlHandler(string connectionString, bool useTransaction)
		{
			_connectionString = connectionString;
			_useTransaction = useTransaction;
			_connection = new MySqlConnection(_connectionString);
			if (_connection.State == ConnectionState.Closed)
				_connection.Open();
			_transaction = _connection.BeginTransaction();
		}

		#region Any
		public bool Any<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new()
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

			using (MySqlConnection conn = new(_connectionString))
			{
				conn.Open();
				MySqlCommand cmd = new MySqlCommand(sql, conn);
				MySqlDataReader dataReader = cmd.ExecuteReader();
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
						PropertyInfo propertyInfo = prop.GetType().GetProperty(prop.Name);
						prop.SetValue(prop, Convert.ChangeType(prop, prop.PropertyType), null);
					}
				}
			}

			if (data != null && data.Count != 0)
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		#endregion

		#region Count
		public int Count<T>(Expression<Func<T, bool>> predicate, string keyName, int? limit = null) where T : class, new()
		{
			T obj = new();
			var data = new List<T>();
			string sql = string.Empty;
			string select = string.Format(@"Select count({0}) as Count from  {1} ", keyName, obj.GetType().Name);
			string where = SQLHelper.CreateWhereClause<T>(predicate);
			if (!string.IsNullOrEmpty(where))
				sql = string.Format("{0} where {1}", select, where);
			else
				sql = select;
			if (limit.HasValue)
				sql = string.Format("{0} Limit {1}", sql, limit.Value);

			using (MySqlConnection conn = new(_connectionString))
			{
				conn.Open();
				MySqlCommand cmd = new MySqlCommand(sql, conn);
				cmd.CommandText = sql;
				sql = cmd.ExecuteScalar().ToString();
			}
			return int.Parse(sql);
		}
		#endregion

		#region Where
		public IEnumerable<T> Where<T>(Expression<Func<T, bool>> predicate, int? limit = null, int? pagesize = null, int? pagenumber = null) where T : class, new()
		{
			T obj = new();
			var data = new List<T>();
			string sql = string.Empty;
			string select;

			select = string.Format(@"Select * from  `{0}` ", obj.GetType().Name);

			string where = SQLHelper.CreateWhereClause<T>(predicate);
			if (!string.IsNullOrEmpty(where))
				sql = string.Format("{0} where {1}", select, where);
			else
				sql = select;
			if (limit.HasValue)
				sql = string.Format("{0} Limit {1}", sql, limit.Value);

			if (pagesize.HasValue && pagenumber.HasValue)
			{
				sql = string.Format("{0} LIMIT {1} OFFSET  {2} ", sql, pagesize, pagesize * pagenumber);
			}
			if (_useTransaction)
			{
				if (_connection.State == ConnectionState.Closed)
					_connection.Open();
				MySqlCommand cmd = new MySqlCommand(sql, _connection);
				cmd.CommandTimeout = int.MaxValue;
				MySqlDataReader dataReader = cmd.ExecuteReader();
				DataSet ds = new DataSet();
				DataTable dataTable = new DataTable();
				ds.Tables.Add(dataTable);
				ds.EnforceConstraints = false;
				dataTable.Fill(dataReader, true);
				dataReader.Close();
				data = SQLHelper.DataTableToList<T>(dataTable);

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
							var get1 = prop.GetGetMethod();
							var fullName1 = get1.GetBaseDefinition().ReturnType.GenericTypeArguments[0].AssemblyQualifiedName;
							var myObj = Activator.CreateInstance(Type.GetType(fullName1));
							var listType = typeof(List<>);
							var constructedListType = listType.MakeGenericType(myObj.GetType());
							var ls = Activator.CreateInstance(constructedListType);
							Type dc = Type.GetType(fullName1);
							PropertyInfo propertyInfo = dc.GetType().GetProperty(prop.Name);
							prop.SetValue(dc, Convert.ChangeType(dc, prop.PropertyType), null);
						}
					}
				}
			}
			else
			{
				using (MySqlConnection conn = new(_connectionString))
				{
					conn.Open();
					MySqlCommand cmd = new MySqlCommand(sql, conn);
					cmd.CommandTimeout = int.MaxValue;
					MySqlDataReader dataReader = cmd.ExecuteReader();
					DataSet ds = new DataSet();
					DataTable dataTable = new DataTable();
					ds.Tables.Add(dataTable);
					ds.EnforceConstraints = false;
					dataTable.Fill(dataReader, true);
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
									PropertyInfo propx = myObj.GetType().GetProperty(propertyInfo.Name);
								}
								else
								{
								}
							}
						}
						else if (!prop.PropertyType.FullName.StartsWith("System."))
						{
							var get1 = prop.GetGetMethod();
							var fullName1 = get1.GetBaseDefinition().ReturnType.GenericTypeArguments[0].AssemblyQualifiedName;
							var myObj = Activator.CreateInstance(Type.GetType(fullName1));
							var listType = typeof(List<>);
							var constructedListType = listType.MakeGenericType(myObj.GetType());
							var ls = Activator.CreateInstance(constructedListType);
							Type dc = Type.GetType(fullName1);
							PropertyInfo propertyInfo = dc.GetType().GetProperty(prop.Name);
							prop.SetValue(dc, Convert.ChangeType(dc, prop.PropertyType), null);
						}
					}
				}
			}
			return data;
		}
		public PagedList<T> WherePagedList<T>(Expression<Func<T, bool>> predicate, int pagesize, int pagenumber, int? limit = null) where T : class, new()
		{
			T obj = new();
			var data = new List<T>();
			string sql = string.Empty;
			string select;
			var countsql = string.Empty;

			select = string.Format(@"Select * from  `{0}`  ", obj.GetType().Name);

			string where = SQLHelper.CreateWhereClause<T>(predicate);
			if (!string.IsNullOrEmpty(where))
				sql = string.Format("{0} where {1}", select, where);
			else
				sql = select;

			countsql = sql.Replace("Select *", "Select Count(*)");
			if (limit.HasValue)
				sql = string.Format("{0} Limit {1}", sql, limit.Value);

			if (pagesize != 0)
			{
				sql = string.Format("{0} LIMIT {1} OFFSET  {2} ", sql, pagesize, pagesize * pagenumber);
			}
			else
			{
				sql = string.Format("{0} where {1}", select, where);
			}
			if (_useTransaction)
			{
				if (_connection.State == ConnectionState.Closed)
					_connection.Open();
				MySqlCommand cmd = new MySqlCommand(sql, _connection);
				cmd.CommandTimeout = int.MaxValue;
				MySqlDataReader dataReader = cmd.ExecuteReader();
				DataSet ds = new DataSet();
				DataTable dataTable = new DataTable();
				ds.Tables.Add(dataTable);
				ds.EnforceConstraints = false;
				dataTable.Fill(dataReader, true);
				dataReader.Close();
				data = SQLHelper.DataTableToList<T>(dataTable);

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
							var get1 = prop.GetGetMethod();
							var fullName1 = get1.GetBaseDefinition().ReturnType.GenericTypeArguments[0].AssemblyQualifiedName;
							var myObj = Activator.CreateInstance(Type.GetType(fullName1));
							var listType = typeof(List<>);
							var constructedListType = listType.MakeGenericType(myObj.GetType());
							var ls = Activator.CreateInstance(constructedListType);
							Type dc = Type.GetType(fullName1);
							PropertyInfo propertyInfo = dc.GetType().GetProperty(prop.Name);
							prop.SetValue(dc, Convert.ChangeType(dc, prop.PropertyType), null);
						}
					}
				}
			}
			else
			{
				using (MySqlConnection conn = new(_connectionString))
				{
					conn.Open();
					MySqlCommand cmd = new MySqlCommand(sql, conn);
					cmd.CommandTimeout = int.MaxValue;
					DataTable dataTable = new DataTable();
					MySqlDataReader dataReader = cmd.ExecuteReader();
					DataSet ds = new DataSet();
					ds.EnforceConstraints = false;
					ds.Tables.Add(dataTable);
					dataTable.Fill(dataReader, true);
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
									PropertyInfo propx = myObj.GetType().GetProperty(propertyInfo.Name);
								}
								else
								{
								}
							}
						}
						else if (!prop.PropertyType.FullName.StartsWith("System."))
						{
							var get1 = prop.GetGetMethod();
							var fullName1 = get1.GetBaseDefinition().ReturnType.GenericTypeArguments[0].AssemblyQualifiedName;
							var myObj = Activator.CreateInstance(Type.GetType(fullName1));
							var listType = typeof(List<>);
							var constructedListType = listType.MakeGenericType(myObj.GetType());
							var ls = Activator.CreateInstance(constructedListType);
							Type dc = Type.GetType(fullName1);
							PropertyInfo propertyInfo = dc.GetType().GetProperty(prop.Name);
							prop.SetValue(dc, Convert.ChangeType(dc, prop.PropertyType), null);
						}
					}
				}
			}
			int totalcount = 0;
			if (data.Any())
			{
				using (MySqlConnection conn = new(_connectionString))
				{
					conn.Open();
					MySqlCommand cmd = new MySqlCommand(countsql, conn);
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

		public async Task<IEnumerable<T>> WhereAsync<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new()
		{
			T obj = new();
			var data = new List<T>();
			string sql = string.Empty;
			string select;

			select = string.Format(@"Select * from  {`{0}` ", obj.GetType().Name);

			string where = SQLHelper.CreateWhereClause<T>(predicate);
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
				MySqlCommand cmd = new MySqlCommand(sql, _connection);
				cmd.CommandTimeout = int.MaxValue;
				var dataReader = await cmd.ExecuteReaderAsync();
				DataSet ds = new DataSet();
				DataTable dataTable = new DataTable();
				ds.Tables.Add(dataTable);
				ds.EnforceConstraints = false;
				dataTable.Load(dataReader);
				dataReader.Close();
				data = SQLHelper.DataTableToList<T>(dataTable);

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
							var get1 = prop.GetGetMethod();
							var fullName1 = get1.GetBaseDefinition().ReturnType.GenericTypeArguments[0].AssemblyQualifiedName;
							var myObj = Activator.CreateInstance(Type.GetType(fullName1));
							var listType = typeof(List<>);
							var constructedListType = listType.MakeGenericType(myObj.GetType());
							var ls = Activator.CreateInstance(constructedListType);
							Type dc = Type.GetType(fullName1);
							PropertyInfo propertyInfo = dc.GetType().GetProperty(prop.Name);
							prop.SetValue(dc, Convert.ChangeType(dc, prop.PropertyType), null);
						}
					}
				}
			}
			else
			{
				using (MySqlConnection conn = new(_connectionString))
				{
					conn.Open();
					MySqlCommand cmd = new MySqlCommand(sql, conn);
					cmd.CommandTimeout = int.MaxValue;
					var dataReader = await cmd.ExecuteReaderAsync();
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
									PropertyInfo propx = myObj.GetType().GetProperty(propertyInfo.Name);
								}
								else
								{
								}
							}
						}
						else if (!prop.PropertyType.FullName.StartsWith("System."))
						{
							var get1 = prop.GetGetMethod();
							var fullName1 = get1.GetBaseDefinition().ReturnType.GenericTypeArguments[0].AssemblyQualifiedName;
							var myObj = Activator.CreateInstance(Type.GetType(fullName1));
							var listType = typeof(List<>);
							var constructedListType = listType.MakeGenericType(myObj.GetType());
							var ls = Activator.CreateInstance(constructedListType);
							Type dc = Type.GetType(fullName1);
							PropertyInfo propertyInfo = dc.GetType().GetProperty(prop.Name);
							prop.SetValue(dc, Convert.ChangeType(dc, prop.PropertyType), null);
						}
					}
				}
			}
			return data;
		}
		#endregion

		#region Query
		public IEnumerable<T> Query<T>(string sql) where T : class, new()
		{
			try
			{
				if (_useTransaction)
				{
					if (_connection.State == ConnectionState.Closed)
						_connection.Open();
					MySqlDataAdapter dataAdapter = new MySqlDataAdapter(sql, _connection);
					dataAdapter.SelectCommand.CommandTimeout = 180;
					DataSet dataSet = new DataSet();
					dataAdapter.Fill(dataSet);
					var dt = dataSet.Tables[0];
					return SQLHelper.DataTableToList<T>(dt);

				}
				else
				{
					using (MySqlConnection conn = new MySqlConnection(_connectionString))
					{
						conn.Open();
						MySqlDataAdapter dataAdapter = new MySqlDataAdapter(sql, conn);
						dataAdapter.SelectCommand.CommandTimeout = 180;
						DataSet dataSet = new DataSet();
						dataAdapter.Fill(dataSet);

						var dt = dataSet.Tables[0];
						conn.Close();
						return SQLHelper.DataTableToList<T>(dt);
					}

				}
			}
			catch (Exception ex)
			{

				throw;
			}
		}
		public PagedList<T> QueryPagedList<T>(string sql, int pagesize, int pagenumber) where T : class, new()
		{
			var data = new List<T>();
			string sqllast = string.Empty;
			try
			{
				if (_useTransaction)
				{
					if (pagesize != 0)
					{
						sqllast = string.Format(" {0} LIMIT {1} OFFSET  {2} ", sql, pagesize, pagesize * pagenumber);
					}
					else
					{
						sqllast = string.Format("{0}", sql);
					}
					if (_connection.State == ConnectionState.Closed)
						_connection.Open();
					MySqlDataAdapter dataAdapter = new MySqlDataAdapter(sqllast, _connection);
					dataAdapter.SelectCommand.CommandTimeout = 180;
					DataSet dataSet = new DataSet();
					dataAdapter.Fill(dataSet);
					var dt = dataSet.Tables[0];
					data = SQLHelper.DataTableToList<T>(dt);

				}
				else
				{
					using (MySqlConnection conn = new MySqlConnection(_connectionString))
					{

						if (pagesize != 0)
						{
							sqllast = string.Format(" {0} LIMIT {1} OFFSET  {2} ", sql, pagesize, pagesize * pagenumber);
						}
						else
						{
							sqllast = string.Format("{0}", sql);
						}
						conn.Open();
						MySqlDataAdapter dataAdapter = new MySqlDataAdapter(sqllast, conn);
						dataAdapter.SelectCommand.CommandTimeout = 180;
						DataSet dataSet = new DataSet();
						dataAdapter.Fill(dataSet);

						var dt = dataSet.Tables[0];
						conn.Close();
						data = SQLHelper.DataTableToList<T>(dt);
					}

				}
				int totalcount = 0;
				if (data.Any())
				{
					var countsql = sql.ToLowerInvariant();
					string toReplace = Regex.Match(countsql, @"select([^\}]+)from").Groups[1].Value;
					countsql = countsql.Replace(toReplace, " count(*) ");
					using (MySqlConnection conn = new(_connectionString))
					{
						conn.Open();
						MySqlCommand cmd = new MySqlCommand(countsql, conn);
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
			catch (Exception ex)
			{

				throw;
			}
		}

		public bool UpdateQuery<T>(string sql) where T : class, new()
		{
			try
			{
				if (_useTransaction)
				{
					if (_connection.State == ConnectionState.Closed)
						_connection.Open();
					MySqlDataAdapter dataAdapter = new MySqlDataAdapter(sql, _connection);
					dataAdapter.SelectCommand.CommandTimeout = 180;
					DataSet dataSet = new DataSet();
					dataAdapter.Fill(dataSet);
					return true;

				}
				else
				{
					using (MySqlConnection conn = new MySqlConnection(_connectionString))
					{
						conn.Open();
						MySqlDataAdapter dataAdapter = new MySqlDataAdapter(sql, conn);
						dataAdapter.SelectCommand.CommandTimeout = 180;
						DataSet dataSet = new DataSet();
						dataAdapter.Fill(dataSet);
						conn.Close();
						return true;
					}

				}
			}
			catch (Exception)
			{

				throw;
			}
		}


		internal DbTransaction GetTransaction()
		{
			using (MySqlConnection conn = new MySqlConnection(_connectionString))
			{
				conn.Open();
				return conn.BeginTransaction();
			}
		}
		#endregion

		#region Insert
		public T Insert<T>(T model, ConnectionType _type) where T : class, new()
		{

			var insertSql = SQLHelper.GetInsertSql<T>(model, _type);
			if (_useTransaction)
			{
				try
				{
					using MySqlCommand cmd = _connection.CreateCommand();
					cmd.Transaction = _transaction;
					cmd.CommandText = insertSql.SqlCommand;
					foreach (var item in insertSql.SqlParameters)
					{
						MySqlParameter p = new MySqlParameter();
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

				}
				catch (Exception ex)
				{
					new InvalidOperationException(ex.Message);
				}
			}
			else
			{
				using (MySqlConnection conn = new MySqlConnection(_connectionString))
				{
					conn.Open();
					using MySqlTransaction transaction = conn.BeginTransaction();
					using MySqlCommand cmd = conn.CreateCommand();
					cmd.Transaction = transaction;
					try
					{
						cmd.CommandText = insertSql.SqlCommand;
						foreach (var item in insertSql.SqlParameters)
						{
							MySqlParameter p = new MySqlParameter();
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
					catch (Exception ex)
					{
						transaction.Rollback();
						throw new InvalidOperationException(ex.Message);
					}
				}
			}
			if (insertSql == null)
			{

			}
			else
			{

				model = Query<T>($"select * from `{model.GetType().Name}` where Id = {insertSql.PrimaryId} ").FirstOrDefault();

			}

			return model;
		}

		#endregion

		#region InsertRange
		public List<T> InsertRange<T>(List<T> model, ConnectionType _type) where T : class, new()
		{
			var insertSqls = SQLHelper.GetInsertSqlList<T>(model, _type);

			foreach (var item in insertSqls)
			{
				if (_useTransaction)
				{
					try
					{
						using MySqlCommand cmd = _connection.CreateCommand();
						cmd.Transaction = _transaction;
						cmd.CommandText = item.SqlCommand;
						foreach (var item2 in item.SqlParameters)
						{
							MySqlParameter p = new MySqlParameter();
							p.ParameterName = item2.ParameterName;
							p.Value = item2.Value;
							cmd.Parameters.Add(p);
						}
						item.PrimaryId = cmd.ExecuteScalar();
						if (item.PrimaryId != null && item.SubSqls.Any())
						{
							var primaryName = string.Format("{0}Id", model.GetType().Name);
							foreach (var isub in item.SubSqls)
							{
								isub.PrimaryName = primaryName;
								InsertSub(isub, cmd, item.PrimaryId);
							}
						}
					}
					catch (Exception ex)
					{
						_transaction.Rollback();
						throw new InvalidOperationException(ex.Message);
					}
				}
				else
				{
					T objs = new();
					using (MySqlConnection conn = new MySqlConnection(_connectionString))
					{

						conn.Open();
						using MySqlTransaction transaction = conn.BeginTransaction();
						using MySqlCommand cmd = conn.CreateCommand();
						cmd.Transaction = transaction;
						try
						{
							cmd.CommandText = item.SqlCommand;
							foreach (var item2 in item.SqlParameters)
							{
								MySqlParameter p = new MySqlParameter();
								p.ParameterName = item2.ParameterName;
								p.Value = item2.Value;
								cmd.Parameters.Add(p);
							}
							item.PrimaryId = cmd.ExecuteScalar();

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
					model.Clear();
					T query = Query<T>($"select * from {objs.GetType().Name} where Id = {item.PrimaryId} ").FirstOrDefault();
					model.Add(query);
				}
			}
			return model;
		}
		#endregion

		#region InsertSub
		private static InsertSql InsertSub(InsertSql insertSql, MySqlCommand cmd, object primaryId)
		{
			try
			{
				cmd.CommandText = insertSql.SqlCommand;
				cmd.Parameters.Clear();
				foreach (var item in insertSql.SqlParameters)
				{
					MySqlParameter p = new MySqlParameter();
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
							MySqlParameter p = new MySqlParameter();
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
		public T Update<T>(T model, ConnectionType _type) where T : class, new()
		{
			var updateSql = SQLHelper.UpdateSql<T>(model, _type);
			if (_useTransaction)
			{
				using MySqlCommand cmd = _connection.CreateCommand();
				try
				{
					cmd.CommandText = updateSql.SqlCommand;
					cmd.Transaction = _transaction;
					foreach (var item in updateSql.SqlParameters)
					{
						MySqlParameter p = new MySqlParameter();
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
				}
				catch (Exception ex)
				{
					throw new InvalidOperationException(ex.Message);
				}
			}
			else
			{
				using (MySqlConnection conn = new MySqlConnection(_connectionString))
				{
					conn.Open();
					using MySqlTransaction transaction = conn.BeginTransaction();
					using MySqlCommand cmd = conn.CreateCommand();
					try
					{
						cmd.CommandText = updateSql.SqlCommand;
						foreach (var item in updateSql.SqlParameters)
						{
							MySqlParameter p = new MySqlParameter();
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
				if (_useTransaction)
				{
					using MySqlCommand cmd = _connection.CreateCommand();
					try
					{
						cmd.CommandText = item.SqlCommand;
						cmd.Transaction = _transaction;
						foreach (var item2 in item.SqlParameters)
						{
							MySqlParameter p = new MySqlParameter();
							p.ParameterName = item2.ParameterName;
							p.Value = item2.Value;
							cmd.Parameters.Add(p);
						}
						item.PrimaryId = cmd.ExecuteScalar();
						if (item.PrimaryId != null || item.SubSqls.Any())
						{
							var primaryName = string.Format("{0}Id", model.GetType().Name);
							foreach (var isub in item.SubSqls)
							{
								isub.PrimaryName = primaryName;
								UpdateSub(isub, cmd, item.PrimaryId);
							}
						}
						//_transaction.Commit();

					}
					catch (Exception ex)
					{
						_transaction.Rollback();
						throw new InvalidOperationException(ex.Message);
					}
				}
				else
				{
					T objs = new();
					using (MySqlConnection conn = new MySqlConnection(_connectionString))
					{

						conn.Open();
						try
						{
							using MySqlCommand cmd = conn.CreateCommand();
							cmd.CommandText = item.SqlCommand;
							foreach (var item2 in item.SqlParameters)
							{
								MySqlParameter p = new MySqlParameter();
								p.ParameterName = item2.ParameterName;
								p.Value = item2.Value;
								cmd.Parameters.Add(p);
							}
							cmd.ExecuteScalar();
							if (item.PrimaryId != null || item.SubSqls.Any())
							{
								var primaryName = string.Format("{0}Id", model.GetType().Name);
								foreach (var isub in item.SubSqls)
								{
									isub.PrimaryName = primaryName;
									UpdateSub(isub, cmd, item.PrimaryId);
								}
							}
							conn.Close();
						}
						catch (Exception ex)
						{
							throw new InvalidOperationException(ex.Message);
						}
					}
					model.Clear();
					T query = Query<T>($"select * from {objs.GetType().Name} where Id = {item.PrimaryId} ").FirstOrDefault();
					model.Add(query);
				}

			}
			return model;
		}
		#endregion

		#region UpdateSub
		private static UpdateSql UpdateSub(UpdateSql updateSql, MySqlCommand cmd, object primaryId)
		{
			try
			{
				cmd.CommandText = updateSql.SqlCommand;
				cmd.Parameters.Clear();
				foreach (var item in updateSql.SqlParameters)
				{
					MySqlParameter p = new MySqlParameter();
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
				throw new InvalidOperationException(ex.Message);
			}
			return updateSql;
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
				T obj = new();
				string sql = string.Format(@"Select * from  {0} ", obj.GetType().Name);
				using (MySqlConnection conn = new(_connectionString))
				{
					conn.Open();
					MySqlCommand cmd = new MySqlCommand(sql, conn);
					MySqlDataReader dataReader = cmd.ExecuteReader();
					DataSet ds = new DataSet();
					DataTable dataTable = new DataTable();
					ds.EnforceConstraints = false;
					ds.Tables.Add(dataTable);
					dataTable.Fill(dataReader, true);
					dataReader.Close();
					conn.Close();
					List<T> list = new List<T>();
					foreach (var row in dataTable.AsEnumerable())
					{
						T obj2 = new T();
						foreach (var prop in obj2.GetType().GetProperties())
						{
							try
							{
								if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
								{
									Console.WriteLine("Not Mapped");
								}
								else
								{

									PropertyInfo propertyInfo = obj2.GetType().GetProperty(prop.Name);
									propertyInfo.SetValue(obj2, Convert.ChangeType(row[prop.Name], propertyInfo.PropertyType), null);
								}
							}
							catch (Exception ex)
							{
								continue;
							}
						}
						list.Add(obj);
					}
					return list;
				}
			}
			catch
			{
				return null;
			}
		}
		#endregion

		#region TableToList
		public Result<DataTable> TableToList(string sql)
		{
			var res = new Result<DataTable>(success: false);
			try
			{
				using (MySqlConnection conn = new(_connectionString))
				{
					conn.Open();
					MySqlDataAdapter mySqlDataAdapter = new();
					MySqlCommand cmd = new MySqlCommand(sql, conn);
					MySqlDataReader dataReader = cmd.ExecuteReader();
					DataSet ds = new DataSet();
					DataTable dataTable = new DataTable();
					ds.Tables.Add(dataTable);
					ds.EnforceConstraints = false;
					dataTable.Fill(dataReader, true);
					dataReader.Close();
					conn.Close();
					res.Message = "Messages.Success";
					res.Success = true;
					res.Data = dataTable;
					return res;
				}
			}
			catch (Exception ex)
			{
				res.Message = ex.Message;
			}
			return res;
		}
		#endregion

		#region CreateDatabaseQuery
		public bool CreateDatabaseQuery(string sql)
		{
			var querySql = $"create database {sql}";
			if (_useTransaction)
			{
				if (_connection.State == ConnectionState.Closed)
					_connection.Open();

				MySqlCommand cmd = new MySqlCommand(querySql, _connection);
				MySqlDataReader dataReader = cmd.ExecuteReader();
				DataSet ds = new DataSet();
				DataTable dataTable = new DataTable();
				ds.Tables.Add(dataTable);
				ds.EnforceConstraints = false;
				dataTable.Load(dataReader);
				return true;
			}
			else
			{
				using (MySqlConnection conn = new MySqlConnection(_connectionString))
				{
					conn.Open();
					MySqlCommand cmd = new MySqlCommand(querySql, conn);
					MySqlDataReader dataReader = cmd.ExecuteReader();
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

		#region InsertTable
		public T InsertTable<T>(string databaseName, T model, ConnectionType _type) where T : class, new()
		{
			T obj = new();
			var insertSql = SQLHelper.GetCreateTableSql<T>(databaseName, _type);
			if (_useTransaction)
			{
				try
				{
					using MySqlCommand cmd = _connection.CreateCommand();
					cmd.Transaction = _transaction;
					cmd.CommandText = insertSql.SqlCommand;
					insertSql.PrimaryId = cmd.ExecuteScalar();

				}
				catch (Exception ex)
				{
					throw new InvalidOperationException(ex.Message);
				}
			}
			else
			{
				using (MySqlConnection conn = new MySqlConnection(_connectionString))
				{
					conn.Open();
					using MySqlTransaction transaction = conn.BeginTransaction();
					using MySqlCommand cmd = conn.CreateCommand();
					cmd.Transaction = transaction;
					try
					{
						cmd.CommandText = insertSql.SqlCommand;
						insertSql.PrimaryId = cmd.ExecuteScalar();

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
			return obj;
		}
		#endregion

		public T InsertTableTest<T>(string databaseName, T model, ConnectionType _type) where T : class, new()
		{
			T obj = new();
			var insertSql = SQLHelper.GetCreateTableSqlTest<T>(databaseName, _type);
			if (_useTransaction)
			{
				try
				{
					using MySqlCommand cmd = _connection.CreateCommand();
					cmd.Transaction = _transaction;
					cmd.CommandText = insertSql.SqlCommand;
					insertSql.PrimaryId = cmd.ExecuteScalar();

				}
				catch (Exception ex)
				{
					throw new InvalidOperationException(ex.Message);
				}
			}
			else
			{
				using (MySqlConnection conn = new MySqlConnection(_connectionString))
				{
					conn.Open();
					using MySqlTransaction transaction = conn.BeginTransaction();
					using MySqlCommand cmd = conn.CreateCommand();
					cmd.Transaction = transaction;
					try
					{
						cmd.CommandText = insertSql.SqlCommand;
						insertSql.PrimaryId = cmd.ExecuteScalar();

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
			return obj;
		}


		#region InsertTable
		public Result<bool> CreateSql(string databaseName, string sql)
		{
			var res = new Result<bool>(success: false);
			var insertSql = SQLHelper.GetCreateSql(databaseName, sql);
			if (_useTransaction)
			{
				try
				{
					using MySqlCommand cmd = _connection.CreateCommand();
					cmd.Transaction = _transaction;
					cmd.CommandText = insertSql.SqlCommand;
					insertSql.PrimaryId = cmd.ExecuteScalar();
					res.Success = true;

				}
				catch (Exception ex)
				{
					res.Message = ex.Message;
				}
			}
			else
			{
				using (MySqlConnection conn = new MySqlConnection(_connectionString))
				{
					conn.Open();
					using MySqlTransaction transaction = conn.BeginTransaction();
					using MySqlCommand cmd = conn.CreateCommand();
					cmd.Transaction = transaction;
					try
					{
						cmd.CommandText = insertSql.SqlCommand;
						insertSql.PrimaryId = cmd.ExecuteScalar();
						res.Success = true;
						transaction.Commit();
						conn.Close();

					}
					catch (Exception ex)
					{
						transaction.Rollback();
						res.Message = ex.Message;
					}
				}
			}
			return res;
		}
		#endregion

		#region Delete

		public bool Delete<T>(Expression<Func<T, bool>> predicate, int? limit = null) where T : class, new()
		{
			bool result = false;
			T obj = new();
			string select;

			select = string.Format(@"delete from `{0}`", obj.GetType().Name);

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
				MySqlCommand cmd = new(sql, _connection)
				{
					CommandTimeout = int.MaxValue,
					CommandText = sql
				};
				int returnResult = cmd.ExecuteNonQuery();
				if (returnResult > 0)
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
				using MySqlConnection conn = new(_connectionString);
				conn.Open();
				MySqlCommand cmd = new(sql, conn)
				{
					CommandTimeout = int.MaxValue,
					CommandText = sql
				};
				cmd.ExecuteNonQuery();
				result = true;


			}
			return result;
		}

		#endregion

	}
}