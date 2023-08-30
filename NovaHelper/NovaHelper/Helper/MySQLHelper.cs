using RetSharp.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace RetSharp.Helper
{
    public static class MySQLHelper
    {
        private static readonly string scopeIdentity = "SELECT LAST_INSERT_ID();";
        public static void Fill(this DataTable table, IDataReader reader, bool createColumns)
        {

            if (createColumns)
            {
                table.Columns.Clear();
                var schemaTable = reader.GetSchemaTable();
                foreach (DataRowView row in schemaTable.DefaultView)
                {
                    var columnName = (string)row["ColumnName"];
                    var type = (Type)row["DataType"];
                    table.Columns.Add(columnName, type);
                }
            }

            table.Load(reader);
        }
        public static string CreateWhereClause<T>(Expression<Func<T, bool>> predicate)
        {
            StringBuilder p = new(predicate.Body.ToString());
            var pName = predicate.Parameters.First();
            p.Replace(pName.Name + ".", "");
            p.Replace("==", "=");
            p.Replace("AndAlso", "and");
            p.Replace("OrElse", "or");
            p.Replace("\"", "\'");
            return p.ToString();
        }
        public static List<T> DataTableToList<T>(DataTable table) where T : class, new()
        {
            try
            {
                List<T> list = new();
                foreach (var row in table.AsEnumerable())
                {
                    T obj = new();
                    foreach (var prop in obj.GetType().GetProperties())
                    {
                        try
                        {
                            PropertyInfo propertyInfo = obj.GetType().GetProperty(prop.Name);
                            propertyInfo.SetValue(obj, Convert.ChangeType(row[prop.Name], propertyInfo.PropertyType), null);
                        }
                        catch
                        {
                            continue;
                        }
                    }
                    list.Add(obj);
                }
                return list;
            }
            catch
            {
                return null;
            }
        }
        public static InsertSql GetInsertSql<T>(T model) where T : class, new()
        {
            var result = new InsertSql
            {
                SubSqls = new List<InsertSql>()
            };
            T obj = new();
            string insert = string.Empty;
            insert += string.Format(@"Insert into {0} (", obj.GetType().Name);
            int i = 0;
            Object entityid = 0;
            string entityprimarykey = "";
            List<SqlParameter> ls = new();
            foreach (var prop in obj.GetType().GetProperties())
            {
                if (prop.PropertyType.FullName.StartsWith("System.") && !prop.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    string s = prop.Name;
                    if (Attribute.GetCustomAttribute(prop, typeof(KeyAttribute)) is KeyAttribute attribute) { }
                    else
                    {
                        if (i == 0)
                            insert += string.Format("[{0}]", s);
                        else
                            insert += string.Format(",[{0}]", s);
                        i++;
                    }
                }
                else if (prop.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    object anArray = prop.GetValue(model, null);
                    if (anArray is IEnumerable enumerable)
                    {
                        foreach (object element in enumerable)
                        {
                            var subinsert = GetSubInsertSql(element.GetType(), element);
                            result.SubSqls.Add(subinsert);
                        }

                    }
                }
                else
                {
                    object anObject = prop.GetValue(model, null);
                    var subinsert = GetSubInsertSql(anObject.GetType(), anObject);
                    result.SubSqls.Add(subinsert);
                }
            }
            insert += ") Values(";

            i = 0;
            foreach (var p in obj.GetType().GetProperties().Where(p => !p.GetGetMethod().GetParameters().Any()))
            {
                if (p.PropertyType.FullName.StartsWith("System.") && !p.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    if (Attribute.GetCustomAttribute(p, typeof(KeyAttribute)) is KeyAttribute attribute)
                    {
                        entityprimarykey = p.Name;
                        entityid = p.GetValue(model, null);
                        ls.Add(new SqlParameter("@" + p.Name, p.GetValue(model, null)));
                    }
                    else
                    {
                        var value = p.GetValue(model, null);
                        if (value == null)
                        {
                            if (p.PropertyType.FullName == "System.String")
                                value = "";
                            else if (p.PropertyType.FullName.Contains("System.Decimal"))
                                value = 0;
                            else if (p.PropertyType.FullName == "System.Int32")
                                value = 0;
                        }
                        ls.Add(new SqlParameter("@" + p.Name, value));
                        if (i == 0)
                            insert += string.Format("@{0}", p.Name);
                        else
                            insert += string.Format(",@{0}  ", p.Name);
                        i++;
                    }
                }
            }
            insert += ");";
            result.SqlCommand = string.Format("{0} {1}", insert, scopeIdentity);
            result.SqlParameters = ls;
            return result;
        }
        public static InsertSql GetSubInsertSql(Type type, object model)
        {
            var result = new InsertSql();
            string subinsert = string.Empty;
            List<SqlParameter> lsub = new();
            var cint = 0;
            var nob = Activator.CreateInstance(type);
            subinsert += string.Format(@"Insert into {0} (", nob.GetType().Name);
            foreach (var ip in nob.GetType().GetProperties())
            {
                if (ip.PropertyType.FullName.StartsWith("System.") && !ip.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    string sn = ip.Name;
                    var attribute = Attribute.GetCustomAttribute(ip, typeof(KeyAttribute)) as KeyAttribute;
                    if (attribute != null) { }
                    else
                    {
                        if (cint == 0)
                            subinsert += string.Format("[{0}]", sn);
                        else
                            subinsert += string.Format(",[{0}]", sn);
                        cint++;
                    }
                }
            }
            cint = 0;
            subinsert += ") Values(";
            foreach (var p in nob.GetType().GetProperties().Where(p => !p.GetGetMethod().GetParameters().Any()))
            {
                var attribute = Attribute.GetCustomAttribute(p, typeof(KeyAttribute)) as KeyAttribute;
                var fk = Attribute.GetCustomAttribute(p, typeof(ForeignKeyAttribute)) as ForeignKeyAttribute;
                if (attribute != null)
                {
                }
                else
                {
                    if (fk == null)
                    {

                        var value = p.GetValue(model, null);
                        if (value == null)
                        {
                            if (p.PropertyType.FullName == "System.String")
                                value = "";
                            else if (p.PropertyType.FullName.Contains("System.Decimal"))
                                value = 0;
                            else if (p.PropertyType.FullName == "System.Int32")
                                value = 0;
                        }
                        lsub.Add(new SqlParameter("@" + p.Name, value));
                        if (cint == 0)
                            subinsert += string.Format("@{0}", p.Name);
                        else
                            subinsert += string.Format(",@{0}  ", p.Name);
                    }
                    else
                    {
                        if (cint == 0)
                            subinsert += string.Format("@DataID");
                        else
                            subinsert += string.Format(",@DataID");
                        lsub.Add(new SqlParameter("@DataID", null));
                    }
                    cint++;

                }
            }
            subinsert += ");";
            InsertSql sq = new InsertSql();
            sq.SqlCommand = string.Format("{0} {1}", subinsert, scopeIdentity);
            sq.SqlParameters = lsub;
            result = sq;
            return result;
        }     
    }
}
