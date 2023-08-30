using RetSharp.ConnectionsModel;
using RetSharp.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace RetSharp.Helper
{
    public static class SQLHelper
    {
        private static string scopeIdentity = string.Empty;
        public static string CreateWhereClause1<T>(Expression<Func<T, bool>> predicate)
        {
            var translator = new MyQueryTranslator();
            string whereClause = translator.Translate(predicate);
            return whereClause.ToString();
        }
        public static string Identity(ConnectionType _type)
        {
            if (_type == ConnectionType.MySql)
                scopeIdentity = "SELECT LAST_INSERT_ID()";
            else if (_type == ConnectionType.MsSQL)
                scopeIdentity = "SELECT SCOPE_IDENTITY()";
            return scopeIdentity;
        }
        public static string CreateWhereClause<T>(Expression<Func<T, bool>> predicate)
        {
            var pName = LambdaToString<T>(predicate);
            //var translator = new MyQueryTranslator();
            //string whereClause = translator.Translate(predicate);
            StringBuilder p = new StringBuilder(pName);
            //var pName = predicate.Parameters.First();
            p.Replace(pName + ".", "");
            p.Replace("==", "=");
            p.Replace("AndAlso", "and");
            p.Replace("OrElse", "or");
            p.Replace("!", "not");
            p.Replace("!=", "not");
            p.Replace("\"", "\'");
            return p.ToString();
        }
        public static List<T> DataTableToList<T>(DataTable table) where T : class, new()
        {
            try
            {
                List<T> list = new List<T>();               
                foreach (var row in table.AsEnumerable())
                {
                    T obj = new T();
                    foreach (var prop in obj.GetType().GetProperties())
                    {
                        try
                        {
                            if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
                            {
                                Console.WriteLine("Not Mapped");
                            }
                            else if (prop.PropertyType.FullName.StartsWith("System.Collections"))
                            {

                            }
                            else if (prop.PropertyType.FullName.StartsWith("System.Nullable"))
                            {
                                PropertyInfo propertyInfo = obj.GetType().GetProperty(prop.Name);
                                propertyInfo.SetValue(obj, Convert.ChangeType(row[prop.Name], propertyInfo.PropertyType.GetGenericArguments()[0]), null);
                            }
                            else
                            {
                                var ColumnExist = row.Table.Columns.Contains(prop.Name);
                                if (ColumnExist)
                                {
                                    PropertyInfo propertyInfo = obj.GetType().GetProperty(prop.Name);
                                    propertyInfo.SetValue(obj, Convert.ChangeType(row[prop.Name], propertyInfo.PropertyType), null);
                                }
                                
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
            catch
            {
                return null;
            }
        }
        public static InsertSql GetInsertSql<T>(T model, ConnectionType _type) where T : class, new()
        {
            var identityScope = Identity(_type);
            var result = new InsertSql();
            result.SubSqls = new List<InsertSql>();
            T obj = new T();
            string insert = string.Empty;
            if (obj.GetType().Name == "Order")
            {
                insert += string.Format(@"Insert into `{0}` (", obj.GetType().Name);
            }
            else
            {
                insert += string.Format(@"Insert into {0} (", obj.GetType().Name);
            }
            int i = 0;
            Object entityid = 0;
            string entityprimarykey = "";
            List<SqlParameter> ls = new List<SqlParameter>();
            var primaryName = string.Format("@{0}Id", model.GetType().Name);
            foreach (var prop in obj.GetType().GetProperties())
            {
                if (prop.PropertyType.FullName.StartsWith("System.") && !prop.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
                    {
                        Console.WriteLine("Not Mapped");
                    }
                    else
                    {
                        string s = prop.Name;
                        var attribute = Attribute.GetCustomAttribute(prop, typeof(KeyAttribute)) as KeyAttribute;
                        if (attribute != null) { }
                        else
                        {
                            if (i == 0)
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    insert += string.Format("`{0}`", s);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    insert += "[" + s + "]";
                                }
                            }
                            else
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    insert += string.Format(",`{0}`", s);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    insert += "," + "[" + s + "]";
                                }
                            }
                            i++;
                        }
                    }
                }
                else if (prop.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    object anArray = prop.GetValue(model, null);
                    IEnumerable enumerable = anArray as IEnumerable;
                    if (enumerable != null)
                    {
                        foreach (object element in enumerable)
                        {
                            var subinsert = GetSubInsertSql(element.GetType(), element, primaryName, _type);
                            result.SubSqls.Add(subinsert);
                        }
                    }
                }
                else
                {
                    object anObject = prop.GetValue(model, null);
                    var subinsert = GetSubInsertSql(anObject.GetType(), anObject, primaryName, _type);
                    result.SubSqls.Add(subinsert);
                }
            }
            insert += ") Values(";
            i = 0;
            foreach (var p in obj.GetType().GetProperties().Where(p => !p.GetGetMethod().GetParameters().Any()))
            {
                if (p.PropertyType.FullName.StartsWith("System.") && !p.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    if (p.GetType().GetProperties().Where(x => Attribute.IsDefined(p, typeof(NotMappedAttribute))).Any())
                    {
                        Console.WriteLine("Not Mapped");
                    }
                    else
                    {
                        var attribute = Attribute.GetCustomAttribute(p, typeof(KeyAttribute)) as KeyAttribute;
                        if (attribute != null)
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
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    insert += string.Format("@{0}", p.Name);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    insert += "@" + p.Name;
                                }
                            }

                            else
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    insert += string.Format(",@{0}  ", p.Name);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    insert += ",@" + p.Name;
                                }
                            }

                            i++;
                        }
                    }
                }
            }
            insert += ");";
            result.SqlCommand = string.Format("{0} {1}", insert, identityScope);
            result.PrimarySubSqls = primaryName;
            result.SqlParameters = ls;
            return result;
        }
        public static InsertSql GetCreateSql(string databaseName,string sql)
        {
            var result = new InsertSql();
            result.SubSqls = new List<InsertSql>();
            string create = string.Empty;
            create += string.Format(@"{0}",sql);
            result.SqlCommand = string.Format("{0}", create);
            return result;
        }
        public static InsertSql GetCreateTableSql<T>(string databaseName,ConnectionType _type) where T : class, new()
        {
            var identityScope = Identity(_type);
            var result = new InsertSql();
            result.SubSqls = new List<InsertSql>();
            T obj = new T();
            string create = string.Empty;
            create += string.Format(@"USE {0}; create table `{1}` (", databaseName,obj.GetType().Name);
            int i = 0;
            Object entityid = 0;
            List<SqlParameter> ls = new List<SqlParameter>();
            //var primaryName = string.Format("@{0}Id", model.GetType().Name);
            foreach (var prop in obj.GetType().GetProperties())
            {
                if (prop.PropertyType.FullName.StartsWith("System.") && !prop.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
                    {
                        Console.WriteLine("Not Mapped");
                    }
                    else
                    {
                        string s = prop.Name;
                        string ss = prop.PropertyType.Name;
                        var attribute = Attribute.GetCustomAttribute(prop, typeof(KeyAttribute)) as KeyAttribute;
                        if (attribute != null)
                        {
                            if (i == 0)
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    if (prop.PropertyType.FullName == "System.String")
                                        ss = "VARCHAR(16)";
                                    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                        ss = "DECIMAL(6,2)";
                                    else if (prop.PropertyType.FullName == "System.Int32")
                                        ss = "INT AUTO_INCREMENT PRIMARY KEY";
                                    else if (prop.PropertyType.FullName == "System.Boolean")
                                        ss = "BIT";
                                    else if (prop.PropertyType.FullName == "System.DateTime")
                                        ss = "DATETIME";
                                    else if (prop.PropertyType.FullName == "System.Char")
                                        ss = "CHAR(3)";
                                    create += string.Format("`{0}` {1}", s, ss);
                                }
                            }
                            else if (i != 0 )
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    if (prop.PropertyType.FullName == "System.String")
                                        ss = "VARCHAR(16)";
                                    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                        ss = "DECIMAL(6,2)";
                                    else if (prop.PropertyType.FullName == "System.Int32")
                                        ss = "INT AUTO_INCREMENT PRIMARY KEY";
                                    else if (prop.PropertyType.FullName == "System.Boolean")
                                        ss = "BIT";
                                    else if (prop.PropertyType.FullName == "System.DateTime")
                                        ss = "DATETIME";
                                    else if (prop.PropertyType.FullName == "System.Char")
                                        ss = "CHAR(3)";
                                    create += string.Format(",`{0}` {1}", s, ss);
                                }
                            }
                            i++;
                        }
                        else
                        {
                            if (i == 0)
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    if (prop.PropertyType.FullName == "System.String")
                                        ss = "VARCHAR(16)";
                                    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                        ss = "DECIMAL(6,2)";
                                    else if (prop.PropertyType.FullName == "System.Int32")
                                        ss = "INT";
                                    else if (prop.PropertyType.FullName == "System.Boolean")
                                        ss = "BIT";
                                    else if (prop.PropertyType.FullName == "System.DateTime")
                                        ss = "DATETIME";
                                    else if (prop.PropertyType.FullName == "System.Char")
                                        ss = "CHAR(3)";
                                    create += string.Format("`{0}` {1} ", s, ss);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    create += "[" + s + "]";
                                }
                            }
                            else
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    if (prop.PropertyType.FullName == "System.String")
                                        ss = "VARCHAR(55)";
                                    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                        ss = "DECIMAL(6,2)";
                                    else if (prop.PropertyType.FullName == "System.Int32")
                                        ss = "INT";
                                    else if (prop.PropertyType.FullName == "System.Boolean")
                                        ss = "BIT";
                                    else if (prop.PropertyType.FullName == "System.DateTime")
                                        ss = "DATETIME";
                                    else if (prop.PropertyType.FullName == "System.Char")
                                        ss = "CHAR(3)";
                                    create += string.Format(",`{0}` {1} ", s,ss);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    create += "," + "[" + s + "]";
                                }
                            }
                            i++;
                        }
                        //var foreignKeyAttribute = Attribute.GetCustomAttribute(prop, typeof(ForeignKeyAttribute)) as ForeignKeyAttribute;
                        //if (_type == ConnectionType.MySql)
                        //{
                        //    if (prop.PropertyType.FullName == "System.String")
                        //        ss = "VARCHAR(16)";
                        //    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                        //        ss = "DECIMAL(6,2)";
                        //    else if (prop.PropertyType.FullName == "System.Int32")
                        //        ss = $"INT  FOREIGN KEY REFERENCES {foreignKeyAttribute.Name}({prop.Name})";
                        //    else if (prop.PropertyType.FullName == "System.Boolean")
                        //        ss = "BIT";
                        //    else if (prop.PropertyType.FullName == "System.DateTime")
                        //        ss = "DATETIME";
                        //    else if (prop.PropertyType.FullName == "System.Char")
                        //        ss = "CHAR(3)";
                        //    create += string.Format("{0} {1}", s, ss);
                        //}
                    }
                }
            }
            create += ");";
            result.SqlCommand = string.Format("{0}", create);
            return result;
        }

        public static InsertSql GetCreateTableSqlTest<T>(string databaseName, ConnectionType _type) where T : class, new()
        {
            var identityScope = Identity(_type);
            var result = new InsertSql();
            result.SubSqls = new List<InsertSql>();
            T obj = new T();
            string create = string.Empty;
            create += string.Format(@"USE {0}; create table `{1}` (", databaseName, obj.GetType().Name);
            int i = 0;
            Object entityid = 0;
            string entityprimarykey = "";
            List<SqlParameter> ls = new List<SqlParameter>();
            //var primaryName = string.Format("@{0}Id", model.GetType().Name);
            foreach (var prop in obj.GetType().GetProperties())
            {
                if (prop.PropertyType.FullName.StartsWith("System.") && !prop.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
                    {
                        Console.WriteLine("Not Mapped");
                    }
                    else
                    {
                        string s = prop.Name;
                        string ss = prop.PropertyType.Name;
                        var attribute = Attribute.GetCustomAttribute(prop, typeof(KeyAttribute)) as KeyAttribute;
                        var foreignKeyAttribute = Attribute.GetCustomAttribute(prop, typeof(ForeignKeyAttribute)) as ForeignKeyAttribute;
                        if (attribute != null)
                        {
                            if (i == 0)
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    if (prop.PropertyType.FullName == "System.String")
                                        ss = "VARCHAR(16)";
                                    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                        ss = "DECIMAL(6,2)";
                                    else if (prop.PropertyType.FullName == "System.Int32")
                                        ss = "INT AUTO_INCREMENT PRIMARY KEY";
                                    else if (prop.PropertyType.FullName == "System.Boolean")
                                        ss = "BIT";
                                    else if (prop.PropertyType.FullName == "System.DateTime")
                                        ss = "DATETIME";
                                    else if (prop.PropertyType.FullName == "System.Char")
                                        ss = "CHAR(3)";
                                    create += string.Format("{0} {1}", s, ss);
                                }
                            }
                            else if (i != 0)
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    if (prop.PropertyType.FullName == "System.String")
                                        ss = "VARCHAR(16)";
                                    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                        ss = "DECIMAL(6,2)";
                                    else if (prop.PropertyType.FullName == "System.Int32")
                                        ss = "INT AUTO_INCREMENT PRIMARY KEY";
                                    else if (prop.PropertyType.FullName == "System.Boolean")
                                        ss = "BIT";
                                    else if (prop.PropertyType.FullName == "System.DateTime")
                                        ss = "DATETIME";
                                    else if (prop.PropertyType.FullName == "System.Char")
                                        ss = "CHAR(3)";
                                    create += string.Format(",{0} {1}", s, ss);
                                }
                            }
                            i++;
                        }
                        else if(foreignKeyAttribute == null)
                        {
                            if (i == 0)
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    if (prop.PropertyType.FullName == "System.String")
                                        ss = "VARCHAR(16)";
                                    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                        ss = "DECIMAL(6,2)";
                                    else if (prop.PropertyType.FullName == "System.Int32")
                                        ss = "INT";
                                    else if (prop.PropertyType.FullName == "System.Boolean")
                                        ss = "BIT";
                                    else if (prop.PropertyType.FullName == "System.DateTime")
                                        ss = "DATETIME";
                                    else if (prop.PropertyType.FullName == "System.Char")
                                        ss = "CHAR(3)";
                                    create += string.Format("{0} {1} ", s, ss);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    create += "[" + s + "]";
                                }
                            }
                            else
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    if (prop.PropertyType.FullName == "System.String")
                                        ss = "VARCHAR(55)";
                                    else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                        ss = "DECIMAL(6,2)";
                                    else if (prop.PropertyType.FullName == "System.Int32")
                                        ss = "INT";
                                    else if (prop.PropertyType.FullName == "System.Boolean")
                                        ss = "BIT";
                                    else if (prop.PropertyType.FullName == "System.DateTime")
                                        ss = "DATETIME";
                                    else if (prop.PropertyType.FullName == "System.Char")
                                        ss = "CHAR(3)";
                                    create += string.Format(",{0} {1} ", s, ss);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    create += "," + "[" + s + "]";
                                }
                            }
                            i++;
                        }
                        else
                        {

                            if (_type == ConnectionType.MySql)
                            {
                                if (prop.PropertyType.FullName == "System.String")
                                    ss = "VARCHAR(16)";
                                else if (prop.PropertyType.FullName.Contains("System.Decimal"))
                                    ss = "DECIMAL(6,2)";
                                else if (prop.PropertyType.FullName == "System.Int32")
                                    ss = $"INT , FOREIGN KEY {prop.Name} REFERENCES {foreignKeyAttribute.Name}({prop.Name})";
                                else if (prop.PropertyType.FullName == "System.Boolean")
                                    ss = "BIT";
                                else if (prop.PropertyType.FullName == "System.DateTime")
                                    ss = "DATETIME";
                                else if (prop.PropertyType.FullName == "System.Char")
                                    ss = "CHAR(3)";
                                create += string.Format("{0} {1}", s, ss);
                            }
                        }

                    }
                }
            }
            create += ");";
            result.SqlCommand = string.Format("{0}", create);
            return result;
        }
        public static List<InsertSql> GetInsertSqlList<T>(List<T> model, ConnectionType _type) where T : new()
        {
            var identityScope = Identity(_type);
            if (_type == ConnectionType.MySql)
                scopeIdentity = "SELECT LAST_INSERT_ID()";
            else if (_type == ConnectionType.MsSQL)
                scopeIdentity = "SELECT SCOPE_IDENTITY()";
            T objs = new();
            var results = new List<InsertSql>();
            Object entityid = 0;
            string entityprimarykey = "";
            foreach (var item in model)
            {
                int i = 0;
                string insert = string.Empty;
                insert += string.Format(@"Insert into {0} (", objs.GetType().Name);
                List<SqlParameter> ls = new List<SqlParameter>();
                InsertSql insertSql = new();
                insertSql.SubSqls = new List<InsertSql>();
                var primaryName = string.Format("@{0}Id", item.GetType().Name);
                foreach (var prop in objs.GetType().GetProperties())
                {
                    if (prop.PropertyType.FullName.StartsWith("System.") && !prop.PropertyType.FullName.StartsWith("System.Collection"))
                    {
                        if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
                        {
                            Console.WriteLine("Not Mapped");
                        }
                        else
                        {
                            string s = prop.Name;
                            var attribute = Attribute.GetCustomAttribute(prop, typeof(KeyAttribute)) as KeyAttribute;
                            if (attribute != null) { }
                            else
                            {
                                if (i == 0)
                                {
                                    if (_type == ConnectionType.MySql)
                                    {
                                        insert += string.Format("{0}", s);
                                    }
                                    else if (_type == ConnectionType.MsSQL)
                                    {
                                        insert += "[" + s + "]";
                                    }
                                }
                                else
                                {
                                    if (_type == ConnectionType.MySql)
                                    {
                                        insert += string.Format(",{0}", s);
                                    }
                                    else if (_type == ConnectionType.MsSQL)
                                    {
                                        insert += "," + "[" + s + "]";
                                    }
                                }
                                i++;
                            }
                        }
                    }
                    else if (prop.PropertyType.FullName.StartsWith("System.Collection"))
                    {
                        object anArray = prop.GetValue(item, null);
                        IEnumerable enumerable = anArray as IEnumerable;
                        if (enumerable != null)
                        {
                            foreach (object element in enumerable)
                            {
                                var subinsert = GetSubInsertSql(element.GetType(), element, primaryName, _type);
                                insertSql.SubSqls.Add(subinsert);
                            }
                        }
                    }
                    else
                    {
                        object anObject = prop.GetValue(item, null);
                        var subinsert = GetSubInsertSql(anObject.GetType(), anObject, primaryName, _type);
                        insertSql.SubSqls.Add(subinsert);
                    }
                }
                insert += ") Values(";
                i = 0;
                foreach (var p in objs.GetType().GetProperties().Where(p => !p.GetGetMethod().GetParameters().Any()))
                {
                    if (p.PropertyType.FullName.StartsWith("System.") && !p.PropertyType.FullName.StartsWith("System.Collection"))
                    {
                        var attribute = Attribute.GetCustomAttribute(p, typeof(KeyAttribute)) as KeyAttribute;
                        if (attribute != null)
                        {
                            entityprimarykey = p.Name;
                            entityid = p.GetValue(item, null);
                            ls.Add(new SqlParameter("@" + p.Name, p.GetValue(item, null)));
                        }
                        else
                        {
                            var value = p.GetValue(item, null);
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
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    insert += string.Format("@{0}", p.Name);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    insert += "@" + p.Name;
                                }
                            }

                            else
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    insert += string.Format(",@{0}  ", p.Name);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    insert += ",@" + p.Name;
                                }
                            }

                            i++;
                        }
                    }
                }
                insert += ");";

                insertSql.SqlCommand = string.Format("{0} {1}", insert, identityScope);
                insertSql.PrimarySubSqls = primaryName;
                insertSql.SqlParameters = ls;
                results.Add(insertSql);
            }
            return results;
        }
        public static InsertSql GetSubInsertSql(Type type, object model, string primaryParamName, ConnectionType _type)
        {
            var identityScope = Identity(_type);
            var result = new InsertSql();
            InsertSql sq = new();
            string insert = string.Empty;
            List<SqlParameter> lsub = new List<SqlParameter>();
            var cint = 0;
            var nob = Activator.CreateInstance(type);
            insert += string.Format(@"Insert into {0} (", nob.GetType().Name);
            foreach (var ip in nob.GetType().GetProperties())
            {
                if (ip.PropertyType.FullName.StartsWith("System.") && !ip.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    if (ip.GetType().GetProperties().Where(x => Attribute.IsDefined(ip, typeof(NotMappedAttribute))).Any())
                    {
                        Console.WriteLine("Not Mapped");
                    }
                    else
                    {
                        string sn = ip.Name;
                        var attribute = Attribute.GetCustomAttribute(ip, typeof(KeyAttribute)) as KeyAttribute;
                        var notmapped = Attribute.GetCustomAttribute(ip, typeof(NotMappedAttribute)) as NotMappedAttribute;
                        if (notmapped == null)
                        {
                            if (attribute != null) { }
                            else
                            {
                                if (cint == 0)
                                {
                                    if (_type == ConnectionType.MySql)
                                    {
                                        insert += string.Format("{0}", sn);
                                    }
                                    else if (_type == ConnectionType.MsSQL)
                                    {
                                        insert += "[" + sn + "]";
                                    }
                                }
                                else
                                {
                                    if (_type == ConnectionType.MySql)
                                    {
                                        insert += string.Format(",{0}", sn);
                                    }
                                    else if (_type == ConnectionType.MsSQL)
                                    {
                                        insert += "," + "[" + sn + "]";
                                    }
                                }
                                //insert += string.Format(",{0}", sn);
                                cint++;
                            }
                        }
                    }
                }
                else if (ip.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    object anArray = ip.GetValue(model, null);
                    IEnumerable enumerable = anArray as IEnumerable;
                    if (enumerable != null)
                    {
                        foreach (object element in enumerable)
                        {
                            var primaryName = string.Format("{0}Id", nob.GetType().Name);
                            sq.PrimarySubSqls = primaryName;
                            var subinsert = GetSubInsertSql(element.GetType(), element, primaryName, _type);
                            sq.PrimarySub2Sqls = subinsert.PrimarySub2Sqls;
                            result.SubSqls.Add(subinsert);
                        }
                    }
                }
                else
                {
                    object anObject = ip.GetValue(model, null);
                    var primaryName = string.Format("{0}Id", nob.GetType().Name);
                    sq.PrimarySubSqls = primaryName;
                    var subinsert = GetSubInsertSql(anObject.GetType(), anObject, primaryName, _type);
                    sq.PrimarySub2Sqls = subinsert.PrimarySub2Sqls;
                    result.SubSqls.Add(subinsert);
                }
            }
            cint = 0;
            insert += ") Values(";

            var res = nob.GetType().GetProperties().Where(p => !p.GetGetMethod().GetParameters().Any() && !p.PropertyType.FullName.StartsWith("System.Collection"));


            foreach (var p in res)
            {
                if (p.PropertyType.FullName.StartsWith("System.") && !p.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    var attribute = Attribute.GetCustomAttribute(p, typeof(KeyAttribute)) as KeyAttribute;
                    var fk = Attribute.GetCustomAttribute(p, typeof(ForeignKeyAttribute)) as ForeignKeyAttribute;
                    var notmapped = Attribute.GetCustomAttribute(p, typeof(NotMappedAttribute)) as NotMappedAttribute;

                    if (attribute != null || notmapped != null)
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
                                    value = "null";
                                else if (p.PropertyType.FullName.Contains("System.Decimal"))
                                    value = 0;
                                else if (p.PropertyType.FullName == "System.Int32")
                                    value = 0;
                                lsub.Add(new SqlParameter("@" + p.Name, value));
                            }
                            else
                            {
                                lsub.Add(new SqlParameter("@" + p.Name, value));
                            }
                            if (cint == 0)
                                insert += string.Format("@{0}", p.Name);
                            else
                                insert += string.Format(",@{0}  ", p.Name);
                        }
                        else
                        {
                            var value = p.GetValue(model, null);
                            if (cint == 0)
                            {
                                insert += string.Format("@{0}", p.Name);
                                lsub.Add(new SqlParameter(p.Name, value));
                            }
                            else
                            {
                                insert += string.Format(",@{0}  ", p.Name);
                                lsub.Add(new SqlParameter(p.Name, value));
                            }
                        }
                        cint++;

                    }
                }
            }
            insert += ");";

            sq.SqlCommand = string.Format("{0} {1}", insert, identityScope);
            sq.SubSqls = result.SubSqls;
            sq.SqlParameters = lsub;
            result = sq;
            return result;
        }
        internal static MethodInfo GetPropertySetter(PropertyInfo propertyInfo, Type type)
        {
            if (propertyInfo.DeclaringType == type) return propertyInfo.GetSetMethod(true);

            return propertyInfo.DeclaringType.GetProperty(
                   propertyInfo.Name,
                   BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                   Type.DefaultBinder,
                   propertyInfo.PropertyType,
                   propertyInfo.GetIndexParameters().Select(p => p.ParameterType).ToArray(),
                   null).GetSetMethod(true);
        }
        public static string LambdaToString<T>(Expression<Func<T, bool>> expression)
        {

            var replacements = new Dictionary<string, string>();
            WalkExpression(replacements, expression);

            string body = expression.Body.ToString();
            foreach (var replacement in replacements)
            {
                body = body.Replace(replacement.Key, replacement.Value);
            }
            foreach (var parm in expression.Parameters)
            {
                var parmName = parm.Name;
                var parmTypeName = parm.Type.Name;
                body = body.Replace(parmName + ".", "");
            }



            return body;
        }
        // dynimcinvoke null gelme durumunu tekrar dene : olmadı
        private static void WalkExpression(Dictionary<string, string> replacements, System.Linq.Expressions.Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.MemberAccess:
                    string replacementExpression = expression.ToString();
                    if (replacementExpression.Contains("value("))
                    {
                        string replacementValue = Expression.Lambda(expression).Compile().DynamicInvoke()?.ToString();
                            if (!replacements.ContainsKey(replacementExpression))
                            {
                                if (expression.Type.Name.Equals("Int32") || expression.Type.Name.Equals("Decimal")
                                    || expression.Type.Name.Equals("Double") || expression.Type.Name.Equals("Boolean"))
                                {
                                    replacements.Add(replacementExpression, replacementValue.ToString());
                                }
                                else if (expression.Type.Name == "Char")
                                {
                                    replacements.Add(replacementExpression, Convert.ToChar(replacementValue).ToString());
                                }
                                else if (expression.Type.Name == "DateTime")
                                {
                                    replacements.Add(replacementExpression, "'" + Convert.ToDateTime(replacementValue).ToString("yyyy-MM-dd HH:mm:ss") + "'");
                                }
                                else
                                {
                                    replacements.Add(replacementExpression, "'" + replacementValue + "'");
                                }
                        }
                    }
                    break;

                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.AndAlso:
                case ExpressionType.Equal:
                    string replacementExpression2 = expression.ToString();
                    if (replacementExpression2.Contains("== null"))
                    {
                        replacements.Add("== null", "is null");
                    }
                    else
                    {
                        var bexp = expression as BinaryExpression;
                        WalkExpression(replacements, bexp.Left);
                        WalkExpression(replacements, bexp.Right);
                    }
                    break;
                case ExpressionType.NotEqual:
                    string replacementExpression1 = expression.ToString();
                    if (replacementExpression1.Contains("!= null"))
                    {
                        if (!replacements.ContainsKey("!= null"))
                            replacements.Add("!= null", "is not null");
                    }
                    else if (replacementExpression1.Contains("!="))
                    {
                        if (!replacements.ContainsKey("!="))
                            replacements.Add("!=", "is not ");
                    }
                    break;
                //case ExpressionType.GreaterThan:
                //    string replacementExpression3 = expression.ToString();
                //    if (replacementExpression3.Contains(">="))
                //    {
                //        if (!replacements.ContainsKey("!= null"))
                //            replacements.Add("!= null", "is not null");
                //    }
                //    break;
                case ExpressionType.Call:
                    if (((MethodCallExpression)expression).Method.Name == "Contains")
                    {
                        var rpv = ParseContainsExpression(expression as MethodCallExpression);
                        if (!string.IsNullOrEmpty(rpv))
                        {
                            replacements.Add(expression.ToString(), rpv);
                        }
                    }
                    else if (((MethodCallExpression)expression).Method.Name == "ToString")
                    {
                        var rpv = expression.ToString().Remove(expression.ToString().Length - 11);
                        //var rpv = expression.ToString().Replace(expression,expression.ToString().ToList().TrimEnd(new char[] { ',' }));
                        if (!string.IsNullOrEmpty(rpv))
                        {
                            replacements.Add(expression.ToString(), $"'{rpv}'");
                        }
                    }
                    break;

                case ExpressionType.Lambda:
                    var lexp = expression as LambdaExpression;
                    WalkExpression(replacements, lexp.Body);
                    break;

                case ExpressionType.Constant:
                    //do nothing
                    break;

                default:
                    Trace.WriteLine("Unknown type");
                    break;
            }

        }
        private static string ParseContainsExpression(MethodCallExpression expression)
        {
            string condition = string.Empty;
            // The method must be called Contains and must return bool
            if (expression.Method.Name != "Contains" || expression.Method.ReturnType != typeof(bool)) return condition;
            var list = expression.Object;
            Expression operand;
            if (list == null)
            {
                // Static method
                // Must be Enumerable.Contains(source, item)
                if (expression.Method.DeclaringType != typeof(Enumerable) || expression.Arguments.Count != 2) return condition;
                list = expression.Arguments[0];
                operand = expression.Arguments[1];
            }
            else
            {
                // Instance method
                // Exclude string.Contains
                if (list.Type == typeof(string)) return condition;
                // Must have a single argument
                if (expression.Arguments.Count != 1) return condition;
                operand = expression.Arguments[0];
                // The list must be IEnumerable<operand.Type>
                if (!typeof(IEnumerable<>).MakeGenericType(operand.Type).IsAssignableFrom(list.Type)) return condition;
            }
            // Try getting the list items
            object listValue;
            if (list.NodeType == ExpressionType.Constant)
                // from constant value
                listValue = ((ConstantExpression)list).Value;
            else
            {
                // from constant value property/field
                var listMember = list as MemberExpression;
                if (listMember == null) return condition;
                var listOwner = listMember.Expression as ConstantExpression;
                if (listOwner == null) return condition;
                var listProperty = listMember.Member as PropertyInfo;
                listValue = listProperty != null ? listProperty.GetValue(listOwner.Value) : ((FieldInfo)listMember.Member).GetValue(listOwner.Value);
            }
            var listItems = listValue as System.Collections.IEnumerable;
            if (listItems == null) return condition;
            condition = operand.ToString() + " in (";
            foreach (var item in listItems)
            {
                if (item.GetType().Name == "Int32" || item.GetType().Name == "Decimal" || item.GetType().Name == "Boolean" || item.GetType().Name == "Double")
                    condition += item.ToString();
                else
                    condition += "'" + item.ToString() + "'";
                condition += ",";
            }
            if (condition.EndsWith(","))
                condition = condition.Remove(condition.Length - 1, 1);

            condition += ")";
            return condition;
        }
        public static TValue UpdateAndGet<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, TKey key, TValue newVal)
        {
            TValue oldVal;
            dictionary.TryGetValue(key, out oldVal);
            dictionary[key] = newVal;

            return oldVal;
        }
        public static UpdateSql SubUpdateSql(Type type, object model, string primaryParamName, ConnectionType _type)
        {
            var result = new UpdateSql();
            string update = string.Empty;
            List<SqlParameter> lsub = new List<SqlParameter>();
            var cint = 0;
            Object entityid = 0;
            string entityprimarykey = "";
            var nob = Activator.CreateInstance(type);
            update += string.Format(@"update `{0}` set ", nob.GetType().Name);
            foreach (var ip in nob.GetType().GetProperties())
            {
                if (ip.PropertyType.FullName.StartsWith("System.") && !ip.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    string sn = ip.Name;
                    var attribute = Attribute.GetCustomAttribute(ip, typeof(KeyAttribute)) as KeyAttribute;
                    var notmapped = Attribute.GetCustomAttribute(ip, typeof(NotMappedAttribute)) as NotMappedAttribute;
                    if (notmapped == null)
                    {
                        if (attribute != null) { }
                        else
                        {
                            if (cint == 0)
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    update += string.Format(sn + "=@" + sn);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    update += "[" + sn + "]" + "=@" + sn;
                                }
                            }
                            else
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    update += string.Format("," + sn + "=@" + sn);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    update += string.Format("," + sn + "=@" + sn);
                                }
                            }
                            cint++;
                        }
                    }
                }
                else if (ip.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    object anArray = ip.GetValue(model, null);
                    IEnumerable enumerable = anArray as IEnumerable;
                    if (enumerable != null)
                    {
                        foreach (object element in enumerable)
                        {
                            var primaryName = string.Format("@{0}Id", nob.GetType().Name);
                            var subupdate = SubUpdateSql(element.GetType(), element, primaryName, _type);
                            result.SubSqls.Add(subupdate);
                        }
                    }
                }
                else
                {

                    object anObject = ip.GetValue(model, null);
                    var primaryName = string.Format("@{0}Id", nob.GetType().Name);
                    var subupdate = SubUpdateSql(anObject.GetType(), anObject, primaryName, _type);
                    result.SubSqls.Add(subupdate);
                }
            }
            foreach (var p in nob.GetType().GetProperties().Where(p => !p.GetGetMethod().GetParameters().Any()))
            {
                if (p.Name == "id" || p.Name == "Id" || p.Name == "ID")
                {
                    entityprimarykey = p.Name;
                    entityid = p.GetValue(model, null);
                    lsub.Add(new SqlParameter("@" + p.Name, p.GetValue(model, null)));
                }
                else
                {
                    var value = p.GetValue(model, null);
                    if (value == null)
                    {
                        if (p.PropertyType.FullName == "System.String")
                        {
                            value = "";
                        }
                        else if (p.PropertyType.FullName.Contains("System.Decimal"))
                        {
                            value = 0;
                        }
                        else if (p.PropertyType.FullName == "System.Int32")
                        {
                            value = 0;
                        }
                    }
                    lsub.Add(new SqlParameter("@" + p.Name, value));
                    cint++;
                }


                var a = p.GetValue(model, null);
            }
            update += " Where " + entityprimarykey + "=@" + entityprimarykey;
            UpdateSql sq = new();
            sq.SqlParameters = lsub;
            sq.SqlCommand = string.Format("{0}", update);
            result = sq;
            return result;
        }
        public static UpdateSql UpdateSql<T>(T model, ConnectionType _type) where T : class, new()
        {
            var result = new UpdateSql();
            T obj = new T();
            string update = string.Empty;
            update += string.Format(@"Update `{0}` set ", obj.GetType().Name);
            int i = 0;
            Object entityid = 0;
            string entityprimarykey = "";
            List<SqlParameter> ls = new List<SqlParameter>();
            var primaryName = string.Format("@{0}Id", model.GetType().Name);
            foreach (var prop in obj.GetType().GetProperties())
            {
                if (prop.PropertyType.FullName.StartsWith("System.") && !prop.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
                    {
                        Console.WriteLine("Not Mapped");
                    }
                    else
                    {
                        string s = prop.Name;
                        var attribute = Attribute.GetCustomAttribute(prop, typeof(KeyAttribute)) as KeyAttribute;
                        if (attribute != null) { }
                        else
                        {
                            if (i == 0)
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    update += string.Format(s + "=@" + s);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    update += "[" + s + "]" + "=@" + s;
                                }
                            }

                            else
                            {
                                if (_type == ConnectionType.MySql)
                                {
                                    update += string.Format("," + s + "=@" + s);
                                }
                                else if (_type == ConnectionType.MsSQL)
                                {
                                    update += "," + "[" + s + "]" + "=@" + s;
                                }
                            }
                            i++;
                        }
                    }
                }
                else if (prop.PropertyType.FullName.StartsWith("System.Collection"))
                {
                    object anArray = prop.GetValue(model, null);
                    IEnumerable enumerable = anArray as IEnumerable;
                    if (enumerable != null)
                    {
                        foreach (object element in enumerable)
                        {
                            var subinsert = SubUpdateSql(element.GetType(), element, primaryName, _type);
                            result.SubSqls.Add(subinsert);
                        }
                    }
                }

            }
            foreach (var p in obj.GetType().GetProperties().Where(p => !p.GetGetMethod().GetParameters().Any()))
            {

                if (p.GetType().GetProperties().Where(x => Attribute.IsDefined(p, typeof(NotMappedAttribute))).Any())
                {
                    Console.WriteLine("Not Mapped");
                }
                else
                {

                    if (p.Name == "id" || p.Name == "Id" || p.Name == "ID")
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
                            {
                                value = "";
                            }
                            else if (p.PropertyType.FullName.Contains("System.Decimal"))
                            {
                                value = 0;
                            }
                            else if (p.PropertyType.FullName == "System.Int32")
                            {
                                value = 0;
                            }
                        }
                        ls.Add(new SqlParameter("@" + p.Name, value));
                        i++;
                    }
                }
                var a = p.GetValue(model, null);
            }
            update += " Where " + entityprimarykey + "=@" + entityprimarykey;
            result.PrimaryId = entityprimarykey;
            result.SqlParameters = ls;
            result.SqlCommand = string.Format("{0}", update);
            return result;
        }
        public static List<UpdateSql> UpdateSqlList<T>(List<T> model, ConnectionType _type) where T : class, new()
        {
            T obj = new();
            var result = new List<UpdateSql>();
            Object entityid = 0;
            string entityprimarykey = "";
            foreach (var item in model)
            {
                int i = 0;
                List<SqlParameter> ls = new List<SqlParameter>();
                UpdateSql us = new();
                us.SubSqls = new List<UpdateSql>();
                string update = string.Empty;
                update += string.Format(@"Update `{0}` set ", obj.GetType().Name);
                var primaryName = string.Format("@{0}Id", item.GetType().Name);
                foreach (var prop in obj.GetType().GetProperties())
                {
                    if (prop.PropertyType.FullName.StartsWith("System.") && !prop.PropertyType.FullName.StartsWith("System.Collection"))
                    {
                        if (prop.GetType().GetProperties().Where(x => Attribute.IsDefined(prop, typeof(NotMappedAttribute))).Any())
                        {
                            Console.WriteLine("Not Mapped");
                        }
                        else
                        {
                            string s = prop.Name;
                            var attribute = Attribute.GetCustomAttribute(prop, typeof(KeyAttribute)) as KeyAttribute;
                            if (attribute != null) { }
                            else
                            {
                                if (i == 0)
                                {
                                    if (_type == ConnectionType.MySql)
                                    {
                                        update += string.Format(s + "=@" + s);
                                    }
                                    else if (_type == ConnectionType.MsSQL)
                                    {
                                        update += "[" + s + "]" + "=@" + s;
                                    }
                                }

                                else
                                {
                                    if (_type == ConnectionType.MySql)
                                    {
                                        update += string.Format("," + s + "=@" + s);
                                    }
                                    else if (_type == ConnectionType.MsSQL)
                                    {
                                        update += "," + "[" + s + "]" + "=@" + s;
                                    }
                                }
                                i++;
                            }
                        }
                    }
                    else if (prop.PropertyType.FullName.StartsWith("System.Collection"))
                    {
                        object anArray = prop.GetValue(item, null);
                        IEnumerable enumerable = anArray as IEnumerable;
                        if (enumerable != null)
                        {
                            foreach (object element in enumerable)
                            {
                                var subinsert = SubUpdateSql(element.GetType(), element, primaryName, _type);
                                us.SubSqls.Add(subinsert);
                            }
                        }
                    }

                }
                foreach (var p in obj.GetType().GetProperties().Where(p => !p.GetGetMethod().GetParameters().Any()))
                {
                    if (p.Name == "id" || p.Name == "Id" || p.Name == "ID")
                    {
                        entityprimarykey = p.Name;
                        entityid = p.GetValue(item, null);
                        ls.Add(new SqlParameter("@" + p.Name, p.GetValue(item, null)));
                    }
                    else
                    {
                        var value = p.GetValue(item, null);
                        if (value == null)
                        {
                            if (p.PropertyType.FullName == "System.String")
                            {
                                value = "";
                            }
                            else if (p.PropertyType.FullName.Contains("System.Decimal"))
                            {
                                value = 0;
                            }
                            else if (p.PropertyType.FullName == "System.Int32")
                            {
                                value = 0;
                            }
                        }
                        ls.Add(new SqlParameter("@" + p.Name, value));
                        i++;
                    }


                    var a = p.GetValue(item, null);
                }
                update += " Where " + entityprimarykey + "=@" + entityprimarykey;
                us.PrimaryId = entityid;
                us.SqlParameters = ls;
                us.SqlCommand = string.Format("{0}", update);
                result.Add(us);
            }
            return result;
        }

       
    }
}
