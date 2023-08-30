using System.Collections.Generic;
using System.Data.SqlClient;

namespace RetSharp.Model
{
    public class InsertSql
    {
        public InsertSql()
        {
            SqlParameters = new List<SqlParameter>();
            SubSqls = new List<InsertSql>();
        }
        public string SqlCommand { get; set; }
        public string PrimaryName { get; set; }
        public object PrimaryId { get; set; }
        public List<SqlParameter> SqlParameters { get; set; }
        public List<InsertSql> SubSqls { get; set; }
        public string PrimarySubSqls { get; set; }
        public object PrimarySub2Sqls { get; set; }
    }
}
