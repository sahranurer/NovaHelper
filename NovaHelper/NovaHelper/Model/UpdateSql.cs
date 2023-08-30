using System.Collections.Generic;
using System.Data.SqlClient;

namespace RetSharp.Model
{
    public class UpdateSql
    {
        public UpdateSql()
        {
            SqlParameters = new List<SqlParameter>();
            SubSqls = new List<UpdateSql>();
        }
        public string SqlCommand { get; set; }
        public string PrimaryName { get; set; }
        public object PrimaryId { get; set; }
        public List<SqlParameter> SqlParameters { get; set; }
        public List<UpdateSql> SubSqls { get; set; }
    }
}
