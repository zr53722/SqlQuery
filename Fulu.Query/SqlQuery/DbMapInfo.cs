using System.Reflection;

namespace Fulu.Query.SqlQuery
{
	internal class DbMapInfo
	{
		public string DbName { get; private set; }

		public string NetName { get; private set; }

		public PropertyInfo PropertyInfo { get; private set; }


		public DbMapInfo(string dbName, string netName,  PropertyInfo prop)
		{
			this.DbName = dbName;
			this.NetName = netName;
	
			this.PropertyInfo = prop;
	
		}

		
	}
}
