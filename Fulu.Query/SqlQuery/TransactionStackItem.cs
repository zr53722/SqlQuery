using System;
using System.Data.Common;

namespace Fulu.Query.SqlQuery
{

	/// <summary>
	/// 事务栈成员
	/// </summary>
	internal class TransactionStackItem
	{
		public ConnectionInfo Info { get; set; }
		public TransactionMode Mode { get; set; }
        //指示在当前函数栈中是否启用事务
		public bool EnableTranscation { get; set; }
        //只是在当前函数栈中是否可以关闭连接
		public bool CanClose { get; set; }

        /// <summary>
        /// 执行的SQL次数
        /// </summary>
        public int HitCount { get; set; }
		
	}

	/// <summary>
	/// 连接信息
	/// </summary>
	internal class ConnectionInfo
	{

	    public ConnectionInfo(string connectionString, string providerName)
	    {
	        this.ConnectionString = connectionString;
	        this.ProviderName = providerName;
	    }

		public string ConnectionString { get; set; }
		public string ProviderName { get; set; }
		public DbConnection Connection { get; set; }
		public DbTransaction Transaction { get; set; }

		public bool IsSame(ConnectionInfo info)
		{
			if( info == null ) {
				throw new ArgumentNullException("info");
			}

			return this.ConnectionString == info.ConnectionString && this.ProviderName == info.ProviderName;
		}
	}
}
