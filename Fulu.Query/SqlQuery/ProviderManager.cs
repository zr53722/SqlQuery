using System;
using System.Collections;
using System.Configuration;
using System.Data;
using System.Data.Common;
using Fulu.Query.SqlQuery;

namespace Fulu.Query.SqlQuery
{
	/// <summary>
	/// 数据库提供程序管理类
	/// </summary>
	public static class ProviderManager
	{


        private static Hashtable s_providerDict = Hashtable.Synchronized(new Hashtable());

		/// <summary>
		/// 返回数据提供程序创建工厂
		/// </summary>
		public static DbProviderFactory ProviderFactory
		{
			get
			{
				string providerName = ConnectionScope.ProviderName;

				object obj = s_providerDict[providerName];

				if( obj == null ) {

					DbProviderFactory factory = DbProviderFactories.GetFactory(providerName);
					s_providerDict[providerName] = factory;
					return factory;

				}
				else {
					return obj as DbProviderFactory;
				}

				
			}
		}

		/// <summary>
		/// 创建DBConnection类的指定数据库提供程序实例
		/// </summary>
		/// <returns>DbConnection类实例</returns>
		public static DbConnection CreateConnection()
		{
			return ProviderFactory.CreateConnection();
		}

		internal static DbConnection CreateConnection(string connectionString)
		{
			if( string.IsNullOrEmpty(connectionString) ) {
				throw new ArgumentNullException("connectionString");
			}

			DbConnection conn = ProviderFactory.CreateConnection();
			conn.ConnectionString = connectionString;

			return conn;
		}

		/// <summary>
		/// 创建DbCommand类的指定数据库程序实例
		/// </summary>
		/// <returns>DbCommand类实例</returns>
		public static DbCommand CreateCommand()
		{
		    DbCommand dbCommand = ProviderFactory.CreateCommand();
            return dbCommand;
		}

	    /// <summary>
		/// 创建DbDataAdapter类的指定数据库程序实例
		/// </summary>
		/// <returns>DbDataAdapter类实例</returns>
		public static DbDataAdapter CreateDataAdapter()
		{
			return ProviderFactory.CreateDataAdapter();
		}

		/// <summary>
		/// 创建DbParameter类的指定数据库程序实例
		/// </summary>
		/// <returns>DbParameter类实例</returns>
		public static DbParameter CreateParameter()
		{
			return ProviderFactory.CreateParameter();
		}

		/// <summary>
		/// 创建DbParameter类的指定数据库程序实例
		/// </summary>
		/// <param name="name">参数名</param>
		/// <param name="value">参数值</param>
		/// <returns>DbParameter类实例</returns>
		public static DbParameter CreateParameter(string name, object value)
		{
			if( string.IsNullOrEmpty(name) ) {
				throw new ArgumentNullException("name");
			}

			if( value == null ) {
				throw new ArgumentNullException("value");
			}

			DbParameter parameter = ProviderFactory.CreateParameter();
			parameter.ParameterName = name;
			parameter.Value = value;

			return parameter;
		}

		/// <summary>
		/// 创建DbParameter类的指定数据库程序实例
		/// </summary>
		/// <param name="name">参数名</param>
		/// <param name="type">参数类型</param>
		/// <param name="value">参数值</param>
		/// <returns>DbParameter类实例</returns>
		internal static DbParameter CreateParameter(string name, DbType type, object value)
		{
			if( string.IsNullOrEmpty(name) ) {
				throw new ArgumentNullException("name");
			}

			if( value == null ) {
				throw new ArgumentNullException("value");
			}

			DbParameter parameter = ProviderFactory.CreateParameter();
			parameter.ParameterName = name;
			parameter.Value = value;
			parameter.DbType = type;

			return parameter;
		}
	}
}
