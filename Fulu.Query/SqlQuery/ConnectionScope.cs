using System;
using System.Data.SqlClient;

namespace Fulu.Query.SqlQuery
{
    /// <summary>
    /// 表示连接与事务的作用域
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item><description>必须放在using作用域,才能保证资源的正确释放.</description></item>
    /// <item><description>类内部维护了一个数据库连接,在作用域内的操作将共用此连接.</description></item>
    /// <item><description>对于非Map项目,连接字符串通过:<see cref="Initializer"/>类的UnSafeInit(string connectionString)方法指定</description></item>
    /// <item><description>对于Map项目,连接字符串通过:<see cref="InitializerModule"/>类初始化,HttpModule只需配置在web.config文件中即可.</description></item>
    /// <item><description>作用域可以嵌套使用,包括在子函数中的嵌套,都将共用一个连接</description></item>
    /// <item><description>MyDB操作是内部单独开启并维护的数据库连接,不从本作用域共享连接与实务</description></item>
    /// </list>
    /// </remarks>
    /// <example>
    /// <para>下面的代码演示了ConnectionScope的用法</para>
    /// <code>
    /// //类需要放在using中使用
    /// using (ConnectionScope scope = new ConnectionScope())
    /// {
    ///     //声明一个CPQuery实例,具体用法请参见文档中关于CPQuery的介绍
    ///     var query = "insert into TestTable(RowGuid, RowString) values(".AsCPQuery()
    ///        + GuidHelper.NewSeqGuid().AsQueryParameter() 
    ///        + "," + "字符串内容".AsQueryParameter() + ")";
    ///     //执行命令
    ///     query.ExecuteNonQuery();
    /// }
    /// </code>
    /// </example>
    public sealed class ConnectionScope : IDisposable
	{
		private static string s_connectionString;
		private static string s_providerName;

		/// <summary>
		/// 设置默认的连接字符串
		/// </summary>
		/// <param name="connectionString">连接字符串</param>
		/// <param name="providerName">提供程序名称</param>
        internal static void SetDefaultConnection(string connectionString, string providerName = "System.Data.SqlClient")
		{
			if( string.IsNullOrEmpty(connectionString) )
				throw new ArgumentNullException("connectionString");

			if( string.IsNullOrEmpty(providerName) )
				throw new ArgumentNullException("providerName");

			s_connectionString = connectionString;

			s_providerName = providerName;
		}

		[ThreadStatic]
		private static ConnectionManager s_connection;

		/// <summary>
		/// 创建一个ConnectionScope对象
		/// <remarks>
		/// <list type="bullet">
		/// <item><description>等同于new ConnectionScope(TransactionMode.Inherits);</description></item>
		/// <item><description>事务行为由<see cref="TransactionMode"/>枚举决定;</description></item>
		/// </list>
		/// </remarks>
		/// </summary>
		public ConnectionScope()
		{
			this.Init(TransactionMode.Inherits, null, null);
		}

		/// <summary>
		/// 根据指定的TransactionMode，创建一个ConnectionScope对象
		/// <remarks>
		/// <list type="bullet">
		/// <item><description>事务行为由<see cref="TransactionMode"/>枚举决定;</description></item>
		/// </list>
		/// </remarks>
		/// </summary>
		/// <param name="mode">事务模式</param>
		public ConnectionScope(TransactionMode mode)
		{
			this.Init(mode, null, null);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mode"></param>
		/// <param name="connectionString"></param>
		/// <param name="providerName"></param>
        public ConnectionScope(TransactionMode mode, string connectionString, string providerName = "System.Data.SqlClient")
		{
			if( string.IsNullOrEmpty(connectionString) ) {
				throw new ArgumentNullException("connectionString");
			}

			if( string.IsNullOrEmpty(providerName) ) {
				throw new ArgumentNullException("providerName");
			}

			this.Init(mode, connectionString, providerName);
		}

		

        private void Init(TransactionMode mode, string connectionString, string providerName = "System.Data.SqlClient")
		{
			string localConnectionString = connectionString;
			string localProviderName = providerName;

			if( s_connection == null ) {
				s_connection = new ConnectionManager();
			}
			else {

				ConnectionInfo info = s_connection.GetTopStackInfo();

				if( info != null ) {

					//如果当前层级没有指定连接字符串或提供程序,则默认从栈顶取
					if( string.IsNullOrEmpty(connectionString) ) {
						localConnectionString = info.ConnectionString;
					}
					if( string.IsNullOrEmpty(providerName) ) {
						localProviderName = info.ProviderName;
					}

				}
			}

			//即没有指定连接字符串,栈顶也没有获取到.则使用默认链接字符串和提供程序
			if( string.IsNullOrEmpty(localConnectionString) ) {
				localConnectionString = s_connectionString;
			}
			if( string.IsNullOrEmpty(localProviderName) ) {
				localProviderName = s_providerName;
			}

			s_connection.PushTransactionMode(mode, localConnectionString, localProviderName);
		}

		/// <summary>
		/// 获取当前作用域提供程序名称
		/// </summary>
		internal static string ProviderName
		{
			get
			{
				if( s_connection == null ) {
					return s_providerName;
				}

				ConnectionInfo info = s_connection.GetTopStackInfo();
				return info.ProviderName;

			}
		}

		/// <summary>
		/// 获取当前作用域连接字符串
		/// </summary>
		internal static string ConnectionString
		{
			get
			{
				if( s_connection == null ) {
					return s_connectionString;
				}

				ConnectionInfo info = s_connection.GetTopStackInfo();
				return info.ConnectionString;
			}
		}

		internal /*static*/ ConnectionManager Current
		{
			get { return s_connection; }
		}

        /// <summary>
        /// 创建SqlBulkCopy类实例,并指定连接和事务
        /// </summary>
        /// <remarks>
        /// <list type="bullet">
        /// <item>事务由ConnectionScope构造函数的枚举控制,请不要指定UseInternalTransaction枚举选项</item>
        /// </list>
        /// </remarks>
        /// <example>
        /// <para>下面的代码演示了CreateSqlBulkCopy的用法</para>
        /// <code>
        /// <![CDATA[
        /// //准备数据
        ///	DataTable dt = new DataTable();
        ///	dt.Columns.Add("GuidVal", typeof(Guid));
        ///	dt.Columns.Add("IntVal", typeof(int));
        ///	dt.Columns.Add("DateTimeVal", typeof(DateTime));
        ///	dt.Columns.Add("StringVal", typeof(string));
        ///	dt.Columns.Add("MoneyVal", typeof(decimal));
        ///	dt.Columns.Add("FloatVal", typeof(double));
        ///
        ///	Random rnd = new Random();
        ///	for( int i = 0; i < 10000; i++ ) {
        ///		DataRow row = dt.NewRow();
        ///		row["GuidVal"] = GuidHelper.NewSeqGuid();
        ///		row["IntVal"] = rnd.Next();
        ///		row["DateTimeVal"] = DateTime.Now;
        ///		row["StringVal"] = "TestValue";
        ///		row["MoneyVal"] = 100m * (decimal)rnd.NextDouble();
        ///		row["FloatVal"] = 100 * rnd.NextDouble();
        ///		dt.Rows.Add(row);
        ///	}
        ///
        ///	//使用事务
        ///	using(ConnectionScope scope = new ConnectionScope( TransactionMode.Required )){
        ///
        ///		//创建SqlBulkCopy对象
        ///		SqlBulkCopy bulkCopy = scope.CreateSqlBulkCopy(SqlBulkCopyOptions.FireTriggers);
        ///		//设置写入目标表
        ///		bulkCopy.DestinationTableName = "TestBulkCopy";
        ///		//写入数据
        ///		bulkCopy.WriteToServer(dt);
        ///		//提交事务
        ///		scope.Commit();
        ///	}
        /// ]]>
        /// </code>
        /// </example>
        /// <param name="copyOptions">SqlBulkCopyOptions枚举</param>
        /// <returns>SqlBulkCopy对象实例</returns>
        public SqlBulkCopy CreateSqlBulkCopy(SqlBulkCopyOptions copyOptions)
		{
			return s_connection.CreateSqlBulkCopy(copyOptions);
		}

		/// <summary>
		/// 销毁当前对象，并根据情况关闭连接
		/// <remarks>
		/// <list type="bullet">
		/// <item><description>using作用域结束时将自动调用,无需在代码中使用</description></item>
		/// </list>
		/// </remarks>
		/// </summary>
		public void Dispose()
		{
			if( s_connection != null ) {
				if (s_connection.PopTransactionMode() == false) {
					//到达栈底.需要销毁
					ForceClose();
				}
			}
		}

        /// <summary>
        /// 提交事务
        /// </summary>
        /// <remarks>
        /// <list type="bullet">
        /// <item><description>必须在开启事务的层级才能提交,其他层级调用提交方法将抛出InvalidOperationException异常</description></item>
        /// </list>
        /// </remarks>
        /// <exception cref="InvalidOperationException">1.如果没有执行过任何SQL,调用此方法将抛出异常;2.当前层级必须开启事务,否则将抛出异常;3.父级开启事务,要在父级提交,否则将抛出异常</exception>
        /// <example>
        /// <para>下面的代码演示了单层事务不提交的使用方法</para>
        /// <code>
        /// //使用TransactionMode.Required开启事务
        /// using (ConnectionScope scope = new ConnectionScope(TransactionMode.Required))
        /// {
        /// 	//函数内包含一些数据库操作
        /// 	SomeMethod1();
        /// }
        /// //由于scope没有提交.最后没有任何操作生效
        /// </code>
        /// <para>下面的代码演示了单层事务提交的使用方法</para>
        /// <code>
        /// //使用TransactionMode.Required开启事务
        /// using (ConnectionScope scope = new ConnectionScope(TransactionMode.Required))
        /// {
        ///	    var product = new {
        ///	        ProductName = GuidHelper.NewSeqGuid().ToString()
        ///	    };
        ///
        ///	    CPQuery.From("INSERT INTO Products(ProductName) VALUES(@ProductName)", product).ExecuteNonQuery();
        ///	    scope.Commit();
        /// }
        /// //事务提交后,将写入一条记录
        /// </code>
        /// <para>下面的代码演示了事务嵌套场景</para>
        /// <code>
        /// //使用TransactionMode.Required开启事务
        ///	using (ConnectionScope scope1 = new ConnectionScope(TransactionMode.Required))
        ///	{
        ///	    //INSERT INTO TestTransaction(RowGuid,RowString) VALUES(newid(),'') //执行命令
        ///		
        ///	    //继承上级的事务行为(开启事务)
        ///	    using (ConnectionScope scope2 = new ConnectionScope())
        ///	    {
        ///	        //INSERT INTO TestTransaction(RowGuid,RowString) VALUES(newid(),'') //执行命令
        ///	        //这里scope2不能调用Commit()方法,调用将产生InvalidOperationException异常
        ///	        //scope2.Commit();
        ///	    }
        ///	    //最终scop1提交,将写入两条记录到数据库
        ///	    scope1.Commit();
        ///	}
        /// </code>
        /// </example>
        public void Commit()
		{
			if( s_connection == null )
                throw new InvalidOperationException("还没有打开数据库连接，无法完成提交请求。");

			s_connection.Commit();
		}


		/// <summary>
		/// 回滚事务
		/// 此方法直接抛出异常,通过异常回滚事务
		/// </summary>
		/// <exception cref="InvalidOperationException">如果数据库连接未打开则抛出此异常</exception>
		/// <param name="message">异常信息</param>
		public void Rollback(string message)
		{
			if( s_connection == null )
                throw new InvalidOperationException("还没有打开数据库连接，无法完成回滚请求。");

			s_connection.Rollback(message);
		}

		internal static void ForceClose()
		{
			if( s_connection != null ) {
				try {
					s_connection.Dispose();
				}
				catch { }
				finally {
					s_connection = null;
				}
			}
		}
	}

	/// <summary>
	/// 事务模式
	/// </summary>
	/// <remarks>
	/// 事务行为的真值表如下:
	/// <list type="table">
	/// <listheader>
	/// <mode>TransactionMode</mode>
	/// <root>根事务</root>
    /// <description>参与范围</description>
    /// </listheader>
    /// <item>
	/// <mode>Inherits</mode>
	/// <root>是</root>
	/// <description>参与根事务</description>
	/// </item>
	/// <item>
	/// <mode>Inherits</mode>
	/// <root>否</root>
	/// <description>不参与事务</description>
	/// </item>
	/// <item>
	/// <mode>Required</mode>
	/// <root>是</root>
	/// <description>参与事务(使用根事务)</description>
	/// </item>
	/// <item>
	/// <mode>Required</mode>
	/// <root>否</root>
	/// <description>参与事务(生成根事务)</description>
	/// </item>
    /// <item>
    /// <mode>Suppress</mode>
    /// <root>是</root>
    /// <description>开启新连接，不支持事务</description>
    /// </item>
    /// <item>
    /// <mode>Suppress</mode>
    /// <root>否</root>
    /// <description>开启新连接，不支持事务</description>
    /// </item>
    /// <item>
    /// <mode>Requires New</mode>
    /// <root>是</root>
    /// <description>参与事务(生成根事务)</description>
    /// </item>
    /// <item>
    /// <mode>Requires New</mode>
    /// <root>否</root>
    /// <description>参与事务(生成根事务)</description>
    /// </item>
	/// </list>
	/// </remarks>
	/// <example>
	/// <para>下面的代码演示了Required的用法</para>
	/// <code>
	/// using (ConnectionScope scope = new ConnectionScope(TransactionMode.Required))
	/// {
	///		//scope作用域开启事务
	/// }
	/// </code>
	/// <para>下面的代码演示了Inherits的用法</para>
	/// <code>
	/// using (ConnectionScope scope0 = new ConnectionScope())
	/// {
	///	    using (ConnectionScope scope1 = new ConnectionScope(TransactionMode.Inherits))
	///     {
	///			//scope1作用域继承上级(scope0)的事务行为,即不开启事务
	///     }
	/// }
	/// using (ConnectionScope scope2 = new ConnectionScope(TransactionMode.Required))
	/// {
	///	    using (ConnectionScope scope3 = new ConnectionScope(TransactionMode.Inherits))
	///     {
	///			//scope1作用域继承上级(scope0)的事务行为,即开启事务
	///     }
	/// }
	/// </code>
	/// </example>
	public enum TransactionMode
	{
		/// <summary>
		/// 继承当前作用域的事务模式
		/// </summary>
		Inherits,

		/// <summary>
		/// 新的作用域请求启用事务
		/// </summary>
		Required,

        /// <summary>
        /// 新的作用域不使用事务
        /// </summary>
        Suppress,

        /// <summary>
        /// 新的作用域开启新事务
        /// </summary>
        RequiresNew

	}
}
