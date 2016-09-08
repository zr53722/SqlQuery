using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;

namespace Fulu.Query.SqlQuery
{
    /// <summary>
    /// 增加MockResult，允许在数据访问时指定模拟数据，绕过真实的SQLSERVER操作。
    /// BY FISH LI
    /// </summary>
    internal class ConnectionManager : IDisposable
	{
		private Stack<TransactionStackItem> _transactionModes = new Stack<TransactionStackItem>();

		public ConnectionManager()
		{
		}

		private ConnectionInfo OpenTopStackInfo(bool hit = false)
		{
			TransactionStackItem item = this._transactionModes.Peek();

			ConnectionInfo info = item.Info;

			// 打开连接，并根据需要开启事务
			if( info.Connection == null ) {
				info.Connection = ProviderManager.CreateConnection(info.ConnectionString);

				info.Connection.Open();

				EventManager.FireConnectionOpened(info.Connection);
			}

			if( item.EnableTranscation && info.Transaction == null) {
				info.Transaction = info.Connection.BeginTransaction();
			}

		    if (hit)
		    {
		        item.HitCount = item.HitCount + 1;
		    }

			return info;
		}

		public T ExecuteCommand<T>(DbCommand command, Func<DbCommand, T> func)
		{
			if( command == null )
				throw new ArgumentNullException("command");


		

			ConnectionInfo info = this.OpenTopStackInfo();

			// 设置命令的连接以及事务对象
			command.Connection = info.Connection;

			if( info.Transaction != null )
				command.Transaction = info.Transaction;

			Hashtable userData = EventManager.FireBeforeExecute(command);

			try
			{
				T result = default(T);

                //using( PerformanceLogScope log = new PerformanceLogScope(command) )
                { 

					result = func(command);
				}

				EventManager.FireAfterExecute(command, userData);

			    foreach (TransactionStackItem tst in this._transactionModes)
			    {
                    tst.HitCount = tst.HitCount + 1;
			    }

				return result;
			}
			catch( System.Exception ex ) {
				EventManager.FireOnException(command, ex, userData);
				throw;
			}
			finally {
				// 让命令与连接，事务断开，避免这些资源外泄。
				command.Connection = null;
				command.Transaction = null;
			}
		}

		public SqlBulkCopy CreateSqlBulkCopy(SqlBulkCopyOptions copyOptions)
		{
            //由于SQLBulkCopy不会执行任何SQL，没有对当前上下文SQL执行计数，最终会导致事务无法提交。
            //此处需要传递true，进行事务提交。
			ConnectionInfo info = this.OpenTopStackInfo(true);

			SqlConnection conn = info.Connection as SqlConnection;

			if( conn == null ) {
                throw new InvalidOperationException("只支持在SqlServer环境下使用SqlBulkCopy。");
			}

			SqlTransaction tran = info.Transaction as SqlTransaction;

			return new SqlBulkCopy(conn, copyOptions, tran);
		}


        internal void PushTransactionMode(TransactionMode mode, string connectionString, string providerName = "System.Data.SqlClient")
		{
			if( string.IsNullOrEmpty(connectionString) )
				throw new ArgumentNullException("connectionString");

			if( string.IsNullOrEmpty(providerName) ) {
				throw new ArgumentNullException("providerName");
			}

			TransactionStackItem stackItem = new TransactionStackItem();
			stackItem.Mode = mode;
            foreach (TransactionStackItem item in this._transactionModes)
            {
                if (item.Info.ConnectionString == connectionString && item.Info.ProviderName == providerName)
                {
                    stackItem.Info = item.Info;
                    stackItem.EnableTranscation = item.EnableTranscation;
                    stackItem.CanClose = false;
                    break;
                }
            }
			
			if( mode == TransactionMode.Required) {
                //info==null说明父级不存在开启事务场景,本层级需要打开事务,并可以在本层级关闭.
                if (stackItem.Info == null)
                {
                    stackItem.Info = new ConnectionInfo(connectionString, providerName);
                    stackItem.CanClose = true;
                }
				stackItem.EnableTranscation = true;
			}
            else if (mode == TransactionMode.RequiresNew)
            {
                stackItem.Info = new ConnectionInfo(connectionString, providerName);
                stackItem.EnableTranscation = true;
                stackItem.CanClose = true;
            }
            else if (mode == TransactionMode.Suppress)
            {
                stackItem.Info = new ConnectionInfo(connectionString, providerName);
                stackItem.EnableTranscation = false;
                stackItem.CanClose = true;
            }
            else
            {
                if (stackItem.Info == null)
                {
                    ConnectionInfo info = new ConnectionInfo(connectionString, providerName);
                    stackItem.Info = info;
                    stackItem.CanClose = true;
                }
            }

			this._transactionModes.Push(stackItem);
		}

		internal bool PopTransactionMode()
		{
			if( this._transactionModes.Count == 0 ) {
				return false;
			}

			TransactionStackItem current = this._transactionModes.Pop();

			if( current.Mode == TransactionMode.Required) {

				bool required = false;

				foreach( TransactionStackItem item in this._transactionModes ) {
					if( item.Info.IsSame(current.Info)
                        && (item.Mode == TransactionMode.Required || item.Mode == TransactionMode.RequiresNew))
                    {
						required = true;
						break;
					}
				}

				if( required == false ) {
					if( current.Info.Transaction != null ) {

						////MySQL的事务如果不提交,必须显示调用Rollback。
						////Dispose不包含回滚事务动作
						//if( current.Info.IsCommit == false ) {
						//    current.Info.Transaction.Rollback();
						//}

						//current.Info.Transaction.Dispose();
						//由于MySQL中,没有重写Dispose(disposing)方法.
						//导致调用DBTransaction类使用了基类的空方法.没有正确释放事务
						IDisposable ids = current.Info.Transaction as IDisposable;
						ids.Dispose();

						current.Info.Transaction = null;
					}
				}

			}
            else if (current.Mode == TransactionMode.RequiresNew)
            {
                if (current.Info.Transaction != null)
                {
                    IDisposable ids = current.Info.Transaction as IDisposable;
                    ids.Dispose();
                    current.Info.Transaction = null;
                }
            }

			if( current.CanClose && current.Info.Connection != null ) {
				//为了确保使用子类的Dispose方法.此处转换为接口调用.
				IDisposable ids = current.Info.Connection as IDisposable;
				ids.Dispose();

				current.Info.Connection = null;
			}

			return this._transactionModes.Count != 0;
		}

		public void Commit()
		{
            //取出栈顶元素进行判断
            TransactionStackItem current = this._transactionModes.Peek();

            //如果启用了事务,且事务段内不执行任何代码,直接Commit().这种场景应该是允许的.
            //对于内部实现,就相当于连接对象都没有创建,所以此处直接返回
            //if (current.Info.Connection == null &&
            //    (current.Mode == TransactionMode.Required || current.Mode == TransactionMode.RequiresNew))
            //{
            //    return;
            //}

		    if (current.HitCount == 0)
		    {
		        return;
		    }

            if (current.Info.Transaction == null)
            {
                throw new InvalidOperationException("当前的作用域不支持事务操作。");
            }

            if (current.Mode != TransactionMode.Required && current.Mode != TransactionMode.RequiresNew)
            {
                throw new InvalidOperationException("未在构造函数中指定TransactionMode.Required参数,不能调用Commit方法。");
            }

            //取出当前元素才能查找父级.
            current = this._transactionModes.Pop();

            bool required = false;
            try
            {
                if (current.Mode == TransactionMode.RequiresNew)
                {
                    required = false;
                }
                //父级是否包含需要开启事务的场景
                else if (this._transactionModes.Any(item => item.Info.IsSame(current.Info) &&
                    (item.Mode == TransactionMode.Required || item.Mode == TransactionMode.RequiresNew)))
                {
                    required = true;
                }
            }
            finally
            {
                //处理完毕后压将当前事务模式压回栈内
                this._transactionModes.Push(current);
            }

            if (required == false)
            {
                current.Info.Transaction.Commit();
            }
		}

		public void Rollback(string message)
		{
			TransactionStackItem item =  this._transactionModes.Peek();
			if( item.Info.Transaction == null )
                throw new InvalidOperationException("当前的作用域不支持事务操作。*");

		}

		public ConnectionInfo GetTopStackInfo()
		{
			if( this._transactionModes.Count > 0 ) {
				return this._transactionModes.Peek().Info;
			}

			return null;
		}

		public void Dispose()
		{
			foreach( TransactionStackItem item in this._transactionModes ) {

				if( item.Info.Transaction != null ) {
					item.Info.Transaction.Dispose();
					item.Info.Transaction = null;
				}

				if( item.Info.Connection != null ) {
					item.Info.Connection.Dispose();
					item.Info.Connection = null;
				}
			}
		}
	}
}
