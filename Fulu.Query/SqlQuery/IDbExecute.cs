using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Fulu.Query.SqlQuery
{
    /// <summary>
    /// 从CPQuery和StoreProcedure类提取出来的公共方法接口
    /// </summary>
    public interface IDbExecute
    {
        /// <summary>
        /// 执行命令,并返回影响函数
        /// </summary>
        /// <returns>影响行数</returns>
        int ExecuteNonQuery();
        /// <summary>
        /// 执行命令,返回第一行,第一列的值,并将结果转换为T类型
        /// </summary>
        /// <typeparam name="T">返回值类型</typeparam>
        /// <returns>结果集的第一行,第一列</returns>
        T ExecuteScalar<T>();
        /// <summary>
        /// 执行命令,返回DbDataReader对象实例,关闭返回的DbReader并不会关闭数据库连接
        /// </summary>
        /// <returns>DbDataReader实例</returns>
        DbDataReader ExecuteReader();
        /// <summary>
        /// 执行查询,并将结果集填充到DataSet
        /// </summary>
        /// <returns>数据集</returns>
        DataSet FillDataSet();
        /// <summary>
        /// 执行命令,并将结果集填充到DataTable
        /// </summary>
        /// <returns>数据表</returns>
        DataTable FillDataTable();
        /// <summary>
        /// 执行命令,将第一列的值填充到类型为T的行集合中
        /// </summary>
        /// <typeparam name="T">返回值类型</typeparam>
        /// <returns>结果集的第一列集合</returns>
        List<T> FillScalarList<T>();
        /// <summary>
        /// 执行命令,将结果集转换为实体集合
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <returns>实体集合</returns>
        List<T> ToList<T>() where T : class;
        /// <summary>
        /// 执行命令,将结果集转换为实体
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <returns>实体</returns>
        T ToSingle<T>() where T : class;
    }
}
