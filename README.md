# SqlQuery
轻量级SQLHelper
高性能数据操作，

1， Initializer.UnSafeInit("数据库字符串");
2,  CPQuery.From("SELECT * FROM dbo.Customers where id=@id",new {id="991AF3C7-457A-479C-9EB0-85E807081D7E"}).ToList<Customer>();

更多方法自己体验
支持事务 嵌套事务

           using (ConnectionScope scope = new ConnectionScope(TransactionMode.Required))
            {
              
                //参数查询
                var ss =CPQuery.From("SELECT * FROM dbo.Customers where id=@id",new {id="991AF3C7-457A-479C-9EB0-85E807081D7E"}).ToList<Customer>();
            }
