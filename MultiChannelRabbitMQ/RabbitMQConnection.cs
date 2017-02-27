using System;
using RabbitMQ.Client;

namespace Dev.Common.MultiChannelRabbitMQ
{
    #region RabbitMQConnection
    /// <summary>
    /// ConnectionFactory管理类
    /// </summary>
    internal sealed class RabbitMQConnection : IDisposable
    {

        #region 变量
        /// <summary>
        /// 连接对象
        /// </summary>
        private IConnection iConnection = null;
        /// <summary>
        /// 线程锁
        /// </summary>
        private static object lockObject = new object();
        /// <summary>
        /// 获取IConnection对象
        /// </summary>
        public IConnection Connection
        {
            get
            {
                return iConnection;
            }
        }
        #endregion

        #region 构造函数
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="uriStr">uri</param>
        /// <param name="virtualHost"></param>
        /// <param name="exchange">交换机名称</param>
        /// <param name="userName">用户名</param>
        /// <param name="password">密码</param>
        public RabbitMQConnection(string uriStr, string virtualHost, string userName, string password)
        {
            if (iConnection == null)
            {
                lock (lockObject)
                {
                    if (iConnection == null)
                    {
                        Uri uri = new Uri(uriStr);
                        ConnectionFactory connectionFactory = new ConnectionFactory();
                        if (string.IsNullOrEmpty(virtualHost))
                        {
                            virtualHost = "/";
                        }
                        connectionFactory.VirtualHost = virtualHost;
                        connectionFactory.Endpoint = new AmqpTcpEndpoint(uri);
                        connectionFactory.UserName = userName;
                        connectionFactory.Password = password;
                        //connectionFactory.RequestedHeartbeat = 20; //默认值 (3.3.1.0版本默认值0; 3.6 版本默认60)
                        iConnection = connectionFactory.CreateConnection();
                    }
                }
            }
        }
        #endregion

        #region 销毁连接
        /// <summary>
        /// 销毁连接
        /// </summary>
        public void Dispose()
        {
            if (iConnection != null)
            {
                iConnection.Close();
                iConnection = null;
            }
        }
        #endregion

    }
    #endregion
}
