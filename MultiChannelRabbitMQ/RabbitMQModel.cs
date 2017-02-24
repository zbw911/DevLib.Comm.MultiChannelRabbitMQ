using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace APP.API.Async.Common.Safeguard.RabbitMQ
{
    #region RabbitMQModel
    /// <summary>
    /// IModel管理类 (基于一个Connection对象上创建的多个Model)
    /// </summary>
    public sealed class RabbitMQModel : IDisposable
    {

        #region 变量
        /// <summary>
        /// 连接池
        /// </summary>
        private static ConcurrentBag<IModel> concurrentBag = new ConcurrentBag<IModel>();

        /// <summary>
        /// Channel 最大连接池数量
        /// </summary>
        private int MaxPoolSize { get; set; }
        /// <summary>
        /// 连接对象
        /// </summary>
        private IConnection Connection { get; set; }
        /// <summary>
        /// 交换机
        /// </summary>
        private string Exchange { get; set; }

        /// <summary>
        /// 交换机类型
        /// </summary>
        private ExchangeType exchangeType { get; set; }
        /// <summary>
        /// 队列名称
        /// </summary>
        private string QueueName { get; set; }
        /// <summary>
        /// 路由键
        /// </summary>
        private string RoutingKey { get; set; }
        /// <summary>
        /// Model对象
        /// </summary>
        private IModel iModel = null;

        private bool exchangeDurable { get; set; }

        private bool queueDurable { get; set; }

        public int ttl { get; set; }
        #endregion

        #region 构造函数
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="iconnection">连接对象</param>
        /// <param name="exchange">交换机</param>
        /// <param name="exchangeType">交换机类型</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="routingKey">路由键</param>
        /// <param name="maxPoolSize">连接池大小</param>
        public RabbitMQModel(IConnection iConnection, string exchange, ExchangeType exchangeType, string queueName, bool exchangeDurable, bool queueDurable, int ttl, string routingKey, int maxPoolSize)
        {
            if (maxPoolSize <= 0)
            {
                throw new Exception("RabbitMQ Channel最大连接池错误");
            }
            this.Connection = iConnection;
            this.Exchange = exchange;
            this.exchangeType = exchangeType;
            this.QueueName = queueName;
            this.MaxPoolSize = maxPoolSize;
            this.RoutingKey = routingKey;
            this.exchangeDurable = exchangeDurable;
            this.queueDurable = queueDurable;
            this.ttl = ttl;

        }
        #endregion

        #region 创建连接
        /// <summary>
        /// 创建连接
        /// </summary>
        /// <param name="uri">uri</param>
        /// <param name="virtualHost">虚拟主机</param>
        /// <param name="userName">用户名</param>
        /// <param name="password">密码</param>
        public IConnection CreateConnection(string uriStr, string virtualHost, string userName, string password)
        {
            this.Connection = new RabbitMQConnection(uriStr, virtualHost, userName, password).Connection;
            return this.Connection;
        }
        #endregion

        #region 创建IModel
        /// <summary>
        /// 创建IModel
        /// </summary>
        /// <returns></returns>
        public IModel CreateModel()
        {
            concurrentBag.TryTake(out iModel);
            if (iModel == null)
            {

                Dictionary<String, Object> args = new Dictionary<String, Object>();
                args.Add("x-message-ttl", ttl); //60000
                iModel = this.Connection.CreateModel();
                iModel.ExchangeDeclare(this.Exchange, this.exchangeType.ToString(), this.exchangeDurable);
                iModel.QueueDeclare(this.QueueName, this.queueDurable, false, false, (ttl > -1 ? args : null));


                if (this.exchangeType == ExchangeType.topic)
                {
                    iModel.QueueBind(this.QueueName, this.Exchange, this.RoutingKey.Substring(0, this.RoutingKey.IndexOf(".")) + ".*");
                }
                else
                {
                    iModel.QueueBind(this.QueueName, this.Exchange, this.RoutingKey);
                }
            }
            return iModel;
        }
        #endregion

        #region 回收
        /// <summary>
        /// 回收
        /// </summary>
        public void Dispose()
        {
            if (iModel != null)
            {
                if (concurrentBag.Count <= this.MaxPoolSize)
                {
                    concurrentBag.Add(iModel);
                }
                else
                {
                    iModel.Close();
                }
            }
        }
        #endregion

    }
    #endregion
}
