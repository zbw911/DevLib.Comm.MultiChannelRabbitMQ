using System;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Content;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Impl;

namespace Dev.Common.MultiChannelRabbitMQ
{
    /// <summary>
    /// RabbitMQ操作类
    /// </summary>
    public class RabbitMQHelper
    {
        #region 属性
        /// <summary>
        /// 交换机名称
        /// </summary>
        private string exchange { set; get; }

        /// <summary>
        /// 交换机类型
        /// </summary>
        private ExchangeType exchangeType { set; get; }

        /// <summary>
        /// 路由键
        /// </summary>
        private string routingKey { set; get; }

        /// <summary>
        /// 队列名称
        /// </summary>
        private string queueName { set; get; }

        /// <summary>
        /// Channel 最大连接池数量
        /// </summary>
        private int maxPoolSize { set; get; }
        /// <summary>
        /// 连接对象(会在一个Connection对象上创建多个Channel(IModel)，然后程序发送数据时，分别共享使用创建好的Channel)
        /// </summary>
        private IConnection iConnection = null;
        /// <summary>
        /// uri
        /// </summary>
        private string uri = string.Empty;

        /// <summary>
        /// 虚拟主机
        /// </summary>
        private string virtualHost = string.Empty;
        /// <summary>
        /// 用户名
        /// </summary>
        private string userName = string.Empty;
        /// <summary>
        /// 密码
        /// </summary>
        private string password = string.Empty;
        /// <summary>
        /// 数据是否持久 true 是; false 否
        /// </summary>
        private bool exchangeDurable { get; set; }

        /// <summary>
        /// 数据是否持久 true 是; false 否
        /// </summary>
        private bool queueDurable { get; set; }
        /// <summary>
        /// message ttl
        /// </summary>
        public int ttl { get; set; }
        #endregion

        #region 构造初始化MQ Connection
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="uri">uri</param>
        /// <param name="virtualHost">虚拟主机</param>
        /// <param name="exchange">交换机名称</param>
        /// <param name="exchangeType">交换机类型</param>
        /// <param name="routingKey">路由键</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="durable">数据是否持久 1 是; 0 否</param>
        /// <param name="userName">用户名</param>
        /// <param name="password">密码</param>
        public RabbitMQHelper(string uriStr, string virtualHost, string exchange, ExchangeType exchangeType, string routingKey, string queueName, string exchangeDurable, string queueDurable, int MQ_QueueMessageTTL, string userName, string password, int maxChannelSize)
        {
            this.uri = uriStr;
            this.virtualHost = virtualHost;
            this.exchange = exchange;

            this.routingKey = routingKey;
            this.queueName = queueName;

            this.userName = userName;
            this.password = password;

            this.exchangeType = exchangeType;
            this.exchangeDurable = exchangeDurable.ToLower() == "durable" ? true : false;
            this.queueDurable = queueDurable.ToLower() == "durable" ? true : false;
            this.ttl = MQ_QueueMessageTTL;

            this.maxPoolSize = maxChannelSize;

            this.iConnection = new RabbitMQConnection(uriStr, virtualHost, userName, password).Connection;

        }
        #endregion

        #region 发布消息
        /// <summary>
        /// 发布消息
        /// </summary>
        /// <param name="message">消息内容</param>
        /// <param name="persistMode">是否持久化</param>
        public void PublishMessage<T>(T message, bool persistMode) where T : class
        {
            using (RabbitMQModel rabbitMQModel = new RabbitMQModel(iConnection, exchange, exchangeType, queueName, exchangeDurable, queueDurable, ttl, routingKey, maxPoolSize))
            {
                IModel iModel = rabbitMQModel.CreateModel();
                IMapMessageBuilder iMapMessageBuilder = new MapMessageBuilder(iModel);
                BasicProperties basicProperties = null;
                if (persistMode)
                {
                    ((IBasicProperties)iMapMessageBuilder.GetContentHeader()).DeliveryMode = 2;
                    basicProperties = (BasicProperties)iMapMessageBuilder.GetContentHeader();
                }
                var body = typeof(T) == typeof(string) ? Encoding.GetEncoding("gb2312").GetBytes(message.ToString()) :
                                    Encoding.GetEncoding("gb2312").GetBytes(JsonSerializer(message));
                if (!iConnection.IsOpen)
                {
                    //RabbitMQClient.Reset();
                    //重新创建连接,销毁旧的连接实例(同时获得新的Connection)
                    var newConnection = rabbitMQModel.CreateConnection(this.uri, this.virtualHost, this.userName, this.password);
                    if (newConnection != null && newConnection.IsOpen)
                    {
                        iModel = rabbitMQModel.CreateModel();
                        this.iConnection.Dispose();
                        this.iConnection = newConnection;
                    }
                }
                if (iModel == null || iModel.IsClosed)
                {
                    iModel = rabbitMQModel.CreateModel();
                }

                if (this.exchangeType == ExchangeType.direct || this.exchangeType == ExchangeType.topic)
                {
                    iModel.BasicPublish(this.exchange, this.routingKey, basicProperties, body);
                }
                else
                {
                    iModel.BasicPublish(this.exchange, "", basicProperties, body);
                }
            }
        }
        #endregion

        #region 阻塞订阅消息
        /// <summary>
        /// 阻塞订阅消息
        /// </summary>
        /// <typeparam name="T">class</typeparam>
        /// <param name="action">消息处理表达式</param>
        /// <param name="noAck">是否确认</param>
        /// <param name="readNumber">读取数量</param>
        public void Subscribe<T>(Func<T, bool> action, bool noAck, ushort readNumber) where T : class
        {
            using (RabbitMQModel rabbitMQModel = new RabbitMQModel(iConnection, exchange, exchangeType, queueName, exchangeDurable, queueDurable, ttl, routingKey, maxPoolSize))
            {
                IModel iModel = rabbitMQModel.CreateModel();
                QueueingBasicConsumer queueingBasicConsumer = new QueueingBasicConsumer(iModel);
                if (readNumber > 0)
                {
                    iModel.BasicConsume(this.queueName, noAck, queueingBasicConsumer);
                }
                iModel.BasicQos(0, readNumber, true);
                while (true)
                {
                    BasicDeliverEventArgs result = queueingBasicConsumer.Queue.Dequeue();
                    T body = typeof(T) == typeof(string) ? Encoding.GetEncoding("gb2312").GetString(result.Body) as T : JsonDeserialize<T>(Encoding.GetEncoding("gb2312").GetString(result.Body));
                    bool success = action(body);
                    if (!noAck)
                    {
                        if (success)
                        {
                            iModel.BasicAck(result.DeliveryTag, false);
                        }
                        else
                        {
                            iModel.BasicNack(result.DeliveryTag, false, true);
                        }
                    }
                }
            }
        }
        #endregion

        #region 获取单条消息
        /// <summary>
        /// 获取单条消息
        /// </summary>
        /// <typeparam name="T">class</typeparam>
        /// <param name="action">消息处理表达式</param>
        /// <param name="noAck">是否确认</param>
        public void BasicGet<T>(Func<T, bool> action, bool noAck) where T : class
        {
            using (RabbitMQModel rabbitMQModel = new RabbitMQModel(iConnection, exchange, exchangeType, queueName, exchangeDurable, queueDurable, ttl, routingKey, maxPoolSize))
            {
                IModel iModel = rabbitMQModel.CreateModel();
                BasicGetResult basicGetResult = iModel.BasicGet(this.queueName, noAck);
                if (basicGetResult != null)
                {
                    T body = typeof(T) == typeof(string) ? Encoding.GetEncoding("gb2312").GetString(basicGetResult.Body) as T :
                                   JsonDeserialize<T>(Encoding.GetEncoding("gb2312").GetString(basicGetResult.Body));
                    bool success = action(body);
                    if (!noAck)
                    {
                        if (success)
                        {
                            iModel.BasicAck(basicGetResult.DeliveryTag, false);
                        }
                        else
                        {
                            iModel.BasicNack(basicGetResult.DeliveryTag, false, true);
                        }
                    }
                }

            }
        }
        #endregion

        #region Json序列化&反序列化(效率可能比较低可以使用Newtonsoft.Json 重构一下)


        public static T JsonDeserialize<T>(string jsonStr)
        {
            using (MemoryStream ms = new MemoryStream(Encoding.Unicode.GetBytes(jsonStr)))
            {
                DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(T));
                return (T)serializer.ReadObject(ms);
            }
        }

        public static string JsonSerializer(object source)
        {
            Type type = source.GetType();
            DataContractJsonSerializer serilializer = new DataContractJsonSerializer(type);
            using (Stream stream = new MemoryStream())
            {
                serilializer.WriteObject(stream, source);
                stream.Flush();
                stream.Position = 0;
                StreamReader reader = new StreamReader(stream);
                return reader.ReadToEnd();
            }
        }


        #endregion
    }
}

/*
//PublishMessage
RabbitMQHelper rabbitMQHelper = RabbitMQClient.geRabbitMQClient();
rabbitMQHelper.PublishMessage<string>("测试数据数据gagahjt" + DateTime.Now.ToString(), true);
 
 
 
 
 
 
//GET 
 
RabbitMQHelper rabbitMQHelper = RabbitMQClient.geRabbitMQClient();
rabbitMQHelper.BasicGet<string>(fun, true);
 
 
 
 
public bool fun(string message)
        {
            if (!string.IsNullOrEmpty(message))
            {
                this.TextBox1.Text = message;
            }

            return true;

        }
 
 
 */
